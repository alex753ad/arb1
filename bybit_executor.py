"""
bybit_executor.py — v38.4 Bybit Demo Trading Executor
Работает с Bybit Demo Trading (demo.bybit.com / api-demo.bybit.com).

ВАЖНО: Это НЕ testnet.bybit.com (он закрыт).
       Это Demo Trading на основном bybit.com:
       bybit.com → переключить на Demo Trading → API Management → Create Key

API URL:  https://api-demo.bybit.com  (отдельный от основного)
Ключи:    создаются в Demo Trading аккаунте на bybit.com

Использование:
    from bybit_executor import BybitExecutor, get_executor
    ex = BybitExecutor(api_key="...", api_secret="...")
    print(ex.test_connection())
    result = ex.open_pair_trade("ETH", "BTC", "LONG", 100)
    close  = ex.close_pair_trade("ETH", "BTC", "LONG")
"""

import time
import json
import os
import hashlib
import hmac
import logging
from datetime import datetime, timezone, timedelta

MSK = timezone(timedelta(hours=3))

SLIPPAGE_LOG_FILE = "bybit_slippage_log.json"
BYBIT_TRADES_FILE = "bybit_trades.json"

logger = logging.getLogger("bybit_executor")

# Bybit Demo Trading API — единственный поддерживаемый URL
DEMO_BASE_URL = "https://api-demo.bybit.com"


class BybitExecutor:
    """Bybit Demo Trading executor для парной торговли."""

    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key    = (api_key    or "").strip()
        self.api_secret = (api_secret or "").strip()
        self.base_url   = DEMO_BASE_URL
        self.enabled    = bool(self.api_key) and bool(self.api_secret)
        # v41: кеш инструментов — загружается один раз, TTL 1 час
        self._instruments_cache: dict = {}
        self._instruments_ts:    float = 0.0
        self._INSTRUMENTS_TTL:   float = 3600.0
        # I-005 FIX: rate limiter — минимальный интервал между HTTP запросами
        # Bybit лимит: 120 req/sec для order, 10 req/sec для position/wallet.
        # Используем 100мс (10 req/sec) как безопасный минимум для всех endpoint.
        self._last_request_ts: float = 0.0
        self._min_request_interval: float = 0.1  # 100мс между запросами
        # C-01 FIX: lock для конкурентной записи в _log_trade/_log_slippage
        import threading as _th
        self._log_lock = _th.Lock()

    # ═══════════════════════════════════════════════════
    # ПОДПИСЬ (Bybit V5)
    # ═══════════════════════════════════════════════════

    def _sign(self, payload: str, timestamp: str, recv_window: str) -> str:
        """HMAC-SHA256. Порядок: timestamp + api_key + recv_window + payload."""
        msg = f"{timestamp}{self.api_key}{recv_window}{payload}"
        return hmac.new(
            self.api_secret.encode("utf-8"),
            msg.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    # ═══════════════════════════════════════════════════
    # HTTP ЗАПРОС
    # ═══════════════════════════════════════════════════

    def _request(self, method: str, endpoint: str, params: dict = None) -> dict:
        """Авторизованный запрос к Bybit V5 API с retry.

        CloudFront WAF FIX: Bybit Demo API за CloudFront CDN, который периодически
        возвращает HTML 403 или timeout. Retry с пересчётом подписи решает проблему.

        Retry policy:
          - До 3 попыток
          - Retriable: CloudFront 403 (HTML), timeout, ConnectionError
          - Каждый retry пересчитывает timestamp + подпись (иначе stale ts → 10002)
          - Backoff: 1s, 2s, 4s
        """
        if not self.enabled:
            return {
                "retCode": -1,
                "retMsg":  "Executor отключён",
                "error":   "Нет api_key / api_secret в config.yaml → секция bybit:",
            }

        try:
            import requests as _req
        except ImportError:
            return self._request_urllib(method, endpoint, params)

        max_retries = 3
        last_error = None

        for attempt in range(max_retries):
            # I-005 FIX: rate limiting
            _elapsed = time.time() - self._last_request_ts
            if _elapsed < self._min_request_interval:
                time.sleep(self._min_request_interval - _elapsed)
            self._last_request_ts = time.time()

            # Fresh timestamp + signature on EVERY attempt (stale ts → 10002)
            timestamp   = str(int(time.time() * 1000))
            recv_window = "5000"
            url         = f"{self.base_url}{endpoint}"

            base_headers = {
                "X-BAPI-API-KEY":     self.api_key,
                "X-BAPI-SIGN-TYPE":   "2",
                "X-BAPI-TIMESTAMP":   timestamp,
                "X-BAPI-RECV-WINDOW": recv_window,
                "User-Agent":         "bybit-pairs-scanner/38.5",
            }

            try:
                if method == "GET":
                    import urllib.parse
                    qs   = urllib.parse.urlencode(params or {})
                    sign = self._sign(qs, timestamp, recv_window)
                    hdrs = {**base_headers, "X-BAPI-SIGN": sign}
                    resp = _req.get(url, params=params or {}, headers=hdrs, timeout=15)
                else:  # POST
                    body = json.dumps(params or {}, separators=(",", ":"))
                    sign = self._sign(body, timestamp, recv_window)
                    hdrs = {
                        **base_headers,
                        "X-BAPI-SIGN":  sign,
                        "Content-Type": "application/json",
                    }
                    resp = _req.post(url, data=body, headers=hdrs, timeout=15)

                # --- Detect retriable errors ---
                _is_cloudfront_block = (
                    resp.status_code == 403
                    and "<!DOCTYPE" in resp.text[:50]
                )
                _is_server_error = resp.status_code in (502, 503, 504, 429)

                if _is_cloudfront_block or _is_server_error:
                    last_error = f"HTTP {resp.status_code} (CloudFront/WAF)"
                    if attempt < max_retries - 1:
                        _backoff = (attempt + 1) * 1.5  # 1.5s, 3s, 4.5s
                        logger.warning(
                            "Bybit %s %s → %s, retry %d/%d in %.1fs",
                            method, endpoint, last_error,
                            attempt + 1, max_retries, _backoff,
                        )
                        time.sleep(_backoff)
                        continue
                    # Last attempt failed
                    logger.error(
                        "Bybit %s %s → %s after %d retries",
                        method, endpoint, last_error, max_retries,
                    )
                    return {
                        "retCode":      resp.status_code,
                        "retMsg":       last_error,
                        "error":        f"{last_error} — все {max_retries} попытки исчерпаны",
                        "_http_status": resp.status_code,
                    }

                # --- Non-retriable HTTP errors ---
                if resp.status_code != 200:
                    logger.error(
                        "Bybit HTTP %s [%s %s] body=%s",
                        resp.status_code, method, endpoint,
                        resp.text[:300],
                    )

                try:
                    data = resp.json()
                except Exception:
                    return {
                        "retCode":      -1,
                        "retMsg":       resp.text[:200] or f"HTTP {resp.status_code}",
                        "error":        f"HTTP {resp.status_code}: не JSON — {resp.text[:200]}",
                        "_http_status": resp.status_code,
                    }

                if resp.status_code != 200:
                    code = data.get("retCode", resp.status_code)
                    msg  = data.get("retMsg",  resp.reason or f"HTTP {resp.status_code}")
                    return {
                        "retCode":      code,
                        "retMsg":       msg,
                        "error":        f"HTTP {resp.status_code}: [{code}] {msg}",
                        "_http_status": resp.status_code,
                    }

                if data.get("retCode") != 0:
                    logger.warning(
                        "Bybit %s %s → retCode=%s retMsg=%s",
                        method, endpoint,
                        data.get("retCode"), data.get("retMsg"),
                    )
                return data

            except (_req.exceptions.Timeout, _req.exceptions.ConnectionError) as e:
                last_error = str(e)[:120]
                _is_timeout = isinstance(e, _req.exceptions.Timeout)
                # SAFETY: timeout on order placement → order MAY have executed → DON'T retry
                _is_order = "/order/create" in endpoint
                _safe_to_retry = not (_is_timeout and _is_order)

                if _safe_to_retry and attempt < max_retries - 1:
                    _backoff = (attempt + 1) * 2  # 2s, 4s
                    logger.warning(
                        "Bybit %s %s → %s, retry %d/%d in %.0fs",
                        method, endpoint, type(e).__name__,
                        attempt + 1, max_retries, _backoff,
                    )
                    time.sleep(_backoff)
                    continue
                if _is_timeout and _is_order:
                    logger.error("Bybit %s %s: TIMEOUT — order may have executed, NOT retrying", method, endpoint)
                else:
                    logger.error("Bybit %s %s: %s after %d retries", method, endpoint, last_error, max_retries)
                return {"retCode": -1, "retMsg": last_error, "error": last_error}

            except Exception as e:
                # Non-retriable exception
                msg = str(e)
                logger.error("Bybit error %s %s: %s", method, endpoint, msg)
                return {"retCode": -1, "retMsg": msg, "error": msg}

        # Should never reach here, but safety net
        return {"retCode": -1, "retMsg": last_error or "Unknown", "error": last_error or "Unknown"}

    def _request_urllib(self, method: str, endpoint: str, params: dict = None) -> dict:
        """Запасной метод через urllib с retry (если requests не установлен)."""
        import urllib.request
        import urllib.parse
        import urllib.error

        max_retries = 3
        last_error = None

        for attempt in range(max_retries):
            # Fresh timestamp + signature per attempt
            timestamp   = str(int(time.time() * 1000))
            recv_window = "5000"
            url         = f"{self.base_url}{endpoint}"

            base_headers = {
                "X-BAPI-API-KEY":     self.api_key,
                "X-BAPI-SIGN-TYPE":   "2",
                "X-BAPI-TIMESTAMP":   timestamp,
                "X-BAPI-RECV-WINDOW": recv_window,
                "User-Agent":         "bybit-pairs-scanner/38.5",
            }

            try:
                if method == "GET":
                    qs   = urllib.parse.urlencode(params or {})
                    sign = self._sign(qs, timestamp, recv_window)
                    hdrs = {**base_headers, "X-BAPI-SIGN": sign}
                    full_url = f"{url}?{qs}" if qs else url
                    req = urllib.request.Request(full_url, headers=hdrs, method="GET")
                else:
                    body = json.dumps(params or {}, separators=(",", ":"))
                    sign = self._sign(body, timestamp, recv_window)
                    hdrs = {**base_headers, "X-BAPI-SIGN": sign, "Content-Type": "application/json"}
                    req  = urllib.request.Request(url, data=body.encode(), headers=hdrs, method="POST")

                with urllib.request.urlopen(req, timeout=15) as r:
                    return json.loads(r.read().decode("utf-8"))

            except urllib.error.HTTPError as e:
                # CloudFront 403 with HTML → retriable
                _is_cf = e.code == 403
                _is_server = e.code in (502, 503, 504, 429)
                if (_is_cf or _is_server) and attempt < max_retries - 1:
                    _backoff = (attempt + 1) * 1.5
                    logger.warning("Bybit urllib %s %s → HTTP %s, retry %d/%d in %.1fs",
                                   method, endpoint, e.code, attempt+1, max_retries, _backoff)
                    try:
                        e.read()  # drain response
                    except Exception:
                        pass
                    time.sleep(_backoff)
                    continue
                try:
                    body_bytes = e.read()
                    data = json.loads(body_bytes.decode("utf-8"))
                    code = data.get("retCode", e.code)
                    msg  = data.get("retMsg",  e.reason)
                except Exception:
                    code, msg = e.code, e.reason
                return {"retCode": code, "retMsg": msg,
                        "error": f"HTTP {e.code}: [{code}] {msg}",
                        "_http_status": e.code}

            except (TimeoutError, OSError) as e:
                last_error = str(e)[:120]
                _is_timeout = isinstance(e, TimeoutError)
                _is_order = "/order/create" in endpoint
                _safe = not (_is_timeout and _is_order)
                if _safe and attempt < max_retries - 1:
                    _backoff = (attempt + 1) * 2
                    logger.warning("Bybit urllib %s %s → %s, retry %d/%d",
                                   method, endpoint, type(e).__name__, attempt+1, max_retries)
                    time.sleep(_backoff)
                    continue
                return {"retCode": -1, "retMsg": last_error, "error": last_error}

            except Exception as e:
                return {"retCode": -1, "retMsg": str(e), "error": str(e)}

        return {"retCode": -1, "retMsg": last_error or "Unknown", "error": last_error or "Unknown"}

    # ═══════════════════════════════════════════════════
    # ПУБЛИЧНОЕ API
    # ═══════════════════════════════════════════════════

    def get_balance(self) -> dict:
        """Баланс USDT. Перебирает UNIFIED → CONTRACT → SPOT.

        Возвращает {'available', 'equity', 'wallet', 'account_type'}
        или {'error': ..., 'retCode': ..., '_http_status': ...}.
        """
        last = {}
        for atype in ("UNIFIED", "CONTRACT", "SPOT"):
            resp = self._request("GET", "/v5/account/wallet-balance",
                                 {"accountType": atype})
            last = resp
            code = resp.get("retCode", -1)

            if code == 0:
                lst = resp.get("result", {}).get("list", [])
                if not lst:
                    return {"available": 0, "equity": 0, "wallet": 0,
                            "account_type": atype,
                            "note": f"Аккаунт {atype} пуст — пополните Demo баланс"}
                coins = lst[0].get("coin", [])
                for c in coins:
                    if c.get("coin") == "USDT":
                        return {
                            "available":    float(
                                c.get("availableBalance") or
                                c.get("availableToWithdraw") or
                                c.get("walletBalance") or 0
                            ),
                            "equity":       float(c.get("equity",             0) or 0),
                            "wallet":       float(c.get("walletBalance",      0) or 0),
                            "account_type": atype,
                        }
                return {"available": 0, "equity": 0, "wallet": 0,
                        "account_type": atype,
                        "note": "USDT не найден — пополните Demo баланс на demo.bybit.com"}

            # Ошибки авторизации — нет смысла пробовать следующий тип
            if code in {10003, 10004, 10005, 33004}:
                return {"available": 0, "equity": 0, "wallet": 0,
                        "retCode": code,
                        "error": f"[{code}] {resp.get('retMsg', '')}",
                        "_http_status": resp.get("_http_status", 0)}

            # 403 — тоже сразу выходим
            if resp.get("_http_status") == 403:
                return {"available": 0, "equity": 0, "wallet": 0,
                        "retCode": 403,
                        "error": f"HTTP 403: {resp.get('retMsg', 'Forbidden')}",
                        "_http_status": 403}

        code = last.get("retCode", -1)
        return {"available": 0, "equity": 0, "wallet": 0,
                "retCode": code,
                "error": f"[{code}] {last.get('retMsg', 'Нет ответа')}",
                "_http_status": last.get("_http_status", 0)}

    def get_ticker(self, symbol: str) -> dict | None:
        resp = self._request("GET", "/v5/market/tickers",
                             {"category": "linear", "symbol": symbol})
        if resp.get("retCode") == 0:
            items = resp.get("result", {}).get("list", [])
            if items:
                t = items[0]
                return {
                    "symbol": t.get("symbol"),
                    "last":   float(t.get("lastPrice", 0) or 0),
                    "bid":    float(t.get("bid1Price",  0) or 0),
                    "ask":    float(t.get("ask1Price",  0) or 0),
                    "volume": float(t.get("volume24h",  0) or 0),
                }
        return None

    def get_instruments(self) -> dict:
        """Загрузить лот-параметры всех linear инструментов.

        v41 fixes:
          - Пагинация через cursor: Bybit отдаёт до 1000 за раз, инструментов >1000.
            Без пагинации WLD/ZEC и другие монеты за пределами первых 500 не попадали
            в кеш → _calc_qty уходил в fallback round → Qty invalid.
          - Кеш TTL=1ч: не делаем HTTP-запрос на каждый ордер.
          - Fallback: если запрос упал — возвращаем последний известный кеш.
        """
        now = time.time()
        if self._instruments_cache and (now - self._instruments_ts) < self._INSTRUMENTS_TTL:
            return self._instruments_cache

        result = {}
        cursor = ""
        page   = 0
        max_pages = 10  # защита от бесконечного цикла

        while page < max_pages:
            params: dict = {"category": "linear", "limit": "1000"}
            if cursor:
                params["cursor"] = cursor

            resp = self._request("GET", "/v5/market/instruments-info", params)
            if resp.get("retCode") != 0:
                logger.warning("get_instruments page=%d failed: %s", page, resp.get("retMsg", "?"))
                break

            for i in resp.get("result", {}).get("list", []):
                if i.get("status") != "Trading":
                    continue
                result[i["symbol"]] = {
                    "minQty":      float(i.get("lotSizeFilter", {}).get("minOrderQty",      0) or 0),
                    "qtyStep":     float(i.get("lotSizeFilter", {}).get("qtyStep",          0) or 0),
                    "minNotional": float(i.get("lotSizeFilter", {}).get("minNotionalValue", 0) or 0),
                    "tickSize":    float(i.get("priceFilter",   {}).get("tickSize",         0) or 0),
                }

            cursor = resp.get("result", {}).get("nextPageCursor", "")
            page  += 1
            if not cursor:
                break

        if result:
            self._instruments_cache = result
            self._instruments_ts    = now
            logger.info("get_instruments: загружено %d инструментов (%d стр.)", len(result), page)
        elif self._instruments_cache:
            logger.warning("get_instruments: запрос упал, используем кеш (%d инстр.)",
                           len(self._instruments_cache))
        else:
            logger.error("get_instruments: пустой ответ и нет кеша — qty будет неточным")

        return self._instruments_cache

    def set_leverage(self, symbol: str, leverage: int = 1) -> bool:
        """Плечо x1 перед торговлей."""
        resp = self._request("POST", "/v5/position/set-leverage", {
            "category": "linear", "symbol": symbol,
            "buyLeverage": str(leverage), "sellLeverage": str(leverage),
        })
        return resp.get("retCode") in (0, 110043)  # 110043 = already set

    def place_order(self, symbol: str, side: str, qty,
                    order_type: str = "Market",
                    price=None, reduce_only: bool = False) -> dict:
        """Разместить ордер."""
        params = {
            "category":    "linear",
            "symbol":      symbol,
            "side":        side,
            "orderType":   order_type,
            "qty":         str(qty),
            "positionIdx": 0,       # One-Way mode
            "timeInForce": "IOC" if order_type == "Market" else "GTC",
        }
        if order_type == "Limit" and price:
            params["price"] = str(price)
        if reduce_only:
            params["reduceOnly"] = True

        t0   = time.time()
        resp = self._request("POST", "/v5/order/create", params)
        lat  = round((time.time() - t0) * 1000, 1)

        code = resp.get("retCode", -1)
        res  = {
            "success":    code == 0,
            "order_id":   resp.get("result", {}).get("orderId", ""),
            "symbol":     symbol,
            "side":       side,
            "qty":        qty,
            "latency_ms": lat,
            "timestamp":  datetime.now(MSK).isoformat(),
        }
        if not res["success"]:
            res["error"] = f"[{code}] {resp.get('retMsg', '?')}"
            logger.error("place_order %s %s %s: %s", symbol, side, qty, res["error"])
        return res

    def get_order_detail(self, symbol: str, order_id: str) -> dict | None:
        for ep in ("/v5/order/realtime", "/v5/order/history"):
            resp = self._request("GET", ep, {
                "category": "linear", "symbol": symbol, "orderId": order_id,
            })
            if resp.get("retCode") == 0:
                items = resp.get("result", {}).get("list", [])
                if items:
                    o = items[0]
                    return {
                        "status":       o.get("orderStatus"),
                        "avg_price":    float(o.get("avgPrice",     0) or 0),
                        "filled_qty":   float(o.get("cumExecQty",   0) or 0),
                        "fee":          float(o.get("cumExecFee",   0) or 0),
                    }
        return None

    def get_position(self, symbol: str) -> dict | None:
        resp = self._request("GET", "/v5/position/list",
                             {"category": "linear", "symbol": symbol})
        if resp.get("retCode") == 0:
            for p in resp.get("result", {}).get("list", []):
                size = float(p.get("size", 0) or 0)
                if size > 0:
                    return {
                        "symbol":        p.get("symbol"),
                        "side":          p.get("side"),
                        "size":          size,
                        "avgPrice":      float(p.get("avgPrice",      0) or 0),
                        "unrealisedPnl": float(p.get("unrealisedPnl", 0) or 0),
                    }
        return None

    # ═══════════════════════════════════════════════════
    # ПАРНАЯ ТОРГОВЛЯ
    # ═══════════════════════════════════════════════════

    def _coin_to_symbol(self, coin: str) -> str:
        c = coin.upper().strip()
        return c if c.endswith("USDT") else f"{c}USDT"

    def _round_qty(self, symbol: str, raw_qty: float, instruments: dict = None) -> float:
        """Округлить qty до qtyStep инструмента.

        FIX BYBIT-QTY: Полная переработка fallback логики.
        При отсутствии инструмента: КОНСЕРВАТИВНЫЙ fallback — floor до 0.1 для qty>=1,
        floor до 0.01 для qty<1. Старый round(qty,2) давал 157.18 вместо 157.1 → Qty invalid.

        Возвращает округлённый qty (>0) или 0 при ошибке.
        """
        from decimal import Decimal, ROUND_DOWN, ROUND_UP

        if instruments is None:
            instruments = self.get_instruments()

        # Если символа нет в кэше — попробовать свежую загрузку (один раз)
        if symbol not in instruments:
            logger.warning("_round_qty %s: нет в кеше, принудительная перезагрузка instruments", symbol)
            self._instruments_ts = 0  # инвалидировать TTL
            instruments = self.get_instruments()

        if symbol not in instruments:
            # FIX BYBIT-QTY: консервативный fallback — floor до целого/0.1/0.01
            # Приоритет: НЕ получить Qty invalid. Лучше чуть меньший размер, чем ошибка.
            # Bybit qtyStep: 1.0 (SUI, DOGE), 0.1 (WLD, TIA, UNI), 0.01 (XMR, LINK), 0.001 (ETH, BTC)
            logger.error("_round_qty %s: инструмент не найден — консервативный floor", symbol)
            from decimal import Decimal, ROUND_DOWN as _RD
            if raw_qty >= 10:
                # qty>=10: step может быть 1.0 (SUI, DOGE) — floor до целого
                return float(Decimal(str(raw_qty)).to_integral_value(rounding=_RD))
            elif raw_qty >= 1:
                # 1 <= qty < 10: step обычно 0.1 (LINK, UNI)
                return float(Decimal(str(raw_qty)).quantize(Decimal('0.1'), rounding=_RD))
            elif raw_qty >= 0.01:
                # 0.01 <= qty < 1: step обычно 0.01 (XMR) или 0.001 (ETH)
                return float(Decimal(str(raw_qty)).quantize(Decimal('0.01'), rounding=_RD))
            else:
                # qty < 0.01: step обычно 0.001 (BTC)
                return float(Decimal(str(raw_qty)).quantize(Decimal('0.001'), rounding=_RD))

        info    = instruments[symbol]
        step    = info["qtyStep"]
        min_qty = info["minQty"]

        if min_qty > 0 and raw_qty < min_qty:
            raw_qty = min_qty
            logger.info("_round_qty %s: qty поднят до minQty=%.6f", symbol, min_qty)

        if step <= 0:
            return max(raw_qty, 0)

        d_qty  = Decimal(str(raw_qty))
        d_step = Decimal(str(step))

        qty = float(
            (d_qty / d_step).to_integral_value(rounding=ROUND_DOWN) * d_step
        )
        # Количество десятичных знаков из step (через Decimal для точности)
        decimals = max(0, -d_step.as_tuple().exponent)
        qty = round(qty, decimals)

        # Подтянуть до minQty если округление уронило ниже
        if qty < min_qty and min_qty > 0:
            qty = float(Decimal(str(min_qty)).quantize(d_step))

        return max(qty, 0)

    def _ensure_notional(self, symbol: str, qty: float, price: float,
                         instruments: dict) -> float:
        """Поднять qty до minNotional если нужно.
        FIX BYBIT-QTY: после подтяжки — повторное выравнивание через _round_qty,
        чтобы qty оставался кратен qtyStep.
        """
        from decimal import Decimal, ROUND_UP
        if symbol not in instruments:
            return qty
        info    = instruments[symbol]
        min_not = info.get("minNotional", 0)
        step    = info["qtyStep"]
        if min_not > 0 and qty * price < min_not and step > 0:
            d_step = Decimal(str(step))
            d_min_not_qty = Decimal(str(min_not)) / Decimal(str(price))
            new_qty = float(
                (d_min_not_qty / d_step).to_integral_value(rounding=ROUND_UP) * d_step
            )
            decimals = max(0, -d_step.as_tuple().exponent)
            new_qty = round(new_qty, decimals)
            logger.info("_ensure_notional %s: qty поднят до notional min=%.2f → %.6f",
                        symbol, min_not, new_qty)
            # FIX BYBIT-QTY: re-validate alignment
            return self._round_qty(symbol, new_qty, instruments)
        return qty

    def _calc_qty(self, symbol: str, size_usdt: float, instruments: dict,
                  side: str = ""):
        """(qty, price) или (None, error_str).

        BYBIT-1 FIX: использует _round_qty() + _ensure_notional()
        вместо inline Decimal-логики. Retry при промахе кэша instruments.
        I-004 FIX: side="Buy" → ask price, side="Sell" → bid price.
        FIX BYBIT-QTY: финальная валидация qty % qtyStep == 0.
        """
        ticker = self.get_ticker(symbol)
        if not ticker or ticker["last"] <= 0:
            return None, f"Нет котировки {symbol}"

        # I-004 FIX: для Buy берём ask, для Sell — bid (точнее отражает fill price)
        if side == "Buy" and ticker.get("ask", 0) > 0:
            price = ticker["ask"]
        elif side == "Sell" and ticker.get("bid", 0) > 0:
            price = ticker["bid"]
        else:
            price = ticker["last"]
        raw_qty = size_usdt / price

        qty = self._round_qty(symbol, raw_qty, instruments)
        if qty <= 0:
            return None, f"qty=0 для {symbol} (size_usdt={size_usdt:.2f}, price={price})"

        qty = self._ensure_notional(symbol, qty, price, instruments)
        if qty <= 0:
            return None, f"qty=0 для {symbol} после notional check"

        # FIX BYBIT-QTY: финальная проверка что qty кратен qtyStep
        if symbol in instruments:
            from decimal import Decimal
            step = instruments[symbol]["qtyStep"]
            if step > 0:
                d_qty = Decimal(str(qty))
                d_step = Decimal(str(step))
                remainder = d_qty % d_step
                if remainder != 0:
                    logger.warning("_calc_qty %s: qty=%.6f не кратен step=%.6f (rem=%.8f), "
                                   "пере-округляю", symbol, qty, step, float(remainder))
                    qty = self._round_qty(symbol, qty, instruments)
                    if qty <= 0:
                        return None, f"qty=0 для {symbol} после финального выравнивания"

        return qty, price

    # ═══════════════════════════════════════════════════
    # I-003 FIX: ПАРАЛЛЕЛЬНЫЕ ОПЕРАЦИИ
    # ═══════════════════════════════════════════════════

    @staticmethod
    def _parallel_pair(fn1, fn2):
        """Запустить две функции параллельно, вернуть (result1, result2).
        I-003 FIX: сокращает латентность парных операций вдвое.
        """
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=2) as ex:
            f1 = ex.submit(fn1)
            f2 = ex.submit(fn2)
            return f1.result(), f2.result()

    def open_pair_trade(self, coin1: str, coin2: str, direction: str,
                        size_usdt: float,
                        expected_price1: float = None,
                        expected_price2: float = None) -> dict:
        """Открыть парную сделку на Demo.

        LONG:  Buy coin1 + Sell coin2
        SHORT: Sell coin1 + Buy coin2

        I-003 FIX: параллельные pre-flight запросы (leverage, position check, qty calc)
        и параллельный fill check. Ордера остаются последовательными (leg1 must pass → leg2).
        Было: ~5-7с. Стало: ~2.5-3.5с.
        """
        if not self.enabled:
            return {"success": False, "error": "Bybit executor отключён"}

        sym1 = self._coin_to_symbol(coin1)
        sym2 = self._coin_to_symbol(coin2)

        # I-003 FIX: параллельный set_leverage (было: последовательно ~0.3с)
        self._parallel_pair(
            lambda: self.set_leverage(sym1, 1),
            lambda: self.set_leverage(sym2, 1),
        )

        instr = self.get_instruments()

        side1, side2 = ("Buy", "Sell") if direction == "LONG" else ("Sell", "Buy")

        # I-003 FIX: параллельный duplicate check (было: последовательно ~0.3с)
        _pos1, _pos2 = self._parallel_pair(
            lambda: self.get_position(sym1),
            lambda: self.get_position(sym2),
        )
        for _existing, _chk_sym, _chk_label in [(_pos1, sym1, coin1), (_pos2, sym2, coin2)]:
            if _existing:
                _msg = (
                    f"DUPLICATE BLOCKED: {_chk_label} уже имеет открытую позицию на Bybit "
                    f"({_existing['side']} qty={_existing['size']}). "
                    f"Закройте её вручную перед открытием новой пары."
                )
                logger.warning("open_pair_trade %s/%s: %s", coin1, coin2, _msg)
                return {"success": False, "error": _msg,
                        "pair": f"{coin1}/{coin2}", "direction": direction,
                        "action": "OPEN", "size_usdt": size_usdt}

        # I-003 FIX: параллельный _calc_qty (было: последовательно ~0.3с, каждый get_ticker)
        (qty1, price1), (qty2, price2) = self._parallel_pair(
            lambda: self._calc_qty(sym1, size_usdt / 2, instr, side=side1),
            lambda: self._calc_qty(sym2, size_usdt / 2, instr, side=side2),
        )
        if qty1 is None:
            return {"success": False, "error": f"{sym1}: {price1}"}
        if qty2 is None:
            return {"success": False, "error": f"{sym2}: {price2}"}

        # Ордера — ПОСЛЕДОВАТЕЛЬНО (leg1 must succeed before leg2)
        leg1 = self.place_order(sym1, side1, qty1)
        time.sleep(0.1)  # I-003 FIX: 0.15→0.1 (rate limiter уже защищает)

        # v41 FIX: Если leg1 упал — не отправляем leg2
        if not leg1["success"]:
            result = {
                "success": False, "pair": f"{coin1}/{coin2}",
                "direction": direction, "size_usdt": size_usdt, "action": "OPEN",
                "error": f"leg1 failed, leg2 NOT sent: {leg1.get('error', '?')}",
                "leg1": {"symbol": sym1, "side": side1, "qty": qty1,
                         "expected_price": expected_price1 or 0, "fill_price": 0,
                         "slippage_pct": 0, "fee": 0, "order_id": "",
                         "latency_ms": leg1.get("latency_ms", 0),
                         "error": leg1.get("error", "")},
                "leg2": {"symbol": sym2, "side": side2, "qty": qty2,
                         "expected_price": expected_price2 or 0, "fill_price": 0,
                         "slippage_pct": 0, "fee": 0, "order_id": "",
                         "latency_ms": 0, "error": "not sent (leg1 failed)"},
                "total_slippage_pct": 0,
                "total_latency_ms": leg1.get("latency_ms", 0),
                "total_fees": 0, "timestamp": datetime.now(MSK).isoformat(),
            }
            self._log_trade(result)
            return result

        leg2 = self.place_order(sym2, side2, qty2)

        # v41 FIX: Если leg2 упал — немедленно откатываем leg1
        # BUG-004 FIX: retry rollback 3x с экспоненциальной задержкой
        if not leg2["success"]:
            logger.error("open_pair_trade %s/%s: leg2 failed (%s) — rolling back leg1",
                         coin1, coin2, leg2.get("error", "?"))
            rollback_side = "Sell" if side1 == "Buy" else "Buy"
            rb = {"success": False, "error": "not attempted"}
            for _rb_attempt in range(3):
                if _rb_attempt > 0:
                    _rb_delay = 1.0 * (2 ** (_rb_attempt - 1))
                    logger.warning("rollback attempt %d/%d for %s, waiting %.1fs",
                                   _rb_attempt + 1, 3, sym1, _rb_delay)
                    time.sleep(_rb_delay)
                rb = self.place_order(sym1, rollback_side, qty1, reduce_only=True)
                if rb["success"]:
                    break
            rb_status = "rollback OK" if rb["success"] else f"rollback FAILED: {rb.get('error', '?')}"
            logger.error("open_pair_trade rollback %s %s qty=%s: %s",
                         sym1, rollback_side, qty1, rb_status)
            if not rb["success"]:
                self._emergency_alert(
                    f"🚨 КРИТИЧНО: rollback ПРОВАЛЕН для {sym1}!\n"
                    f"Пара: {coin1}/{coin2} {direction}\n"
                    f"Qty: {qty1} | Ошибка: {rb.get('error', '?')}\n"
                    f"⚠️ Голая позиция на бирже — закрыть вручную немедленно!"
                )
            result = {
                "success": False, "pair": f"{coin1}/{coin2}",
                "direction": direction, "size_usdt": size_usdt, "action": "OPEN",
                "error": f"leg2 failed ({leg2.get('error', '?')}) | leg1 {rb_status}",
                "leg1": {"symbol": sym1, "side": side1, "qty": qty1,
                         "expected_price": expected_price1 or 0, "fill_price": 0,
                         "slippage_pct": 0, "fee": 0,
                         "order_id": leg1.get("order_id", ""),
                         "latency_ms": leg1.get("latency_ms", 0),
                         "error": f"opened then {rb_status}"},
                "leg2": {"symbol": sym2, "side": side2, "qty": qty2,
                         "expected_price": expected_price2 or 0, "fill_price": 0,
                         "slippage_pct": 0, "fee": 0, "order_id": "",
                         "latency_ms": leg2.get("latency_ms", 0),
                         "error": leg2.get("error", "")},
                "total_slippage_pct": 0,
                "total_latency_ms": round(leg1.get("latency_ms", 0) + leg2.get("latency_ms", 0), 1),
                "total_fees": 0, "timestamp": datetime.now(MSK).isoformat(),
            }
            self._log_trade(result)
            return result

        # I-003 FIX: параллельный fill check (было: sleep(0.6)+get1 → sleep(0.6)+get2 = 1.8с)
        # Стало: sleep(0.5) → parallel get1+get2 = ~0.7с
        time.sleep(0.5)
        fill1 = fill2 = None
        if leg1["success"] and leg2["success"]:
            fill1, fill2 = self._parallel_pair(
                lambda: self.get_order_detail(sym1, leg1["order_id"]),
                lambda: self.get_order_detail(sym2, leg2["order_id"]),
            )
        elif leg1["success"]:
            fill1 = self.get_order_detail(sym1, leg1["order_id"])

        def slip(fill, exp):
            if fill and exp and exp > 0:
                return round(abs(fill["avg_price"] - exp) / exp * 100, 4)
            return 0.0

        result = {
            "success":            leg1["success"] and leg2["success"],
            "pair":               f"{coin1}/{coin2}",
            "direction":          direction,
            "size_usdt":          size_usdt,
            "action":             "OPEN",
            "leg1": {
                "symbol":         sym1, "side": side1, "qty": qty1,
                "expected_price": expected_price1 or price1,
                "fill_price":     fill1["avg_price"] if fill1 else 0,
                "slippage_pct":   slip(fill1, expected_price1 or price1),
                "fee":            fill1["fee"] if fill1 else 0,
                "order_id":       leg1.get("order_id", ""),
                "latency_ms":     leg1.get("latency_ms", 0),
                "error":          leg1.get("error", ""),
            },
            "leg2": {
                "symbol":         sym2, "side": side2, "qty": qty2,
                "expected_price": expected_price2 or price2,
                "fill_price":     fill2["avg_price"] if fill2 else 0,
                "slippage_pct":   slip(fill2, expected_price2 or price2),
                "fee":            fill2["fee"] if fill2 else 0,
                "order_id":       leg2.get("order_id", ""),
                "latency_ms":     leg2.get("latency_ms", 0),
                "error":          leg2.get("error", ""),
            },
            "total_slippage_pct": round(
                slip(fill1, expected_price1 or price1) +
                slip(fill2, expected_price2 or price2), 4),
            "total_latency_ms":   round(
                leg1.get("latency_ms", 0) + leg2.get("latency_ms", 0), 1),
            "total_fees":         round(
                (fill1["fee"] if fill1 else 0) + (fill2["fee"] if fill2 else 0), 6),
            "timestamp":          datetime.now(MSK).isoformat(),
        }

        if not result["success"]:
            errs = []
            if not leg1["success"]: errs.append(f"leg1: {leg1.get('error','?')}")
            if not leg2["success"]: errs.append(f"leg2: {leg2.get('error','?')}")
            result["error"] = " | ".join(errs)

        self._log_trade(result)
        self._log_slippage(result)
        return result

    def close_pair_trade(self, coin1: str, coin2: str, direction: str,
                         expected_price1: float = None,
                         expected_price2: float = None) -> dict:
        """Закрыть парную сделку на Demo.

        v41.2 FIX BYBIT-2: Стороны закрытия определяются по ФАКТИЧЕСКОЙ позиции.
        I-003 FIX: параллельный get_position + параллельный fill check.
        """
        if not self.enabled:
            return {"success": False, "error": "Bybit executor отключён"}

        sym1 = self._coin_to_symbol(coin1)
        sym2 = self._coin_to_symbol(coin2)

        # I-003 FIX: параллельный get_position (было: последовательно ~0.3с)
        pos1, pos2 = self._parallel_pair(
            lambda: self.get_position(sym1),
            lambda: self.get_position(sym2),
        )

        qty1 = pos1["size"] if pos1 else 0
        qty2 = pos2["size"] if pos2 else 0

        if qty1 == 0 and qty2 == 0:
            return {"success": False,
                    "error": f"Нет открытых позиций {sym1}/{sym2} на Bybit Demo"}

        # v41.2 FIX BYBIT-2: определяем сторону закрытия по фактической позиции
        # Bybit: Buy position → закрыть Sell, Sell position → закрыть Buy
        # FIX BYBIT-CLOSE: НЕ ИСПОЛЬЗУЕМ fallback direction если позиция не найдена.
        # Если pos=None → qty=0, leg пропускается (не отправляем ордер вслепую).
        # Если pos.side не совпадает с ожидаемым — используем ФАКТИЧЕСКИЙ side.
        if pos1:
            side1 = "Sell" if pos1["side"] == "Buy" else "Buy"
        else:
            side1 = ""  # FIX: не угадываем, leg пропустится (qty1=0)
        if pos2:
            side2 = "Sell" if pos2["side"] == "Buy" else "Buy"
        else:
            side2 = ""  # FIX: не угадываем

        # Логируем если фактическая сторона не совпала с ожидаемой
        _fallback_side1, _fallback_side2 = (
            ("Sell", "Buy") if direction == "LONG" else ("Buy", "Sell")
        )
        if pos1 and side1 != _fallback_side1:
            logger.warning(
                "close_pair_trade %s: direction=%s → expected close side=%s, "
                "but actual position side=%s → using close side=%s",
                sym1, direction, _fallback_side1, pos1["side"], side1
            )
        if pos2 and side2 != _fallback_side2:
            logger.warning(
                "close_pair_trade %s: direction=%s → expected close side=%s, "
                "but actual position side=%s → using close side=%s",
                sym2, direction, _fallback_side2, pos2["side"], side2
            )

        leg1_r = {"success": True, "latency_ms": 0}
        leg2_r = {"success": True, "latency_ms": 0}
        fill1 = fill2 = None

        # I-001 FIX: retry 3x с exponential backoff при сбое close ордера.
        def _close_leg_with_retry(sym, side, qty, leg_label):
            """Закрыть одну ногу с 3 попытками."""
            last_r = {"success": False, "latency_ms": 0, "error": "not attempted"}
            for attempt in range(3):
                if attempt > 0:
                    _delay = 1.0 * (2 ** (attempt - 1))
                    logger.warning("close %s %s attempt %d/3, waiting %.1fs",
                                   sym, leg_label, attempt + 1, _delay)
                    time.sleep(_delay)
                last_r = self.place_order(sym, side, qty, reduce_only=True)
                if last_r["success"]:
                    return last_r
                logger.error("close %s %s attempt %d/3 failed: %s",
                             sym, leg_label, attempt + 1, last_r.get("error", "?"))
            return last_r

        # I-003 FIX: параллельное закрытие обеих ног (reduce_only — независимы)
        # Было: leg1 → sleep → leg2 → sleep → fill1 → sleep → fill2 = ~3с
        # Стало: parallel(leg1, leg2) → sleep → parallel(fill1, fill2) = ~1.5с
        if qty1 > 0 and qty2 > 0:
            leg1_r, leg2_r = self._parallel_pair(
                lambda: _close_leg_with_retry(sym1, side1, qty1, "leg1"),
                lambda: _close_leg_with_retry(sym2, side2, qty2, "leg2"),
            )
        elif qty1 > 0:
            leg1_r = _close_leg_with_retry(sym1, side1, qty1, "leg1")
        elif qty2 > 0:
            leg2_r = _close_leg_with_retry(sym2, side2, qty2, "leg2")

        # I-003 FIX: параллельный fill check
        if leg1_r["success"] or leg2_r["success"]:
            time.sleep(0.5)  # дать бирже зафиксировать fill
        if leg1_r["success"] and leg2_r["success"]:
            fill1, fill2 = self._parallel_pair(
                lambda: self.get_order_detail(sym1, leg1_r["order_id"]),
                lambda: self.get_order_detail(sym2, leg2_r["order_id"]),
            )
        elif leg1_r["success"]:
            fill1 = self.get_order_detail(sym1, leg1_r["order_id"])
        elif leg2_r["success"]:
            fill2 = self.get_order_detail(sym2, leg2_r["order_id"])

        # I-001 FIX: если одна нога закрыта, а другая нет — emergency alert
        if leg1_r["success"] != leg2_r["success"]:
            _failed_sym = sym2 if leg1_r["success"] else sym1
            _failed_leg = "leg2" if leg1_r["success"] else "leg1"
            self._emergency_alert(
                f"🚨 CLOSE PARTIAL FAIL: {coin1}/{coin2} {direction}\n"
                f"{_failed_leg} ({_failed_sym}) не закрыта после 3 попыток!\n"
                f"Ошибка: {(leg2_r if _failed_leg == 'leg2' else leg1_r).get('error', '?')}\n"
                f"⚠️ Голая позиция — закрыть вручную немедленно!"
            )

        def slip(fill, exp):
            if fill and exp and exp > 0:
                return round(abs(fill["avg_price"] - exp) / exp * 100, 4)
            return 0.0

        result = {
            "success":   leg1_r["success"] and leg2_r["success"],
            "pair":      f"{coin1}/{coin2}",
            "direction": direction,
            "action":    "CLOSE",
            "leg1": {
                "symbol":       sym1, "side": side1, "qty": qty1,
                "fill_price":   fill1["avg_price"] if fill1 else 0,
                "slippage_pct": slip(fill1, expected_price1),
                "fee":          fill1["fee"] if fill1 else 0,
                "latency_ms":   leg1_r.get("latency_ms", 0),  # BYBIT-3 FIX
                "error":        leg1_r.get("error", ""),
            },
            "leg2": {
                "symbol":       sym2, "side": side2, "qty": qty2,
                "fill_price":   fill2["avg_price"] if fill2 else 0,
                "slippage_pct": slip(fill2, expected_price2),
                "fee":          fill2["fee"] if fill2 else 0,
                "latency_ms":   leg2_r.get("latency_ms", 0),  # BYBIT-3 FIX
                "error":        leg2_r.get("error", ""),
            },
            "total_slippage_pct": round(
                slip(fill1, expected_price1) + slip(fill2, expected_price2), 4),
            # BYBIT-3 FIX: total_latency_ms отсутствовал → _log_slippage записывал 0
            "total_latency_ms": round(
                leg1_r.get("latency_ms", 0) + leg2_r.get("latency_ms", 0), 1),
            "total_fees": round(
                (fill1["fee"] if fill1 else 0) + (fill2["fee"] if fill2 else 0), 6),
            "timestamp":  datetime.now(MSK).isoformat(),
        }

        if not result["success"]:
            errs = []
            if not leg1_r["success"]: errs.append(f"leg1: {leg1_r.get('error','?')}")
            if not leg2_r["success"]: errs.append(f"leg2: {leg2_r.get('error','?')}")
            result["error"] = " | ".join(errs)

        self._log_trade(result)
        self._log_slippage(result)
        return result

    # ═══════════════════════════════════════════════════
    # ЛОГИРОВАНИЕ
    # ═══════════════════════════════════════════════════

    def _emergency_alert(self, message: str) -> None:
        """Критический алерт при неудаче rollback/close.
        E-005 FIX: retry Telegram 3x + webhook fallback + config TG credentials.
        
        Порядок доставки:
          1. Файл bybit_emergency.log (всегда)
          2. Telegram (3 попытки, env vars → config.yaml)
          3. Webhook URL из config (Slack/Discord) — если настроен
        """
        EMERGENCY_LOG = "bybit_emergency.log"
        # 1. Всегда пишем в файл
        try:
            with open(EMERGENCY_LOG, "a", encoding="utf-8") as f:
                f.write(f"{datetime.now(MSK).isoformat()} {message}\n{'='*60}\n")
        except Exception as e:
            logger.error("_emergency_alert: не удалось записать лог: %s", e)

        logger.critical(message)

        # 2. Telegram — собираем credentials из env vars, затем config.yaml
        import os as _os
        _tg_token = _os.environ.get("TG_TOKEN", "")
        _tg_chat  = _os.environ.get("TG_CHAT_ID", "")
        if not _tg_token or not _tg_chat:
            try:
                from config_loader import CFG as _eCFG
                _tg_token = _tg_token or _eCFG("telegram", "token", "")
                _tg_chat  = _tg_chat  or _eCFG("telegram", "chat_id", "")
            except Exception:
                pass

        if _tg_token and _tg_chat:
            import urllib.request, urllib.parse
            _tg_hosts = ["api.telegram.org", "149.154.167.220"]
            _tg_sent = False
            for _attempt in range(3):
                if _tg_sent:
                    break
                for _host in _tg_hosts:
                    try:
                        _data = urllib.parse.urlencode({
                            "chat_id": _tg_chat, "text": message, "parse_mode": "HTML"
                        }).encode()
                        _req = urllib.request.Request(
                            f"https://{_host}/bot{_tg_token}/sendMessage",
                            data=_data,
                            headers={"Host": "api.telegram.org"}
                        )
                        # B-02 FIX: SSL context для IP-фолбэка (без него — SSLCertVerificationError)
                        import ssl as _ea_ssl
                        _ea_ctx = _ea_ssl.create_default_context()
                        if _host != "api.telegram.org":
                            _ea_ctx.check_hostname = False
                            _ea_ctx.verify_mode = _ea_ssl.CERT_NONE
                        urllib.request.urlopen(_req, timeout=10, context=_ea_ctx)
                        _tg_sent = True
                        break
                    except Exception as e:
                        logger.warning("_emergency_alert TG attempt %d host=%s: %s",
                                       _attempt + 1, _host, str(e)[:80])
                if not _tg_sent and _attempt < 2:
                    time.sleep(2 * (_attempt + 1))  # 2s, 4s
            if not _tg_sent:
                logger.error("_emergency_alert: Telegram НЕДОСТУПЕН после 3 попыток")

        # 3. Webhook fallback (Slack/Discord/custom)
        try:
            from config_loader import CFG as _eCFG
            _webhook_url = _eCFG("bybit", "emergency_webhook", "")
            if _webhook_url:
                import urllib.request
                _payload = json.dumps({"text": message, "content": message}).encode()
                _req = urllib.request.Request(
                    _webhook_url, data=_payload,
                    headers={"Content-Type": "application/json"}
                )
                urllib.request.urlopen(_req, timeout=10)
        except Exception as e:
            logger.warning("_emergency_alert: webhook failed: %s", str(e)[:80])

    def _log_trade(self, r: dict):
        # C-01 FIX: thread-safe write
        with self._log_lock:
            try:
                trades = []
                if os.path.exists(BYBIT_TRADES_FILE):
                    with open(BYBIT_TRADES_FILE) as f:
                        trades = json.load(f)
                trades.append(r)
                with open(BYBIT_TRADES_FILE, "w") as f:
                    json.dump(trades[-500:], f, indent=2, default=str)
            except Exception as e:
                logger.error("_log_trade: %s", e)

    def _log_slippage(self, r: dict):
        # C-01 FIX: thread-safe write
        with self._log_lock:
            try:
                log = []
                if os.path.exists(SLIPPAGE_LOG_FILE):
                    with open(SLIPPAGE_LOG_FILE) as f:
                        log = json.load(f)
                log.append({
                    "pair":           r.get("pair", ""),
                    "action":         r.get("action", "OPEN"),
                    "total_slippage": r.get("total_slippage_pct", 0),
                    "latency_ms":     r.get("total_latency_ms",  0),
                    "fees":           r.get("total_fees",        0),
                    "success":        r.get("success",           False),
                    "timestamp":      r.get("timestamp",         ""),
                })
                with open(SLIPPAGE_LOG_FILE, "w") as f:
                    json.dump(log[-1000:], f, indent=2, default=str)
            except Exception:
                pass

    def get_slippage_stats(self) -> dict:
        try:
            if not os.path.exists(SLIPPAGE_LOG_FILE):
                return {"n_trades": 0}
            with open(SLIPPAGE_LOG_FILE) as f:
                log = json.load(f)
            if not log:
                return {"n_trades": 0}
            # I-002 FIX: фильтруем только успешные сделки (было: все включая failed)
            ok  = [e for e in log if e.get("success", False)]
            sl  = [e["total_slippage"] for e in ok if e.get("total_slippage", 0) > 0]
            lat = [e["latency_ms"]     for e in ok if e.get("latency_ms",    0) > 0]
            fee = [e["fees"]           for e in ok if e.get("fees",          0) > 0]
            return {
                "n_trades":         len(ok),  # DAT-04 FIX: primary count = successful only
                "n_total_records":  len(log),  # DAT-04 FIX: total including failed
                "n_successful":     len(ok),
                "n_failed":         len(log) - len(ok),  # DAT-04 FIX: explicit failed count
                "avg_slippage_pct": round(sum(sl)  / max(1, len(sl)),  4),
                "max_slippage_pct": round(max(sl)  if sl  else 0,      4),
                "avg_latency_ms":   round(sum(lat) / max(1, len(lat)), 1),
                "total_fees":       round(sum(fee), 4),
                "pairs":            list({e.get("pair","") for e in ok}),
            }
        except Exception:
            return {"n_trades": 0}

    # ═══════════════════════════════════════════════════
    # ДИАГНОСТИКА
    # ═══════════════════════════════════════════════════

    def test_connection(self) -> dict:
        """Полная диагностика с понятными подсказками."""
        if not self.enabled:
            return {
                "connected": False,
                "error":     "Нет API ключей",
                "hint":      "Добавьте в config.yaml:\nbybit:\n  enabled: true\n  api_key: \"...\"\n  api_secret: \"...\"",
            }

        balance = self.get_balance()

        if "error" in balance:
            code = balance.get("retCode", -1)
            http = balance.get("_http_status", 0)
            err  = balance["error"]

            _hints = {
                403:   (
                    "HTTP 403 — ключ создан для ОСНОВНОГО аккаунта, а не для Demo.\n\n"
                    "Пошагово:\n"
                    "1) bybit.com → нажать оранжевую кнопку 'Demo Trading' вверху\n"
                    "2) Аватар профиля → API Management\n"
                    "3) Create New Key → API Transaction\n"
                    "4) Read-Write + No IP restriction + Contract (Orders+Positions)\n"
                    "5) Скопировать Key и Secret в config.yaml\n\n"
                    "❗ Ключи от основного и Demo аккаунтов РАЗНЫЕ"
                ),
                10003: "Неверный API Key — скопируйте заново из Demo Trading → API Management",
                10004: "Неверная подпись — скопируйте Secret заново, без пробелов/переносов строк",
                10005: "Нет прав — при создании ключа включите Read-Write + Contract (Orders+Positions)",
                33004: "Ключ истёк или неактивен — пересоздайте в Demo Trading",
                10002: "Ошибка времени — проверьте системные часы",
            }

            hint = _hints.get(http) or _hints.get(code, "")
            if not hint:
                if any(x in err for x in ("Errno -3", "Name or service", "nodename")):
                    hint = "Нет DNS / интернета с этого сервера"
                elif "timed out" in err.lower():
                    hint = "Таймаут — проверьте интернет-соединение"
                elif "403" in err:
                    hint = _hints[403]
                else:
                    hint = (
                        "Чеклист:\n"
                        "1. bybit.com → переключитесь в Demo Trading (оранжевая кнопка)\n"
                        "2. Аватар → API Management → Create New Key\n"
                        "3. Права: Read-Write + Contract (Orders + Positions)\n"
                        "4. IP: No IP restriction\n"
                        "5. Key и Secret скопировать без пробелов в config.yaml"
                    )

            return {
                "connected": False,
                "error":     err,
                "hint":      hint,
                "base_url":  self.base_url,
                "key_prefix": self.api_key[:6] + "..." if self.api_key else "—",
            }

        return {
            "connected":    True,
            "balance_usdt": balance.get("available", 0),
            "equity_usdt":  balance.get("equity",    0),
            "wallet_usdt":  balance.get("wallet",    0),
            "account_type": balance.get("account_type", "?"),
            "base_url":     self.base_url,
            "note":         balance.get("note", ""),
        }

    # ═══════════════════════════════════════════════════
    # X-008 FIX: RECONCILIATION — получить все открытые позиции на бирже
    # ═══════════════════════════════════════════════════

    def get_all_positions(self) -> list:
        """Получить ВСЕ открытые позиции на Bybit Demo (linear perpetuals).
        
        X-008 FIX: используется при startup reconciliation для обнаружения
        'сиротских' позиций, оставшихся после аварийного завершения.
        
        Returns: list of dicts {symbol, side, size, avgPrice, unrealisedPnl}
        """
        if not self.enabled:
            return []
        result = []
        cursor = ""
        for _page in range(5):  # max 5 pages
            params = {"category": "linear", "limit": "200"}
            if cursor:
                params["cursor"] = cursor
            resp = self._request("GET", "/v5/position/list", params)
            if resp.get("retCode") != 0:
                break
            for p in resp.get("result", {}).get("list", []):
                size = float(p.get("size", 0) or 0)
                if size > 0:
                    result.append({
                        "symbol":        p.get("symbol"),
                        "side":          p.get("side"),
                        "size":          size,
                        "avgPrice":      float(p.get("avgPrice", 0) or 0),
                        "unrealisedPnl": float(p.get("unrealisedPnl", 0) or 0),
                    })
            cursor = resp.get("result", {}).get("nextPageCursor", "")
            if not cursor:
                break
        return result


# ═══════════════════════════════════════════════════════
# ФАБРИКА
# ═══════════════════════════════════════════════════════

# BUG-023 FIX: singleton — создаём BybitExecutor один раз, переиспользуем.
# Раньше каждый вызов get_executor() создавал новый объект, сбрасывая кеш
# инструментов (get_instruments) и добавляя лишние round-trip к Bybit API.
# Сброс через reset_executor() нужен только при смене ключей в config.
_executor_instance: "BybitExecutor | None" = None
_executor_lock = __import__('threading').Lock()  # C-02 FIX


def get_executor() -> BybitExecutor:
    """Вернуть singleton BybitExecutor, создав при первом вызове.
    C-02 FIX: double-checked lock для thread safety."""
    global _executor_instance
    if _executor_instance is None:
        with _executor_lock:
            if _executor_instance is None:  # double-checked
                try:
                    from config_loader import CFG
                    _executor_instance = BybitExecutor(
                        api_key=    CFG("bybit", "api_key",    ""),
                        api_secret= CFG("bybit", "api_secret", ""),
                    )
                except Exception:
                    _executor_instance = BybitExecutor()
    return _executor_instance


def reset_executor() -> None:
    """Сбросить singleton — вызвать при смене API-ключей или testnet-флага."""
    global _executor_instance
    _executor_instance = None


def validate_option_d(direction: str, coin1: str, coin2: str) -> list:
    errors = []
    try:
        from config_loader import CFG, is_whitelisted
        if CFG("strategy", "short_only", False) and direction == "LONG":
            errors.append("LONG заблокирован (Option D)")
        if CFG("strategy", "whitelist_enabled", True) and not is_whitelisted(coin1, coin2):
            errors.append(f"{coin1}/{coin2} не в whitelist")
    except ImportError:
        pass
    return errors
