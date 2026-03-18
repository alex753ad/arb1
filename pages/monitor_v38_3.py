"""
Pairs Position Monitor v38.3 (BUG-031 FIX: version unified across docstring/UI/filename)
v38.2: Manual filter control + per-pair TP/SL (by user request):
  - REMOVED: ❌BT Hard Filter (now configurable checkbox)
  - REMOVED: ⚠️WL Whitelist block (now configurable checkbox)
  - NEW: 14 manual filter checkboxes in sidebar (entry/BT/WL/NK/direction)
  - NEW: Per-pair TP/SL config (detail pairs pre-loaded, default ±2%)
  - NEW: DEEP_RALLY manual toggle (override scanner block)
  - NEW: WL and NK in separate columns (removed from Вход cell)
  - NEW: Bybit tab restored with download buttons
  - KEPT: ⚪ ЖДАТЬ always blocked (hardcoded)
  - FIX: Anti-repeat — block re-entry same pair+dir after SL
  - FIX: Dynamic SL — grace 15min + wider SL early (-2.0% first 2h)
  - FIX: Wider trailing — activate 1.0%, drawdown 0.7% (phantom-proven)
  - FIX: Cooldowns bypassed for 🟢 ВХОД signals
  - FIX: BT/WL/NK flags saved to trade history
v38.0: Option D base:
  - Q >= 50 minimum (only profitable cluster)
  - Halflife < 8h
  - Z-based exit DISABLED
  - Wide trailing: MAD-dependent
  - Timeout = 8h
v35.0: Strategic improvements from analysis_v33-34:
  - Position sizing based on conviction score
  - Volatility regime detection (BTC ATR-based)
  - Two-phase exit: Phase 1 (Z-phase) → Phase 2 (Trail-phase)
  - Phantom auto-calibration (adjust trail params from phantom data)
  - FIX: Exact entry time (HH:MM:SS MSK) shown in position cards
  - FIX: Monitor messages tracked per-position, shown in trade history
  - FIX: Pattern analysis reads from both CSV and positions.json
v34.0: Critical improvements based on 26-27.02 trading analysis:
  - Z-exit → TRAIL mode (Z→0 activates trailing, NOT close position)
  - Hard BT filter (❌BT = block auto-entry)
  - Trailing widened: activate 1.2%, drawdown 0.6% (phantom-proven)
  - Adaptive TP based on |entry_z| (higher Z = higher TP target)
  - Max 2 positions per coin (prevent UNI-like concentration)
  - Live config read (params not frozen at entry time)
  - Pair memory blocking (0 wins after 2+ trades = blocked)
  - Cooldown 4h after SL (prevents ZEC/OKB double-loss)
  - BT metrics passed from scanner (bt_pnl, mu_bt_wr, v_quality)
  - Position-level trailing state tracking
v33.0: Critical Calibration + Coin Conflict Hard Block + Phantom CSV + Size Fix

Запуск: streamlit run monitor_v38_3.py
"""

# UX-03 FIX: единая версия — ссылаться из UI и логов
__version__ = "38.3"

import streamlit as st
import pandas as pd
import numpy as np
import ccxt
import time
import copy
import json
import os

# v41 FIX: абсолютные пути — файлы данных ищутся рядом с monitor_v38_3.py,
# независимо от cwd при запуске streamlit.
# v41 FIX: monitor_v38_3.py лежит в pages/, но ВСЕ файлы данных
# (positions.json, pair_cooldowns.json, monitor_import/ и т.д.)
# находятся в РОДИТЕЛЬСКОЙ папке (scaner2e/).
# __file__ = scaner2e/pages/monitor_v38_3.py
# _BASE_DIR = scaner2e/
_BASE_DIR           = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# N-08 FIX: если monitor запущен не из pages/, а из корня проекта — fallback
if not os.path.isdir(os.path.join(_BASE_DIR, "pages")) and not os.path.exists(os.path.join(_BASE_DIR, "config.yaml")):
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_MONITOR_IMPORT_DIR = os.path.join(_BASE_DIR, "monitor_import")
_SCAN_EXPORTS_DIR   = os.path.join(_BASE_DIR, "scan_exports")

# v42 FIX: удаляем осиротевшие .tmp файлы от предыдущих сессий
# Они появляются когда tempfile.mkstemp не завершил os.replace (сбой питания)
try:
    import glob as _glob_startup
    for _tmp in _glob_startup.glob(os.path.join(_BASE_DIR, "positions_*.tmp")):
        try:
            os.remove(_tmp)
        except Exception:
            pass
except Exception:
    pass

# v27: Unified config
try:
    from config_loader import CFG, CFG_auto_reload
except ImportError:
    def CFG(section, key=None, default=None):
        # F-001—F-005 FIX: fallback синхронизирован с config.yaml TRADE-1
        # B-07 FIX: добавлены ключи v35-38 (cascade_sl, max_coin_positions и др.)
        _d = {'strategy': {'entry_z': 2.5, 'exit_z': 0.5, 'stop_z_offset': 2.0,
              'min_stop_z': 4.0, 'max_hold_hours': 8, 'commission_pct': 0.10,
              'whitelist_enabled': True, 'short_only': False,
              'bt_filter_mode': 'HARD', 'wf_filter_mode': 'SOFT', 'ubt_filter_mode': 'HARD'},
              'monitor': {'refresh_interval_sec': 60, 'exit_z_target': 0.5,
              'pnl_stop_pct': -5.0, 'hurst_critical': 0.50, 'hurst_warning': 0.48,
              'hurst_border': 0.45, 'pvalue_warning': 0.10, 'correlation_warning': 0.20,
              'trailing_z_bounce': 0.8, 'time_warning_ratio': 1.0,
              'time_exit_ratio': 1.5, 'time_critical_ratio': 2.0,
              'overshoot_deep_z': 1.0, 'pnl_trailing_threshold': 0.5,
              'pnl_trailing_fraction': 0.4,
              'auto_exit_enabled': True, 'auto_tp_pct': 2.0, 'auto_sl_pct': -2.5,
              'auto_exit_z': 0.3, 'auto_exit_z_min_pnl': 0.6,
              'auto_exit_z_mode': 'TRAIL',
              'trailing_enabled': True, 'trailing_activate_pct': 1.0, 'trailing_drawdown_pct': 0.5,
              'auto_flip_enabled': True, 'pair_cooldown_hours': 4, 'pair_loss_limit_pct': -2.5,
              'coin_loss_warn_pct': -3.0, 'daily_loss_limit_pct': -5.0,
              'max_positions': 10, 'max_coin_exposure': 4, 'entry_grace_minutes': 5,
              'phantom_track_hours': 12,
              # B-07 FIX: v35-38 ключи (ранее отсутствовали)
              'cooldown_after_sl_hours': 12, 'cooldown_after_2sl_hours': 12,
              'cascade_sl_enabled': True, 'cascade_sl_window_hours': 6,
              'cascade_sl_threshold': 3, 'cascade_sl_pause_hours': 4,
              'max_coin_positions': 2,
              'phase2_pnl_fallback': 1.2,
              'hr_drift_warn_pct': 30, 'hr_drift_critical_pct': 50,
              'recovery_trail_threshold': 0.3, 'recovery_trail_drawdown': 0.5}}
        if key is None:
            return _d.get(section, {})
        return _d.get(section, {}).get(key, default)
    def CFG_auto_reload():
        return False
from datetime import datetime, timedelta, timezone

MSK = timezone(timedelta(hours=3))

# ═══════════════════════════════════════════════════════
# v38.3: BYBIT DEMO EXECUTOR — зеркалирование сделок
# ═══════════════════════════════════════════════════════
try:
    from bybit_executor import BybitExecutor, get_executor
    _BYBIT_AVAILABLE = True
except ImportError:
    _BYBIT_AVAILABLE = False

def _get_bybit_executor():
    """Get Bybit executor, respecting session_state toggle."""
    if not _BYBIT_AVAILABLE:
        return None
    mirror_enabled = st.session_state.get('bybit_demo_mirror', False)
    if not mirror_enabled:
        return None
    try:
        executor = get_executor()
        if executor and executor.enabled:
            return executor
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════
# X-008 FIX: INFLIGHT TRACKING + GRACEFUL SHUTDOWN + RECONCILIATION
#
# Проблема: при kill/Ctrl+C во время open_pair_trade leg1 может быть
# исполнена, leg2 нет → голая позиция на бирже без записи в positions.json.
#
# Решение:
#   1. inflight.json — записывается ДО вызова Bybit, удаляется ПОСЛЕ.
#      При старте: если файл существует → незавершённая операция.
#   2. atexit + SIGTERM handler — при остановке проверяет inflight,
#      логирует предупреждение.
#   3. startup_reconciliation() — сравнивает позиции Bybit с positions.json,
#      алертит о расхождениях.
# ═══════════════════════════════════════════════════════

INFLIGHT_FILE = os.path.join(_BASE_DIR, "inflight.json")

def _inflight_save(operation: str, coin1: str, coin2: str, direction: str,
                   extra: dict = None) -> None:
    """Записать текущую in-flight операцию. Вызывается ДО отправки на Bybit."""
    try:
        data = {
            "operation": operation,  # "OPEN" / "CLOSE"
            "coin1": coin1,
            "coin2": coin2,
            "direction": direction,
            "started_at": now_msk().isoformat(),
            "pid": os.getpid(),
        }
        if extra:
            data.update(extra)
        with open(INFLIGHT_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
    except Exception:
        pass

def _inflight_clear() -> None:
    """Удалить inflight маркер. Вызывается ПОСЛЕ завершения операции."""
    try:
        if os.path.exists(INFLIGHT_FILE):
            os.remove(INFLIGHT_FILE)
    except Exception:
        pass

def _inflight_load() -> dict:
    """Загрузить inflight данные (если есть)."""
    if os.path.exists(INFLIGHT_FILE):
        try:
            with open(INFLIGHT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _shutdown_handler(*args) -> None:
    """Graceful shutdown: проверить inflight операции при завершении."""
    inflight = _inflight_load()
    if inflight:
        _msg = (
            f"⚠️ SHUTDOWN с незавершённой операцией!\n"
            f"  Operation: {inflight.get('operation', '?')}\n"
            f"  Pair: {inflight.get('coin1', '?')}/{inflight.get('coin2', '?')} "
            f"{inflight.get('direction', '?')}\n"
            f"  Started: {inflight.get('started_at', '?')}\n"
            f"  ⚠️ Проверьте позиции на Bybit вручную!"
        )
        import logging as _sd_log
        _sd_log.getLogger("monitor").critical(_msg)
        # Попытка записать в emergency лог
        try:
            with open(os.path.join(_BASE_DIR, "bybit_emergency.log"), "a", encoding="utf-8") as f:
                f.write(f"{now_msk().isoformat()} SHUTDOWN_INFLIGHT\n{_msg}\n{'='*60}\n")
        except Exception:
            pass

# Регистрация shutdown handlers
import atexit, signal
atexit.register(_shutdown_handler)
try:
    signal.signal(signal.SIGTERM, _shutdown_handler)
except (OSError, ValueError):
    pass  # Windows / non-main thread

def startup_reconciliation() -> list:
    """X-008 FIX: Проверка при старте — сравнить позиции Bybit с positions.json.
    
    Возвращает список предупреждений (пустой = всё OK).
    Вызывается один раз при загрузке монитора.
    """
    warnings = []
    
    # 1. Проверить незавершённые inflight операции
    inflight = _inflight_load()
    if inflight:
        _age_min = 0
        try:
            _started = datetime.fromisoformat(inflight['started_at'])
            _age_min = (now_msk() - _started).total_seconds() / 60
        except Exception:
            pass
        warnings.append(
            f"🚨 INFLIGHT: незавершённая {inflight.get('operation', '?')} "
            f"{inflight.get('coin1', '?')}/{inflight.get('coin2', '?')} "
            f"{inflight.get('direction', '?')} ({_age_min:.0f} мин назад). "
            f"Проверьте Bybit вручную!"
        )
        # Очищаем маркер — предупреждение показано
        _inflight_clear()
    
    # 2. Сравнить позиции Bybit с positions.json
    executor = _get_bybit_executor()
    if executor is None:
        return warnings
    
    try:
        bybit_positions = executor.get_all_positions()
    except Exception:
        return warnings
    
    if not bybit_positions:
        return warnings
    
    # Собрать монеты из OPEN позиций в positions.json
    try:
        local_positions = [p for p in load_positions() if p.get('status') == 'OPEN']
        local_symbols = set()
        for p in local_positions:
            local_symbols.add(f"{p['coin1'].upper()}USDT")
            local_symbols.add(f"{p['coin2'].upper()}USDT")
    except Exception:
        local_symbols = set()
    
    # Найти сиротские позиции на Bybit (есть на бирже, нет в positions.json)
    for bp in bybit_positions:
        sym = bp.get("symbol", "")
        if sym and sym not in local_symbols:
            warnings.append(
                f"⚠️ ORPHAN: {sym} {bp['side']} qty={bp['size']} "
                f"на Bybit, но нет в positions.json. "
                f"Закройте вручную или добавьте позицию."
            )
    
    return warnings


def _bybit_open(coin1, coin2, direction, size_usdt, price1=None, price2=None):
    """Mirror open trade to Bybit Demo. Returns result dict or None.
    X-008 FIX: inflight tracking — записывает маркер до и очищает после.
    """
    executor = _get_bybit_executor()
    if executor is None:
        return None
    # X-008: записать inflight маркер ДО отправки
    _inflight_save("OPEN", coin1, coin2, direction, {"size_usdt": size_usdt})
    try:
        result = executor.open_pair_trade(coin1, coin2, direction, size_usdt,
                                          expected_price1=price1,
                                          expected_price2=price2)
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        # X-008: очистить маркер ПОСЛЕ завершения (успех или ошибка)
        _inflight_clear()

def _bybit_close(coin1, coin2, direction, price1=None, price2=None):
    """Mirror close trade to Bybit Demo. Returns result dict or None.
    X-008 FIX: inflight tracking.
    """
    executor = _get_bybit_executor()
    if executor is None:
        return None
    _inflight_save("CLOSE", coin1, coin2, direction)
    try:
        result = executor.close_pair_trade(coin1, coin2, direction,
                                           expected_price1=price1,
                                           expected_price2=price2)
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        _inflight_clear()
def _save_bybit_fill(pos_id: int, bb_res: dict) -> None:
    """BUG-015 FIX: persist Bybit fill prices and qty into positions.json.
    Called after a successful open_pair_trade() so PnL calculations can use
    real exchange fills instead of the theoretical 50/50 entry prices.
    Fields saved:  bybit_qty1, bybit_price1, bybit_qty2, bybit_price2.
    Falls back silently — never raises.
    """
    if not bb_res or not bb_res.get('success'):
        return
    try:
        leg1 = bb_res.get('leg1', {})
        leg2 = bb_res.get('leg2', {})
        qty1   = leg1.get('qty', 0)
        price1 = leg1.get('fill_price') or leg1.get('expected_price', 0)
        qty2   = leg2.get('qty', 0)
        price2 = leg2.get('fill_price') or leg2.get('expected_price', 0)
        if not (qty1 and price1 and qty2 and price2):
            return
        all_pos = load_positions()
        for p in all_pos:
            if p['id'] == pos_id:
                p['bybit_qty1']   = qty1
                p['bybit_price1'] = price1
                p['bybit_qty2']   = qty2
                p['bybit_price2'] = price2
                break
        save_positions(all_pos)
    except Exception:
        pass


def now_msk():
    return datetime.now(MSK)

def to_msk(dt_str):
    """Convert ISO datetime string to HH:MM МСК."""
    if not dt_str:
        return ''
    try:
        dt = datetime.fromisoformat(str(dt_str))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MSK)
        msk_dt = dt.astimezone(MSK)
        return msk_dt.strftime('%H:%M')
    except Exception:
        return str(dt_str)[-5:]

def to_msk_full(dt_str):
    """Convert ISO datetime string to DD.MM HH:MM МСК."""
    if not dt_str:
        return ''
    try:
        dt = datetime.fromisoformat(str(dt_str))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MSK)
        msk_dt = dt.astimezone(MSK)
        return msk_dt.strftime('%d.%m %H:%M')
    except Exception:
        return str(dt_str)[:16]
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from statsmodels.tsa.stattools import coint

# ═══════════════════════════════════════════════════════
# DRY: Import shared utilities from analysis module
# ═══════════════════════════════════════════════════════
try:
    from mean_reversion_analysis import (
        assess_entry_readiness,
        calculate_hurst_exponent,
        calculate_hurst_ema,
        calculate_adaptive_robust_zscore,
        calculate_garch_zscore,
        calc_halflife_from_spread,
        check_pnl_z_disagreement,
        smart_exit_analysis,
        z_velocity_analysis,
        kalman_hedge_ratio,       # X-002 FIX: DRY — was duplicated as kalman_hr
    )
    _USE_MRA = True
except ImportError:
    _USE_MRA = False


# v30: Telegram helper for exit alerts
def send_telegram(token, chat_id, message):
    """Send Telegram via HTTPS POST with DNS fallback.

    MON-8 FIX: Перенесена реализация из app.py с перебором хостов:
      1) api.telegram.org (DNS) — стандартный путь
      2) 149.154.167.220 (прямой IP DC2) — fallback при DNS-блокировке

    При DNS-блокировке Telegram в России (типичная проблема облачных серверов)
    старая реализация молча теряла все уведомления из монитора.
    """
    import urllib.request, urllib.parse, json as _json_tg, ssl as _ssl_tg

    if not token or not chat_id:
        return False, "Token или Chat ID не заданы"

    _TG_HOSTS = [
        "api.telegram.org",       # Primary DNS
        "149.154.167.220",        # Direct IP fallback (DC2)
    ]

    payload = _json_tg.dumps({
        "chat_id": chat_id, "text": message,
        "parse_mode": "HTML", "disable_web_page_preview": True
    }).encode('utf-8')

    last_err = ""
    for host in _TG_HOSTS:
        try:
            url = f"https://{host}/bot{token}/sendMessage"
            req = urllib.request.Request(
                url, data=payload,
                headers={
                    "Content-Type": "application/json",
                    "Host": "api.telegram.org"  # SNI for IP fallback
                }
            )
            # Allow self-signed for IP fallback
            ctx = _ssl_tg.create_default_context()
            if host != "api.telegram.org":
                ctx.check_hostname = False
                ctx.verify_mode = _ssl_tg.CERT_NONE

            with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
                body = resp.read().decode()
                data = _json_tg.loads(body)
                if data.get('ok'):
                    return True, f"OK (via {host})"
                return False, data.get('description', 'Unknown error')
        except Exception as e:
            last_err = f"{host}: {str(e)[:80]}"
            continue

    return False, f"All hosts failed. Last: {last_err}"

# v5.3: assess_entry_readiness — imported from analysis module when available
# v37 Wave 1.6: ВСЕГДА импортируем из MRA — единый источник истины
# Local fallback удалён. Если MRA недоступен — функция из MRA import выше.

# v37 Wave 1.6: assess_entry_readiness УДАЛЁН отсюда.
# Единственный источник: mean_reversion_analysis.py → импорт ниже через _USE_MRA
# Если MRA недоступен, используется fallback-обёртка:
if not _USE_MRA:
    def assess_entry_readiness(p):
        """Minimal fallback when MRA module unavailable."""
        mandatory_ok = (
            p.get('signal', 'NEUTRAL') in ('SIGNAL', 'READY') and
            abs(p.get('zscore', 0)) >= p.get('threshold', 2.0) and
            p.get('quality_score', 0) >= 50 and
            p.get('direction', 'NONE') != 'NONE'
        )
        if mandatory_ok:
            level, label = 'CONDITIONAL', '🟡 УСЛОВНО'
        else:
            level, label = 'WAIT', '⚪ ЖДАТЬ'
        return {'level': level, 'label': label, 'all_mandatory': mandatory_ok,
                'mandatory': [], 'optional': [], 'fdr_bypass': False, 'opt_count': 0}

# ═══════════════════════════════════════════════════════
# CORE MATH (standalone — не зависит от analysis module)
# ═══════════════════════════════════════════════════════

# v23.0: Commission round-trip (4 legs × commission_pct per leg)
# BUG-N15 FIX: заменяем модульную константу на функцию — значение читается из CFG
# при каждом вызове, поэтому изменение commission_pct в config.yaml применяется
# без перезапуска Streamlit. Все места использования обновлены на вызов функции.
def COMMISSION_ROUND_TRIP_PCT():
    """BUG-N15 FIX: динамически читает commission_pct из CFG (было: модульная константа)."""
    return CFG('strategy', 'commission_pct', 0.10) * 4

# v38.2: SHORT_ONLY replaced by manual checkbox filter (block_long / block_short)

# ═══════════════════════════════════════════════════════
# E-001 FIX: Атомарная запись JSON-файлов
# Паттерн: write .new → backup → os.replace (атомарная операция на POSIX/NTFS)
# При сбое питания: .new неполный, но оригинал и .bak целы.
# ═══════════════════════════════════════════════════════

def _atomic_json_save(filepath, data, ensure_ascii=False):
    """Атомарная запись JSON с backup. E-001 FIX.
    
    1. Пишем в filepath.new (если упадёт — оригинал не тронут)
    2. Бэкапим оригинал в filepath.bak (если есть и >10 байт)
    3. os.replace(.new → оригинал) — атомарно на всех ОС
    4. Fallback: прямая запись при ошибке replace
    """
    import shutil
    new_path = filepath + ".new"
    bak_path = filepath + ".bak"
    try:
        with open(new_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=ensure_ascii, default=str)
            f.flush()
            os.fsync(f.fileno())
        if os.path.exists(filepath) and os.path.getsize(filepath) > 10:
            try:
                shutil.copy2(filepath, bak_path)
            except Exception:
                pass
        os.replace(new_path, filepath)
    except Exception:
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=ensure_ascii, default=str)
        except Exception:
            pass
    finally:
        try:
            if os.path.exists(new_path):
                os.remove(new_path)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════
# v32: PAIR COOLDOWN & LOSS TRACKING
# ═══════════════════════════════════════════════════════
COOLDOWN_FILE = os.path.join(_BASE_DIR, "pair_cooldowns.json")

# E-006 FIX: SQLite для cooldowns
try:
    from db_store import db_load_cooldowns, db_save_cooldowns, db_update_cooldown, db_get_cooldown
    _USE_SQLITE_CD = True
except ImportError:
    _USE_SQLITE_CD = False

# P-002 FIX: in-memory кэш cooldowns с mtime-проверкой.
# Было: 11 вызовов _load_cooldowns() за цикл, каждый — полное чтение JSON с диска.
# Стало: один read при изменении файла, O(1) для повторных вызовов в том же цикле.
_cooldowns_cache = None
_cooldowns_mtime = 0.0

def _load_cooldowns():
    """Load cooldowns. E-006 FIX: SQLite primary, JSON fallback with mtime cache.
    DAT-03 FIX: SQLite path uses PRAGMA data_version for cache invalidation."""
    global _cooldowns_cache, _cooldowns_mtime

    # E-006: SQLite path
    if _USE_SQLITE_CD:
        try:
            # DAT-03 FIX: check data_version — changes when ANY process writes
            from db_store import _get_conn as _cd_get_conn
            with _cd_get_conn(readonly=True) as _vc:
                _ver = _vc.execute("PRAGMA data_version").fetchone()[0]
            if _cooldowns_cache is not None and _cooldowns_mtime == _ver:
                return _cooldowns_cache
            _cooldowns_cache = db_load_cooldowns()
            _cooldowns_mtime = _ver
            return _cooldowns_cache
        except Exception:
            pass

    # JSON fallback
    try:
        if not os.path.exists(COOLDOWN_FILE):
            _cooldowns_cache = {}
            _cooldowns_mtime = 0.0
            return {}
        current_mtime = os.path.getmtime(COOLDOWN_FILE)
        if _cooldowns_cache is not None and current_mtime == _cooldowns_mtime:
            return _cooldowns_cache
        with open(COOLDOWN_FILE, 'r', encoding='utf-8') as f:
            _cooldowns_cache = json.load(f)
        _cooldowns_mtime = current_mtime
        return _cooldowns_cache
    except Exception:
        if _cooldowns_cache is not None:
            return _cooldowns_cache
        return {}

def _invalidate_cooldowns_cache():
    """P-002 FIX: сброс кэша после записи. Следующий _load_cooldowns() перечитает файл."""
    global _cooldowns_cache, _cooldowns_mtime
    _cooldowns_cache = None
    _cooldowns_mtime = 0.0

def _save_cooldowns(data):
    """E-006 FIX: SQLite primary + JSON backup."""
    if _USE_SQLITE_CD:
        try:
            db_save_cooldowns(data)
        except Exception:
            pass
    _atomic_json_save(COOLDOWN_FILE, data)
    _invalidate_cooldowns_cache()

def record_trade_for_cooldown(pair_name, pnl_pct, direction, exit_reason=""):
    """Record closed trade for cooldown tracking. v38.2: track SL direction for anti-repeat."""
    cd = _load_cooldowns()
    # F-009 FIX: now_msk().date().isoformat() — гарантированно MSK-дата
    # (strftime тоже MSK-aware через now_msk(), но .date() явнее)
    today = now_msk().date().isoformat()
    if pair_name not in cd:
        cd[pair_name] = {'session_pnl': 0, 'last_loss_time': None, 'last_dir': None,
                         'date': today, 'sl_exit': False, 'consecutive_sl': 0}
    # Reset session_pnl if new day, but LOG-08 FIX: preserve consecutive_sl
    # (SL streak survives midnight — prevents abuse of day boundary)
    if cd[pair_name].get('date') != today:
        _prev_consecutive_sl = cd[pair_name].get('consecutive_sl', 0)
        _prev_sl_exit = cd[pair_name].get('sl_exit', False)
        _prev_last_loss = cd[pair_name].get('last_loss_time')
        cd[pair_name] = {'session_pnl': 0, 'last_loss_time': _prev_last_loss, 'last_dir': None,
                         'date': today, 'sl_exit': _prev_sl_exit,
                         'consecutive_sl': _prev_consecutive_sl}
    cd[pair_name]['session_pnl'] = round(cd[pair_name].get('session_pnl', 0) + pnl_pct, 3)
    if pnl_pct < 0:
        cd[pair_name]['last_loss_time'] = now_msk().isoformat()
        _is_sl = 'AUTO_SL' in str(exit_reason) or 'PNLSTOP' in str(exit_reason)
        cd[pair_name]['sl_exit'] = _is_sl
        if _is_sl:
            cd[pair_name]['consecutive_sl'] = cd[pair_name].get('consecutive_sl', 0) + 1
        else:
            cd[pair_name]['consecutive_sl'] = 0
    else:
        cd[pair_name]['sl_exit'] = False
        cd[pair_name]['consecutive_sl'] = 0
    cd[pair_name]['last_dir'] = direction
    _save_cooldowns(cd)

def check_pair_cooldown(pair_name, entry_label="", cd_data=None):
    """Thin wrapper: reads CFG, delegates to core.risk.check_pair_cooldown.
    B-04 FIX: accepts pre-loaded cd_data to avoid redundant disk reads."""
    try:
        from pairs_scanner.core.risk import check_pair_cooldown as _core_cooldown
        cd = cd_data if cd_data is not None else _load_cooldowns()
        return _core_cooldown(
            pair_name=pair_name,
            cd_data=cd,
            entry_label=entry_label,
            cooldown_after_sl_hours=float(CFG('monitor', 'cooldown_after_sl_hours', 12)),
            cooldown_after_2sl_hours=float(CFG('monitor', 'cooldown_after_2sl_hours', 12)),
            pair_cooldown_hours=float(CFG('monitor', 'pair_cooldown_hours', 4)),
        )
    except ImportError:
        # Fallback: inline logic if core module not yet deployed
        is_green = '🟢' in str(entry_label) or 'ВХОД' in str(entry_label).upper()
        cd = cd_data if cd_data is not None else _load_cooldowns()
        entry = cd.get(pair_name, {})
        if entry.get('last_loss_time'):
            try:
                loss_dt = datetime.fromisoformat(entry['last_loss_time'])
                hours_since = (now_msk() - loss_dt).total_seconds() / 3600
                is_sl = entry.get('sl_exit', False)
                consecutive_sl = entry.get('consecutive_sl', 0)
                session_pnl = entry.get('session_pnl', 0)
                if consecutive_sl >= 2:
                    cooldown_h = float(CFG('monitor', 'cooldown_after_2sl_hours', 12))
                    reason_tag = "2+ SL подряд"
                elif is_sl:
                    cooldown_h = float(CFG('monitor', 'cooldown_after_sl_hours', 12))
                    reason_tag = "SL"
                elif session_pnl < -0.5:
                    cooldown_h = float(CFG('monitor', 'pair_cooldown_hours', 4))
                    reason_tag = "убыток"
                else:
                    return False, ""
                if is_green and not is_sl and consecutive_sl < 2:
                    return False, ""
                if cooldown_h > 0 and hours_since < cooldown_h:
                    remaining = cooldown_h - hours_since
                    return True, f"⏳ {pair_name}: {cooldown_h:.0f}ч блок ({reason_tag}) {remaining:.1f}ч осталось"
            except Exception:
                pass
        return False, ""

def check_daily_loss_limit(cd_data=None, live_open_pnls=None):
    """Thin wrapper: reads CFG + session_state, delegates to core.risk.check_daily_loss_limit."""
    cd = cd_data if cd_data is not None else _load_cooldowns()
    # Resolve limit from session_state → CFG → default
    try:
        import streamlit as _st_dl
        limit = float(_st_dl.session_state.get('daily_loss_limit_pct',
                      CFG('monitor', 'daily_loss_limit_pct', -5.0)))
    except Exception:
        limit = float(CFG('monitor', 'daily_loss_limit_pct', -5.0))
    # Resolve live PnLs: use passed value, or fallback to disk
    if live_open_pnls is None:
        try:
            open_positions = [p for p in load_positions() if p.get('status') == 'OPEN']
            live_open_pnls = [p.get('pnl_pct', 0) for p in open_positions]
        except Exception:
            live_open_pnls = []
    try:
        from pairs_scanner.core.risk import check_daily_loss_limit as _core_daily
        return _core_daily(cd, live_open_pnls, limit)
    except ImportError:
        # Fallback: inline logic
        today = now_msk().date().isoformat()
        total_pnl = sum(e.get('session_pnl', 0) for e in cd.values() if e.get('date') == today)
        unrealised_loss = sum(pnl for pnl in live_open_pnls if pnl < 0)
        total_pnl += unrealised_loss
        if total_pnl <= limit:
            return True, f"🛑 ДНЕВНОЙ ЛИМИТ: потери {total_pnl:+.2f}% (лимит {limit}%, включая открытые)"
        return False, ""

# ── BUG-N14 FIX: Cascade SL protection ──────────────────────────────────────
# Параметры config.yaml: cascade_sl_enabled, cascade_sl_window_hours,
# cascade_sl_threshold, cascade_sl_pause_hours.
# Ранее параметры существовали, но функция отсутствовала — защита не работала.
#
# Логика: если за последние window_hours зафиксировано >= threshold SL-выходов,
# система входит в паузу на pause_hours. Время начала паузы хранится в
# cascade_sl_state.json (рядом с pair_cooldowns.json).

CASCADE_SL_STATE_FILE = os.path.join(_BASE_DIR, "cascade_sl_state.json")

# P-002 FIX: mtime-кэш для cascade state
_cascade_cache = None
_cascade_mtime = 0.0

def _load_cascade_state():
    """Загрузить состояние cascade-паузы. P-002 FIX: mtime cache."""
    global _cascade_cache, _cascade_mtime
    try:
        if not os.path.exists(CASCADE_SL_STATE_FILE):
            _cascade_cache = {}
            _cascade_mtime = 0.0
            return {}
        current_mtime = os.path.getmtime(CASCADE_SL_STATE_FILE)
        if _cascade_cache is not None and current_mtime == _cascade_mtime:
            return _cascade_cache
        with open(CASCADE_SL_STATE_FILE, 'r', encoding='utf-8') as _f:
            _cascade_cache = json.load(_f)
        _cascade_mtime = current_mtime
        return _cascade_cache
    except Exception:
        if _cascade_cache is not None:
            return _cascade_cache
        return {}

def _save_cascade_state(data):
    """Сохранить состояние cascade-паузы. E-001 FIX: атомарная запись."""
    _atomic_json_save(CASCADE_SL_STATE_FILE, data)
    # P-002 FIX: инвалидация кэша
    global _cascade_cache, _cascade_mtime
    _cascade_cache = None
    _cascade_mtime = 0.0

def check_cascade_sl(cd_data=None):
    """Thin wrapper: reads CFG, manages cascade state file, delegates check to core.risk."""
    if not CFG('monitor', 'cascade_sl_enabled', False):
        return False, ""

    window_h  = float(CFG('monitor', 'cascade_sl_window_hours', 2))
    threshold = int(CFG('monitor', 'cascade_sl_threshold', 3))
    pause_h   = float(CFG('monitor', 'cascade_sl_pause_hours', 1))
    cd = cd_data if cd_data is not None else _load_cooldowns()
    state = _load_cascade_state()

    try:
        from pairs_scanner.core.risk import check_cascade_sl as _core_cascade
        blocked, reason = _core_cascade(
            cd_data=cd,
            cascade_enabled=True,
            window_hours=window_h,
            threshold=threshold,
            pause_hours=pause_h,
            cascade_state=state,
        )
        # If newly triggered (no existing pause, but blocked) — save state
        if blocked and not state.get('pause_start'):
            _save_cascade_state({
                'pause_start': now_msk().isoformat(),
                'sl_count': threshold,
                'window_h': window_h,
                'pause_h': pause_h,
            })
        # If existing pause expired — clear state
        elif not blocked and state.get('pause_start'):
            _save_cascade_state({})
        return blocked, reason
    except ImportError:
        # Fallback: inline logic
        now = now_msk()
        if state.get('pause_start'):
            try:
                pause_start = datetime.fromisoformat(state['pause_start'])
                elapsed_h = (now - pause_start).total_seconds() / 3600
                if elapsed_h < pause_h:
                    remaining = pause_h - elapsed_h
                    sl_count = state.get('sl_count', threshold)
                    return (True, f"🛑 CASCADE SL ПАУЗА: {sl_count} стоп-лоссов "
                            f"за {window_h:.0f}ч → пауза {pause_h:.0f}ч ({remaining:.1f}ч осталось)")
                else:
                    _save_cascade_state({})
                    return False, ""
            except Exception:
                _save_cascade_state({})
        cutoff = now - timedelta(hours=window_h)
        sl_count = 0
        for entry in cd.values():
            if not entry.get('sl_exit', False):
                continue
            try:
                loss_dt = datetime.fromisoformat(entry.get('last_loss_time', ''))
                if loss_dt >= cutoff:
                    sl_count += 1
            except Exception:
                continue
        if sl_count >= threshold:
            _save_cascade_state({'pause_start': now.isoformat(), 'sl_count': sl_count,
                                 'window_h': window_h, 'pause_h': pause_h})
            return (True, f"🛑 CASCADE SL ПАУЗА: {sl_count} стоп-лоссов "
                    f"за {window_h:.0f}ч → пауза {pause_h:.0f}ч ({pause_h:.1f}ч осталось)")
        return False, ""

def check_coin_losses(coin):
    """Check total losses for a single coin across all pairs."""
    cd = _load_cooldowns()
    today = now_msk().strftime('%Y-%m-%d')
    total = 0
    for pair, entry in cd.items():
        if entry.get('date') != today:
            continue
        if coin in pair.split('/'):
            total += entry.get('session_pnl', 0)
    warn_limit = CFG('monitor', 'coin_loss_warn_pct', -3.0)
    if total <= warn_limit:
        return True, f"⚠️ {coin}: суммарные потери {total:+.2f}% (порог {warn_limit}%)"
    return False, ""

# ═══════════════════════════════════════════════════════
# v32: AUTO-EXIT ENGINE
# ═══════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════
# v38.2: PER-PAIR TP/SL CONFIG
# ═══════════════════════════════════════════════════════
PAIR_TP_SL_FILE = os.path.join(_BASE_DIR, "pair_tp_sl.json")

# P-002 FIX: mtime-кэш для pair_tp_sl (читается при каждом check_auto_exit × N позиций)
_tp_sl_cache = None
_tp_sl_mtime = 0.0

def _load_pair_tp_sl():
    """Load pair TP/SL config with mtime cache."""
    global _tp_sl_cache, _tp_sl_mtime
    try:
        if not os.path.exists(PAIR_TP_SL_FILE):
            _tp_sl_cache = {}
            _tp_sl_mtime = 0.0
            return {}
        current_mtime = os.path.getmtime(PAIR_TP_SL_FILE)
        if _tp_sl_cache is not None and current_mtime == _tp_sl_mtime:
            return _tp_sl_cache
        with open(PAIR_TP_SL_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # F-006 FIX: отфильтровать метаданные (ключи начинающиеся с _)
        _tp_sl_cache = {k: v for k, v in data.items() if not k.startswith('_')}
        _tp_sl_mtime = current_mtime
        return _tp_sl_cache
    except Exception:
        if _tp_sl_cache is not None:
            return _tp_sl_cache
        return {}

def _save_pair_tp_sl(data):
    # F-006 FIX: добавляем _updated_at для отслеживания версии файла
    data['_updated_at'] = now_msk().isoformat()
    _atomic_json_save(PAIR_TP_SL_FILE, data)
    # P-002 FIX: инвалидация кэша
    global _tp_sl_cache, _tp_sl_mtime
    _tp_sl_cache = None
    _tp_sl_mtime = 0.0

def get_pair_tp_sl(pair_name):
    """Get TP/SL for a specific pair. Returns (tp_pct, sl_pct).
    Falls back to config auto_tp_pct / auto_sl_pct if not configured per-pair."""
    _default_tp = CFG('monitor', 'auto_tp_pct', 2.0)
    _default_sl = CFG('monitor', 'auto_sl_pct', -2.5)
    cfg = _load_pair_tp_sl()
    if pair_name in cfg:
        return float(cfg[pair_name].get('tp', _default_tp)), float(cfg[pair_name].get('sl', _default_sl))
    # Try reverse pair
    parts = pair_name.split('/')
    if len(parts) == 2:
        rev = f"{parts[1]}/{parts[0]}"
        if rev in cfg:
            return float(cfg[rev].get('tp', _default_tp)), float(cfg[rev].get('sl', _default_sl))
    return _default_tp, _default_sl

# ═══════════════════════════════════════════════════════
# v38.2: ENTRY FILTER SYSTEM (checkbox-based)
# ═══════════════════════════════════════════════════════

def parse_entry_flags(entry_label_raw):
    """Parse entry label into base label + separate flags.
    Returns dict: {base_label, bt_flag, wl_flag, nk_flag}"""
    s = str(entry_label_raw or '')
    # Extract flags
    bt_flag = ''
    if '❌BT' in s: bt_flag = '❌BT'
    elif '⚠️BT' in s: bt_flag = '⚠️BT'
    
    wl_flag = ''
    if '❌WL' in s: wl_flag = '❌WL'
    elif '⚠️WL' in s: wl_flag = '⚠️WL'
    
    nk_flag = ''
    if '❌NK' in s: nk_flag = '❌NK'
    elif '⚠️NK' in s: nk_flag = '⚠️NK'
    
    # Clean base label — remove flags
    base = s
    for flag in ['❌BT', '⚠️BT', '❌WL', '⚠️WL', '❌NK', '⚠️NK']:
        base = base.replace(flag, '')
    base = base.strip()
    
    return {
        'base_label': base,
        'bt_flag': bt_flag,
        'wl_flag': wl_flag,
        'nk_flag': nk_flag,
        'raw': s,
    }

def check_entry_filters(entry_label_raw, direction, filters_state):
    """Check if entry should be blocked by user-configured filters.
    Returns (blocked, reason) tuple.
    
    filters_state is a dict of checkbox states from st.session_state:
      - block_green: block all 🟢 ВХОД
      - block_green_bt_fail: block 🟢 ВХОД + ❌BT
      - block_green_bt_warn: block 🟢 ВХОД + ⚠️BT
      - block_yellow: block all 🟡 УСЛОВНО
      - block_yellow_bt_fail: block 🟡 УСЛОВНО + ❌BT
      - block_yellow_bt_warn: block 🟡 УСЛОВНО + ⚠️BT
      - block_wl_warn: block ⚠️WL
      - block_wl_fail: block ❌WL
      - block_nk_warn: block ⚠️NK
      - block_nk_fail: block ❌NK
      - block_long: block LONG
      - block_short: block SHORT
    """
    flags = parse_entry_flags(entry_label_raw)
    base = flags['base_label']
    bt = flags['bt_flag']
    wl = flags['wl_flag']
    nk = flags['nk_flag']
    
    is_green = '🟢' in base or 'ВХОД' in base.upper()
    is_yellow = '🟡' in base or 'УСЛОВНО' in base.upper()
    is_wait = '⚪' in base or 'ЖДАТЬ' in base.upper()

    # BUG-009 FIX: direction filters moved FIRST — cheapest check, no point
    # running label/bt/wl/nk logic for a direction that is categorically blocked.
    if filters_state.get('block_long', False) and direction == 'LONG':
        return True, "🚫 LONG заблокирован (фильтр)"
    if filters_state.get('block_short', False) and direction == 'SHORT':
        return True, "🚫 SHORT заблокирован (фильтр)"

    # 7. ЖДАТЬ always blocked (hardcoded, no checkbox)
    if is_wait:
        return True, "⚪ ЖДАТЬ заблокирован (WR=35%, системный запрет)"

    # 2. 🟢 ВХОД filters
    if is_green:
        if filters_state.get('block_green', False):
            return True, "🚫 🟢 ВХОД заблокирован (фильтр)"
        if bt == '❌BT' and filters_state.get('block_green_bt_fail', False):
            return True, "🚫 🟢 ВХОД + ❌BT заблокирован (фильтр)"
        if bt == '⚠️BT' and filters_state.get('block_green_bt_warn', False):
            return True, "🚫 🟢 ВХОД + ⚠️BT заблокирован (фильтр)"
    
    # 3. 🟡 УСЛОВНО filters
    if is_yellow:
        if filters_state.get('block_yellow', False):
            return True, "🚫 🟡 УСЛОВНО заблокирован (фильтр)"
        if bt == '❌BT' and filters_state.get('block_yellow_bt_fail', False):
            return True, "🚫 🟡 УСЛОВНО + ❌BT заблокирован (фильтр)"
        if bt == '⚠️BT' and filters_state.get('block_yellow_bt_warn', False):
            return True, "🚫 🟡 УСЛОВНО + ⚠️BT заблокирован (фильтр)"
    
    # 4. WL filters (apply to any entry)
    if wl == '⚠️WL' and filters_state.get('block_wl_warn', False):
        return True, "🚫 ⚠️WL заблокирован (фильтр)"
    if wl == '❌WL' and filters_state.get('block_wl_fail', False):
        return True, "🚫 ❌WL заблокирован (фильтр)"
    
    # 5. NK filters (apply to any entry)
    if nk == '⚠️NK' and filters_state.get('block_nk_warn', False):
        return True, "🚫 ⚠️NK заблокирован (фильтр)"
    if nk == '❌NK' and filters_state.get('block_nk_fail', False):
        return True, "🚫 ❌NK заблокирован (фильтр)"
    
    return False, ""
def check_auto_exit(pos, mon):
    """v34: Check if position should be auto-closed. Returns (should_close, reason) or (False, None).
    
    v34 KEY CHANGES:
    - Z-exit → TRAIL mode: Z→0 activates trailing, doesn't close
    - Adaptive TP: higher |entry_z| → higher TP target
    - Live config read: uses current config, not frozen params
    - Trailing state tracked per-position ('trail_activated' flag)
    """
    if not CFG('monitor', 'auto_exit_enabled', True):
        return False, None

    pnl = mon.get('pnl_pct', 0)
    z_static = mon.get('z_static', mon.get('z_now', 0))
    # BUG-029 FIX (clarification): two distinct high-water mark fields exist:
    #   mon['best_pnl']              — current-render HWM (updated this tick)
    #   pos['best_pnl_during_trade'] — persistent HWM (written to positions.json)
    # check_auto_exit() uses mon['best_pnl'] which already merges both via
    # max(best_pnl_during_trade, pnl_pct) computed in monitor_position().
    # Do NOT read pos['best_pnl'] directly here — it may be one render behind.
    best_pnl = mon.get('best_pnl', 0)
    hours_in = mon.get('hours_in', 0)

    # 0. Entry grace period — MON-1 FIX: синхронизировано с config.yaml (было 15)
    grace_min = CFG('monitor', 'entry_grace_minutes', 5)
    if hours_in < grace_min / 60:
        return False, None

    # v38.2: Per-pair TP/SL
    _pair_name = f"{pos.get('coin1', '')}/{pos.get('coin2', '')}"
    _pair_tp = pos.get('pair_tp_pct', None)
    _pair_sl = pos.get('pair_sl_pct', None)
    if _pair_tp is None or _pair_sl is None:
        _pair_tp, _pair_sl = get_pair_tp_sl(_pair_name)

    # 1. Stop Loss — v39 FIX: pair_sl is THE primary SL, always -2.0%
    # No time-dependent tightening (was tightening to -1.5% after 4h)
    sl = float(_pair_sl)  # from per-pair config (default -2.0%)
    if pnl <= sl:
        return True, f"AUTO_SL: P&L={pnl:+.2f}% ≤ {sl}% (pair SL={_pair_sl}%)"

    # 2. Take Profit — v38.2: per-pair TP → activates trailing
    tp = float(_pair_tp)  # from per-pair config (default +2.0%)
    if pnl >= tp:
        # v38.2: TP hit → activate trailing instead of immediate close
        if CFG('monitor', 'trailing_enabled', True):
            pos['_tp_trail_activated'] = True
            pos['_tp_trail_peak'] = max(best_pnl, pnl)
            # Don't close — let trailing handle it (fall through to trailing section)
        else:
            return True, f"AUTO_TP: P&L={pnl:+.2f}% ≥ {tp}% (pair TP)"

    # 3. Z-based exit — v38: DISABLED (false convergence kills profits)
    z_exit_thresh = CFG('monitor', 'auto_exit_z', 0.3)
    z_min_pnl = CFG('monitor', 'auto_exit_z_min_pnl', 0.5)
    z_mode = CFG('monitor', 'auto_exit_z_mode', 'TRAIL')  # v41.2 FIX MON-9: was 'DISABLED'

    if z_mode != 'DISABLED' and abs(z_static) <= z_exit_thresh and pnl >= z_min_pnl:
        if z_mode == 'TRAIL':
            # v34: Don't close — activate trailing stop from current PnL
            # Mark position as trail-activated (handled by caller)
            pos['_z_trail_activated'] = True
            pos['_z_trail_peak'] = max(best_pnl, pnl)
            # Don't return True — let trailing handle it below
        else:
            # Legacy CLOSE mode
            return True, f"AUTO_Z: |Z|={abs(z_static):.2f} ≤ {z_exit_thresh}, P&L={pnl:+.2f}%"

    # 4. Trailing Stop — v38.2: phantom-proven wider trail + TP-trail
    if CFG('monitor', 'trailing_enabled', True):
        # BUG-018 FIX: trail_act / trail_dd рассчитываются ОДИН РАЗ при первой активации
        # и фиксируются в позиции (_trail_act_locked / _trail_dd_locked).
        # До этого параметры пересчитывались на каждом тике — autocalibrate и MAD могли
        # изменить значения после входа в trailing, что делало trailing нестабильным.
        #
        # Логика:
        #   _trail_params_locked == False → trailing ещё не активирован, вычисляем параметры
        #   _trail_params_locked == True  → trailing уже активирован, читаем из позиции
        #
        _trail_locked = pos.get('_trail_params_locked', False)

        if _trail_locked:
            # Параметры зафиксированы при первой активации — используем их без изменений
            trail_act = float(pos.get('_trail_act_locked', CFG('monitor', 'trailing_activate_pct', 1.0)))
            trail_dd  = float(pos.get('_trail_dd_locked',  CFG('monitor', 'trailing_drawdown_pct', 0.7)))
        else:
            # Trailing ещё не активирован — вычисляем параметры как прежде
            trail_act = CFG('monitor', 'trailing_activate_pct', 1.0)   # v38.2: lowered from 1.5
            trail_dd = CFG('monitor', 'trailing_drawdown_pct', 0.7)    # v38.2: lowered from 1.0
            # v38.2: Time-adaptive trailing
            if hours_in < 2.0:
                trail_dd = max(trail_dd, 1.0)  # Wider trail first 2h
            elif hours_in > 7.0:
                trail_dd = min(trail_dd, 0.5)  # Tighter near timeout
        
        # v39: RECOVERY TRAILING — 2h in loss, then +0.5% → activate trailing
        # If position was losing for 2+ hours and recovers to +0.5%, lock in via trailing
        # F-008 FIX: вынесено в конфиг (было захардкожено 0.5 / 0.3)
        _recovery_threshold = CFG('monitor', 'recovery_trail_threshold', 0.5)
        _recovery_dd = CFG('monitor', 'recovery_trail_drawdown', 0.3)
        # BUG-021 FIX: best_pnl passed in from mon{} is the value persisted at the
        # END of the previous render tick.  On the activation tick the current pnl
        # may already exceed _recovery_threshold while best_pnl is still stale.
        # Recompute from max(stored best_pnl_during_trade, current pnl) so the
        # condition uses the true high-water mark for this render cycle.
        _live_best_pnl = max(pos.get('best_pnl_during_trade', best_pnl), best_pnl, pnl)
        if hours_in >= 2.0 and _live_best_pnl < _recovery_threshold and pnl >= _recovery_threshold:
            # Position spent most time in loss, just recovered — activate tight trail
            pos['_recovery_trail_activated'] = True
            pos['_recovery_trail_peak'] = pnl
        
        _recovery_trail = pos.get('_recovery_trail_activated', False)
        if _recovery_trail:
            _rec_peak = pos.get('_recovery_trail_peak', pnl)
            _actual_rec_peak = max(_rec_peak, pnl)
            pos['_recovery_trail_peak'] = _actual_rec_peak
            if pnl > 0 and (_actual_rec_peak - pnl) >= _recovery_dd:
                return True, (f"AUTO_RECOVERY_TRAIL: 2ч+ в убытке → восстановление, "
                             f"peak={_actual_rec_peak:+.2f}%, now={pnl:+.2f}%, "
                             f"drop={_actual_rec_peak-pnl:.2f}%")
        # BUG-018 FIX: correct priority chain for trail params:
        #   1. config defaults  (already set above)
        #   2. time-adaptive    (already applied above)
        #   3. phantom autocalibrate — historical aggregate, sets the baseline
        #   4. MAD              — per-pair volatility, refines the baseline
        # Previously MAD ran first then autocalibrate overwrote it entirely,
        # so MAD corrections were silently lost on every tick.

        # Step 3: Phantom auto-calibration — adjust trail params from history
        try:
            from config_loader import phantom_autocalibrate
            _ph_cal = phantom_autocalibrate()
            if _ph_cal.get('adjusted'):
                trail_act = _ph_cal['trail_activate']
                trail_dd = _ph_cal['trail_drawdown']
        except ImportError:
            pass

        # Step 4: MAD-dependent refinement — widens trail for volatile pairs.
        # Runs AFTER autocalibrate so pair-specific volatility always has
        # the final word (autocalibrate is an aggregate across all pairs).
        try:
            _entry_mad = pos.get('entry_mad', 0)
            if _entry_mad > 0:
                trail_dd  = max(trail_dd,  max(1.0, _entry_mad * 0.7))  # MAD*0.7, min 1.0%
                trail_act = max(trail_act, max(1.5, _entry_mad * 1.0))  # MAD*1.0, min 1.5%
        except Exception:
            pass
        
        # BUG-018 FIX: фиксируем trail_act / trail_dd при первой активации trailing.
        # Проверяем, активировался ли trailing в этом тике (или ранее).
        # Если да — сохраняем параметры в позицию и ставим флаг _trail_params_locked.
        # На следующих тиках блок выше (if _trail_locked) будет читать их из позиции.
        # Флаг активации проверяем после MAD-шага, когда trail_act/trail_dd финальны.
        _any_trail_active = (
            pos.get('_z_trail_activated', False) or
            pos.get('_tp_trail_activated', False) or
            pos.get('_recovery_trail_activated', False)
        )
        if _any_trail_active and not _trail_locked:
            # Первая активация — фиксируем параметры
            pos['_trail_act_locked'] = trail_act
            pos['_trail_dd_locked']  = trail_dd
            pos['_trail_params_locked'] = True

        # v35: Two-phase exit — only activate trail in Phase 2
        _use_two_phase = CFG('monitor', 'two_phase_exit', True)
        _in_phase2 = True  # default: always allow trail
        
        if _use_two_phase:
            try:
                from config_loader import determine_exit_phase
                _phase_info = determine_exit_phase(pos, z_static, pnl)
                pos['exit_phase'] = _phase_info['phase']
                _in_phase2 = (_phase_info['phase'] == 2)
                # Use phase-2 trail params if available
                if _in_phase2 and _phase_info.get('trail_params'):
                    # Phase 2 uses tighter trailing (Z already crossed)
                    trail_act = min(trail_act, _phase_info['trail_params']['activate'])
                    trail_dd = min(trail_dd, _phase_info['trail_params']['drawdown'])
            except ImportError:
                pass
            
            # v41 FIX: Phase 2 fallback — если Z так и не вернулся к 0,
            # но позиция уже в хорошей прибыли или близко к таймауту,
            # принудительно переходим в Phase 2 чтобы trailing защитил прибыль.
            # Без этого SOL/BNB с PnL=+2.37% закрылся по таймауту вместо trailing.
            if not _in_phase2:
                _p2_pnl_fb  = CFG('monitor', 'phase2_pnl_fallback',   1.5)  # %
                _p2_hours_fb = CFG('monitor', 'phase2_hours_fallback', 6.0)  # ч
                _max_h = CFG('strategy', 'max_hold_hours', 8)
                if best_pnl >= _p2_pnl_fb or hours_in >= min(_p2_hours_fb, _max_h * 0.75):
                    _in_phase2 = True
                    pos['exit_phase'] = 2  # обновляем для логов
        
        # v34: Z-trail has lower activation threshold
        z_trail_active = pos.get('_z_trail_activated', False)
        
        if z_trail_active and _in_phase2:
            # Z reached zero → use tighter trail from current level
            z_peak = pos.get('_z_trail_peak', best_pnl)
            actual_peak = max(z_peak, best_pnl)
            if pnl > 0 and (actual_peak - pnl) >= trail_dd:
                return True, (f"AUTO_Z_TRAIL: Z→0 trail, "
                             f"peak={actual_peak:+.2f}%, now={pnl:+.2f}%, "
                             f"drop={actual_peak-pnl:.2f}%")
        
        # Standard trailing (high-water mark) — only in Phase 2
        if _in_phase2 and best_pnl >= trail_act and (best_pnl - pnl) >= trail_dd:
            return True, (f"AUTO_TRAIL: peak={best_pnl:+.2f}%, "
                         f"now={pnl:+.2f}%, drop={best_pnl-pnl:.2f}%")
        
        # v38.2: TP-trail — trailing after TP level hit
        tp_trail_active = pos.get('_tp_trail_activated', False)
        if tp_trail_active and _in_phase2:
            tp_peak = pos.get('_tp_trail_peak', best_pnl)
            actual_peak = max(tp_peak, best_pnl)
            pos['_tp_trail_peak'] = actual_peak  # update peak
            _tp_trail_dd = min(trail_dd, 0.5)  # Tighter trail after TP (protect profits)
            if pnl > 0 and (actual_peak - pnl) >= _tp_trail_dd:
                return True, (f"AUTO_TP_TRAIL: TP hit → trail, "
                             f"peak={actual_peak:+.2f}%, now={pnl:+.2f}%, "
                             f"drop={actual_peak-pnl:.2f}%")
        # F-007 FIX: TP был достигнут, trailing активирован, но Phase 1 блокирует trail.
        # Если PnL начал откатываться от TP — закрыть, не ждать Phase 2.
        elif tp_trail_active and not _in_phase2:
            tp_peak = pos.get('_tp_trail_peak', best_pnl)
            actual_peak = max(tp_peak, best_pnl)
            pos['_tp_trail_peak'] = actual_peak
            _tp_fallback_dd = 0.5  # допустимый откат от TP peak в Phase 1
            if pnl > 0 and (actual_peak - pnl) >= _tp_fallback_dd:
                return True, (f"AUTO_TP_PHASE1: TP hit в Phase 1, "
                             f"peak={actual_peak:+.2f}%, now={pnl:+.2f}%, "
                             f"drop={actual_peak-pnl:.2f}%")

    # 5. P&L emergency stop (must be WIDER than pair_sl — this is a safety net)
    pnl_stop = pos.get('pnl_stop_pct', -5.0)
    # v39 FIX: pnl_stop must not be tighter than pair_sl (prevents -1.2% overriding -2.0%)
    if pnl_stop > float(_pair_sl):
        pnl_stop = float(_pair_sl) - 1.0  # at least 1% wider than pair_sl
    if pnl <= pnl_stop:
        return True, f"AUTO_PNLSTOP: P&L={pnl:+.2f}% ≤ {pnl_stop}%"

    # 6. Timeout — v34: live config read
    # v42: берём таймаут из session_state (настройка в сайдбаре) или из CFG
    try:
        import streamlit as _st_mh
        max_hours = int(_st_mh.session_state.get('max_hold_hours',
                        CFG('strategy', 'max_hold_hours', 8)))
    except Exception:
        max_hours = int(CFG('strategy', 'max_hold_hours', 8))
    if hours_in > max_hours:
        return True, f"AUTO_TIMEOUT: {hours_in:.1f}ч > {max_hours}ч"

    return False, None


def calc_static_spread(p1_arr, p2_arr, entry_hr, entry_intercept=0.0):
    """v23.0: Static spread using FIXED entry HR and intercept.
    This reflects the ACTUAL position held, not dynamic Kalman."""
    return np.array(p1_arr) - entry_hr * np.array(p2_arr) - entry_intercept


def calc_static_zscore(static_spread, halflife_bars=None, min_w=10, max_w=60):
    """v23.0: Z-score of static spread (same windowing as calc_zscore).
    P-005 FIX: vectorized через pandas rolling (было: Python for-loop O(N*W)).
    """
    spread = np.array(static_spread, float)
    n = len(spread)
    if halflife_bars and not np.isinf(halflife_bars) and halflife_bars > 0:
        w = int(np.clip(2.5 * halflife_bars, min_w, max_w))
    else:
        w = 30
    w = min(w, max(10, n // 2))
    zs = np.full(n, np.nan)

    s = pd.Series(spread)
    roll_med = s.rolling(w).median()
    roll_mad = s.rolling(w).apply(lambda x: np.median(np.abs(x - np.median(x))) * 1.4826, raw=True)
    roll_mean = s.rolling(w).mean()
    roll_std = s.rolling(w).std()

    for i in range(w, n):
        mad = roll_mad.iloc[i]
        if mad < 1e-10:
            std_val = roll_std.iloc[i]
            zs[i] = (spread[i] - roll_mean.iloc[i]) / std_val if std_val > 1e-10 else 0
        else:
            zs[i] = (spread[i] - roll_med.iloc[i]) / mad
    return zs, w


# F-012 FIX: recommend_position_size импортируется из config_loader (единый источник)
try:
    from config_loader import recommend_position_size
except ImportError:
    def recommend_position_size(quality_score, confidence, entry_readiness,
                                hurst=0.4, correlation=0.5, base_size=100):
        """Fallback if config_loader unavailable."""
        return base_size * 0.5

# X-002 FIX: kalman_hr заменён на обёртку MRA kalman_hedge_ratio (единый источник).
# Старая версия: упрощённый Kalman без Ridge regularization.
# MRA версия: полный Kalman с Ridge init, Bayesian delta selection.
def kalman_hr(s1, s2, delta=1e-4, ve=1e-3):
    """Wrapper: delegates to MRA kalman_hedge_ratio when available."""
    if _USE_MRA:
        result = kalman_hedge_ratio(s1, s2, delta=delta, ve=ve)
        if result is not None:
            # Normalize keys: MRA returns 'hedge_ratios'/'hr_final', we need 'hrs'/'hr'
            if 'hedge_ratios' in result and 'hrs' not in result:
                result['hrs'] = result['hedge_ratios']
            if 'hr_final' in result and 'hr' not in result:
                result['hr'] = result['hr_final']
            if 'intercept_final' in result and 'intercept' not in result:
                result['intercept'] = result['intercept_final']
            return result
    # Fallback: simplified Kalman (only when MRA unavailable)
    s1, s2 = np.array(s1, float), np.array(s2, float)
    n = min(len(s1), len(s2))
    if n < 10: return None
    s1, s2 = s1[:n], s2[:n]
    init_n = min(30, n // 3)
    try:
        X = np.column_stack([np.ones(init_n), s2[:init_n]])
        beta = np.linalg.lstsq(X, s1[:init_n], rcond=None)[0]
    except: beta = np.array([0.0, 1.0])
    P = np.eye(2); Q = np.eye(2) * delta; R = ve
    hrs, ints, spread = np.zeros(n), np.zeros(n), np.zeros(n)
    for t in range(n):
        x = np.array([1.0, s2[t]]); P += Q
        e = s1[t] - x @ beta; S = x @ P @ x + R
        K = P @ x / S; beta += K * e
        P -= np.outer(K, x) @ P; P = (P + P.T) / 2
        np.fill_diagonal(P, np.maximum(np.diag(P), 1e-10))
        hrs[t], ints[t] = beta[1], beta[0]
        spread[t] = s1[t] - beta[1] * s2[t] - beta[0]
    return {'hrs': hrs, 'intercepts': ints, 'spread': spread,
            'hr': float(hrs[-1]), 'intercept': float(ints[-1])}


def calc_zscore(spread, halflife_bars=None, min_w=10, max_w=60):
    spread = np.array(spread, float); n = len(spread)
    if halflife_bars and not np.isinf(halflife_bars) and halflife_bars > 0:
        w = int(np.clip(2.5 * halflife_bars, min_w, max_w))
    else: w = 30
    w = min(w, max(10, n // 2))
    zs = np.full(n, np.nan)
    for i in range(w, n):
        lb = spread[i - w:i]; med = np.median(lb)
        mad = np.median(np.abs(lb - med)) * 1.4826
        if mad < 1e-10:
            s = np.std(lb)
            zs[i] = (spread[i] - np.mean(lb)) / s if s > 1e-10 else 0
        else: zs[i] = (spread[i] - med) / mad
    return zs, w


def calc_halflife(spread, dt=None):
    """OU halflife через регрессию. dt=1/24 для 1h, 1/6 для 4h, 1 для 1d."""
    s = np.array(spread, float)
    if len(s) < 20: return 999
    sl, sd = s[:-1], np.diff(s)
    n = len(sl)
    sx, sy = np.sum(sl), np.sum(sd)
    sxy, sx2 = np.sum(sl * sd), np.sum(sl**2)
    denom = n * sx2 - sx**2
    if abs(denom) < 1e-10: return 999
    b = (n * sxy - sx * sy) / denom
    if dt is None: dt = 1.0
    theta = max(0.001, min(10.0, -b / dt))
    hl = np.log(2) / theta  # в единицах dt
    return float(hl) if hl < 999 else 999


# F-011 FIX: calc_hurst УДАЛЁН — дубликат MRA calculate_hurst_exponent.
# MRA версия работает на diff(series) (корректно для DFA), эта — на исходных данных (неверно).
# calculate_hurst_exponent уже импортирован выше через _USE_MRA.


def calc_correlation(p1, p2, window=60):
    """Rolling корреляция."""
    n = min(len(p1), len(p2))
    if n < window: return 0.0
    r1 = np.diff(np.log(p1[-n:] + 1e-10))
    r2 = np.diff(np.log(p2[-n:] + 1e-10))
    if len(r1) < 10: return 0.0
    return float(np.corrcoef(r1[-window:], r2[-window:])[0, 1])


def calc_cointegration_pvalue(p1, p2):
    """P-value коинтеграции."""
    try:
        _, pval, _ = coint(p1, p2)
        return float(pval)
    except:
        return 1.0


# ═══════════════════════════════════════════════════════
# POSITIONS FILE (JSON persistence)
# P-001 FIX: in-memory cache с mtime-проверкой.
# Было: 17 вызовов load_positions() за рендер, каждый читает 347KB с диска.
# Стало: один read при изменении файла, O(1) для повторных вызовов.
# save_positions() обновляет кэш напрямую → следующий load без I/O.
# E-006 FIX: SQLite как primary store, JSON как fallback.
# ═══════════════════════════════════════════════════════
POSITIONS_FILE = os.path.join(_BASE_DIR, "positions.json")

# E-006 FIX: SQLite primary store
try:
    from db_store import (
        db_load_positions, db_save_positions, db_update_position,
        db_get_open_positions, db_get_next_id, ensure_db,
    )
    _USE_SQLITE = True
    ensure_db()
except ImportError:
    _USE_SQLITE = False

_positions_cache: list | None = None
_positions_mtime: float = 0.0

def load_positions():
    """E-006 FIX: SQLite primary, JSON fallback. Кэш в памяти."""
    global _positions_cache, _positions_mtime

    # E-006: SQLite path
    if _USE_SQLITE:
        try:
            # In-memory cache: перечитывать из SQLite только если save был после последнего load
            if _positions_cache is not None and _positions_mtime > 0:
                return list(_positions_cache)
            result = db_load_positions()
            _positions_cache = result
            _positions_mtime = 1.0  # sentinel: loaded
            return list(result)
        except Exception:
            pass  # fallback to JSON

    # JSON fallback (original logic with mtime cache)
    try:
        if _positions_cache is not None and os.path.exists(POSITIONS_FILE):
            current_mtime = os.path.getmtime(POSITIONS_FILE)
            if current_mtime == _positions_mtime:
                return list(_positions_cache)
    except Exception:
        pass
    result = _load_positions_from_disk()
    try:
        if os.path.exists(POSITIONS_FILE):
            _positions_mtime = os.path.getmtime(POSITIONS_FILE)
    except Exception:
        pass
    _positions_cache = result
    return list(result)


def _load_positions_from_disk():
    """Внутренний метод: чтение positions.json с диска (без кэша)."""
    for fpath in [POSITIONS_FILE, POSITIONS_FILE + ".bak", POSITIONS_FILE + ".new"]:
        if not os.path.exists(fpath):
            continue
        try:
            size = os.path.getsize(fpath)
            if size < 3:
                continue
            with open(fpath, 'r', encoding='utf-8') as f:
                raw = f.read()
            if raw.startswith('\x00') or raw[:10].count('\x00') > 3:
                continue
            data = json.loads(raw)
            if isinstance(data, list):
                result = [p for p in data if isinstance(p, dict)]
                if result or fpath == POSITIONS_FILE:
                    return result
            if isinstance(data, dict):
                return [data] if data else []
        except Exception:
            continue
    return []


def invalidate_positions_cache():
    """Принудительный сброс кэша."""
    global _positions_cache, _positions_mtime
    _positions_cache = None
    _positions_mtime = 0.0


def save_positions(positions):
    """E-006 FIX: SQLite primary write + JSON backup + cache update."""
    global _positions_cache, _positions_mtime

    # E-006: SQLite primary
    if _USE_SQLITE:
        try:
            db_save_positions(positions)
            _positions_cache = list(positions)
            _positions_mtime = 0.0  # invalidate → next load re-reads from SQLite
            # Also write JSON as backup (non-blocking)
            try:
                _save_positions_json(positions)
            except Exception:
                pass
            return
        except Exception:
            pass  # fallback to JSON-only

    _save_positions_json(positions)
    _positions_cache = list(positions)
    try:
        if os.path.exists(POSITIONS_FILE):
            _positions_mtime = os.path.getmtime(POSITIONS_FILE)
    except Exception:
        pass


def _save_positions_json(positions):
    """JSON backup write (atomic)."""
    import shutil
    new_path = POSITIONS_FILE + ".new"
    bak_path = POSITIONS_FILE + ".bak"
    try:
        with open(new_path, 'w', encoding='utf-8') as f:
            json.dump(positions, f, indent=2, ensure_ascii=False, default=str)
            f.flush()
            os.fsync(f.fileno())
        if os.path.exists(POSITIONS_FILE) and os.path.getsize(POSITIONS_FILE) > 10:
            try:
                shutil.copy2(POSITIONS_FILE, bak_path)
            except Exception:
                pass
        os.replace(new_path, POSITIONS_FILE)
    except Exception:
        try:
            with open(POSITIONS_FILE, 'w', encoding='utf-8') as f:
                json.dump(positions, f, indent=2, ensure_ascii=False, default=str)
        except Exception:
            pass
    finally:
        try:
            if os.path.exists(new_path):
                os.remove(new_path)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════
# v38.4 DIAG: Persistent entry-refusal log
# Определяется ДО add_position, чтобы быть доступным везде
# ═══════════════════════════════════════════════════════
# F-01 FIX: JSONL append-only (was: full JSON read+write on every call)
DIAG_LOG_FILE = os.path.join(_BASE_DIR, "entry_diag_log.jsonl")
_DIAG_MAX_SIZE_MB = 2

def _diag_load():
    """Load diag entries (only for UI display, not on every write)."""
    if not os.path.exists(DIAG_LOG_FILE):
        # Migrate old JSON format
        _old = os.path.join(_BASE_DIR, "entry_diag_log.json")
        if os.path.exists(_old):
            try:
                with open(_old, 'r', encoding='utf-8') as _f:
                    return json.load(_f)
            except Exception:
                pass
        return []
    try:
        records = []
        with open(DIAG_LOG_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except Exception:
                        pass
        return records[-200:]
    except Exception:
        return []

def _diag_append(entry):
    """F-01 FIX: append-only write (O(1) instead of O(N))."""
    try:
        if os.path.exists(DIAG_LOG_FILE):
            size_mb = os.path.getsize(DIAG_LOG_FILE) / 1024 / 1024
            if size_mb > _DIAG_MAX_SIZE_MB:
                # Simple rotation: keep last half of file
                try:
                    with open(DIAG_LOG_FILE, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                    with open(DIAG_LOG_FILE, 'w', encoding='utf-8') as f:
                        f.writelines(lines[len(lines)//2:])
                except Exception:
                    pass
        with open(DIAG_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + '\n')
    except Exception:
        pass

def diag_log_refusal(pair_name, direction, reason, source="auto", details=None):
    """Log an entry refusal to persistent diag log."""
    _diag_append({
        'ts': now_msk().isoformat(),
        'pair': pair_name,
        'direction': direction,
        'reason': reason,
        'source': source,
        'details': details or {},
    })

def diag_log_attempt(pair_name, direction, notes, source="auto"):
    """Log a successful open attempt."""
    _diag_append({
        'ts': now_msk().isoformat(),
        'pair': pair_name,
        'direction': direction,
        'reason': '✅ ОТКРЫТА',
        'source': source,
        'details': {'notes': str(notes)[:120]},
    })


def add_position(coin1, coin2, direction, entry_z, entry_hr, 
                 entry_price1, entry_price2, timeframe, notes="",
                 max_hold_hours=None, pnl_stop_pct=None,
                 entry_intercept=0.0, recommended_size=100.0,
                 z_window=None, bt_verdict=None, bt_pnl=None,
                 mu_bt_wr=None, v_quality=None,
                 auto_opened=False):
    positions = load_positions()
    open_positions = [p for p in positions if isinstance(p, dict) and p.get('status') == 'OPEN']

    _notes_str = str(notes or '')
    
    # v38.2: Parse entry label flags (WL, NK, BT) from notes
    _flags = parse_entry_flags(_notes_str)
    _is_green = '🟢' in _flags['base_label'] or 'ВХОД' in _flags['base_label'].upper()

    # v38.2: ENTRY FILTER SYSTEM — user-configurable checkboxes
    _filters = {}
    for _fk in ['block_green', 'block_green_bt_fail', 'block_green_bt_warn',
                 'block_yellow', 'block_yellow_bt_fail', 'block_yellow_bt_warn',
                 'block_wl_warn', 'block_wl_fail', 'block_nk_warn', 'block_nk_fail',
                 'block_long', 'block_short']:
        _filters[_fk] = st.session_state.get(_fk, False)
    
    filter_blocked, filter_reason = check_entry_filters(_notes_str, direction, _filters)
    if filter_blocked:
        st.warning(f"🚫 {filter_reason}")
        st.session_state['_last_add_pos_err'] = filter_reason
        diag_log_refusal(f"{coin1}/{coin2}", direction, filter_reason,
                         source=("auto" if auto_opened else "manual"),
                         details={'entry_label': _notes_str[:120], 'check': 'entry_filter'})
        return None

    # v38 Option D: Quality gate (kept — not a checkbox, always active)
    _min_q = CFG('scanner', 'min_quality', 50)
    _pos_q = 0
    try:
        import re as _re
        _qm = _re.search(r'Q=(\d+)', _notes_str)
        if _qm:
            _pos_q = int(_qm.group(1))
    except Exception:
        pass
    if _pos_q > 0 and _pos_q < _min_q:
        _qr = f"Quality gate: Q={_pos_q} < {_min_q}"
        st.warning(f"🚫 {_qr}")
        st.session_state['_last_add_pos_err'] = _qr
        diag_log_refusal(f"{coin1}/{coin2}", direction, _qr,
                         source=("auto" if auto_opened else "manual"),
                         details={'quality_score': _pos_q, 'min_quality': _min_q})
        return None

    # v38.2: DEEP_RALLY check (manual toggle)
    if st.session_state.get('deep_rally_block', False):
        try:
            _rally_file = os.path.join(_BASE_DIR, "rally_state.json")
            if os.path.exists(_rally_file):
                with open(_rally_file, 'r', encoding='utf-8') as _rf:
                    _rally = json.load(_rf)
                if _rally.get('status') in ('DEEP_RALLY', 'RALLY'):
                    # v41 FIX: RALLY блокирует только LONG (рынок растёт = SHORT безопасен)
                    # Раньше блокировал оба направления — SHORT ошибочно отклонялся
                    if direction == 'LONG' and not _is_green:
                        _rr = f"DEEP_RALLY (BTC Z={_rally.get('btc_z', '?')})"
                        st.warning(f"🚫 {_rr}. Блок LONG. SHORT разрешён. 🟢 ВХОД bypass.")
                        st.session_state['_last_add_pos_err'] = _rr
                        diag_log_refusal(f"{coin1}/{coin2}", direction, _rr,
                                         source=("auto" if auto_opened else "manual"),
                                         details={'rally_status': _rally.get('status')})
                        return None
        except Exception:
            pass

    # PERF-1 FIX: загружаем cooldowns ОДИН раз и передаём во все проверки.
    # Раньше: _load_cooldowns() × 3 (daily_limit + cascade + anti-repeat) = 3 disk reads.
    # Теперь: 1 disk read, данные передаются как параметр.
    _cd_data = _load_cooldowns()

    # v32: Check daily loss limit
    # B-03 FIX: передаём PnL из загруженных open_positions (вместо повторного чтения с диска)
    _live_pnls = [p.get('pnl_pct', 0) for p in open_positions if p.get('pnl_pct', 0) != 0]
    daily_blocked, daily_reason = check_daily_loss_limit(cd_data=_cd_data, live_open_pnls=_live_pnls)
    if daily_blocked:
        st.error(daily_reason)
        st.session_state['_last_add_pos_err'] = daily_reason
        diag_log_refusal(f"{coin1}/{coin2}", direction, daily_reason,
                         source=("auto" if auto_opened else "manual"),
                         details={'check': 'daily_loss_limit'})
        return None

    # BUG-N14 FIX: Cascade SL protection
    cascade_blocked, cascade_reason = check_cascade_sl(cd_data=_cd_data)
    if cascade_blocked:
        st.error(cascade_reason)
        st.session_state['_last_add_pos_err'] = cascade_reason
        diag_log_refusal(f"{coin1}/{coin2}", direction, cascade_reason,
                         source=("auto" if auto_opened else "manual"),
                         details={'check': 'cascade_sl'})
        return None

    # v32: Check max positions limit
    max_pos = CFG('monitor', 'max_positions', 10)
    if len(open_positions) >= max_pos:
        _mpr = f"Лимит позиций: {len(open_positions)}/{max_pos}"
        st.error(f"🚫 {_mpr}")
        st.session_state['_last_add_pos_err'] = _mpr
        diag_log_refusal(f"{coin1}/{coin2}", direction, _mpr,
                         source=("auto" if auto_opened else "manual"),
                         details={'open': len(open_positions), 'max': max_pos})
        return None

    pair_name = f"{coin1}/{coin2}"
    pair_name_rev = f"{coin2}/{coin1}"

    # v38.1: Anti-repeat — delegates to core.risk.check_anti_repeat
    _today_str = now_msk().strftime('%Y-%m-%d')
    try:
        from pairs_scanner.core.risk import check_anti_repeat as _core_ar
        for _check_pn in [pair_name, pair_name_rev]:
            _ar_blocked, _ar = _core_ar(_check_pn, direction, _cd_data,
                                         is_green=_is_green, today_str=_today_str)
            if _ar_blocked:
                st.warning(f"🚫 {_ar}")
                st.session_state['_last_add_pos_err'] = _ar
                diag_log_refusal(pair_name, direction, _ar,
                                 source=("auto" if auto_opened else "manual"),
                                 details={'check_pair': _check_pn, 'is_green': _is_green})
                return None
    except ImportError:
        for _check_pn in [pair_name, pair_name_rev]:
            _cd_entry = _cd_data.get(_check_pn, {})
            if (_cd_entry.get('date') == _today_str and
                _cd_entry.get('sl_exit', False) and
                _cd_entry.get('last_dir') == direction):
                if not _is_green:
                    _ar = f"Anti-repeat: SL в {direction} по {_check_pn} сегодня (bypass: 🟢 ВХОД)"
                    st.warning(f"🚫 {_ar}")
                    st.session_state['_last_add_pos_err'] = _ar
                    diag_log_refusal(pair_name, direction, _ar,
                                     source=("auto" if auto_opened else "manual"),
                                     details={'check_pair': _check_pn, 'is_green': _is_green})
                    return None

    # v34: Pair memory blocking — 0 wins after 2+ trades
    try:
        from config_loader import pair_memory_is_blocked
        mem_blocked, mem_reason = pair_memory_is_blocked(pair_name)
        if not mem_blocked:
            mem_blocked, mem_reason = pair_memory_is_blocked(pair_name_rev)
        if mem_blocked:
            st.warning(mem_reason)
            st.session_state['_last_add_pos_err'] = mem_reason
            diag_log_refusal(pair_name, direction, mem_reason,
                             source=("auto" if auto_opened else "manual"),
                             details={'check': 'pair_memory'})
            return None
    except ImportError:
        pass

    # v34: Max coin positions — prevent concentration
    max_coin_pos = CFG('monitor', 'max_coin_positions', 2)
    try:
        from pairs_scanner.core.risk import check_coin_position_limit as _core_cpl
        for coin in [coin1, coin2]:
            _coin_blocked, _cpr = _core_cpl(coin, open_positions, max_coin_pos)
            if _coin_blocked:
                st.error(f"🚫 {_cpr}")
                st.session_state['_last_add_pos_err'] = _cpr
                diag_log_refusal(pair_name, direction, _cpr,
                                 source=("auto" if auto_opened else "manual"),
                                 details={'coin': coin, 'max': max_coin_pos})
                return None
    except ImportError:
        for coin in [coin1, coin2]:
            coin_count = sum(1 for p in open_positions
                            if coin in (p.get('coin1', ''), p.get('coin2', '')))
            if coin_count >= max_coin_pos:
                _cpr = f"{coin} уже в {coin_count} позициях (лимит {max_coin_pos})"
                st.error(f"🚫 {_cpr}")
                st.session_state['_last_add_pos_err'] = _cpr
                diag_log_refusal(pair_name, direction, _cpr,
                                 source=("auto" if auto_opened else "manual"),
                                 details={'coin': coin, 'count': coin_count, 'max': max_coin_pos})
                return None

    # v38.2: Cooldown — 🟢 ВХОД bypasses cooldown
    _lbl_for_cd = _notes_str if _is_green else ""
    cd_blocked, cd_reason = check_pair_cooldown(pair_name, _lbl_for_cd, cd_data=_cd_data)
    if not cd_blocked:
        cd_blocked, cd_reason = check_pair_cooldown(pair_name_rev, _lbl_for_cd, cd_data=_cd_data)
    if cd_blocked:
        st.warning(cd_reason)
        st.session_state['_last_add_pos_err'] = cd_reason
        diag_log_refusal(pair_name, direction, cd_reason,
                         source=("auto" if auto_opened else "manual"),
                         details={'is_green': _is_green, 'check': 'cooldown'})
        return None

    # v32: Check coin losses (warning only, not blocking)
    for coin in [coin1, coin2]:
        coin_warn, coin_msg = check_coin_losses(coin)
        if coin_warn:
            st.warning(coin_msg)

    # v32: AUTO-FLIP — close conflicting positions
    # v33: COIN CONFLICT HARD BLOCK — prevent hedging same coin in different pairs
    if CFG('monitor', 'auto_flip_enabled', True):
        coins_new = {coin1, coin2}
        flipped = []
        for pos in open_positions:
            coins_existing = {pos['coin1'], pos['coin2']}
            overlap = coins_new & coins_existing
            # Same pair, opposite direction → flip
            if ({pos['coin1'], pos['coin2']} == {coin1, coin2}) and pos['direction'] != direction:
                try:
                    ep1 = entry_price1
                    ep2 = entry_price2
                    close_position(pos['id'], ep1, ep2, entry_z,
                                   f"AUTO_FLIP: {direction} сигнал")
                    flipped.append(f"#{pos['id']} {pos['coin1']}/{pos['coin2']} {pos['direction']}")
                except Exception as ex:
                    st.warning(f"⚠️ Не удалось закрыть #{pos['id']} для flip: {ex}")
            # v33: COIN-LEVEL CONFLICT HARD BLOCK
            elif overlap:
                for shared_coin in overlap:
                    if pos['direction'] == 'LONG':
                        existing_long_coin = pos['coin1']
                    else:
                        existing_long_coin = pos['coin2']
                    if direction == 'LONG':
                        new_long_coin = coin1
                    else:
                        new_long_coin = coin2
                    existing_is_long = (shared_coin == existing_long_coin)
                    new_is_long = (shared_coin == new_long_coin)
                    if existing_is_long != new_is_long:
                        _cfr = (f"CONFLICT BLOCK: {shared_coin} "
                                f"{'LONG' if existing_is_long else 'SHORT'} в "
                                f"#{pos['id']} {pos['coin1']}/{pos['coin2']} {pos['direction']} + "
                                f"{'LONG' if new_is_long else 'SHORT'} в новой "
                                f"{pair_name} {direction} → хедж самого себя!")
                        st.error(f"🚫 {_cfr}")
                        st.session_state['_last_add_pos_err'] = _cfr
                        diag_log_refusal(pair_name, direction, _cfr,
                                         source=("auto" if auto_opened else "manual"),
                                         details={'shared_coin': shared_coin, 'existing_pos': pos.get('id')})
                        return None
        if flipped:
            st.info(f"🔄 AUTO-FLIP: закрыты {', '.join(flipped)} → новый {direction} {pair_name}")
            positions = load_positions()

    # v33: defaults from unified config
    if max_hold_hours is None:
        max_hold_hours = CFG('strategy', 'max_hold_hours', 12)
    
    # v38.2: Per-pair TP/SL
    _pair_tp, _pair_sl = get_pair_tp_sl(pair_name)
    # v39 FIX: pnl_stop_pct is EMERGENCY stop (wider than pair_sl)
    # pair_sl is the PRIMARY SL checked in check_auto_exit()
    # pnl_stop_pct should NOT compete — set to wide fallback
    if pnl_stop_pct is None:
        pnl_stop_pct = CFG('monitor', 'pnl_stop_pct', -5.0)
    
    # v5.0: Adaptive stop_z
    _stop_offset = CFG('strategy', 'stop_z_offset', 2.0)
    _min_stop = CFG('strategy', 'min_stop_z', 4.0)
    adaptive_stop = max(abs(entry_z) + _stop_offset, _min_stop)
    
    pos = {
        # BUG-007 FIX: len(positions)+1 causes ID collisions after manual
        # deletion because gaps in the sequence get reused. Use max existing
        # ID instead — always strictly greater than any current position.
        'id': max((p.get('id', 0) for p in positions), default=0) + 1,
        'coin1': coin1, 'coin2': coin2,
        'direction': direction,
        'entry_z': entry_z,
        'entry_hr': entry_hr,
        'entry_intercept': entry_intercept,
        'entry_price1': entry_price1,
        'entry_price2': entry_price2,
        'entry_time': now_msk().isoformat(),
        'timeframe': timeframe,
        'status': 'OPEN',
        'notes': notes,
        'exit_z_target': CFG('monitor', 'exit_z_target', 0.5),
        'stop_z': adaptive_stop,
        'max_hold_hours': max_hold_hours,
        'pnl_stop_pct': pnl_stop_pct,
        'recommended_size': recommended_size,
        'best_pnl_during_trade': 0.0,
        'z_window': z_window,
        # v34: BT metrics from scanner
        'bt_verdict': bt_verdict,
        'bt_pnl': bt_pnl,
        'mu_bt_wr': mu_bt_wr,
        'v_quality': v_quality,
        # v34: trailing state
        '_z_trail_activated': False,
        '_z_trail_peak': 0.0,
        # v39: recovery trail state
        '_recovery_trail_activated': False,
        '_recovery_trail_peak': 0.0,
        # BUG-018 FIX: locked trail params (set once at first trailing activation)
        '_trail_params_locked': False,
        '_trail_act_locked': 0.0,
        '_trail_dd_locked': 0.0,
        # v35: monitor messages
        'monitor_messages': [],
        'exit_phase': 1,
        # v38.2: Separate flags for history
        'entry_label_clean': _flags['base_label'],
        'flag_bt': _flags['bt_flag'],
        'flag_wl': _flags['wl_flag'],
        'flag_nk': _flags['nk_flag'],
        # v38.2: Per-pair TP/SL
        'pair_tp_pct': _pair_tp,
        'pair_sl_pct': _pair_sl,
        # v41 FIX: auto_opened flag — True=автоматически открыто сканером, False=вручную
        'auto_opened': bool(auto_opened),
    }
    positions.append(pos)
    save_positions(positions)
    return pos


def close_position(pos_id, exit_price1, exit_price2, exit_z, reason,
                   exit_z_static=None):
    positions = load_positions()
    closed_pos = None
    for p in positions:
        if not isinstance(p, dict):
            continue
        if p.get('id') == pos_id and p.get('status') == 'OPEN':
            p['status'] = 'CLOSED'
            p['exit_price1'] = exit_price1
            p['exit_price2'] = exit_price2
            p['exit_z'] = exit_z
            p['exit_z_static'] = exit_z_static  # v23.0
            p['exit_time'] = now_msk().isoformat()
            p['exit_reason'] = reason
            # P&L
            r1 = (exit_price1 - p['entry_price1']) / p['entry_price1']
            r2 = (exit_price2 - p['entry_price2']) / p['entry_price2']
            hr = p['entry_hr']
            if p['direction'] == 'LONG':
                raw = r1 - hr * r2
            else:
                raw = -r1 + hr * r2
            pnl_gross = raw / (1 + abs(hr)) * 100
            # v23.0: Commission deduction
            p['pnl_gross_pct'] = round(pnl_gross, 3)
            p['pnl_pct'] = round(pnl_gross - COMMISSION_ROUND_TRIP_PCT(), 3)
            # v33: Phantom tracking — configurable hours (was hardcoded 24h)
            _phantom_hours = CFG('monitor', 'phantom_track_hours', 12)
            p['phantom_track_until'] = (now_msk() + timedelta(hours=_phantom_hours)).isoformat()
            p['phantom_max_pnl'] = p['pnl_pct']
            p['phantom_min_pnl'] = p['pnl_pct']
            p['phantom_last_pnl'] = p['pnl_pct']
            p['phantom_last_check'] = now_msk().isoformat()
            closed_pos = p.copy()
            break
    save_positions(positions)
    
    # v25: R8 Performance Tracker — save to persistent history
    if closed_pos:
        try:
            save_trade_to_history(closed_pos)
        except Exception:
            pass
        # v32: Record trade for cooldown tracking
        try:
            _pair = f"{closed_pos['coin1']}/{closed_pos['coin2']}"
            record_trade_for_cooldown(
                _pair, closed_pos.get('pnl_pct', 0),
                closed_pos.get('direction', ''),
                closed_pos.get('exit_reason', ''))  # v34: pass reason for SL detection
        except Exception:
            pass
        # v27: Update pair memory
        try:
            from config_loader import pair_memory_update
            _pair = f"{closed_pos['coin1']}/{closed_pos['coin2']}"
            _entry_dt = closed_pos.get('entry_time', '')
            _exit_dt = closed_pos.get('exit_time', '')
            try:
                from datetime import datetime
                _et = datetime.fromisoformat(str(_entry_dt))
                _xt = datetime.fromisoformat(str(_exit_dt))
                _hold_h = (_xt - _et).total_seconds() / 3600
            except Exception:
                _hold_h = 0
            pair_memory_update(
                _pair, closed_pos.get('pnl_pct', 0), _hold_h,
                closed_pos.get('direction', ''), 
                closed_pos.get('entry_z', 0),
                closed_pos.get('exit_z', 0)
            )
        except Exception:
            pass


def save_trade_to_history(trade):
    """R8: Save closed trade to persistent CSV history."""
    import csv
    history_file = os.path.join(_BASE_DIR, "trade_history.csv")
    fields = [
        # BUG-022 FIX: 'pair' removed from passthrough list — it is always
        # reconstructed from coin1/coin2 in the row dict below, so listing
        # it here caused a duplicate key written to the CSV on some code paths.
        'id', 'coin1', 'coin2', 'direction', 'timeframe',
        'entry_z', 'exit_z', 'exit_z_static', 'entry_hr', 'pnl_pct', 'pnl_gross_pct',
        'entry_time', 'exit_time', 'exit_reason',
        'entry_price1', 'entry_price2', 'exit_price1', 'exit_price2',
        'notes', 'best_pnl', 'best_pnl_during_trade', 'recommended_size',
        'phantom_max_pnl', 'phantom_min_pnl',
        'signal_type', 'entry_label', 'auto_opened',
        'monitor_messages',
        # v38.2: Separate flag columns
        'entry_label_clean', 'flag_bt', 'flag_wl', 'flag_nk',
        'pair_tp_pct', 'pair_sl_pct',
    ]
    
    row = {
        'id': trade.get('id', 0),
        'pair': f"{trade.get('coin1', '')}/{trade.get('coin2', '')}",
        'coin1': trade.get('coin1', ''),
        'coin2': trade.get('coin2', ''),
        'direction': trade.get('direction', ''),
        'timeframe': trade.get('timeframe', '4h'),
        'entry_z': trade.get('entry_z', 0),
        'exit_z': trade.get('exit_z', 0),
        'exit_z_static': trade.get('exit_z_static', 0),
        'entry_hr': trade.get('entry_hr', 0),
        'pnl_pct': trade.get('pnl_pct', 0),
        'pnl_gross_pct': trade.get('pnl_gross_pct', 0),
        'entry_time': trade.get('entry_time', ''),
        'exit_time': trade.get('exit_time', ''),
        'exit_reason': trade.get('exit_reason', ''),
        'entry_price1': trade.get('entry_price1', 0),
        'entry_price2': trade.get('entry_price2', 0),
        'exit_price1': trade.get('exit_price1', 0),
        'exit_price2': trade.get('exit_price2', 0),
        'notes': trade.get('notes', ''),
        'best_pnl': trade.get('best_pnl', 0),
        'best_pnl_during_trade': trade.get('best_pnl_during_trade', trade.get('best_pnl', 0)),
        'recommended_size': trade.get('recommended_size', 100),
        'phantom_max_pnl': trade.get('phantom_max_pnl'),
        'phantom_min_pnl': trade.get('phantom_min_pnl'),
        'signal_type': trade.get('signal_type', ''),
        'entry_label': trade.get('entry_label', ''),
        'auto_opened': trade.get('auto_opened', False),
        # v35: monitor messages
        'monitor_messages': '; '.join(trade.get('monitor_messages', [])),
        # v38.2: Separate flags
        'entry_label_clean': trade.get('entry_label_clean', ''),
        'flag_bt': trade.get('flag_bt', ''),
        'flag_wl': trade.get('flag_wl', ''),
        'flag_nk': trade.get('flag_nk', ''),
        'pair_tp_pct': trade.get('pair_tp_pct', CFG('monitor', 'auto_tp_pct', 2.0)),
        'pair_sl_pct': trade.get('pair_sl_pct', CFG('monitor', 'auto_sl_pct', -2.5)),
    }
    
    file_exists = os.path.exists(history_file)
    with open(history_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def load_trade_history():
    """R8: Load all trade history."""
    import csv
    history_file = os.path.join(_BASE_DIR, "trade_history.csv")
    if not os.path.exists(history_file):
        return []
    
    with open(history_file, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        trades = []
        for row in reader:
            # Convert numeric fields
            for k in ['entry_z', 'exit_z', 'entry_hr', 'pnl_pct', 
                       'entry_price1', 'entry_price2', 'exit_price1', 'exit_price2', 'best_pnl']:
                try:
                    row[k] = float(row.get(k, 0) or 0)
                except (ValueError, TypeError):
                    row[k] = 0
            try:
                row['id'] = int(row.get('id', 0) or 0)
            except:
                row['id'] = 0
            # v35.1 FIX: Convert monitor_messages from CSV string back to list
            _mm = row.get('monitor_messages', '')
            if isinstance(_mm, str) and _mm:
                row['monitor_messages'] = [m.strip() for m in _mm.split(';') if m.strip()]
            elif not isinstance(_mm, list):
                row['monitor_messages'] = []
            trades.append(row)
    return trades


# ═══════════════════════════════════════════════════════
# DATA FETCHING
# ═══════════════════════════════════════════════════════

# v4.0: Exchange fallback chain (Binance/Bybit block cloud servers)
EXCHANGE_FALLBACK = ['okx', 'kucoin', 'bybit', 'binance']

def _get_exchange(exchange_name):
    """Получить рабочую биржу с fallback и кешированием.
    v40: Кеш exchange объекта — load_markets() вызывается ОДИН раз."""
    # Cache key in session_state or module-level
    cache_key = f'_exchange_cache_{exchange_name}'
    if hasattr(_get_exchange, '_cache') and exchange_name in _get_exchange._cache:
        _cached = _get_exchange._cache[exchange_name]
        # Validate cache is still alive (check every 5 min)
        if time.time() - _cached['ts'] < 300:
            return _cached['ex'], _cached['name']
    
    if not hasattr(_get_exchange, '_cache'):
        _get_exchange._cache = {}
    
    tried = set()
    chain = [exchange_name] + [e for e in EXCHANGE_FALLBACK if e != exchange_name]
    for exch in chain:
        if exch in tried: continue
        tried.add(exch)
        try:
            ex = getattr(ccxt, exch)({'enableRateLimit': True})
            ex.load_markets()
            _get_exchange._cache[exchange_name] = {'ex': ex, 'name': exch, 'ts': time.time()}
            return ex, exch
        except:
            continue
    return None, None


# BUG-N16 FIX: TTL кеша fetch_prices разделён по таймфрейму вместо единого 120с.
# Мотивация: monitor_position вызывается каждые 60с. При TTL=120с цена могла быть
# двухминутной давности — критично для 1h таймфрейма (бар = 3600с, но движение за
# 2 мин может быть значимым). Для 1d TTL=600с безопасен.
#
# Таблица TTL по таймфрейму:
#   1h  →  60с  (1 мин — обновляем каждый цикл монитора)
#   4h  → 180с  (3 мин — баланс между свежестью и нагрузкой на API)
#   1d  → 600с  (10 мин — дневные данные не меняются быстро)
#   иное→ 120с  (прежний дефолт как безопасный fallback)
#
# Паттерн: основная логика в _fetch_prices_impl(), три кешированные обёртки
# с разными TTL, роутер fetch_prices() выбирает нужную по timeframe.

@st.cache_data(ttl=60)
def _fetch_prices_1h(exchange_name, coin, lookback_bars):
    return _fetch_prices_impl(exchange_name, coin, '1h', lookback_bars)

@st.cache_data(ttl=180)
def _fetch_prices_4h(exchange_name, coin, lookback_bars):
    return _fetch_prices_impl(exchange_name, coin, '4h', lookback_bars)

@st.cache_data(ttl=600)
def _fetch_prices_1d(exchange_name, coin, lookback_bars):
    return _fetch_prices_impl(exchange_name, coin, '1d', lookback_bars)

@st.cache_data(ttl=120)
def _fetch_prices_default(exchange_name, coin, timeframe, lookback_bars):
    return _fetch_prices_impl(exchange_name, coin, timeframe, lookback_bars)

def _fetch_prices_impl(exchange_name, coin, timeframe, lookback_bars=300):
    """v27: Fetch with retry + futures first. BUG-N16: вызывается через TTL-обёртки."""
    import ccxt as _ccxt
    # Try futures first, then spot
    symbols = [f"{coin}/USDT:USDT", f"{coin}/USDT"]
    for symbol in symbols:
        for _attempt in range(3):
            try:
                ex, actual = _get_exchange(exchange_name)
                if ex is None: return None
                ohlcv = ex.fetch_ohlcv(symbol, timeframe, limit=lookback_bars)
                df = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
                df['ts'] = pd.to_datetime(df['ts'], unit='ms')
                return df
            except (_ccxt.NetworkError, _ccxt.RequestTimeout, _ccxt.ExchangeNotAvailable):
                time.sleep([2, 5, 15][_attempt])
            except:
                break  # try next symbol
    return None

def fetch_prices(exchange_name, coin, timeframe, lookback_bars=300):
    """BUG-N16 FIX: роутер с TTL по таймфрейму (1h=60с, 4h=180с, 1d=600с, иное=120с)."""
    if timeframe == '1h':
        return _fetch_prices_1h(exchange_name, coin, lookback_bars)
    elif timeframe == '4h':
        return _fetch_prices_4h(exchange_name, coin, lookback_bars)
    elif timeframe == '1d':
        return _fetch_prices_1d(exchange_name, coin, lookback_bars)
    else:
        return _fetch_prices_default(exchange_name, coin, timeframe, lookback_bars)


def get_current_price(exchange_name, coin):
    """v27: Get price with retry + futures."""
    import ccxt as _ccxt
    symbols = [f"{coin}/USDT:USDT", f"{coin}/USDT"]
    for symbol in symbols:
        for _attempt in range(3):
            try:
                ex, actual = _get_exchange(exchange_name)
                if ex is None: return None
                ticker = ex.fetch_ticker(symbol)
                return ticker['last']
            except (_ccxt.NetworkError, _ccxt.RequestTimeout, _ccxt.ExchangeNotAvailable):
                time.sleep([2, 5, 15][_attempt])
            except:
                break
    return None


# ═══════════════════════════════════════════════════════
# MONITOR LOGIC
# ═══════════════════════════════════════════════════════

def monitor_position(pos, exchange_name):
    """Полный мониторинг одной позиции v3.0 — с quality metrics."""
    c1, c2 = pos['coin1'], pos['coin2']
    tf = pos['timeframe']
    
    bars_map = {'1h': 300, '4h': 300, '1d': 120}
    n_bars = bars_map.get(tf, 300)
    
    df1 = fetch_prices(exchange_name, c1, tf, n_bars)
    df2 = fetch_prices(exchange_name, c2, tf, n_bars)
    
    if df1 is None or df2 is None:
        return None
    
    # Align timestamps
    merged = pd.merge(df1[['ts', 'c']], df2[['ts', 'c']], on='ts', suffixes=('_1', '_2'))
    if len(merged) < 50:
        return None
    
    p1 = merged['c_1'].values
    p2 = merged['c_2'].values
    ts = merged['ts'].tolist()
    
    # Kalman
    kf = kalman_hr(p1, p2)
    if kf is None:
        return None
    
    spread = kf['spread']
    hr_current = kf['hr']

    # BUG-026 FIX: Kalman spread may contain nan/inf when price series has
    # zero-variance segments or outliers. Downstream zscore/hurst functions
    # fail silently and return garbage. Bail out early with None.
    if not np.all(np.isfinite(spread)):
        n_bad = int(np.sum(~np.isfinite(spread)))
        if n_bad > len(spread) * 0.05:  # >5% bad values — abort
            return None
        # <5% bad values — forward-fill using last valid value
        spread = spread.copy()
        for _i in range(len(spread)):
            if not np.isfinite(spread[_i]):
                spread[_i] = spread[_i - 1] if _i > 0 and np.isfinite(spread[_i - 1]) else 0.0
    
    # v3.0: OU Half-life (dt-correct, как в сканере)
    dt_ou = {'1h': 1/24, '4h': 1/6, '1d': 1.0}.get(tf, 1/6)
    hpb = {'1h': 1, '4h': 4, '1d': 24}.get(tf, 4)
    
    # v18: Use SAME halflife function as scanner (critical for Z-window sync)
    if _USE_MRA:
        hl_days = calc_halflife_from_spread(spread, dt=dt_ou)
    else:
        hl_days = calc_halflife(spread, dt=dt_ou)
    hl_hours = hl_days * 24 if hl_days < 999 else 999
    hl_bars = (hl_hours / hpb) if hl_hours < 999 else None
    
    # v15: Use SAME Z-score function as scanner for consistency
    if _USE_MRA:
        z_now, zs, zw = calculate_adaptive_robust_zscore(spread, halflife_bars=hl_bars)
        # v18: GARCH Z for false convergence detection
        garch_info = calculate_garch_zscore(spread, halflife_bars=hl_bars)
        z_garch = garch_info.get('z_garch', z_now)
        garch_vol_ratio = garch_info.get('vol_ratio', 1.0)
        garch_var_expanding = garch_info.get('variance_expanding', False)
    else:
        zs, zw = calc_zscore(spread, halflife_bars=hl_bars)
        z_now = float(zs[~np.isnan(zs)][-1]) if any(~np.isnan(zs)) else 0
        z_garch = z_now
        garch_vol_ratio = 1.0
        garch_var_expanding = False
    
    # v3.0: Quality metrics (как в сканере)
    # v14: CRITICAL FIX — use SAME Hurst as scanner (DFA on increments)
    # v16: Hurst EMA smoothing
    if _USE_MRA:
        hurst_ema_info = calculate_hurst_ema(spread)
        hurst_raw = hurst_ema_info.get('hurst_raw', hurst_ema_info.get('hurst_ema', 0.5))
        hurst_ema_val = hurst_ema_info.get('hurst_ema', 0.5)
        hurst_std = hurst_ema_info.get('hurst_std', 0)
    else:
        # BUG-033 FIX: use calculate_hurst_exponent from mean_reversion_analysis
        # instead of the local calc_hurst() which has a different R²-threshold
        # (0.80 vs 0.70) — divergence caused inconsistent Hurst between monitor
        # and scanner on the same spread. Local calc_hurst() is kept as last-resort.
        try:
            from mean_reversion_analysis import calculate_hurst_exponent as _mra_hurst
            hurst_raw = _mra_hurst(spread)
        except ImportError:
            hurst_raw = calc_hurst(spread)  # last-resort local fallback
        hurst_ema_val = hurst_raw
        hurst_std = 0

    # BUG-024 + D1 FIX: синхронизация логики fallback с app.py (сканером).
    # Заменяем жёсткое сравнение hurst == 0.5 на hurst_std > 0.08:
    # hurst_std — std скользящей EMA-серии, высокое значение = ненадёжная оценка.
    # Если EMA не считалась (hurst_std == 0) и DFA вернул 0.5 — тоже fallback.
    _hurst_std_val = float(hurst_std) if hurst_std else 0.0
    hurst_is_fallback = (
        _hurst_std_val > 0.08 or
        (hurst_raw == 0.5 and _hurst_std_val == 0.0)
    )
    # use_hurst_ema_fallback привязан к реальному выбору значения (как в сканере)
    _use_ema_fb = CFG('strategy', 'use_hurst_ema_fallback', True)
    if _use_ema_fb and hurst_is_fallback:
        hurst = hurst_ema_val   # ненадёжный DFA → стабильная EMA-оценка
    else:
        hurst = hurst_raw       # надёжный DFA → используем напрямую
    corr = calc_correlation(p1, p2, window=min(60, len(p1) // 3))
    pvalue = calc_cointegration_pvalue(p1, p2)
    
    # ═══ v23.0: STATIC Z-SCORE (CRITICAL FIX) ═══
    # Static spread uses FIXED entry HR + intercept — reflects ACTUAL position held
    entry_hr_static = pos.get('entry_hr', hr_current)
    entry_intercept = pos.get('entry_intercept', 0.0)
    static_spread = calc_static_spread(p1, p2, entry_hr_static, entry_intercept)
    static_zs, static_zw = calc_static_zscore(static_spread, halflife_bars=hl_bars)
    z_static = float(static_zs[~np.isnan(static_zs)][-1]) if any(~np.isnan(static_zs)) else z_now
    
    # z_dynamic = Kalman (recalculated HR each bar) — for cointegration HEALTH only
    z_dynamic = z_now  # already computed above
    
    # z_drift: how far static and dynamic Z diverged
    z_drift = abs(z_static - z_dynamic)
    
    # v3.0: Entry readiness data
    quality_data = {
        'signal': 'SIGNAL' if abs(z_now) >= 2.0 else ('READY' if abs(z_now) >= 1.5 else 'NEUTRAL'),
        'zscore': z_now,
        'threshold': 2.0,
        'quality_score': max(0, int(100 - pvalue * 200 - max(0, hurst - 0.35) * 200)),
        'direction': pos['direction'],
        'fdr_passed': pvalue < 0.01,
        'confidence': 'HIGH' if (hurst < 0.4 and pvalue < 0.03) else ('MEDIUM' if pvalue < 0.05 else 'LOW'),
        'signal_score': max(0, int(abs(z_now) / 2.0 * 50 + (0.5 - hurst) * 100)),
        'correlation': corr,
        'stability_passed': 3 if pvalue < 0.05 else 1,
        'stability_total': 4,
        'hurst': hurst,
        'adf_passed': pvalue < 0.05,
    }
    
    # P&L (v4.0: price-based + spread-based + disagreement warning)
    r1 = (p1[-1] - pos['entry_price1']) / pos['entry_price1']
    r2 = (p2[-1] - pos['entry_price2']) / pos['entry_price2']
    hr = pos['entry_hr']
    if pos['direction'] == 'LONG':
        raw_pnl = r1 - hr * r2
    else:
        raw_pnl = -r1 + hr * r2
    pnl_gross = raw_pnl / (1 + abs(hr)) * 100
    pnl_pct = pnl_gross - COMMISSION_ROUND_TRIP_PCT()  # v23.0: after commission (BUG-N15 FIX: dynamic)
    
    # v23.0: Track best P&L during trade (after commission)
    best_pnl = max(pos.get('best_pnl_during_trade', 0), pnl_pct)
    
    # v4.0: Spread-based P&L (фиксированный HR от входа)
    entry_spread_val = pos['entry_price1'] - hr * pos['entry_price2']
    current_spread_val = p1[-1] - hr * p2[-1]
    spread_change = current_spread_val - entry_spread_val
    if pos['direction'] == 'LONG':
        spread_direction = 'profit' if spread_change > 0 else 'loss'
    else:
        spread_direction = 'profit' if spread_change < 0 else 'loss'
    
    # v4.0: Z-direction check
    z_entry = pos['entry_z']
    # v22: Directional Z check (fixes SOL/OKSOL false disagree on overshoot)
    # OLD: z_towards_zero = abs(z_now) < abs(z_entry) — WRONG for overshoot!
    # NEW: Check if Z moved in the CORRECT direction for our trade
    if pos['direction'] == 'LONG':
        # LONG entered at Z<0, wants Z to go UP (toward 0 and beyond)
        z_towards_zero = z_now > z_entry
    else:
        # SHORT entered at Z>0, wants Z to go DOWN (toward 0 and beyond)
        z_towards_zero = z_now < z_entry
    
    # v4.0: Предупреждение при расхождении P&L и Z-направления
    # v14: Enhanced with variance collapse detection (рассуждение #1)
    pnl_z_disagree = False
    pnl_z_warning = ""
    
    # Use shared function if available
    if _USE_MRA:
        disagree_info = check_pnl_z_disagreement(z_entry, z_now, pnl_pct, pos['direction'])
        if disagree_info.get('disagreement'):
            pnl_z_disagree = True
            pnl_z_warning = disagree_info.get('warning', '')
    
    # Legacy checks (still useful as fallback)
    if not pnl_z_disagree:
        if pnl_pct > 0 and not z_towards_zero:
            pnl_z_disagree = True
            pnl_z_warning = (
                f"⚠️ P&L положительный (+{pnl_pct:.2f}%), но Z ушёл дальше от нуля "
                f"({z_entry:+.2f} → {z_now:+.2f}). "
                f"HR изменился ({pos['entry_hr']:.4f} → {hr_current:.4f})."
            )
        elif pnl_pct < -0.5 and z_towards_zero:
            pnl_z_disagree = True
            pnl_z_warning = (
                f"⚠️ Z → 0 ({z_entry:+.2f} → {z_now:+.2f}), но P&L={pnl_pct:+.2f}%. "
                f"Возможно ложное схождение (σ спреда выросла)."
            )
    
    # Time in trade (вычисляем ДО использования)
    entry_dt = datetime.fromisoformat(pos['entry_time'])
    if entry_dt.tzinfo is None:
        entry_dt = entry_dt.replace(tzinfo=MSK)  # assume MSK if no tz
    hours_in = (now_msk() - entry_dt).total_seconds() / 3600
    
    # Exit signals
    # v23.0 CRITICAL: Use z_static (fixed entry HR) for ALL exit decisions
    # z_dynamic (Kalman) can drift and disagree with actual P&L
    z_exit = z_static  # ← v23.0: honest Z of actual position
    exit_signal = None
    exit_urgency = 0
    ez = pos.get('exit_z_target', 0.5)
    # v5.0: Adaptive stop — at least 2.0 Z-units beyond entry, minimum 4.0
    default_stop = max(abs(pos['entry_z']) + 2.0, 4.0)
    sz = pos.get('stop_z', default_stop)
    max_hours = pos.get('max_hold_hours', CFG('strategy', 'max_hold_hours', 12))
    pnl_stop = pos.get('pair_sl_pct', CFG('monitor', 'auto_sl_pct', -2.5))
    
    # v32: Entry grace period — MON-1 FIX: синхронизировано с config.yaml (было 15)
    grace_min = CFG('monitor', 'entry_grace_minutes', 5)
    in_grace = hours_in < grace_min / 60
    
    if in_grace:
        exit_signal = f"⏳ Grace period: {grace_min - hours_in*60:.0f} мин до мониторинга"
        exit_urgency = 0
    elif pos['direction'] == 'LONG':
        if z_exit >= -ez and z_exit <= ez:
            # v16: Check PnL before declaring convergence (рассуждение #1)
            # v18: Also check GARCH Z — if GARCH still far, it's variance collapse
            garch_still_far = abs(z_garch) > 1.5
            if pnl_pct > -0.3 and not garch_still_far:
                exit_signal = '✅ MEAN REVERT (static Z) — закрывать!'
                exit_urgency = 2
            elif garch_still_far:
                exit_signal = (f'⚠️ ЛОЖНОЕ СХОЖДЕНИЕ: Z_std→0 но Z_GARCH={z_garch:+.1f}. '
                               f'σ выросла в {garch_vol_ratio:.1f}x. Реального возврата нет.')
                exit_urgency = 1
            else:
                exit_signal = (f'⚠️ ЛОЖНОЕ СХОЖДЕНИЕ: Z→0 но P&L={pnl_pct:+.2f}%. '
                               f'σ спреда выросла. Ждите реального возврата цен.')
                exit_urgency = 1
        elif z_exit > 1.0:
            exit_signal = '✅ OVERSHOOT (static Z) — фиксировать прибыль!'
            exit_urgency = 2
        elif z_exit < -sz:
            exit_signal = '🛑 STOP LOSS (static Z) — экстренный выход!'
            exit_urgency = 2
    else:
        if z_exit <= ez and z_exit >= -ez:
            garch_still_far = abs(z_garch) > 1.5
            if pnl_pct > -0.3 and not garch_still_far:
                exit_signal = '✅ MEAN REVERT (static Z) — закрывать!'
                exit_urgency = 2
            elif garch_still_far:
                exit_signal = (f'⚠️ ЛОЖНОЕ СХОЖДЕНИЕ: Z_std→0 но Z_GARCH={z_garch:+.1f}. '
                               f'σ выросла в {garch_vol_ratio:.1f}x. Реального возврата нет.')
                exit_urgency = 1
            else:
                exit_signal = (f'⚠️ ЛОЖНОЕ СХОЖДЕНИЕ: Z→0 но P&L={pnl_pct:+.2f}%. '
                               f'σ спреда выросла. Ждите реального возврата цен.')
                exit_urgency = 1
        elif z_exit < -1.0:
            exit_signal = '✅ OVERSHOOT (static Z) — фиксировать прибыль!'
            exit_urgency = 2
        elif z_exit > sz:
            exit_signal = '🛑 STOP LOSS (static Z) — экстренный выход!'
            exit_urgency = 2
    
    # P&L stop (active even during grace for extreme losses)
    if pnl_pct <= pnl_stop and exit_urgency < 2 and not in_grace:
        exit_signal = f'🛑 STOP LOSS (P&L {pnl_pct:.1f}% < {pnl_stop:.1f}%) — выход!'
        exit_urgency = 2
    
    # Time-based
    if not in_grace:
        if hours_in > max_hours and exit_urgency < 2:
            if exit_signal is None:
                exit_signal = f'⏰ TIMEOUT ({hours_in:.1f}ч > {max_hours:.0f}ч) — рассмотрите выход'
                exit_urgency = 1
        elif hours_in > max_hours * 0.75 and exit_urgency == 0:
            exit_signal = f'⚠️ Позиция открыта {hours_in:.1f}ч (лимит {max_hours:.0f}ч)'
            exit_urgency = 1
    
    # v27: Quality warnings — thresholds from unified config
    quality_warnings = []
    _h_crit = CFG('monitor', 'hurst_critical', 0.50)
    _h_warn = CFG('monitor', 'hurst_warning', 0.48)
    _h_border = CFG('monitor', 'hurst_border', 0.45)
    _pv_warn = CFG('monitor', 'pvalue_warning', 0.10)
    _corr_warn = CFG('monitor', 'correlation_warning', 0.20)
    
    if hurst >= _h_crit:
        quality_warnings.append(
            f"🚨 Hurst(EMA)={hurst:.3f} ≥ {_h_crit} — нет mean reversion!"
            + (f" (raw={hurst_raw:.3f}, σ={hurst_std:.3f})" if hurst_std > 0 else ""))
    elif hurst >= _h_warn:
        quality_warnings.append(f"⚠️ Hurst(EMA)={hurst:.3f} ≥ {_h_warn} — ослабевает")
    elif hurst >= _h_border:
        quality_warnings.append(f"💡 Hurst(EMA)={hurst:.3f} — пограничное")
    if pvalue >= _pv_warn:
        quality_warnings.append(f"⚠️ P-value={pvalue:.3f} — коинтеграция ослабла!")
    if corr < _corr_warn:
        quality_warnings.append(f"⚠️ Корреляция ρ={corr:.2f} < {_corr_warn} — хедж не работает!")
    
    # v18: Direction sanity check — warn if direction contradicts entry Z
    entry_z = pos.get('entry_z', 0)
    direction = pos.get('direction', '')
    if entry_z < -0.5 and direction == 'SHORT':
        quality_warnings.append(
            f"🚨 НАПРАВЛЕНИЕ ИНВЕРТИРОВАНО: Entry_Z={entry_z:+.2f} (отрицательный) "
            f"но Dir=SHORT. Для Z<0 должен быть LONG! Проверьте ввод.")
    elif entry_z > 0.5 and direction == 'LONG':
        quality_warnings.append(
            f"🚨 НАПРАВЛЕНИЕ ИНВЕРТИРОВАНО: Entry_Z={entry_z:+.2f} (положительный) "
            f"но Dir=LONG. Для Z>0 должен быть SHORT! Проверьте ввод.")
    
    # Build base result dict
    base_result = {
        'z_now': z_now,
        'z_entry': pos['entry_z'],
        'z_static': z_static,           # v23.0: Static Z (entry HR)
        'z_dynamic': z_dynamic,          # v23.0: Dynamic Z (Kalman)
        'z_drift': z_drift,              # v23.0: |static - dynamic|
        'static_zscore_series': static_zs,  # v23.0: for charting
        'pnl_pct': pnl_pct,
        'pnl_gross_pct': pnl_gross,      # v23.0: P&L before commission
        'best_pnl': best_pnl,            # v23.0: best P&L during trade
        'spread_direction': spread_direction,
        'z_towards_zero': z_towards_zero,
        'pnl_z_disagree': pnl_z_disagree,
        'pnl_z_warning': pnl_z_warning,
        'price1_now': p1[-1],
        'price2_now': p2[-1],
        'hr_now': hr_current,
        'hr_entry': pos['entry_hr'],
        'exit_signal': exit_signal,
        'exit_urgency': exit_urgency,
        'hours_in': hours_in,
        'spread': spread,
        'zscore_series': zs,
        'timestamps': ts,
        'hr_series': kf['hrs'],
        'halflife_hours': hl_hours,
        'z_window': zw,
        'hurst': hurst,
        'correlation': corr,
        'pvalue': pvalue,
        'quality_data': quality_data,
        'quality_warnings': quality_warnings,
        'z_garch': z_garch,
        'garch_vol_ratio': garch_vol_ratio,
        'garch_var_expanding': garch_var_expanding,
    }
    
    # v27: R6 Correlation Monitor — track quality degradation
    _pair_key = f"{pos['coin1']}/{pos['coin2']}"
    _qh_key = f"_quality_history_{pos['id']}"
    if _qh_key not in st.session_state:
        st.session_state[_qh_key] = []
    _qh = st.session_state[_qh_key]
    _qh.append({'ts': time.time(), 'corr': corr, 'hurst': hurst, 'pval': pvalue})
    if len(_qh) > 30:
        st.session_state[_qh_key] = _qh[-30:]
    
    # R6: Quality degradation alerts
    if len(_qh) >= 3:
        _recent_corr = [q['corr'] for q in _qh[-5:]]
        _recent_hurst = [q['hurst'] for q in _qh[-5:]]
        _corr_trend = _recent_corr[-1] - _recent_corr[0] if len(_recent_corr) > 1 else 0
        _hurst_trend = _recent_hurst[-1] - _recent_hurst[0] if len(_recent_hurst) > 1 else 0
        
        if _corr_trend < -0.1:
            quality_warnings.append(f"📉 R6: ρ падает ({_recent_corr[0]:.2f}→{_recent_corr[-1]:.2f}). Хедж деградирует!")
        if _hurst_trend > 0.05:
            quality_warnings.append(f"📈 R6: Hurst растёт ({_recent_hurst[0]:.3f}→{_recent_hurst[-1]:.3f}). MR ослабевает!")
    
    base_result['quality_warnings'] = quality_warnings
    
    # v24: R5 Smart Exit Analysis (was dead code — FIXED in v27)
    base_result['smart_exit'] = None
    base_result['smart_signals'] = []
    base_result['smart_recommendation'] = ''
    base_result['smart_urgency'] = 0
    
    if _USE_MRA:
        try:
            smart_exit = smart_exit_analysis(
                z_entry=pos['entry_z'],
                z_now=z_now,
                z_history=zs[~np.isnan(zs)] if len(zs) > 0 else np.array([z_now]),
                pnl_pct=pnl_pct,
                hours_in=hours_in,
                halflife_hours=hl_hours,
                direction=pos['direction'],
                best_pnl=pos.get('best_pnl', max(pnl_pct, 0)),
            )
            base_result['smart_exit'] = smart_exit
            base_result['smart_signals'] = smart_exit.get('signals', [])
            base_result['smart_recommendation'] = smart_exit.get('recommendation', '')
            base_result['smart_urgency'] = smart_exit.get('urgency', 0)
            
            # Override exit_signal if smart exit has higher urgency
            if smart_exit.get('urgency', 0) > exit_urgency:
                base_result['exit_urgency'] = smart_exit['urgency']
                smart_msgs = [s['message'] for s in smart_exit.get('signals', [])]
                if smart_msgs:
                    base_result['exit_signal'] = ' | '.join(smart_msgs[:2])
        except Exception:
            pass
    
    return base_result


# ═══════════════════════════════════════════════════════
# STREAMLIT UI
# ═══════════════════════════════════════════════════════

st.set_page_config(page_title="Position Monitor", page_icon="📍", layout="wide")

st.markdown("""
<style>
    .exit-signal { padding: 15px; border-radius: 10px; font-size: 1.2em; 
                   font-weight: bold; text-align: center; margin: 10px 0; }
    .signal-exit { background: #1b5e20; color: #a5d6a7; }
    .signal-stop { background: #b71c1c; color: #ef9a9a; }
</style>
""", unsafe_allow_html=True)

st.title("📍 Pairs Position Monitor")
st.caption("v38.3 | Filter Control + Per-Pair TP/SL + DEEP_RALLY Toggle")  # FIX MON-CAP

# X-008 FIX: Startup reconciliation — проверить inflight и сиротские позиции
if '_reconciliation_done' not in st.session_state:
    st.session_state['_reconciliation_done'] = True
    _recon_warnings = startup_reconciliation()
    if _recon_warnings:
        st.session_state['_recon_warnings'] = _recon_warnings

_recon_w = st.session_state.get('_recon_warnings', [])
if _recon_w:
    for _rw in _recon_w:
        st.error(_rw)
    if st.button("✅ Подтвердить (скрыть предупреждения)", key="recon_dismiss"):
        st.session_state['_recon_warnings'] = []
        st.rerun()

# Sidebar
with st.sidebar:
    st.header("⚙️ Настройки")
    _exch_options = ['bybit', 'okx', 'kucoin', 'binance']
    _exch_default = CFG('scanner', 'exchange', 'bybit')
    if _exch_default not in _exch_options:
        _exch_default = 'bybit'
    exchange = st.selectbox("Биржа", _exch_options, 
                           index=_exch_options.index(_exch_default),
                           help="⚠️ Binance может быть заблокирован на облачных серверах.")
    # v33: Auto-refresh ON by default, configurable interval
    # v39 FIX: use session_state keys so values survive st.rerun()
    if 'auto_refresh' not in st.session_state:
        st.session_state['auto_refresh'] = True
    _refresh_sec_cfg = CFG('monitor', 'refresh_interval_sec', 60)
    if 'refresh_interval_sec' not in st.session_state:
        st.session_state['refresh_interval_sec'] = _refresh_sec_cfg
    st.checkbox("🔄 Авто-обновление", key='auto_refresh')
    st.slider("⏱ Интервал обновления (сек)", 30, 300,
              key='refresh_interval_sec', step=15,
              help="v33: 60с по умолчанию (было 120с)")
    auto_refresh = st.session_state['auto_refresh']
    refresh_interval_sec = st.session_state['refresh_interval_sec']
    
    # ═══════════════════════════════════════════════════
    # v42: ДНЕВНОЙ ЛИМИТ И ТАЙМАУТ — настраиваемые
    # ═══════════════════════════════════════════════════
    st.divider()
    st.markdown("### 🎛 Лимиты и таймауты")

    # Дневной лимит потерь
    if 'daily_loss_limit_pct' not in st.session_state:
        st.session_state['daily_loss_limit_pct'] = float(CFG('monitor', 'daily_loss_limit_pct', -5.0))
    st.number_input(
        "🛑 Дневной лимит потерь (%)",
        min_value=-30.0, max_value=-0.5, step=0.5,
        key='daily_loss_limit_pct',
        help="Суммарный PnL за день при котором блокируются все новые входы. "
             "По умолчанию -5.0%. Сбрасывается в 00:00 МСК."
    )

    # Таймаут позиции
    if 'max_hold_hours' not in st.session_state:
        st.session_state['max_hold_hours'] = int(CFG('strategy', 'max_hold_hours', 8))
    st.number_input(
        "⏰ Таймаут позиции (часов)",
        min_value=1, max_value=72, step=1,
        key='max_hold_hours',
        help="Максимальное время в позиции до AUTO_TIMEOUT. По умолчанию 8 часов."
    )

    # ═══════════════════════════════════════════════════
    # v38.2: DEEP_RALLY MANUAL TOGGLE
    # ═══════════════════════════════════════════════════
    st.divider()
    st.markdown("### 🌊 Rally Protection")
    _rally_status = "—"
    _rally_z = "—"
    try:
        _rally_path = os.path.join(_BASE_DIR, "rally_state.json")
        if os.path.exists(_rally_path):
            with open(_rally_path, 'r', encoding='utf-8') as _rf:
                _rally_data = json.load(_rf)
            _rally_status = _rally_data.get('status', '—')
            _rally_z = _rally_data.get('btc_z', '—')
    except Exception:
        pass
    st.caption(f"Текущий статус: **{_rally_status}** | BTC Z={_rally_z}")
    
    # Default: block ON only if DEEP_RALLY detected
    # v41 FIX: auto-включаем блок только при реальном DEEP_RALLY (btc_z > 2.5)
    # Статус RALLY при btc_z < 2.0 — устаревший, не должен блокировать входы
    _default_rally_block = (_rally_status == 'DEEP_RALLY')
    if 'deep_rally_block' not in st.session_state:
        st.session_state['deep_rally_block'] = _default_rally_block
    st.checkbox("🛑 Блокировать входы при Rally", 
                key='deep_rally_block',
                help="ВКЛ: блокирует все входы при DEEP_RALLY (кроме 🟢 ВХОД). "
                     "ВЫКЛ: Rally игнорируется, входы разрешены.")

    # BUG-N14 FIX B2: UI-индикатор статуса Cascade SL
    # Без индикатора пользователь не видит причину молчаливых блокировок входов
    st.divider()
    st.markdown("### 🔗 Cascade SL Protection")
    try:
        _casc_enabled = CFG('monitor', 'cascade_sl_enabled', False)
        if not _casc_enabled:
            st.caption("⚪ Cascade SL: **отключён** (cascade_sl_enabled: false)")
        else:
            _casc_blocked, _casc_reason = check_cascade_sl()
            if _casc_blocked:
                st.error(_casc_reason)
            else:
                _casc_w  = float(CFG('monitor', 'cascade_sl_window_hours', 2))
                _casc_th = int(CFG('monitor', 'cascade_sl_threshold', 3))
                _casc_ph = float(CFG('monitor', 'cascade_sl_pause_hours', 1))
                # Считаем текущее количество SL за окно для информации
                from datetime import timedelta as _td
                _cd_now = now_msk()
                _cd_cut = _cd_now - _td(hours=_casc_w)
                _cd_data = _load_cooldowns()
                _sl_now = sum(
                    1 for _e in _cd_data.values()
                    if _e.get('sl_exit') and _e.get('last_loss_time') and
                    datetime.fromisoformat(_e['last_loss_time']) >= _cd_cut
                )
                st.success(
                    f"✅ Cascade SL: активен | "
                    f"SL за {_casc_w:.0f}ч: **{_sl_now}/{_casc_th}** | "
                    f"пауза при достижении: {_casc_ph:.0f}ч"
                )
    except Exception as _ce:
        st.caption(f"Cascade SL: ошибка проверки ({_ce})")

    # ═══════════════════════════════════════════════════
    # v38.2: ENTRY FILTER CHECKBOXES
    # v39 FIX: initialise all keys BEFORE rendering so st.rerun() doesn't reset them
    # ═══════════════════════════════════════════════════
    for _fkey in ['block_green', 'block_green_bt_fail', 'block_green_bt_warn',
                  'block_yellow', 'block_yellow_bt_fail', 'block_yellow_bt_warn',
                  'block_wl_warn', 'block_wl_fail',
                  'block_nk_warn', 'block_nk_fail',
                  'block_long', 'block_short']:
        if _fkey not in st.session_state:
            st.session_state[_fkey] = False

    st.divider()
    st.markdown("### 🎛️ Фильтры входа")
    st.caption("☑️ = **заблокировано**. Снимите галочку чтобы разрешить.")
    
    with st.expander("🟢 ВХОД фильтры", expanded=False):
        st.checkbox("🚫 🟢 ВХОД (все)", key='block_green',
                    help="Блокировать ВСЕ 🟢 ВХОД сигналы")
        st.checkbox("🚫 🟢 ВХОД + ❌BT", key='block_green_bt_fail',
                    help="Блокировать 🟢 ВХОД с ❌BT (backtest FAIL)")
        st.checkbox("🚫 🟢 ВХОД + ⚠️BT", key='block_green_bt_warn',
                    help="Блокировать 🟢 ВХОД с ⚠️BT (backtest WARN)")
    
    with st.expander("🟡 УСЛОВНО фильтры", expanded=False):
        st.checkbox("🚫 🟡 УСЛОВНО (все)", key='block_yellow',
                    help="Блокировать ВСЕ 🟡 УСЛОВНО сигналы")
        st.checkbox("🚫 🟡 УСЛОВНО + ❌BT", key='block_yellow_bt_fail',
                    help="Блокировать 🟡 УСЛОВНО с ❌BT")
        st.checkbox("🚫 🟡 УСЛОВНО + ⚠️BT", key='block_yellow_bt_warn',
                    help="Блокировать 🟡 УСЛОВНО с ⚠️BT")
    
    with st.expander("🏷️ WL / NK / Dir фильтры", expanded=False):
        st.markdown("**Whitelist (WL)**")
        st.checkbox("🚫 ⚠️WL (не в whitelist)", key='block_wl_warn',
                    help="Блокировать пары с ⚠️WL")
        st.checkbox("🚫 ❌WL (whitelist fail)", key='block_wl_fail',
                    help="Блокировать пары с ❌WL")
        st.markdown("**NK флаги**")
        st.checkbox("🚫 ⚠️NK", key='block_nk_warn',
                    help="Блокировать пары с ⚠️NK")
        st.checkbox("🚫 ❌NK", key='block_nk_fail',
                    help="Блокировать пары с ❌NK")
        st.markdown("**Направление**")
        st.checkbox("🚫 LONG", key='block_long',
                    help="Блокировать все LONG входы")
        st.checkbox("🚫 SHORT", key='block_short',
                    help="Блокировать все SHORT входы")
    
    # Show active filter summary
    _active_filters = []
    for _fk, _fl in [('block_green', '🟢ALL'), ('block_green_bt_fail', '🟢❌BT'),
                      ('block_green_bt_warn', '🟢⚠️BT'), ('block_yellow', '🟡ALL'),
                      ('block_yellow_bt_fail', '🟡❌BT'), ('block_yellow_bt_warn', '🟡⚠️BT'),
                      ('block_wl_warn', '⚠️WL'), ('block_wl_fail', '❌WL'),
                      ('block_nk_warn', '⚠️NK'), ('block_nk_fail', '❌NK'),
                      ('block_long', 'LONG'), ('block_short', 'SHORT')]:
        if st.session_state.get(_fk, False):
            _active_filters.append(_fl)
    if _active_filters:
        st.warning(f"🚫 Активные блоки: {', '.join(_active_filters)}")
    else:
        st.success("✅ Все фильтры выключены (⚪ ЖДАТЬ всегда заблокирован)")
    
    # ═══════════════════════════════════════════════════
    # v38.2: PER-PAIR TP/SL CONFIG
    # ═══════════════════════════════════════════════════
    st.divider()
    st.markdown("### 🎯 TP/SL по парам")
    # BUG-032 FIX: read defaults from config instead of hardcoding 2.0
    _tp_default = CFG('monitor', 'auto_tp_pct', 2.0)
    _sl_default = CFG('monitor', 'auto_sl_pct', -2.5)
    st.caption(f"Настройте TP/SL для каждой пары. По умолчанию: TP=+{_tp_default}% / SL={_sl_default}%")
    
    _tp_sl_cfg = _load_pair_tp_sl()
    
    # Pre-populate from detail files if empty
    _detail_pairs = ['BNB/RIVER', 'ETH/RIVER', 'LINK/LTC', 'LINK/UNI', 'SOL/LTC', 'SOL/RIVER']
    _tp_sl_changed = False
    for _dp in _detail_pairs:
        if _dp not in _tp_sl_cfg:
            _tp_sl_cfg[_dp] = {'tp': _tp_default, 'sl': _sl_default}  # BUG-032 FIX
            _tp_sl_changed = True
    if _tp_sl_changed:
        _save_pair_tp_sl(_tp_sl_cfg)
    
    with st.expander(f"🎯 Настройки TP/SL ({len(_tp_sl_cfg)} пар)", expanded=False):
        # Existing pairs
        for _pname in sorted(_tp_sl_cfg.keys()):
            _pcfg = _tp_sl_cfg[_pname]
            _pc1, _pc2, _pc3 = st.columns([3, 2, 2])
            with _pc1:
                st.caption(f"**{_pname}**")
            with _pc2:
                _new_tp = st.number_input(f"TP% {_pname}", value=float(_pcfg.get('tp', _tp_default)),
                                          step=0.5, min_value=0.1, max_value=20.0,
                                          key=f"tp_{_pname}", label_visibility="collapsed")
            with _pc3:
                _new_sl = st.number_input(f"SL% {_pname}", value=float(_pcfg.get('sl', _sl_default)),
                                          step=0.5, min_value=-20.0, max_value=-0.1,
                                          key=f"sl_{_pname}", label_visibility="collapsed")
            if _new_tp != float(_pcfg.get('tp', _tp_default)) or _new_sl != float(_pcfg.get('sl', _sl_default)):
                _tp_sl_cfg[_pname] = {'tp': _new_tp, 'sl': _new_sl}
                _save_pair_tp_sl(_tp_sl_cfg)
        
        # Add new pair
        st.markdown("---")
        st.caption("➕ Добавить пару")
        _add_cols = st.columns([3, 2, 2])
        with _add_cols[0]:
            _new_pair_name = st.text_input("Пара (COIN1/COIN2)", "", key="new_tp_sl_pair")
        with _add_cols[1]:
            _add_tp = st.number_input("TP%", value=2.0, step=0.5, key="add_tp_val")
        with _add_cols[2]:
            _add_sl = st.number_input("SL%", value=-2.0, step=0.5, key="add_sl_val")
        if _new_pair_name and st.button("➕ Добавить TP/SL", key="add_tp_sl_btn"):
            _tp_sl_cfg[_new_pair_name.upper()] = {'tp': _add_tp, 'sl': _add_sl}
            _save_pair_tp_sl(_tp_sl_cfg)
            st.success(f"✅ {_new_pair_name.upper()}: TP={_add_tp}% / SL={_add_sl}%")
            st.rerun()
    
    st.caption(f"📌 Default для новых пар: TP=+2.0% / SL=-2.0%")
    
    # v30: Telegram exit alerts
    st.markdown("---")
    st.caption("📱 **Telegram** — настройки берутся из сканера (общий session_state)")
    # BUG-027 FIX: prefer env vars TG_TOKEN / TG_CHAT_ID over session_state
    # so the token is never stored in Streamlit's in-memory state dict
    # (visible in debug dumps). session_state is used only as a UI fallback
    # when env vars are absent — and only the chat_id (not the token) is kept.
    _env_tg_token = __import__('os').environ.get('TG_TOKEN', '')
    _env_tg_chat  = __import__('os').environ.get('TG_CHAT_ID', '')
    if _env_tg_token:
        st.session_state['tg_token']   = _env_tg_token
        st.session_state['tg_chat_id'] = _env_tg_chat
        st.session_state['tg_enabled'] = True
        st.caption(f"✅ Telegram: токен из переменной окружения TG_TOKEN")
    elif 'tg_token' not in st.session_state:
        _tg_t = st.text_input("TG Bot Token", type="password", key='tg_token_mon')
        if _tg_t: st.session_state['tg_token'] = _tg_t
        _tg_c = st.text_input("TG Chat ID", key='tg_chat_mon')
        if _tg_c: st.session_state['tg_chat_id'] = _tg_c
        st.session_state['tg_enabled'] = st.checkbox("TG включён", key='tg_en_mon')
        st.session_state['tg_alert_exits'] = st.checkbox("Алерты выхода", value=True, key='tg_ex_mon')
    
    # ═══════════════════════════════════════════════════
    # v38.3: BYBIT DEMO ЗЕРКАЛО
    # ═══════════════════════════════════════════════════
    st.divider()
    st.markdown("### 🔗 Bybit Demo — зеркало")
    
    if not _BYBIT_AVAILABLE:
        st.warning("⚠️ bybit_executor.py не найден. Положите его рядом с монитором.")
    else:
        _bybit_key = CFG('bybit', 'api_key', '')
        _bybit_secret = CFG('bybit', 'api_secret', '')
        _bybit_configured = bool(_bybit_key) and bool(_bybit_secret)

        if not _bybit_configured:
            st.caption("⚙️ API ключи не заданы в config.yaml → секция `bybit:`")
            st.code('bybit:\n  enabled: true\n  api_key: "ВАШ_DEMO_KEY"\n  api_secret: "ВАШ_DEMO_SECRET"', language="yaml")
        else:
            st.caption("🔑 API: ✅ | Режим: **Demo Trading (api-demo.bybit.com)**")
            with st.expander("📋 Как получить Demo Trading API ключ", expanded=False):
                st.markdown("""
**Bybit Demo Trading — пошаговая инструкция:**

1. Зайдите на **[bybit.com](https://bybit.com)** и войдите в аккаунт
2. В левом верхнем углу переключитесь на **Demo Trading** (оранжевый бейдж)
3. Profile (иконка) → **API Management** → **Create New Key**
4. Тип: **API Transaction**
5. Права: ✅ **Read-Write**
6. Unified Trading → Contract: ✅ **Orders** + ✅ **Positions**
7. IP: **No IP restriction**
8. Нажмите **Submit**, скопируйте Key + Secret в config.yaml

**config.yaml:**
```yaml
bybit:
  enabled: true
  api_key: "ВАШ_DEMO_KEY"
  api_secret: "ВАШ_DEMO_SECRET"
```

⚠️ Ключи от основного аккаунта здесь **не работают** — нужны именно из Demo Trading!
""")

        
        if 'bybit_demo_mirror' not in st.session_state:
            st.session_state['bybit_demo_mirror'] = CFG('bybit', 'demo_mirror', False)
        
        _mirror_enabled = st.checkbox(
            "🤖 Зеркалировать сделки на Bybit Demo",
            key='bybit_demo_mirror',
            disabled=not _bybit_configured,
            help="ВКЛ: каждое открытие/закрытие внутри монитора дублируется "
                 "на Bybit Demo. API ключи — config.yaml → секция bybit."
        )
        
        if _mirror_enabled and _bybit_configured:
            st.success("✅ Зеркало АКТИВНО")
            if st.button("🔌 Тест подключения", key="bybit_test_conn"):
                with st.spinner("Подключаюсь к Bybit Demo..."):
                    try:
                        _ex_test = BybitExecutor(_bybit_key, _bybit_secret)
                        _conn = _ex_test.test_connection()
                        if _conn.get('connected'):
                            _note = f" ({_conn['note']})" if _conn.get('note') else ""
                            st.success(
                                f"✅ Подключено! Баланс: ${_conn.get('balance_usdt',0):.2f} USDT"
                                f" | Equity: ${_conn.get('equity_usdt',0):.2f}"
                                f" | Тип: {_conn.get('account_type','?')}{_note}"
                            )
                        else:
                            st.error(f"❌ Ошибка: {_conn.get('error','?')}")
                            if _conn.get('hint'):
                                st.warning(f"💡 {_conn['hint']}")
                            st.caption(
                                f"URL: {_conn.get('base_url','?')} | "
                                f"Ключ начинается с: {_conn.get('key_prefix','?')}"
                            )
                    except Exception as _ce:
                        st.error(f"❌ {_ce}")
            try:
                _ex_sl = get_executor()
                if _ex_sl and _ex_sl.enabled:
                    _sl = _ex_sl.get_slippage_stats()
                    if _sl.get('n_trades', 0) > 0:
                        st.caption(f"📊 {_sl['n_trades']} сд | slippage avg: {_sl.get('avg_slippage_pct',0):.4f}% | latency: {_sl.get('avg_latency_ms',0):.0f}ms")
            except Exception:
                pass
        elif not _bybit_configured:
            st.caption("Задайте API ключи в config.yaml")
    
    st.divider()
    st.header("➕ Новая позиция")
    import glob, json as _json

    def _is_auto_pending(path):
        """v41 FIX: принимает любой валидный pending файл с coin1+coin2.
        Убрана проверка auto_opened=True — она блокировала файлы без этого поля."""
        try:
            with open(path, 'r', encoding='utf-8') as _f:
                _d = _json.load(_f)
            if isinstance(_d, list):
                _d = _d[0] if _d and isinstance(_d[0], dict) else {}
            return isinstance(_d, dict) and bool(_d.get('coin1')) and bool(_d.get('coin2'))
        except Exception:
            return False

    pending_files = sorted(glob.glob(os.path.join(_MONITOR_IMPORT_DIR, "pending_*.json")))
    
    # v27: Cleanup — remove pending files if pair already open
    if pending_files:
        _open_pairs = set()
        for _op in load_positions():
            if _op.get('status') == 'OPEN' and _op.get('coin1') and _op.get('coin2'):
                _open_pairs.add(f"{_op['coin1']}/{_op['coin2']}")
        
        _remaining = []
        for pf in pending_files:
            try:
                with open(pf, 'r', encoding='utf-8') as f:
                    imp = _json.load(f)
                # v39 FIX: Handle list format [{}]
                if isinstance(imp, list):
                    imp = imp[0] if len(imp) == 1 and isinstance(imp[0], dict) else None
                if not isinstance(imp, dict):
                    _remaining.append(pf)
                    continue
                _pname = f"{imp['coin1']}/{imp['coin2']}"
                if _pname in _open_pairs:
                    import os; os.remove(pf)  # Already imported
                else:
                    _remaining.append(pf)
            except Exception:
                _remaining.append(pf)
        pending_files = _remaining
    
    if pending_files:
        # ── v39 FIX: Авто-обработка pending файлов от сканера (auto_opened=True) ──
        # Файлы с auto_opened=True создаются сканером (app.py auto_monitor).
        # Они обрабатываются автоматически — без нажатия кнопки.
        # Файлы без auto_opened показываются как раньше с кнопкой подтверждения.
        _auto_pf   = [pf for pf in pending_files if _is_auto_pending(pf)]
        _manual_pf = [pf for pf in pending_files if pf not in _auto_pf]

        if _auto_pf:
            _auto_opened_count = 0
            _auto_errors = []
            for pf in _auto_pf:
                try:
                    with open(pf, 'r', encoding='utf-8') as f:
                        imp = _json.load(f)
                    if isinstance(imp, list):
                        imp = imp[0] if imp and isinstance(imp[0], dict) else None
                    if not isinstance(imp, dict) or 'coin1' not in imp:
                        diag_log_refusal(os.path.basename(pf), '?', 'Невалидный JSON или отсутствует coin1', source='auto_pending',
                                         details={'file': pf})
                        import os; os.remove(pf)
                        continue
                    pair_name = f"{imp['coin1']}/{imp['coin2']}"
                    p1 = imp.get('entry_price1', 0)
                    p2 = imp.get('entry_price2', 0)
                    if p1 == 0 or p2 == 0:
                        p1 = get_current_price(exchange, imp['coin1']) or 0
                        p2 = get_current_price(exchange, imp['coin2']) or 0
                    if p1 > 0 and p2 > 0:
                        _rec_size = float(imp.get('risk_size_usdt', imp.get('recommended_size', 100)))
                        _intercept = imp.get('entry_intercept', imp.get('intercept', 0.0))
                        _zw = imp.get('z_window', None)
                        # v38.4 FIX: merge entry_label into notes if missing
                        _imp_notes = imp.get('notes', '')
                        _imp_lbl = imp.get('entry_label', '')
                        if _imp_lbl and _imp_lbl not in _imp_notes:
                            _imp_notes = f"{_imp_lbl} | {_imp_notes}"
                        pos = add_position(
                            imp['coin1'], imp['coin2'], imp['direction'],
                            imp['entry_z'], imp['entry_hr'],
                            p1, p2, imp.get('timeframe', '4h'),
                            _imp_notes,
                            entry_intercept=_intercept,
                            recommended_size=_rec_size,
                            z_window=_zw,
                            auto_opened=bool(imp.get('auto_opened', False)),
                            # B-08 FIX: передаём BT-метрики и per-pair параметры
                            bt_verdict=imp.get('bt_verdict'),
                            bt_pnl=imp.get('bt_pnl'),
                            mu_bt_wr=imp.get('mu_bt_wr'),
                            v_quality=imp.get('v_quality'),
                            max_hold_hours=imp.get('max_hold_hours'),
                            pnl_stop_pct=imp.get('pnl_stop_pct'),
                        )
                        if pos:
                            # Отправляем ордер на Bybit Demo
                            _bb_res = _bybit_open(imp['coin1'], imp['coin2'],
                                                  imp['direction'], _rec_size, p1, p2)
                            _bb_ok  = _bb_res and _bb_res.get('success')
                            _bb_err = (_bb_res or {}).get('error', '')
                            _bb_status = '🔗 Bybit ✅' if _bb_ok else (('⚠️ ' + str(_bb_err)[:40]) if _bb_res else '(Bybit выкл)')
                            if _bb_ok:  # BUG-015 FIX: save real fill prices/qty
                                _save_bybit_fill(pos['id'], _bb_res)
                            st.toast(f"🤖 #{pos['id']} {pair_name} {_bb_status}", icon="📍")
                            diag_log_attempt(pair_name, imp['direction'], _imp_notes, source='auto_pending')
                            import os; os.remove(pf)
                            _auto_opened_count += 1
                        else:
                            # v41 FIX: НЕ удаляем pending при отказе — retry до 3 раз
                            _err_reason = st.session_state.get('_last_add_pos_err', 'лимит/cooldown/фильтр')
                            _auto_errors.append(f"{pair_name}: {_err_reason}")
                            diag_log_refusal(pair_name, imp.get('direction','?'),
                                             _err_reason, source='auto_pending',
                                             details={'notes': _imp_notes[:120], 'entry_label': _imp_lbl,
                                                      'quality': imp.get('quality_score',0)})
                            imp['_attempts'] = imp.get('_attempts', 0) + 1
                            if imp['_attempts'] >= 3:
                                import os; os.remove(pf)
                            else:
                                try:
                                    with open(pf, 'w', encoding='utf-8') as _pff:
                                        _json.dump(imp, _pff, indent=2, ensure_ascii=False)
                                except Exception:
                                    import os; os.remove(pf)
                    else:
                        _reason_np = f"нет цен: p1={p1} p2={p2}"
                        _auto_errors.append(f"{pair_name}: {_reason_np}")
                        diag_log_refusal(pair_name, imp.get('direction','?'), _reason_np, source='auto_pending',
                                         details={'price1': p1, 'price2': p2})
                        import os; os.remove(pf)
                except Exception as _ae:
                    _auto_errors.append(f"{pf}: {_ae}")
                    diag_log_refusal(os.path.basename(pf), '?', f'Exception: {_ae}', source='auto_pending')
            if _auto_opened_count:
                st.rerun()

        # ── Ручные pending файлы — показываем с кнопкой ──
        if _manual_pf:
            st.markdown("#### 📥 Импорт из сканера")
            for pf in _manual_pf:
                try:
                    with open(pf, 'r', encoding='utf-8') as f:
                        imp = _json.load(f)
                    if isinstance(imp, list):
                        imp = imp[0] if len(imp) == 1 and isinstance(imp[0], dict) else None
                    if not isinstance(imp, dict):
                        st.warning(f"⚠️ {pf}: неверный формат JSON (ожидается dict)")
                        continue
                    pair_name = f"{imp['coin1']}/{imp['coin2']}"
                    st.info(
                        f"📤 **{pair_name}** {imp['direction']} | "
                        f"Z={imp['entry_z']:.2f} HR={imp['entry_hr']:.4f} "
                        f"| {imp.get('notes', '')}"
                    )
                    if st.button(f"✅ Импортировать {pair_name}", key=f"imp_{pair_name}"):
                        with st.spinner(f"Загружаю цены {pair_name}..."):
                            p1 = imp.get('entry_price1', 0)
                            p2 = imp.get('entry_price2', 0)
                            if p1 == 0 or p2 == 0:
                                p1 = get_current_price(exchange, imp['coin1']) or 0
                                p2 = get_current_price(exchange, imp['coin2']) or 0
                            if p1 > 0 and p2 > 0:
                                _rec_size = float(imp.get('risk_size_usdt', imp.get('recommended_size', 100)))
                                _intercept = imp.get('intercept', imp.get('entry_intercept', 0.0))
                                _zw = imp.get('z_window', None)
                                pos = add_position(
                                    imp['coin1'], imp['coin2'], imp['direction'],
                                    imp['entry_z'], imp['entry_hr'],
                                    p1, p2, imp.get('timeframe', '4h'),
                                    imp.get('notes', ''),
                                    entry_intercept=_intercept,
                                    recommended_size=_rec_size,
                                    z_window=_zw,
                                    auto_opened=False)
                                if pos:
                                    st.success(f"✅ #{pos['id']} {pair_name} добавлена! 💰 ${_rec_size:.0f}")
                                    _bb_res = _bybit_open(imp['coin1'], imp['coin2'],
                                                          imp['direction'], _rec_size, p1, p2)
                                    if _bb_res is not None:
                                        if _bb_res.get('success'):
                                            _save_bybit_fill(pos['id'], _bb_res)  # BUG-015 FIX
                                            st.success(f"🔗 Bybit Demo: открыта | slippage: {_bb_res.get('total_slippage_pct',0):.4f}%")
                                        else:
                                            st.warning(f"⚠️ Bybit Demo ошибка: {_bb_res.get('error','?')}")
                                    import os; os.remove(pf)
                                    st.rerun()
                                else:
                                    st.warning("⚠️ Позиция не открыта (лимит/cooldown)")
                            else:
                                st.error("Не удалось получить цены")
                except Exception as ex:
                    st.warning(f"⚠️ {pf}: {ex}")
            st.divider()
    
    # Upload JSON manually
    uploaded_json = st.file_uploader("📤 Или загрузи JSON из сканера", type=['json'], key='json_import')
    if uploaded_json:
        try:
            raw_data = _json.load(uploaded_json)
            # v39 FIX: Handle list format [{}] and multi-entry lists
            if isinstance(raw_data, list):
                valid_items = [x for x in raw_data if isinstance(x, dict) and 'coin1' in x]
                if len(valid_items) == 0:
                    st.error("⚠️ JSON list пуст или не содержит валидных записей")
                    imp = None
                elif len(valid_items) == 1:
                    imp = valid_items[0]
                else:
                    labels = [f"{x.get('coin1','?')}/{x.get('coin2','?')} {x.get('direction','?')}" for x in valid_items]
                    sel = st.selectbox("Выбери пару из списка:", labels, key='json_list_sel')
                    imp = valid_items[labels.index(sel)]
            elif isinstance(raw_data, dict):
                imp = raw_data
            else:
                st.error("⚠️ Неверный формат JSON")
                imp = None
            
            if imp:
                pair_name = f"{imp['coin1']}/{imp['coin2']}"
                st.info(f"📤 **{pair_name}** {imp['direction']} Z={imp['entry_z']:.2f} HR={imp['entry_hr']:.4f}")
                if st.button(f"✅ Импортировать {pair_name}", key="imp_upload"):
                    with st.spinner("Загружаю цены..."):
                        p1 = imp.get('entry_price1', 0) or get_current_price(exchange, imp['coin1']) or 0
                        p2 = imp.get('entry_price2', 0) or get_current_price(exchange, imp['coin2']) or 0
                        if p1 > 0 and p2 > 0:
                            _rec_size = imp.get('risk_size_usdt', imp.get('recommended_size', 100))
                            _intercept = imp.get('intercept', imp.get('entry_intercept', 0.0))
                            _zw = imp.get('z_window', None)
                            pos = add_position(imp['coin1'], imp['coin2'], imp['direction'],
                                             imp['entry_z'], imp['entry_hr'], p1, p2,
                                             imp.get('timeframe', '4h'), imp.get('notes', ''),
                                             entry_intercept=_intercept,
                                             recommended_size=_rec_size,
                                             z_window=_zw,
                                             auto_opened=False)
                            if pos:
                                st.success(f"✅ #{pos['id']} импортирована! 💰 ${_rec_size:.0f}")
                                # v38.3: Mirror to Bybit Demo
                                _bb_res = _bybit_open(imp['coin1'], imp['coin2'], imp['direction'],
                                                      _rec_size, p1, p2)
                                if _bb_res is not None:
                                    if _bb_res.get('success'):
                                        _save_bybit_fill(pos['id'], _bb_res)  # BUG-015 FIX
                                        st.success(f"🔗 Bybit Demo: открыта | slippage: {_bb_res.get('total_slippage_pct',0):.4f}%")
                                    else:
                                        st.warning(f"⚠️ Bybit Demo ошибка: {_bb_res.get('error','?')}")
                                st.rerun()
                            else:
                                st.warning("⚠️ Позиция не открыта (лимит/cooldown)")
        except Exception as ex:
            st.error(f"Ошибка JSON: {ex}")
    
    st.divider()
    
    with st.form("add_position"):
        col1, col2 = st.columns(2)
        with col1:
            new_c1 = st.text_input("Coin 1", "ETH").upper().strip()
        with col2:
            new_c2 = st.text_input("Coin 2", "STETH").upper().strip()
        
        new_dir = st.selectbox("Направление", ["LONG", "SHORT"])
        new_tf = st.selectbox("Таймфрейм", ['1h', '4h', '1d'], index=1)
        
        col3, col4 = st.columns(2)
        with col3:
            new_z = st.number_input("Entry Z", value=2.0, step=0.1)
        with col4:
            new_hr = st.number_input("Hedge Ratio", value=1.0, step=0.01, format="%.4f")
        
        # v23.0: Intercept for static spread calculation
        new_intercept = st.number_input("Intercept (из сканера)", value=0.0, step=0.001, format="%.6f",
                                        help="Kalman intercept — для точного static Z-score")
        
        col5, col6 = st.columns(2)
        with col5:
            new_p1 = st.number_input("Цена Coin1", value=0.0, step=0.01, format="%.4f")
        with col6:
            new_p2 = st.number_input("Цена Coin2", value=0.0, step=0.01, format="%.4f")
        
        new_notes = st.text_input("Заметки", "")
        
        # v33: Position size in manual form (was missing → all positions had size=NONE)
        st.markdown("**💰 Размер позиции**")
        new_size = st.number_input("Размер ($)", value=100, min_value=25, max_value=500, step=25,
                                    help="v33: Размер позиции в USDT. Ранее не передавался из ручной формы.")
        
        # v2.0: Risk management
        st.markdown("**⚠️ Риск-менеджмент**")
        col_r1, col_r2 = st.columns(2)
        with col_r1:
            new_max_hours = st.number_input("Max часов в позиции", value=int(CFG('strategy', 'max_hold_hours', 12)), step=1)
        with col_r2:
            new_pnl_stop = st.number_input("P&L Stop (%)", value=float(CFG('monitor', 'auto_sl_pct', -2.5)), step=0.1)
        
        # Автозагрузка цен
        fetch_prices_btn = st.form_submit_button("📥 Загрузить цены + Добавить")
    
    if fetch_prices_btn and new_c1 and new_c2:
        if new_p1 == 0 or new_p2 == 0:
            with st.spinner("Загружаю текущие цены..."):
                p1_live = get_current_price(exchange, new_c1)
                p2_live = get_current_price(exchange, new_c2)
                if p1_live and p2_live:
                    new_p1 = p1_live
                    new_p2 = p2_live
                    st.info(f"💰 {new_c1}: ${p1_live:.4f} | {new_c2}: ${p2_live:.4f}")
                else:
                    st.error("Не удалось загрузить цены")
        
        # BUG-034 FIX: explicit validation — block submission if either price is 0
        if new_p1 <= 0 or new_p2 <= 0:
            st.error(
                f"❌ Цена {'Coin1' if new_p1 <= 0 else 'Coin2'} = 0. "
                "Введите цены вручную или нажмите '📥 Загрузить цены' ещё раз."
            )
        elif new_p1 > 0 and new_p2 > 0:
            # v22: HR sanity check — warn if HR doesn't match price ratio
            expected_hr_approx = new_p1 / new_p2 if new_p2 > 0 else 0
            if new_hr > 0 and expected_hr_approx > 0:
                ratio = new_hr / expected_hr_approx if expected_hr_approx > 0 else 999
                if ratio > 10 or ratio < 0.1:
                    st.warning(
                        f"⚠️ **HR подозрительный!** HR={new_hr:.4f}, "
                        f"P1/P2={expected_hr_approx:.4f} (отношение {ratio:.1f}x). "
                        f"Проверьте правильность HR. Возможно опечатка.")
            
            pos = add_position(new_c1, new_c2, new_dir, new_z, new_hr,
                             new_p1, new_p2, new_tf, new_notes,
                             max_hold_hours=new_max_hours,
                             pnl_stop_pct=new_pnl_stop,
                             entry_intercept=new_intercept,
                             recommended_size=new_size,
                             auto_opened=False)
            if pos:
                st.success(f"✅ Позиция #{pos['id']} добавлена: {new_dir} {new_c1}/{new_c2} | 💰 ${pos.get('recommended_size', 100):.0f}")
                # v38.3: Mirror to Bybit Demo
                _bb_res = _bybit_open(new_c1, new_c2, new_dir, new_size, new_p1, new_p2)
                if _bb_res is not None:
                    if _bb_res.get('success'):
                        _save_bybit_fill(pos['id'], _bb_res)  # BUG-015 FIX
                        st.success(f"🔗 Bybit Demo: открыта | slippage: {_bb_res.get('total_slippage_pct',0):.4f}%")
                    else:
                        st.warning(f"⚠️ Bybit Demo ошибка: {_bb_res.get('error','?')}")
                st.rerun()
            else:
                st.warning("⚠️ Позиция не открыта (лимит/cooldown/daily loss)")

# ═══════ MAIN AREA ═══════
positions = load_positions()
# v35.1 FIX: Ensure all positions are dicts with 'status' key
positions = [p for p in positions if isinstance(p, dict) and 'status' in p]
open_positions = [p for p in positions if p.get('status') == 'OPEN']
closed_positions = [p for p in positions if p.get('status') == 'CLOSED']

# F-010 FIX: отфильтровать позиции, открытые авто-входом с меткой ЖДАТЬ (баг старых версий).
# Эти сделки не должны были открываться (WR=35%) — искажают статистику.
_bogus_wait = [p for p in closed_positions
               if p.get('auto_opened') and 'ЖДАТЬ' in str(p.get('entry_label', ''))]
if _bogus_wait:
    closed_positions = [p for p in closed_positions if p not in _bogus_wait]

# BUG-017 FIX: single per-render cache for monitor_position() results.
# Previously the main monitor loop (tab1) and the CSV-export loop both called
# monitor_position() for every open position — doubling API requests each tick.
# _mon_cache is populated on first access and reused everywhere in this render.
_mon_cache: dict = {}  # {pos_id: mon_dict}


def _get_mon(pos: dict, exchange_name: str) -> dict | None:
    """Return cached monitor_position() result, computing it on first call."""
    pid = pos['id']
    if pid not in _mon_cache:
        result = monitor_position(pos, exchange_name)
        if result is not None:
            _mon_cache[pid] = result
    return _mon_cache.get(pid)


# Tabs — v38.2: restored Bybit tab; v38.4: added Diag tab
tab1, tab2, tab_phantom, tab3, tab_bybit, tab4, tab_diag = st.tabs([
                       f"📍 Открытые ({len(open_positions)})", 
                       f"📋 История ({len(closed_positions)})",
                       f"👻 Phantom",
                       f"📊 Портфель",
                       f"🔗 Bybit/Скачать",
                       f"📈 Performance (R8)",
                       f"🔍 Диагностика"])

with tab1:
    if not open_positions:
        st.info("📭 Нет открытых позиций. Добавьте через боковую панель.")
    else:
        # Dashboard metrics
        total_pnl = 0
        
        for pos in open_positions:
            with st.container():
                st.markdown("---")
                
                # Header
                dir_emoji = '🟢' if pos['direction'] == 'LONG' else '🔴'
                pair_name = f"{pos['coin1']}/{pos['coin2']}"
                
                # Monitor
                with st.spinner(f"Обновляю {pair_name}..."):
                    mon = _get_mon(pos, exchange)  # BUG-017 FIX: cached
                
                if mon is None:
                    st.error(f"❌ Не удалось получить данные для {pair_name}")
                    continue
                
                total_pnl += mon['pnl_pct']
                
                # v23.0: Track best P&L for trailing stop (with commission)
                current_best = pos.get('best_pnl_during_trade', pos.get('best_pnl', 0))
                new_best = mon.get('best_pnl', mon['pnl_pct'])
                if new_best > current_best:
                    pos['best_pnl_during_trade'] = new_best
                    pos['best_pnl'] = new_best
                    try:
                        all_pos = load_positions()
                        for p in all_pos:
                            if p['id'] == pos['id']:
                                p['best_pnl_during_trade'] = new_best
                                p['best_pnl'] = new_best
                        save_positions(all_pos)
                    except Exception:
                        pass
                
                # v35: Collect monitor messages for trade history
                _new_messages = []
                if mon.get('exit_signal'):
                    _msg_text = str(mon['exit_signal'])
                    # Shorten known messages
                    for _kw in ['MEAN REVERT', 'OVERSHOOT', 'STOP LOSS', 'TIMEOUT',
                                'ЛОЖНОЕ СХОЖДЕНИЕ', 'Grace period']:
                        if _kw in _msg_text:
                            _new_messages.append(_kw)
                            break
                    else:
                        if len(_msg_text) < 50:
                            _new_messages.append(_msg_text)
                for _qw in mon.get('quality_warnings', []):
                    _qw_short = str(_qw)[:60]
                    if 'HR ДРЕЙФ КРИТИЧЕСКИЙ' in _qw_short:
                        _new_messages.append('HR ДРЕЙФ КРИТИЧЕСКИЙ')
                    elif 'Hurst' in _qw_short and 'нет mean reversion' in _qw_short:
                        _new_messages.append('Hurst: нет MR')
                    elif 'P-value' in _qw_short:
                        _new_messages.append('P-value: коинт. ослабла')
                    elif 'Корреляция' in _qw_short:
                        _new_messages.append('Корреляция низкая')
                    elif 'НАПРАВЛЕНИЕ ИНВЕРТИРОВАНО' in _qw_short:
                        _new_messages.append('Dir ИНВЕРТИРОВАНО')
                    elif 'R6' in _qw_short:
                        _new_messages.append('R6: деградация')
                if mon.get('pnl_z_disagree'):
                    _new_messages.append('PnL/Z расхождение')
                
                if _new_messages:
                    try:
                        all_pos = load_positions()
                        for p in all_pos:
                            if p['id'] == pos['id']:
                                existing = p.get('monitor_messages', [])
                                for m in _new_messages:
                                    if m not in existing:
                                        existing.append(m)
                                # Keep last 20 messages
                                p['monitor_messages'] = existing[-20:]
                        save_positions(all_pos)
                    except Exception:
                        pass
                
                # Exit signal banner
                # v32: AUTO-EXIT — check and auto-close if criteria met
                # BUG-003 + BUG-008 FIX: save trailing flags BEFORE check_auto_exit so that
                # any flags set inside it (on this tick) are persisted immediately.
                # This covers two failure modes:
                #   BUG-008 — flags set in check_auto_exit() are lost after st.rerun()
                #             because load_positions() re-reads the file on the next render.
                #   BUG-003 — if close_position() raises, flags were already mutated in `pos`
                #             but never written to disk; next tick starts with stale state.
                # Solution: flush all trailing keys to file BEFORE calling check_auto_exit(),
                # so the file is always consistent regardless of what happens next.
                _TRAIL_KEYS = (
                    '_z_trail_activated', '_z_trail_peak',
                    '_tp_trail_activated', '_tp_trail_peak',
                    '_recovery_trail_activated', '_recovery_trail_peak',
                    'exit_phase',
                    # BUG-018 FIX: persist locked trail params
                    '_trail_params_locked', '_trail_act_locked', '_trail_dd_locked',
                )
                try:
                    _pre_all_p = load_positions()
                    _pre_dirty = False
                    for _pre_pp in _pre_all_p:
                        if _pre_pp['id'] == pos['id']:
                            for _tk in _TRAIL_KEYS:
                                if _tk in pos and _pre_pp.get(_tk) != pos[_tk]:
                                    _pre_pp[_tk] = pos[_tk]
                                    _pre_dirty = True
                            break
                    if _pre_dirty:
                        save_positions(_pre_all_p)
                except Exception:
                    pass

                _auto_close, _auto_reason = check_auto_exit(pos, mon)

                # BUG-003 + BUG-008 FIX: flush flags set by check_auto_exit() before
                # calling close_position() — if close_position() throws, the trailing
                # state is already on disk and the next tick will pick it up correctly.
                try:
                    _post_all_p = load_positions()
                    _post_dirty = False
                    for _post_pp in _post_all_p:
                        if _post_pp['id'] == pos['id']:
                            for _tk in _TRAIL_KEYS:
                                if _tk in pos and _post_pp.get(_tk) != pos[_tk]:
                                    _post_pp[_tk] = pos[_tk]
                                    _post_dirty = True
                            break
                    if _post_dirty:
                        save_positions(_post_all_p)
                except Exception:
                    pass

                if _auto_close:
                    st.warning(f"🤖 AUTO-EXIT: {_auto_reason}")
                    try:
                        close_position(
                            pos['id'], mon['price1_now'], mon['price2_now'],
                            mon.get('z_static', mon['z_now']),
                            _auto_reason,
                            exit_z_static=mon.get('z_static'))
                        st.success(f"✅ #{pos['id']} {pair_name} закрыта автоматически | P&L: {mon['pnl_pct']:+.2f}%")
                        
                        # v38.3: Mirror close to Bybit Demo
                        _bb_close = _bybit_close(pos['coin1'], pos['coin2'], pos['direction'],
                                                  mon['price1_now'], mon['price2_now'])
                        if _bb_close is not None:
                            if _bb_close.get('success'):
                                st.success(f"🔗 Bybit Demo: закрыта (AUTO) | slippage: {_bb_close.get('total_slippage_pct',0):.4f}%")
                            else:
                                st.warning(f"⚠️ Bybit Demo close ошибка: {_bb_close.get('error','?')}")
                        
                        # v33: Enhanced Telegram alert for auto-close
                        if (st.session_state.get('tg_enabled') and 
                            st.session_state.get('tg_alert_exits', True)):
                            try:
                                _tg_tok = st.session_state.get('tg_token', '')
                                _tg_cid = st.session_state.get('tg_chat_id', '')
                                if _tg_tok and _tg_cid:
                                    _pnl_emoji = '✅' if mon['pnl_pct'] > 0 else '❌'
                                    _close_msg = (
                                        f"{_pnl_emoji} <b>AUTO-CLOSE</b>\n"
                                        f"#{pos['id']} {pair_name} {pos['direction']}\n"
                                        f"P&L: {mon['pnl_pct']:+.2f}% | Best: {mon.get('best_pnl', 0):+.2f}%\n"
                                        f"Reason: {_auto_reason}\n"
                                        f"Hold: {mon['hours_in']:.1f}ч"
                                    )
                                    send_telegram(_tg_tok, _tg_cid, _close_msg)
                            except Exception:
                                pass
                        
                        st.rerun()
                    except Exception as ex:
                        st.error(f"❌ Ошибка авто-закрытия: {ex}")
                else:
                    # BUG-003 + BUG-008 FIX (safety net): trailing flags were already flushed
                    # to disk before check_auto_exit() above. This block is kept as a
                    # belt-and-suspenders fallback for any keys not covered by _TRAIL_KEYS,
                    # e.g. keys added in future versions of check_auto_exit().
                    try:
                        _all_p = load_positions()
                        for _pp in _all_p:
                            if _pp['id'] == pos['id']:
                                for _tk in ('_recovery_trail_activated', '_recovery_trail_peak',
                                            '_tp_trail_activated', '_tp_trail_peak',
                                            '_z_trail_activated', '_z_trail_peak'):
                                    if _tk in pos:
                                        _pp[_tk] = pos[_tk]
                                break
                        save_positions(_all_p)
                    except Exception:
                        pass
                
                if mon['exit_signal']:
                    if 'STOP' in mon['exit_signal'] or 'СРОЧН' in str(mon['exit_signal']):
                        st.error(mon['exit_signal'])
                    elif 'MEAN REVERT' in mon['exit_signal'] or 'OVERSHOOT' in mon['exit_signal']:
                        st.success(mon['exit_signal'])
                    else:
                        st.warning(mon['exit_signal'])
                    
                    # v30: Telegram exit alert
                    _tg_exit_key = f"_tg_exit_sent_{pos['id']}"
                    if (st.session_state.get('tg_enabled') and 
                        st.session_state.get('tg_alert_exits', True) and
                        not st.session_state.get(_tg_exit_key)):
                        try:
                            _tg_tok = st.session_state.get('tg_token', '')
                            _tg_cid = st.session_state.get('tg_chat_id', '')
                            if _tg_tok and _tg_cid:
                                _exit_msg = (
                                    f"📤 <b>Exit Signal</b> — {pair_name}\n"
                                    f"⏰ {now_msk().strftime('%H:%M МСК')}\n"
                                    f"📍 {pos['direction']} | Z: {pos['entry_z']:+.2f} → {mon['z_now']:+.2f}\n"
                                    f"💰 P&L: {mon['pnl_pct']:+.2f}%\n"
                                    f"⚠️ {mon['exit_signal']}"
                                )
                                send_telegram(_tg_tok, _tg_cid, _exit_msg)
                                st.session_state[_tg_exit_key] = True
                        except Exception:
                            pass
                
                # v24: R5 Smart Exit Signals panel
                smart_sigs = mon.get('smart_signals', [])
                smart_rec = mon.get('smart_recommendation', '')
                if smart_sigs:
                    with st.expander(f"🧠 Smart Exit: {smart_rec} ({len(smart_sigs)} сигнал{'ов' if len(smart_sigs) > 1 else ''})", expanded=mon.get('smart_urgency', 0) >= 2):
                        for sig in smart_sigs:
                            sig_type = sig.get('type', '')
                            sig_urg = sig.get('urgency', 0)
                            if sig_urg >= 3:
                                st.error(sig['message'])
                            elif sig_urg >= 2:
                                st.warning(sig['message'])
                            else:
                                st.info(sig['message'])
                
                # Header row
                dir_emoji_c1 = '🟢 LONG' if pos['direction'] == 'LONG' else '🔴 SHORT'
                dir_emoji_c2 = '🔴 SHORT' if pos['direction'] == 'LONG' else '🟢 LONG'
                # v35 FIX: Show exact entry time in header
                _entry_time_str = to_msk_full(pos.get('entry_time', ''))
                st.subheader(f"{dir_emoji} {pos['direction']} | {pair_name} | #{pos['id']} | ⏰ {_entry_time_str} МСК")
                st.caption(f"{pos['coin1']}: {dir_emoji_c1} | {pos['coin2']}: {dir_emoji_c2}")
                
                # v38.2: Show flags and TP/SL
                _flag_parts = []
                if pos.get('flag_bt'): _flag_parts.append(pos['flag_bt'])
                if pos.get('flag_wl'): _flag_parts.append(pos['flag_wl'])
                if pos.get('flag_nk'): _flag_parts.append(pos['flag_nk'])
                _tp_show = pos.get('pair_tp_pct', CFG('monitor', 'auto_tp_pct', 2.0))
                _sl_show = pos.get('pair_sl_pct', CFG('monitor', 'auto_sl_pct', -2.5))
                _flags_str = f"Flags: {' '.join(_flag_parts)}" if _flag_parts else "Flags: ✅"
                st.caption(f"🎯 TP={_tp_show:+.1f}% / SL={_sl_show:+.1f}% | {_flags_str}")
                
                # v30: Show trade basis — auto vs manual, signal type
                _is_auto = pos.get('auto_opened', False)
                _sig_type = pos.get('signal_type', '')
                _entry_lbl = pos.get('entry_label', '')
                if _is_auto or _sig_type:
                    _basis_parts = []
                    if _is_auto: _basis_parts.append("🤖 АВТО")
                    else: _basis_parts.append("👤 РУЧНОЙ")
                    if _sig_type: _basis_parts.append(f"📊 {_sig_type}")
                    if _entry_lbl: _basis_parts.append(_entry_lbl)
                    _ml_g = pos.get('ml_grade', '')
                    if _ml_g: _basis_parts.append(f"🧠 {_ml_g}")
                    st.caption(" | ".join(_basis_parts))
                
                # v4.0: P&L / Z disagreement warning
                if mon.get('pnl_z_disagree'):
                    st.warning(mon['pnl_z_warning'])
                
                # KPI row
                c1, c2, c3, c4, c5, c6 = st.columns(6)
                
                # v23.0: P&L with commission
                pnl_val = mon['pnl_pct']
                pnl_emoji = "🟢" if pnl_val > 0.01 else "🔴" if pnl_val < -0.01 else "⚪"
                c1.metric(
                    f"P&L {pnl_emoji} (−{COMMISSION_ROUND_TRIP_PCT():.1f}%)", 
                    f"{pnl_val:+.2f}%", 
                    delta=f"{pnl_val:+.2f}%",
                    delta_color="normal"
                )
                
                # v23.0: DUAL Z-score — Static (green, real position) vs Dynamic (gray, Kalman)
                z_s = mon.get('z_static', mon['z_now'])
                z_d = mon.get('z_dynamic', mon['z_now'])
                z_dr = mon.get('z_drift', 0)
                drift_warn = " ⚠️" if z_dr > 1.5 else ""
                c2.metric("Z Static 🟢", f"{z_s:+.2f}",
                         delta=f"Dynamic: {z_d:+.2f} | drift: {z_dr:.2f}{drift_warn}")
                
                c3.metric("HR", f"{mon['hr_now']:.4f}",
                         delta=f"вход: {mon['hr_entry']:.4f}")
                
                # v23.0: Price display with directional coloring
                p1_now = mon['price1_now']
                p1_entry = pos['entry_price1']
                p1_change = (p1_now - p1_entry) / p1_entry * 100 if p1_entry > 0 else 0
                p1_good = (pos['direction'] == 'LONG' and p1_change >= 0) or \
                          (pos['direction'] == 'SHORT' and p1_change <= 0)
                c4.metric(
                    f"{pos['coin1']} {'🟢' if pos['direction']=='LONG' else '🔴'}", 
                    f"${p1_now:.4f}",
                    delta=f"{p1_change:+.2f}% (вход: ${p1_entry:.4f})",
                    delta_color="normal" if p1_good else "inverse")
                
                p2_now = mon['price2_now']
                p2_entry = pos['entry_price2']
                p2_change = (p2_now - p2_entry) / p2_entry * 100 if p2_entry > 0 else 0
                p2_good = (pos['direction'] == 'LONG' and p2_change <= 0) or \
                          (pos['direction'] == 'SHORT' and p2_change >= 0)
                c5.metric(
                    f"{pos['coin2']} {'🔴' if pos['direction']=='LONG' else '🟢'}", 
                    f"${p2_now:.4f}",
                    delta=f"{p2_change:+.2f}% (вход: ${p2_entry:.4f})",
                    delta_color="normal" if p2_good else "inverse")
                
                c6.metric("В позиции", f"{mon['hours_in']:.0f}ч",
                         delta=f"HL: {mon['halflife_hours']:.0f}ч")
                
                # v23.0: Position sizing + best P&L row
                sz1, sz2, sz3 = st.columns(3)
                sz1.metric("💰 Размер", f"${pos.get('recommended_size', 100):.0f}")
                sz2.metric("📈 Best P&L", f"{mon.get('best_pnl', 0):+.2f}%")
                sz3.metric("📊 P&L gross", f"{mon.get('pnl_gross_pct', 0):+.2f}%",
                          delta=f"−{COMMISSION_ROUND_TRIP_PCT():.2f}% комиссия")
                
                # v3.0: Quality metrics row
                q1, q2, q3, q4 = st.columns(4)
                q1.metric("Hurst", f"{mon.get('hurst', 0.5):.3f}",
                         delta="🟢 MR" if mon.get('hurst', 0.5) < 0.45 else "🔴 No MR")
                q2.metric("P-value", f"{mon.get('pvalue', 1.0):.4f}",
                         delta="✅ Coint" if mon.get('pvalue', 1.0) < 0.05 else "⚠️ Weak")
                q3.metric("Корреляция ρ", f"{mon.get('correlation', 0):.3f}",
                         delta="🟢" if mon.get('correlation', 0) >= 0.5 else "⚠️")
                q4.metric("Z-window", f"{mon.get('z_window', 30)} баров")
                
                # v18: GARCH Z row
                if mon.get('z_garch') is not None:
                    gq1, gq2, gq3, gq4 = st.columns(4)
                    gq1.metric("Z GARCH", f"{mon.get('z_garch', 0):+.2f}",
                               f"vs std={mon.get('z_now',0):+.2f}")
                    vr = mon.get('garch_vol_ratio', 1.0)
                    gq2.metric("σ ratio", f"{vr:.2f}x",
                               "🔴 растёт" if mon.get('garch_var_expanding') else "✅ стабильна")
                    gq3.metric("HL часов", f"{mon.get('halflife_hours', 0):.1f}")
                    gq4.metric("Z-window", f"{mon.get('z_window', 30)} бар")
                
                # v20: Dynamic HR Drift Monitoring (P4 Roadmap)
                hr_entry = pos.get('entry_hr', 0)
                hr_now = mon.get('hr_now', hr_entry)
                if hr_entry > 0 and hr_now > 0:
                    hr_drift_pct = abs(hr_now - hr_entry) / hr_entry * 100
                    # Пункт 10 FIX: пороги из конфига (было hardcoded 15/20)
                    _hr_warn = float(CFG('monitor', 'hr_drift_warn_pct', 15))
                    _hr_crit = float(CFG('monitor', 'hr_drift_critical_pct', 40))
                    
                    if hr_drift_pct > 5:  # Only show if drift is significant
                        st.markdown("#### 📐 HR Drift Monitor")
                        hd1, hd2, hd3 = st.columns(3)
                        with hd1:
                            dr_emoji = ('✅' if hr_drift_pct < _hr_warn * 0.67
                                        else '🟡' if hr_drift_pct < _hr_warn
                                        else '🔴')
                            st.metric("HR дрейф", f"{dr_emoji} {hr_drift_pct:.1f}%",
                                     f"Entry: {hr_entry:.4f} → Now: {hr_now:.4f}")
                        with hd2:
                            # Calculate impact: how much spread changed due to HR drift alone
                            p2_now = mon.get('price2_now', pos.get('entry_price2', 1))
                            hr_impact = abs(hr_now - hr_entry) * p2_now
                            st.metric("Влияние на спред", f"{hr_impact:.4f}",
                                     "USD сдвиг от дрейфа HR")
                        with hd3:
                            if hr_drift_pct > _hr_warn:
                                st.metric("Ребаланс", "🔴 НУЖЕН",
                                         f"HR изменился на {hr_drift_pct:.0f}%")
                            elif hr_drift_pct > _hr_warn * 0.67:
                                st.metric("Ребаланс", "🟡 Рассмотрите",
                                         f"HR дрейфует")
                            else:
                                st.metric("Ребаланс", "✅ Не нужен", "Дрейф в норме")
                        
                        if hr_drift_pct > 100:
                            st.error(
                                f"🚨 **ОШИБКА ВВОДА HR?** Дрейф {hr_drift_pct:.0f}% — "
                                f"Entry={hr_entry:.4f}, Now={hr_now:.4f}. "
                                f"Вероятно HR был введён неверно при открытии позиции. "
                                f"**Проверьте и пересоздайте позицию с правильным HR!**")
                        elif hr_drift_pct > _hr_crit:
                            st.error(
                                f"🚨 **HR ДРЕЙФ КРИТИЧЕСКИЙ: {hr_drift_pct:.1f}%** "
                                f"(порог {_hr_crit:.0f}%). "
                                f"Entry HR={hr_entry:.4f}, текущий={hr_now:.4f}. "
                                f"Коинтеграция могла разрушиться. Рассмотрите закрытие.")
                        elif hr_drift_pct > _hr_warn:
                            st.warning(
                                f"⚠️ **HR дрейф {hr_drift_pct:.1f}%** "
                                f"(порог {_hr_warn:.0f}%): Entry={hr_entry:.4f}, "
                                f"Now={hr_now:.4f}. Ребалансируйте позицию или закройте.")
                
                # v3.0: Quality warnings
                for qw in mon.get('quality_warnings', []):
                    st.warning(qw)
                
                # v3.0: Entry readiness assessment
                qd = mon.get('quality_data', {})
                if qd:
                    ea = assess_entry_readiness(qd)
                    with st.expander("📋 Критерии входа (как в сканере)", expanded=False):
                        ec1, ec2 = st.columns(2)
                        with ec1:
                            st.markdown("**🟢 Обязательные:**")
                            for name, met, val in ea['mandatory']:
                                st.markdown(f"  {'✅' if met else '❌'} **{name}** → `{val}`")
                        with ec2:
                            st.markdown("**🔵 Желательные:**")
                            for name, met, val in ea['optional']:
                                st.markdown(f"  {'✅' if met else '⬜'} {name} → `{val}`")
                
                # Chart
                with st.expander("📈 Графики", expanded=False):
                    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                                       vertical_spacing=0.08,
                                       subplot_titles=['Z-Score (Static 🟢 / Dynamic ⚪)', 'Спред'],
                                       row_heights=[0.6, 0.4])
                    
                    ts = mon['timestamps']
                    
                    # v23.0: Static Z-score (green, solid) — real position
                    fig.add_trace(go.Scatter(
                        x=ts, y=mon.get('static_zscore_series', mon['zscore_series']),
                        name='Z Static', line=dict(color='#4caf50', width=2)
                    ), row=1, col=1)
                    
                    # v23.0: Dynamic Z-score (gray, dotted) — Kalman
                    fig.add_trace(go.Scatter(
                        x=ts, y=mon['zscore_series'],
                        name='Z Dynamic', line=dict(color='#9e9e9e', width=1, dash='dot')
                    ), row=1, col=1)
                    
                    fig.add_hline(y=0, line_dash='dash', line_color='gray', 
                                 opacity=0.5, row=1, col=1)
                    fig.add_hline(y=pos.get('exit_z_target', 0.5), 
                                 line_dash='dot', line_color='#4caf50',
                                 opacity=0.5, row=1, col=1)
                    fig.add_hline(y=-pos.get('exit_z_target', 0.5), 
                                 line_dash='dot', line_color='#4caf50',
                                 opacity=0.5, row=1, col=1)
                    
                    # Entry Z marker
                    entry_dt = datetime.fromisoformat(pos['entry_time'])
                    fig.add_trace(go.Scatter(
                        x=[entry_dt], y=[pos['entry_z']],
                        mode='markers', marker=dict(size=14, color='yellow',
                                                     symbol='star'),
                        name='Entry', showlegend=True
                    ), row=1, col=1)
                    
                    # Spread
                    fig.add_trace(go.Scatter(
                        x=ts, y=mon['spread'],
                        name='Spread', line=dict(color='#ffa726', width=1.5)
                    ), row=2, col=1)
                    
                    fig.update_layout(height=400, template='plotly_dark',
                                     showlegend=False,
                                     margin=dict(l=50, r=30, t=30, b=30))
                    st.plotly_chart(fig, width='stretch')
                
                # Close button + Exchange links
                col_close1, col_close2, col_close3 = st.columns([2, 2, 1])
                with col_close1:
                    # v35.1: Exchange trading links
                    _c1_sym = pos['coin1'].upper()
                    _c2_sym = pos['coin2'].upper()
                    _bybit_url1 = f"https://www.bybit.com/trade/usdt/{_c1_sym}USDT"
                    _bybit_url2 = f"https://www.bybit.com/trade/usdt/{_c2_sym}USDT"
                    _okx_url1 = f"https://www.okx.com/trade-swap/{_c1_sym.lower()}-usdt-swap"
                    _okx_url2 = f"https://www.okx.com/trade-swap/{_c2_sym.lower()}-usdt-swap"
                    if exchange == 'bybit':
                        st.markdown(f"🔗 Bybit: [{_c1_sym}]({_bybit_url1}) | [{_c2_sym}]({_bybit_url2})")
                    elif exchange == 'okx':
                        st.markdown(f"🔗 OKX: [{_c1_sym}]({_okx_url1}) | [{_c2_sym}]({_okx_url2})")
                    else:
                        st.markdown(f"🔗 [{_c1_sym} Bybit]({_bybit_url1}) | [{_c2_sym} Bybit]({_bybit_url2})")
                with col_close3:
                    if st.button(f"❌ Закрыть #{pos['id']}", key=f"close_{pos['id']}"):
                        close_position(
                            pos['id'], 
                            mon['price1_now'], mon['price2_now'],
                            mon['z_now'], 'MANUAL',
                            exit_z_static=mon.get('z_static')
                        )
                        st.success(f"Позиция #{pos['id']} закрыта | P&L: {mon['pnl_pct']:+.2f}% (после комиссии)")
                        # v38.3: Mirror close to Bybit Demo
                        _bb_close = _bybit_close(pos['coin1'], pos['coin2'], pos['direction'],
                                                  mon['price1_now'], mon['price2_now'])
                        if _bb_close is not None:
                            if _bb_close.get('success'):
                                st.success(f"🔗 Bybit Demo: закрыта (MANUAL) | slippage: {_bb_close.get('total_slippage_pct',0):.4f}%")
                            else:
                                st.warning(f"⚠️ Bybit Demo: {_bb_close.get('error','?')}")
                        st.rerun()
        
        # Total P&L
        st.markdown("---")
        st.metric("📊 Суммарный P&L (открытые)", f"{total_pnl:+.2f}%")
        
        # v5.2: FULL open positions CSV with live monitoring data
        open_rows = []
        for pos in open_positions:
            row = {
                '#': pos['id'],
                'Пара': f"{pos['coin1']}/{pos['coin2']}",
                'Dir': pos['direction'],
                'TF': pos['timeframe'],
                # v30: Trade basis
                'Основание': ('🤖' if pos.get('auto_opened') else '👤') + 
                             (f" {pos.get('signal_type','')}" if pos.get('signal_type') else '') +
                             (f" {pos.get('entry_label','')}" if pos.get('entry_label') else ''),
                'Entry_Z': pos['entry_z'],
                'Entry_HR': pos.get('entry_hr', 0),
                'Stop_Z': pos.get('stop_z', 4.0),
                'Entry_Time': pos['entry_time'][:16],
                'Entry_Price1': pos.get('entry_price1', 0),
                'Entry_Price2': pos.get('entry_price2', 0),
            }
            # Add live data if available
            try:
                mon = _get_mon(pos, exchange)  # BUG-017 FIX: cached
                if mon:
                    row.update({
                        'Current_Z': round(mon['z_now'], 4),
                        'Current_HR': round(mon['hr_now'], 4),
                        'P&L_%': round(mon['pnl_pct'], 4),
                        'Hours_In': round(mon['hours_in'], 1),
                        'HL_hours': round(mon['halflife_hours'], 1),
                        'Price1_Now': round(mon['price1_now'], 6),
                        'Price2_Now': round(mon['price2_now'], 6),
                        'Hurst': round(mon.get('hurst', 0.5), 4),
                        'Correlation': round(mon.get('correlation', 0), 4),
                        'P-value': round(mon.get('pvalue', 1.0), 6),
                        'Z_Window': mon.get('z_window', 30),
                        'Exit_Signal': mon.get('exit_signal', ''),
                        'Exit_Urgency': mon.get('exit_urgency', ''),
                        'Z_Toward_Zero': mon.get('z_towards_zero', False),
                        'PnL_Z_Disagree': mon.get('pnl_z_disagree', False),
                        'Quality_Warnings': '; '.join(mon.get('quality_warnings', [])),
                    })
            except Exception:
                pass
            open_rows.append(row)
        
        if open_rows:
            csv_open = pd.DataFrame(open_rows).to_csv(index=False)
            st.download_button("📥 Скачать открытые позиции (CSV)", csv_open,
                f"positions_open_{now_msk().strftime('%Y%m%d_%H%M')}.csv", "text/csv",
                key="open_pos_csv")
            
            # v20.1: Auto-save positions to disk every 10 minutes
            try:
                import os
                os.makedirs("position_exports", exist_ok=True)
                last_auto_save = st.session_state.get('_last_pos_save', 0)
                now_ts = time.time()
                if now_ts - last_auto_save > 600:  # 10 minutes
                    save_path = f"position_exports/positions_open_{now_msk().strftime('%Y%m%d_%H%M')}.csv"
                    pd.DataFrame(open_rows).to_csv(save_path, index=False)
                    st.session_state['_last_pos_save'] = now_ts
                    st.toast(f"💾 Позиции сохранены: {save_path}")
            except Exception:
                pass

with tab2:
    if not closed_positions:
        st.info("📭 Нет закрытых позиций")
    else:
        # Summary
        pnls = [float(p.get('pnl_pct', 0) or 0) for p in closed_positions]
        wins = [p for p in pnls if p > 0]
        
        sc1, sc2, sc3, sc4 = st.columns(4)
        sc1.metric("Сделок", len(closed_positions))
        sc2.metric("Win Rate", f"{len(wins)/len(closed_positions)*100:.0f}%" if closed_positions else "0%")
        sc3.metric("Total P&L", f"{sum(pnls):+.2f}%")
        sc4.metric("Avg P&L", f"{np.mean(pnls):+.2f}%" if pnls else "0%")
        
        # Table
        # v39: Build per-pair aggregate stats (total trades / net P&L)
        _pair_stats = {}
        for p in closed_positions:
            _pn = f"{p['coin1']}/{p['coin2']}"
            if _pn not in _pair_stats:
                _pair_stats[_pn] = {'n': 0, 'pnl': 0.0, 'wins': 0}
            _pair_stats[_pn]['n'] += 1
            _ppnl = float(p.get('pnl_pct', 0) or 0)
            _pair_stats[_pn]['pnl'] += _ppnl
            if _ppnl > 0:
                _pair_stats[_pn]['wins'] += 1
        # Also load from trade_history.csv for full picture
        try:
            _hist_all = load_trade_history()
            for h in _hist_all:
                _hn = f"{h.get('coin1','')}/{h.get('coin2','')}"
                if _hn and _hn not in _pair_stats:
                    _pair_stats[_hn] = {'n': 0, 'pnl': 0.0, 'wins': 0}
                if _hn and _hn in _pair_stats:
                    # Only add if not already counted from closed_positions
                    _hid = h.get('id', -1)
                    _already = any(cp['id'] == _hid for cp in closed_positions)
                    if not _already:
                        _pair_stats[_hn]['n'] += 1
                        _hpnl = float(h.get('pnl_pct', 0) or 0)
                        _pair_stats[_hn]['pnl'] += _hpnl
                        if _hpnl > 0:
                            _pair_stats[_hn]['wins'] += 1
        except Exception:
            pass
        
        rows = []
        for p in reversed(closed_positions):
            # v31: Trade basis with fallback from notes
            _basis = '👤' if not p.get('auto_opened') else '🤖'
            _sig = p.get('signal_type', '')
            _lbl = p.get('entry_label', '')
            if not _sig and not _lbl:
                _notes = str(p.get('notes', ''))
                if 'SIGNAL' in _notes: _sig = 'SIGNAL'
                elif 'READY' in _notes: _sig = 'READY'
                if 'ВХОД' in _notes: _lbl = '🟢ВХОД'
                elif 'УСЛОВНО' in _notes: _lbl = '🟡УСЛОВНО'
                elif 'СЛАБЫЙ' in _notes: _lbl = '🟡СЛАБЫЙ'
            if _sig: _basis += f" {_sig}"
            if _lbl and len(_lbl) < 15: _basis += f" {_lbl}"
            
            rows.append({
                '#': p['id'],
                'Пара': f"{p['coin1']}/{p['coin2']}",
                'Dir': p['direction'],
                'Пара (всего)': (lambda _pn: f"{_pair_stats.get(_pn, {}).get('n', 0)} сд / {_pair_stats.get(_pn, {}).get('pnl', 0):+.1f}%")(f"{p['coin1']}/{p['coin2']}"),
                'Основание': _basis,
                'BT': p.get('flag_bt', '') or ('❌BT' if 'BT' in str(p.get('notes','')) and '❌' in str(p.get('notes','')) else ('⚠️BT' if '⚠️BT' in str(p.get('notes','')) else '')),
                'WL': p.get('flag_wl', '') or ('⚠️WL' if '⚠️WL' in str(p.get('notes','')) else ''),
                'NK': p.get('flag_nk', '') or ('⚠️NK' if '⚠️NK' in str(p.get('notes','')) else ('❌NK' if '❌NK' in str(p.get('notes','')) else '')),
                'Entry Z': f"{p['entry_z']:+.2f}",
                'Exit Z': f"{p.get('exit_z', 0):+.2f}",
                'P&L %': f"{p.get('pnl_pct', 0):+.2f}",
                'TP/SL': f"+{p.get('pair_tp_pct', CFG('monitor', 'auto_tp_pct', 2.0))}/-{abs(p.get('pair_sl_pct', CFG('monitor', 'auto_sl_pct', -2.5)))}",
                'Причина': p.get('exit_reason', ''),
                'Вход МСК': to_msk_full(p.get('entry_time', '')),
                'Выход МСК': to_msk_full(p.get('exit_time', '')),
            })
        st.dataframe(pd.DataFrame(rows).astype(str), width='stretch', hide_index=True)
        
        # v30: 3.3 Pattern Analysis
        try:
            from config_loader import pattern_analysis, pattern_summary
            _pa = pattern_analysis()
            if not _pa.get('error') and _pa.get('n_trades', 0) >= 3:
                with st.expander(f"🔬 Pattern Analysis ({_pa['n_trades']} сделок)", expanded=False):
                    pa1, pa2, pa3 = st.columns(3)
                    
                    # Direction
                    with pa1:
                        st.markdown("**По направлению**")
                        for d, v in _pa.get('by_direction', {}).items():
                            if v['n'] > 0:
                                _e = '🟢' if v['avg'] > 0 else '🔴'
                                st.caption(f"{_e} {d}: {v['n']} шт, WR={v['wr']:.0f}%, avg={v['avg']:+.2f}%")
                    
                    # Z-range
                    with pa2:
                        st.markdown("**По Z-score при входе**")
                        for rng, v in _pa.get('by_z_range', {}).items():
                            _e = '🟢' if v['avg'] > 0 else '🔴'
                            st.caption(f"{_e} Z={rng}: {v['n']} шт, WR={v['wr']:.0f}%, avg={v['avg']:+.2f}%")
                    
                    # Hold time
                    with pa3:
                        st.markdown("**По времени удержания**")
                        for h, v in _pa.get('by_hold', {}).items():
                            _e = '🟢' if v['avg'] > 0 else '🔴'
                            st.caption(f"{_e} {h}: {v['n']} шт, WR={v['wr']:.0f}%, avg={v['avg']:+.2f}%")
                    
                    # Auto vs Manual
                    _avm = _pa.get('auto_vs_manual', {})
                    if _avm.get('auto', {}).get('n', 0) > 0 and _avm.get('manual', {}).get('n', 0) > 0:
                        st.markdown("**🤖 Авто vs 👤 Ручной**")
                        _a = _avm['auto']
                        _m = _avm['manual']
                        st.caption(f"🤖 Авто: {_a['n']} шт, WR={_a['wr']:.0f}%, avg={_a['avg']:+.2f}% | "
                                  f"👤 Ручной: {_m['n']} шт, WR={_m['wr']:.0f}%, avg={_m['avg']:+.2f}%")
                    
                    # Winner/Loser profile
                    wp = _pa.get('winner_profile')
                    lp = _pa.get('loser_profile')
                    if wp and lp:
                        st.markdown("**Профиль сделок**")
                        st.caption(f"✅ Профитные: avg |Z|={wp['avg_z']}, hold={wp['avg_hold']:.1f}ч")
                        st.caption(f"❌ Убыточные: avg |Z|={lp['avg_z']}, hold={lp['avg_hold']:.1f}ч")
                    
                    # Top pairs
                    _bp = _pa.get('by_pair', {})
                    if _bp:
                        st.markdown("**Топ пары**")
                        for pair, v in list(_bp.items())[:5]:
                            _e = '🟢' if v['total'] > 0 else '🔴'
                            st.caption(f"{_e} {pair}: {v['n']} шт, total={v['total']:+.2f}%, WR={v['wr']:.0f}%")
        except Exception:
            pass
        
        # v31: Pattern Analysis by Signal Basis (local, from history)
        if len(closed_positions) >= 3:
            with st.expander("🔬 Pattern Analysis по основанию сделки", expanded=False):
                # Analyze by signal type (SIGNAL vs READY)
                _by_sig = {}
                _by_readiness = {}
                for cp in closed_positions:
                    _pnl = float(cp.get('pnl_pct', 0) or 0)
                    # Signal type
                    _st = cp.get('signal_type', '')
                    if not _st:
                        _notes = str(cp.get('notes', ''))
                        if 'SIGNAL' in _notes: _st = 'SIGNAL'
                        elif 'READY' in _notes: _st = 'READY'
                        else: _st = 'UNKNOWN'
                    if _st not in _by_sig: _by_sig[_st] = []
                    _by_sig[_st].append(_pnl)
                    # Entry readiness
                    _er = cp.get('entry_label', '')
                    if not _er:
                        _notes = str(cp.get('notes', ''))
                        if 'ВХОД' in _notes: _er = '🟢 ВХОД'
                        elif 'УСЛОВНО' in _notes: _er = '🟡 УСЛОВНО'
                        elif 'СЛАБЫЙ' in _notes: _er = '🟡 СЛАБЫЙ'
                        else: _er = 'НЕ УКАЗАНО'
                    if _er not in _by_readiness: _by_readiness[_er] = []
                    _by_readiness[_er].append(_pnl)
                
                bs1, bs2 = st.columns(2)
                with bs1:
                    st.markdown("**📊 По типу сигнала**")
                    for sig, pnls in sorted(_by_sig.items()):
                        n = len(pnls)
                        if n == 0: continue
                        wr = sum(1 for p in pnls if p > 0) / n * 100
                        avg = np.mean(pnls)
                        _e = '🟢' if avg > 0 else '🔴'
                        st.caption(f"{_e} {sig}: {n} шт, WR={wr:.0f}%, avg={avg:+.2f}%")
                with bs2:
                    st.markdown("**🎯 По готовности входа**")
                    for er, pnls in sorted(_by_readiness.items()):
                        n = len(pnls)
                        if n == 0: continue
                        wr = sum(1 for p in pnls if p > 0) / n * 100
                        avg = np.mean(pnls)
                        _e = '🟢' if avg > 0 else '🔴'
                        st.caption(f"{_e} {er}: {n} шт, WR={wr:.0f}%, avg={avg:+.2f}%")
        
        # v5.1: CSV export with date in filename
        csv_history = pd.DataFrame(rows).to_csv(index=False)
        # Date range from trades
        dates = [p.get('exit_time', '')[:10] for p in closed_positions if p.get('exit_time')]
        date_suffix = dates[-1] if dates else now_msk().strftime('%Y-%m-%d')
        st.download_button("📥 Скачать историю сделок (CSV)", csv_history,
                          f"trades_history_{date_suffix}_{now_msk().strftime('%H%M')}.csv", "text/csv")

# ═══════════════════════════════════════════════════════
# TAB PHANTOM: 👻 Post-close tracking (v23.0)
# ═══════════════════════════════════════════════════════
with tab_phantom:
    st.markdown("### 👻 Phantom Tracking (v33.0)")
    _phantom_h = CFG('monitor', 'phantom_track_hours', 12)
    st.caption(f"Отслеживание пар {_phantom_h}ч после закрытия — показывает, не вышли ли вы слишком рано")
    
    # Find positions with active phantom tracking
    phantom_positions = [p for p in positions 
                        if isinstance(p, dict)
                        and p.get('status') == 'CLOSED' 
                        and p.get('phantom_track_until')]
    
    active_phantoms = []
    expired_phantoms = []
    for p in phantom_positions:
        try:
            track_until = datetime.fromisoformat(p['phantom_track_until'])
            if track_until.tzinfo is None:
                track_until = track_until.replace(tzinfo=MSK)
            if now_msk() < track_until:
                active_phantoms.append(copy.deepcopy(p))  # BUG-010 FIX: copy so phantom updates don't mutate the original positions dict
            else:
                expired_phantoms.append(p)
        except Exception:
            expired_phantoms.append(p)
    
    if not phantom_positions:
        st.info(f"📭 Нет фантомных позиций. Закройте сделку — монитор будет следить ещё {_phantom_h}ч.")
    else:
        # v33: Phantom CSV export button
        if active_phantoms or expired_phantoms:
            phantom_rows = []
            for ph in active_phantoms + expired_phantoms:
                exit_pnl = float(ph.get('pnl_pct', 0) or 0)
                phantom_max = float(ph.get('phantom_max_pnl', exit_pnl) or exit_pnl)
                phantom_last = float(ph.get('phantom_last_pnl', exit_pnl) or exit_pnl)
                left_on_table = max(0, phantom_max - exit_pnl)
                best_during = float(ph.get('best_pnl_during_trade', ph.get('best_pnl', 0)) or 0)
                
                # Calculate hours tracked
                try:
                    _entry_dt = datetime.fromisoformat(str(ph.get('entry_time', '')))
                    _exit_dt = datetime.fromisoformat(str(ph.get('exit_time', '')))
                    _hours_in = (_exit_dt - _entry_dt).total_seconds() / 3600
                except Exception:
                    _hours_in = 0
                
                phantom_rows.append({
                    'Pair': f"{ph['coin1']}/{ph['coin2']}",
                    'Direction': ph['direction'],
                    'Exit_PnL': round(exit_pnl, 3),
                    'Best_During': round(best_during, 3),
                    'Phantom_Max': round(phantom_max, 3),
                    'Phantom_Last': round(phantom_last, 3),
                    'Left_On_Table': round(left_on_table, 3),
                    'Exit_Reason': ph.get('exit_reason', ''),
                    'Entry_Time': ph.get('entry_time', ''),
                    'Exit_Time': ph.get('exit_time', ''),
                    'Hours_In_Trade': round(_hours_in, 1),
                    'Entry_Z': round(float(ph.get('entry_z', 0) or 0), 2),
                    'Size_USD': ph.get('recommended_size', 100),
                })
            
            df_phantom = pd.DataFrame(phantom_rows)
            
            # Summary metrics
            if phantom_rows:
                total_left = sum(r['Left_On_Table'] for r in phantom_rows)
                avg_left = total_left / len(phantom_rows) if phantom_rows else 0
                n_cut_short = sum(1 for r in phantom_rows if r['Left_On_Table'] > 0.2)
                
                pm1, pm2, pm3, pm4 = st.columns(4)
                pm1.metric("Всего фантомов", len(phantom_rows))
                pm2.metric("💸 Упущено всего", f"+{total_left:.2f}%")
                pm3.metric("Avg упущено", f"+{avg_left:.2f}%")
                pm4.metric("Рано закрыто", f"{n_cut_short}/{len(phantom_rows)}")
            
            csv_data = df_phantom.to_csv(index=False)
            st.download_button(
                "📥 Скачать Phantom CSV",
                csv_data,
                f"phantom_{now_msk().strftime('%Y%m%d_%H%M')}.csv",
                "text/csv",
                key="phantom_csv_btn")
            
            st.markdown("---")
        # Update phantom tracking for active phantoms
        for ph in active_phantoms:
            try:
                pair_name = f"{ph['coin1']}/{ph['coin2']}"
                with st.spinner(f"Обновляю phantom {pair_name}..."):
                    df1 = fetch_prices(exchange, ph['coin1'], ph['timeframe'], 10)
                    df2 = fetch_prices(exchange, ph['coin2'], ph['timeframe'], 10)
                    if df1 is not None and df2 is not None:
                        p1_now = float(df1['c'].values[-1])
                        p2_now = float(df2['c'].values[-1])
                        hr = ph['entry_hr']
                        r1 = (p1_now - ph['entry_price1']) / ph['entry_price1']
                        r2 = (p2_now - ph['entry_price2']) / ph['entry_price2']
                        if ph['direction'] == 'LONG':
                            raw = r1 - hr * r2
                        else:
                            raw = -r1 + hr * r2
                        phantom_pnl = round(raw / (1 + abs(hr)) * 100 - COMMISSION_ROUND_TRIP_PCT(), 3)
                        
                        # Update phantom fields
                        all_pos = load_positions()
                        for pp in all_pos:
                            if pp['id'] == ph['id']:
                                pp['phantom_last_pnl'] = phantom_pnl
                                pp['phantom_last_check'] = now_msk().isoformat()
                                _pm = float(pp.get('phantom_max_pnl') or -999)
                                if phantom_pnl > _pm:
                                    pp['phantom_max_pnl'] = phantom_pnl
                                _pn = float(pp.get('phantom_min_pnl') or 999)
                                if phantom_pnl < _pn:
                                    pp['phantom_min_pnl'] = phantom_pnl
                                ph.update(copy.deepcopy(pp))  # BUG-010 FIX: deepcopy — don't let ph.update() overwrite
                                # the original positions entry with stale phantom fields
                        save_positions(all_pos)
            except Exception:
                pass
        
        # Display
        for ph in active_phantoms + expired_phantoms[:5]:
            is_active = ph in active_phantoms
            pair_name = f"{ph['coin1']}/{ph['coin2']}"
            exit_pnl = float(ph.get('pnl_pct', 0) or 0)
            phantom_max = float(ph.get('phantom_max_pnl', exit_pnl) or exit_pnl)
            phantom_last = float(ph.get('phantom_last_pnl', exit_pnl) or exit_pnl)
            left_on_table = max(0, phantom_max - exit_pnl) if phantom_max else 0
            
            status_emoji = "👻" if is_active else "💀"
            with st.container():
                st.markdown(f"#### {status_emoji} {pair_name} | #{ph['id']} | {ph['direction']}")
                h1, h2, h3, h4 = st.columns(4)
                h1.metric("P&L при выходе", f"{exit_pnl:+.2f}%")
                h2.metric("Phantom Last", f"{phantom_last:+.2f}%",
                          delta=f"{phantom_last - exit_pnl:+.2f}% vs выход")
                h3.metric("Phantom MAX", f"{phantom_max:+.2f}%",
                          delta=f"+{left_on_table:.2f}% упущено" if left_on_table > 0.1 else "✅ не упущено")
                h4.metric("Best во время сделки", f"{float(ph.get('best_pnl_during_trade', ph.get('best_pnl', 0)) or 0):+.2f}%")
                
                if is_active:
                    try:
                        track_until = datetime.fromisoformat(ph['phantom_track_until'])
                        if track_until.tzinfo is None:
                            track_until = track_until.replace(tzinfo=MSK)
                        remaining = (track_until - now_msk()).total_seconds() / 3600
                        st.caption(f"⏱ Осталось отслеживать: {remaining:.1f}ч | Причина выхода: {ph.get('exit_reason', '')}")
                    except Exception:
                        pass
                st.markdown("---")

# ═══════════════════════════════════════════════════════
# TAB 3: PORTFOLIO RISK MANAGER (v19.0)
# ═══════════════════════════════════════════════════════
with tab3:
    if not open_positions:
        st.info("📭 Нет открытых позиций для анализа портфеля.")
    else:
        st.markdown("### 📊 Portfolio Risk Manager v2.0")
        
        # === 1. Collect all monitoring data upfront ===
        # BUG-017 FIX: reuse _mon_cache populated in tab1 — no redundant API calls
        for pos in open_positions:
            try:
                _get_mon(pos, exchange)
            except Exception:
                pass
        mon_cache = _mon_cache  # alias for rest of portfolio tab
        
        # === 2. Portfolio summary metrics ===
        total_pnl_port = sum(m['pnl_pct'] for m in mon_cache.values())
        n_pos = len(open_positions)
        n_profit = sum(1 for m in mon_cache.values() if m['pnl_pct'] > 0)
        n_loss = sum(1 for m in mon_cache.values() if m['pnl_pct'] < 0)
        
        pc1, pc2, pc3, pc4 = st.columns(4)
        pc1.metric("Позиций", n_pos)
        pc2.metric("Совокупный P&L", f"{total_pnl_port:+.3f}%")
        pc3.metric("Прибыльных", f"{n_profit}/{n_pos}",
                  f"{n_profit/n_pos*100:.0f}%" if n_pos > 0 else "—")
        avg_hours = sum(pos.get('hours_in', 0) for pos in open_positions) / n_pos if n_pos > 0 else 0
        pc4.metric("Ср. время в позиции", f"{avg_hours:.1f}ч")
        
        # === 3. Coin exposure map ===
        st.markdown("#### 🪙 Экспозиция по монетам")
        coin_exposure = {}
        for pos in open_positions:
            c1, c2 = pos['coin1'], pos['coin2']
            d = pos['direction']
            for coin, coin_dir in [(c1, d), (c2, 'SHORT' if d == 'LONG' else 'LONG')]:
                if coin not in coin_exposure:
                    coin_exposure[coin] = {'long': 0, 'short': 0, 'pairs': [], 'pnl': 0.0}
                if coin_dir == 'LONG':
                    coin_exposure[coin]['long'] += 1
                else:
                    coin_exposure[coin]['short'] += 1
                coin_exposure[coin]['pairs'].append(f"{c1}/{c2}")
                mon = mon_cache.get(pos['id'])
                if mon:
                    coin_exposure[coin]['pnl'] += mon['pnl_pct'] / 2  # Split P&L between legs
        
        for coin, data in coin_exposure.items():
            data['net'] = data['long'] - data['short']
            data['total'] = data['long'] + data['short']
        
        sorted_coins = sorted(coin_exposure.items(), key=lambda x: x[1]['total'], reverse=True)
        
        # Concentration metric
        max_coin = sorted_coins[0] if sorted_coins else ('—', {'total': 0})
        max_exposure_pct = max_coin[1]['total'] / (n_pos * 2) * 100 if n_pos > 0 else 0
        
        # Exposure table
        coin_rows = []
        for coin, data in sorted_coins:
            conflict = '🚨 КОНФЛИКТ' if data['long'] > 0 and data['short'] > 0 else ''
            pct_of_port = data['total'] / (n_pos * 2) * 100 if n_pos > 0 else 0
            bar = '█' * int(pct_of_port / 5) + '░' * (20 - int(pct_of_port / 5))
            coin_rows.append({
                'Монета': coin,
                'LONG': data['long'],
                'SHORT': data['short'],
                'Всего': data['total'],
                'Net': f"+{data['net']}" if data['net'] > 0 else str(data['net']),
                '% порт.': f"{pct_of_port:.0f}%",
                'P&L': f"{data['pnl']:+.3f}%",
                'Конфликт': conflict,
                'Пары': ', '.join(set(data['pairs'])),
            })
        if coin_rows:
            st.dataframe(pd.DataFrame(coin_rows).astype(str), width='stretch', hide_index=True)
        
        # === 4. RISK LIMITS CHECK ===
        st.markdown("#### ⚠️ Лимиты риска")
        
        MAX_POSITIONS = CFG('monitor', 'max_positions', 10)
        MAX_COIN_EXPOSURE = CFG('monitor', 'max_coin_exposure', 4)
        MAX_CONCENTRATION_PCT = 40  # max % of portfolio in one coin
        
        lc1, lc2, lc3 = st.columns(3)
        
        with lc1:
            pos_ok = n_pos <= MAX_POSITIONS
            st.metric(
                "Позиций", f"{n_pos}/{MAX_POSITIONS}",
                "✅ OK" if pos_ok else "🔴 ПРЕВЫШЕН",
                delta_color="normal" if pos_ok else "inverse"
            )
        
        with lc2:
            max_c = max_coin[1]['total'] if sorted_coins else 0
            coin_ok = max_c <= MAX_COIN_EXPOSURE
            st.metric(
                f"Макс на монету ({max_coin[0]})", f"{max_c}/{MAX_COIN_EXPOSURE}",
                "✅ OK" if coin_ok else "🔴 ПРЕВЫШЕН",
                delta_color="normal" if coin_ok else "inverse"
            )
        
        with lc3:
            conc_ok = max_exposure_pct <= MAX_CONCENTRATION_PCT
            st.metric(
                "Концентрация", f"{max_exposure_pct:.0f}%/{MAX_CONCENTRATION_PCT}%",
                "✅ OK" if conc_ok else "🔴 ПРЕВЫШЕНА",
                delta_color="normal" if conc_ok else "inverse"
            )
        
        # Warnings
        warnings_found = False
        for coin, data in sorted_coins:
            if data['total'] >= MAX_COIN_EXPOSURE:
                st.error(
                    f"🚨 **{coin}** в {data['total']} позициях (лимит: {MAX_COIN_EXPOSURE}). "
                    f"При обвале {coin} на 10% ВСЕ {data['total']} позиции пострадают! "
                    f"**Закройте {data['total'] - MAX_COIN_EXPOSURE + 1} наименее прибыльную.**")
                warnings_found = True
            elif data['total'] >= 2:
                st.warning(f"⚠️ **{coin}** в {data['total']} позициях ({data['long']}L/{data['short']}S)")
                warnings_found = True
            
            if data['long'] > 0 and data['short'] > 0:
                st.error(
                    f"🚨 **{coin}** КОНФЛИКТ: LONG×{data['long']} + SHORT×{data['short']} "
                    f"одновременно → хеджирование самого себя!")
                warnings_found = True
        
        if not warnings_found:
            st.success("✅ Портфель в пределах лимитов.")
        
        # === 5. Position P&L table ===
        st.markdown("#### 📈 P&L по позициям")
        pnl_data = []
        for pos in open_positions:
            pair = f"{pos['coin1']}/{pos['coin2']}"
            mon = mon_cache.get(pos['id'])
            if mon:
                hours_in = pos.get('hours_in', 0)
                pnl_data.append({
                    '#': pos['id'],
                    'Пара': pair,
                    'Dir': pos['direction'],
                    'Entry Z': f"{mon['z_entry']:+.2f}",
                    'Now Z': f"{mon['z_now']:+.2f}",
                    'Z Static': f"{mon.get('z_static', mon['z_now']):+.2f}",
                    'P&L': f"{mon['pnl_pct']:+.3f}%",
                    'Z→0': '✅' if mon['z_towards_zero'] else '❌',
                    'Часов': f"{hours_in:.1f}",
                    'Сигнал': (mon.get('exit_signal') or '—')[:35],
                })
        if pnl_data:
            st.dataframe(pd.DataFrame(pnl_data).astype(str), width='stretch', hide_index=True)
        
        # v31: Close buttons per position
        st.markdown("#### ❌ Закрытие позиций")
        close_cols = st.columns(min(len(open_positions) + 1, 6))
        
        for i, pos in enumerate(open_positions):
            col_idx = i % (len(close_cols) - 1) if len(close_cols) > 1 else 0
            mon = mon_cache.get(pos['id'])
            if mon:
                with close_cols[col_idx]:
                    pair = f"{pos['coin1']}/{pos['coin2']}"
                    pnl_str = f"{mon['pnl_pct']:+.2f}%"
                    if st.button(f"❌ #{pos['id']} {pair} ({pnl_str})", 
                                key=f"portfolio_close_{pos['id']}"):
                        close_position(pos['id'], mon['price1_now'], mon['price2_now'],
                                      mon['z_now'], 'MANUAL (портфель)',
                                      exit_z_static=mon.get('z_static'))
                        st.success(f"✅ Закрыта #{pos['id']} {pair} | P&L: {pnl_str}")
                        # v38.3: Mirror close to Bybit Demo
                        _bb_close = _bybit_close(pos['coin1'], pos['coin2'], pos['direction'],
                                                  mon['price1_now'], mon['price2_now'])
                        if _bb_close is not None and not _bb_close.get('success'):
                            st.warning(f"⚠️ Bybit Demo: {_bb_close.get('error','?')}")
                        st.rerun()
        
        # v31: Close ALL button
        if len(open_positions) > 1:
            with close_cols[-1]:
                if st.button("🔴 ЗАКРЫТЬ ВСЕ", key="close_all_portfolio", type="primary"):
                    st.session_state['_confirm_close_all'] = True
            
            if st.session_state.get('_confirm_close_all'):
                st.warning("⚠️ **Вы уверены?** Это закроет ВСЕ открытые позиции!")
                cc1, cc2 = st.columns(2)
                with cc1:
                    if st.button("✅ ДА, закрыть все", key="confirm_close_all"):
                        closed_count = 0
                        for pos in open_positions:
                            mon = mon_cache.get(pos['id'])
                            if mon:
                                close_position(pos['id'], mon['price1_now'], mon['price2_now'],
                                              mon['z_now'], 'CLOSE ALL',
                                              exit_z_static=mon.get('z_static'))
                                # v38.3: Mirror close to Bybit Demo
                                _bybit_close(pos['coin1'], pos['coin2'], pos['direction'],
                                             mon['price1_now'], mon['price2_now'])
                                closed_count += 1
                        st.session_state['_confirm_close_all'] = False
                        st.success(f"✅ Закрыто {closed_count} позиций")
                        st.rerun()
                with cc2:
                    if st.button("❌ Отмена", key="cancel_close_all"):
                        st.session_state['_confirm_close_all'] = False
                        st.rerun()
        
        # === 6. Quick recommendations ===
        st.markdown("#### 💡 Рекомендации")
        recs = []
        
        # Find worst position
        worst_pos = None
        worst_pnl = 0
        for pos in open_positions:
            mon = mon_cache.get(pos['id'])
            if mon and mon['pnl_pct'] < worst_pnl:
                worst_pnl = mon['pnl_pct']
                worst_pos = pos
        
        if worst_pos and worst_pnl < -0.5:
            recs.append(f"🔴 Худшая позиция: **{worst_pos['coin1']}/{worst_pos['coin2']}** "
                       f"(P&L={worst_pnl:+.3f}%). Рассмотрите закрытие.")
        
        # Exit signals
        exits = []
        for pos in open_positions:
            mon = mon_cache.get(pos['id'])
            if mon and mon.get('exit_signal'):
                exits.append(f"**{pos['coin1']}/{pos['coin2']}**: {mon['exit_signal'][:40]}")
        if exits:
            recs.append(f"📍 Сигналы выхода: " + "; ".join(exits))
        
        # Concentration
        for coin, data in sorted_coins:
            if data['total'] >= 3:
                # Find least profitable pair with this coin
                least_profit = None
                least_pnl = 999
                for pos in open_positions:
                    if pos['coin1'] == coin or pos['coin2'] == coin:
                        mon = mon_cache.get(pos['id'])
                        if mon and mon['pnl_pct'] < least_pnl:
                            least_pnl = mon['pnl_pct']
                            least_profit = pos
                if least_profit:
                    recs.append(
                        f"⚠️ Для снижения экспозиции на **{coin}** закройте "
                        f"**{least_profit['coin1']}/{least_profit['coin2']}** "
                        f"(наименее прибыльная: {least_pnl:+.3f}%)")
        
        if recs:
            for r in recs:
                st.markdown(r)
        else:
            st.success("✅ Нет критических рекомендаций. Портфель выглядит здоровым.")
        
        # === 7. Portfolio Download ===
        st.markdown("#### 📥 Экспорт портфеля")
        portfolio_rows = []
        for pos in open_positions:
            mon = mon_cache.get(pos['id'])
            portfolio_rows.append({
                '#': pos['id'],
                'Пара': f"{pos['coin1']}/{pos['coin2']}",
                'Dir': pos['direction'],
                'TF': pos.get('timeframe', '4h'),
                'Entry_Z': pos.get('entry_z', 0),
                'Current_Z': mon['z_now'] if mon else '',
                'Entry_HR': pos.get('entry_hr', 0),
                'Current_HR': mon['hr_now'] if mon else '',
                'HR_Drift_%': round(abs(mon['hr_now'] - pos.get('entry_hr', 0)) / max(0.0001, pos.get('entry_hr', 0)) * 100, 1) if mon else '',
                'P&L_%': round(mon['pnl_pct'], 4) if mon else '',
                'Hours_In': round(mon['hours_in'], 1) if mon else '',
                'HL_hours': round(mon.get('halflife_hours', 0), 1) if mon else '',
                'Hurst': round(mon.get('hurst', 0.5), 3) if mon else '',
                'P-value': round(mon.get('pvalue', 1.0), 4) if mon else '',
                'Z_Toward_Zero': mon.get('z_towards_zero', '') if mon else '',
                'Exit_Signal': (mon.get('exit_signal', '') or '')[:40] if mon else '',
                'Entry_Time': pos.get('entry_time', ''),
                'Entry_P1': pos.get('entry_price1', ''),
                'Entry_P2': pos.get('entry_price2', ''),
                'Now_P1': mon.get('price1_now', '') if mon else '',
                'Now_P2': mon.get('price2_now', '') if mon else '',
            })
        if portfolio_rows:
            portfolio_df = pd.DataFrame(portfolio_rows)
            csv_portfolio = portfolio_df.to_csv(index=False)
            
            dl1, dl2 = st.columns(2)
            with dl1:
                st.download_button("📥 Портфель (CSV)", csv_portfolio,
                    f"portfolio_{now_msk().strftime('%Y%m%d_%H%M')}.csv", "text/csv",
                    key="portfolio_csv_btn")
            with dl2:
                # Also auto-save to disk
                try:
                    import os
                    os.makedirs("position_exports", exist_ok=True)
                    pf_path = f"position_exports/portfolio_{now_msk().strftime('%Y%m%d_%H%M')}.csv"
                    portfolio_df.to_csv(pf_path, index=False)
                    st.caption(f"💾 Сохранено: {pf_path}")
                except Exception:
                    pass
        
        # v35: Volatility Regime Display
        st.markdown("---")
        st.markdown("### 🌡️ Volatility Regime (v35)")
        try:
            from config_loader import check_volatility_regime
            # Try to fetch BTC closes
            _btc_df = fetch_prices(exchange, 'BTC', '1h', 100)
            if _btc_df is not None:
                _btc_closes = _btc_df['c'].values
                _vol_info = check_volatility_regime(_btc_closes)
                _vol_emoji = {'NORMAL': '🟢', 'ELEVATED': '🟡', 'EXTREME': '🔴'}.get(_vol_info['regime'], '⚪')
                vc1, vc2, vc3 = st.columns(3)
                vc1.metric("Режим", f"{_vol_emoji} {_vol_info['regime']}")
                vc2.metric("BTC ATR%", f"{_vol_info['atr_pct']:.2f}%")
                vc3.metric("SL/Size мульт.", f"SL x{_vol_info['sl_mult']:.1f} | Size x{_vol_info['size_mult']:.1f}")
                if _vol_info['block_entries']:
                    st.error("🛑 EXTREME VOLATILITY — новые входы заблокированы!")
                elif _vol_info['regime'] == 'ELEVATED':
                    st.warning("⚠️ Повышенная волатильность — размеры уменьшены, SL расширен")
            else:
                st.info("Не удалось загрузить данные BTC для определения режима волатильности")
        except Exception as _ve:
            st.caption(f"Volatility regime: {_ve}")
        
        # v35: Phantom Auto-Calibration Display
        st.markdown("### 📊 Phantom Auto-Calibration (v35)")
        try:
            from config_loader import phantom_autocalibrate
            _ph = phantom_autocalibrate()
            pc1, pc2, pc3 = st.columns(3)
            pc1.metric("Avg Left on Table", f"{_ph['avg_left']:+.2f}%" if _ph['n_trades'] > 0 else "N/A")
            pc2.metric("Trail Activate", f"{_ph['trail_activate']:.1f}%",
                       "📈 Расширен" if _ph['adjusted'] else "✅ Стандарт")
            pc3.metric("Trail Drawdown", f"{_ph['trail_drawdown']:.1f}%",
                       f"({_ph['n_trades']} сделок)")
            if _ph['adjusted']:
                st.info(f"🔧 {_ph['reason']}")
            else:
                st.caption(_ph['reason'])
        except Exception as _pe:
            st.caption(f"Phantom autocalibrate: {_pe}")

# ═══════════════════════════════════════════════════════
# TAB BYBIT: 🔗 Bybit + Download (v38.2)
# ═══════════════════════════════════════════════════════
with tab_bybit:
    st.markdown("### 🔗 Bybit Ссылки + Скачать всё")
    
    # ═══ v38.3: Bybit Demo Status & Stats ═══
    if _BYBIT_AVAILABLE:
        _mirror_active = st.session_state.get('bybit_demo_mirror', False)
        _bkey = CFG('bybit', 'api_key', '')
        _bsec = CFG('bybit', 'api_secret', '')
        _bconfigured = bool(_bkey) and bool(_bsec)
        
        if _mirror_active and _bconfigured:
            st.success("🤖 **Bybit Demo зеркало АКТИВНО** — сделки дублируются на Demo счёт")
            
            col_bb1, col_bb2 = st.columns(2)
            with col_bb1:
                if st.button("🔌 Проверить подключение к Bybit Demo", key="bybit_conn_tab"):
                    with st.spinner("Подключаюсь..."):
                        try:
                            _ex = BybitExecutor(_bkey, _bsec)
                            _conn = _ex.test_connection()
                            if _conn.get('connected'):
                                st.success(
                                    f"✅ Подключено | Баланс: ${_conn.get('balance_usdt',0):.2f} USDT"
                                    f" | Equity: ${_conn.get('equity_usdt',0):.2f}"
                                    f" | Тип: {_conn.get('account_type','?')}"
                                )
                                if _conn.get('note'):
                                    st.info(f"ℹ️ {_conn['note']}")
                            else:
                                st.error(f"❌ Ошибка: {_conn.get('error','?')}")
                                if _conn.get('hint'):
                                    st.warning(f"💡 {_conn['hint']}")
                                st.caption(f"URL: {_conn.get('base_url','?')}")
                        except Exception as _cex:
                            st.error(f"❌ {_cex}")
            
            with col_bb2:
                # Slippage stats
                try:
                    _ex_s = BybitExecutor(_bkey, _bsec)
                    _sl_st = _ex_s.get_slippage_stats()
                    if _sl_st.get('n_trades', 0) > 0:
                        st.metric("Сделок на Demo", _sl_st['n_trades'])
                        st.caption(
                            f"Avg slippage: {_sl_st.get('avg_slippage_pct',0):.4f}% | "
                            f"Max: {_sl_st.get('max_slippage_pct',0):.4f}% | "
                            f"Avg latency: {_sl_st.get('avg_latency_ms',0):.0f}ms | "
                            f"Fees: ${_sl_st.get('total_fees',0):.4f}"
                        )
                    else:
                        st.caption("Пока нет сделок на Demo счёте")
                except Exception:
                    pass
            
            # Bybit trades log
            try:
                import json as _bj
                _bt_path = os.path.join(_BASE_DIR, "bybit_trades.json")
                if os.path.exists(_bt_path):
                    with open(_bt_path, 'r', encoding='utf-8') as _btf:
                        _bt_data = _bj.load(_btf)
                    if _bt_data:
                        st.markdown("#### 📋 Последние сделки на Bybit Demo")
                        _bt_rows = []
                        for _bt in reversed(_bt_data[-20:]):
                            _bt_rows.append({
                                'Время': str(_bt.get('timestamp',''))[:16],
                                'Пара': _bt.get('pair',''),
                                'Dir': _bt.get('direction',''),
                                'Тип': _bt.get('action','OPEN'),
                                'Slippage': f"{_bt.get('total_slippage_pct',0):.4f}%",
                                'Latency': f"{_bt.get('total_latency_ms',0):.0f}ms",
                                'Fees': f"${_bt.get('total_fees',0):.6f}",
                                'OK': '✅' if _bt.get('success') else '❌',
                            })
                        st.dataframe(pd.DataFrame(_bt_rows).astype(str), width='stretch', hide_index=True)
                        
                        # Download Bybit trades log
                        st.download_button("📥 Скачать Bybit Demo лог",
                            pd.DataFrame(_bt_rows).to_csv(index=False),
                            f"bybit_demo_trades_{now_msk().strftime('%Y%m%d_%H%M')}.csv",
                            "text/csv", key="dl_bybit_trades")
            except Exception as _bte:
                st.caption(f"Лог Bybit: {_bte}")
            
            st.markdown("---")
        else:
            if not _bconfigured:
                st.info("💡 **Bybit Demo**: задайте API ключи в config.yaml (секция `bybit:`) и включите галочку в сайдбаре")
            else:
                st.info("💡 **Bybit Demo**: включите галочку 🤖 в сайдбаре для зеркалирования сделок")
        st.markdown("---")
    
    if open_positions:
        st.markdown("#### 🔗 Быстрые ссылки Bybit")
        _bybit_coins = set()
        for pos in open_positions:
            _bybit_coins.add(pos['coin1'])
            _bybit_coins.add(pos['coin2'])
        _links = []
        for coin in sorted(_bybit_coins):
            _url = f"https://www.bybit.com/trade/usdt/{coin}USDT"
            _links.append(f"[{coin}]({_url})")
        st.markdown(" | ".join(_links))
        st.markdown("---")
    
    # Scan files
    st.markdown("#### 📊 Последние сканы")
    import glob as _glob
    _scan_files = sorted(
        _glob.glob(os.path.join(_SCAN_EXPORTS_DIR, "scan_bybit_*.csv")) +
        _glob.glob(os.path.join(_SCAN_EXPORTS_DIR, "scan_*.csv")) +
        _glob.glob(os.path.join(_BASE_DIR, "scan_bybit_*.csv")) +
        _glob.glob(os.path.join(_BASE_DIR, "scan_*.csv")), reverse=True)
    
    if _scan_files:
        _latest_scan = _scan_files[0]
        st.caption(f"Последний: {_latest_scan}")
        try:
            _scan_df = pd.read_csv(_latest_scan)
            
            # v38.2: Separate WL/NK into own columns if present in Вход
            if 'Вход' in _scan_df.columns:
                _scan_df['BT'] = _scan_df['Вход'].apply(lambda x: '❌BT' if '❌BT' in str(x) else ('⚠️BT' if '⚠️BT' in str(x) else ''))
                _scan_df['WL'] = _scan_df['Вход'].apply(lambda x: '❌WL' if '❌WL' in str(x) else ('⚠️WL' if '⚠️WL' in str(x) else ''))
                _scan_df['NK'] = _scan_df['Вход'].apply(lambda x: '❌NK' if '❌NK' in str(x) else ('⚠️NK' if '⚠️NK' in str(x) else ''))
                # Clean Вход column
                def _clean_entry(v):
                    s = str(v)
                    for flag in ['❌BT', '⚠️BT', '❌WL', '⚠️WL', '❌NK', '⚠️NK']:
                        s = s.replace(flag, '')
                    return s.strip()
                _scan_df['Вход'] = _scan_df['Вход'].apply(_clean_entry)
            
            st.dataframe(_scan_df.astype(str), width='stretch', hide_index=True)
            
            st.download_button("📥 Скачать последний скан", _scan_df.to_csv(index=False),
                _latest_scan, "text/csv", key="dl_latest_scan")
        except Exception as _se:
            st.warning(f"Ошибка: {_se}")
        
        # All scans combined
        if len(_scan_files) > 1:
            st.markdown("---")
            _all_scans = []
            for _sf in _scan_files[:50]:
                try:
                    _sdf = pd.read_csv(_sf)
                    _sdf['_source'] = os.path.basename(_sf)
                    _all_scans.append(_sdf)
                except Exception:
                    pass
            if _all_scans:
                _combined = pd.concat(_all_scans, ignore_index=True)
                st.download_button(f"📥 ВСЕ сканы ({len(_all_scans)} файлов)",
                    _combined.to_csv(index=False),
                    f"all_scans_{now_msk().strftime('%Y%m%d_%H%M')}.csv",
                    "text/csv", key="dl_all_scans")
    else:
        st.info("📭 Нет файлов сканов.")
    
    # Download ALL stats
    st.markdown("---")
    st.markdown("#### 📥 Скачать ВСЮ статистику")
    _dl1, _dl2, _dl3 = st.columns(3)
    
    with _dl1:
        if closed_positions:
            _all_t = []
            for p in closed_positions:
                _all_t.append({'#': p['id'], 'Пара': f"{p['coin1']}/{p['coin2']}",
                    'Dir': p['direction'], 'PnL': p.get('pnl_pct', 0),
                    'Причина': p.get('exit_reason', ''), 'BT': p.get('flag_bt', ''),
                    'WL': p.get('flag_wl', ''), 'NK': p.get('flag_nk', ''),
                    'Вход': p.get('entry_time', ''), 'Выход': p.get('exit_time', ''),
                    'TP': p.get('pair_tp_pct', CFG('monitor', 'auto_tp_pct', 2.0)), 'SL': p.get('pair_sl_pct', CFG('monitor', 'auto_sl_pct', -2.5))})
            st.download_button("📥 Все сделки (CSV)",
                pd.DataFrame(_all_t).to_csv(index=False),
                f"all_trades_{now_msk().strftime('%Y%m%d_%H%M')}.csv",
                "text/csv", key="dl_all_trades_bybit")
    
    with _dl2:
        try:
            with open(COOLDOWN_FILE, 'r', encoding='utf-8') as _cf:
                st.download_button("📥 Кулдауны (JSON)", _cf.read(),
                    f"cooldowns_{now_msk().strftime('%Y%m%d_%H%M')}.json",
                    "application/json", key="dl_cooldowns")
        except Exception:
            st.caption("Файл кулдаунов не найден")
    
    with _dl3:
        try:
            with open(os.path.join(_BASE_DIR, "pair_memory.json"), 'r', encoding='utf-8') as _mf:
                st.download_button("📥 Pair Memory (JSON)", _mf.read(),
                    f"pair_memory_{now_msk().strftime('%Y%m%d_%H%M')}.json",
                    "application/json", key="dl_memory")
        except Exception:
            st.caption("Файл pair_memory не найден")
    
    # TP/SL config download
    try:
        with open(PAIR_TP_SL_FILE, 'r', encoding='utf-8') as _tpf:
            st.download_button("📥 TP/SL Config (JSON)", _tpf.read(),
                f"pair_tp_sl_{now_msk().strftime('%Y%m%d_%H%M')}.json",
                "application/json", key="dl_tp_sl")
    except Exception:
        pass

# ═══════════════════════════════════════════════════════
# TAB 4: R8 Performance Tracker
# ═══════════════════════════════════════════════════════
with tab4:
    st.markdown("### 📈 Performance Tracker (R8)")
    st.caption("Накопительная статистика по всем закрытым сделкам")
    
    # Load history from persistent file + current session closed
    history = load_trade_history()
    
    # Also include closed positions from current session that might not be in history yet
    history_ids = {t.get('id', 0) for t in history}
    for cp in closed_positions:
        if cp.get('id', 0) not in history_ids:
            history.append({
                'id': cp.get('id', 0),
                'pair': f"{cp.get('coin1', '')}/{cp.get('coin2', '')}",
                'coin1': cp.get('coin1', ''), 'coin2': cp.get('coin2', ''),
                'direction': cp.get('direction', ''),
                'timeframe': cp.get('timeframe', '4h'),
                'entry_z': cp.get('entry_z', 0), 'exit_z': cp.get('exit_z', 0),
                'entry_hr': cp.get('entry_hr', 0), 'pnl_pct': cp.get('pnl_pct', 0),
                'entry_time': cp.get('entry_time', ''),
                'exit_time': cp.get('exit_time', ''),
                'exit_reason': cp.get('exit_reason', ''),
                'entry_price1': cp.get('entry_price1', 0),
                'entry_price2': cp.get('entry_price2', 0),
                'exit_price1': cp.get('exit_price1', 0),
                'exit_price2': cp.get('exit_price2', 0),
                'notes': cp.get('notes', ''),
                'best_pnl': cp.get('best_pnl', 0),
            })
    
    if not history:
        st.info("📭 Нет закрытых сделок в истории. Закройте позицию чтобы начать накапливать статистику.")
        st.markdown("💡 **Ручной импорт:** Загрузите CSV с прошлыми сделками.")
        
        uploaded_hist = st.file_uploader("📤 Импорт истории (CSV)", type=['csv'], key='hist_import')
        if uploaded_hist:
            try:
                import io
                hist_df = pd.read_csv(io.StringIO(uploaded_hist.getvalue().decode()))
                st.dataframe(hist_df.astype(str))
                
                if st.button("✅ Импортировать эти сделки"):
                    for _, row in hist_df.iterrows():
                        trade = {
                            'id': int(row.get('#', row.get('id', 0))),
                            'coin1': str(row.get('Пара', '')).split('/')[0] if '/' in str(row.get('Пара', '')) else '',
                            'coin2': str(row.get('Пара', '')).split('/')[1] if '/' in str(row.get('Пара', '')) else '',
                            'direction': row.get('Dir', row.get('direction', '')),
                            'timeframe': row.get('TF', row.get('timeframe', '4h')),
                            'entry_z': float(str(row.get('Entry Z', row.get('entry_z', 0))).replace('+', '')),
                            'exit_z': float(str(row.get('Exit Z', row.get('exit_z', 0))).replace('+', '')),
                            'entry_hr': float(row.get('entry_hr', 1.0)),
                            'pnl_pct': float(str(row.get('P&L %', row.get('pnl_pct', 0))).replace('+', '').replace('%', '')),
                            'entry_time': str(row.get('Вход', row.get('entry_time', ''))),
                            'exit_time': str(row.get('Выход', row.get('exit_time', ''))),
                            'exit_reason': str(row.get('Причина', row.get('exit_reason', 'MANUAL'))),
                            'notes': '',
                            'best_pnl': 0,
                            'entry_price1': 0, 'entry_price2': 0,
                            'exit_price1': 0, 'exit_price2': 0,
                        }
                        save_trade_to_history(trade)
                    st.success(f"✅ Импортировано {len(hist_df)} сделок!")
                    st.rerun()
            except Exception as ex:
                st.error(f"❌ Ошибка импорта: {ex}")
    else:
        # === DASHBOARD ===
        pnls = [float(t.get('pnl_pct', 0)) for t in history]
        n_trades = len(history)
        total_pnl = sum(pnls)
        winners = sum(1 for p in pnls if p > 0)
        losers = sum(1 for p in pnls if p < 0)
        win_rate = winners / n_trades * 100 if n_trades > 0 else 0
        avg_pnl = total_pnl / n_trades if n_trades > 0 else 0
        avg_win = np.mean([p for p in pnls if p > 0]) if winners > 0 else 0
        avg_loss = np.mean([p for p in pnls if p < 0]) if losers > 0 else 0
        pf = abs(sum(p for p in pnls if p > 0) / sum(p for p in pnls if p < 0)) if losers > 0 and sum(p for p in pnls if p < 0) != 0 else float('inf')
        
        # Max drawdown
        cumulative = np.cumsum(pnls)
        peak = np.maximum.accumulate(cumulative)
        drawdown = cumulative - peak
        max_dd = min(drawdown) if len(drawdown) > 0 else 0
        
        # Metrics row 1
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Всего сделок", n_trades)
        m2.metric("Суммарный P&L", f"{total_pnl:+.2f}%",
                 delta=f"{total_pnl:+.2f}%", delta_color="normal")
        m3.metric("Win Rate", f"{win_rate:.0f}%",
                 delta=f"{winners}W / {losers}L")
        m4.metric("Avg P&L", f"{avg_pnl:+.3f}%")
        m5.metric("Profit Factor", f"{pf:.2f}" if pf < 100 else "∞")
        
        # Metrics row 2
        m6, m7, m8, m9 = st.columns(4)
        m6.metric("Avg Win", f"{avg_win:+.3f}%")
        m7.metric("Avg Loss", f"{avg_loss:+.3f}%")
        m8.metric("Max Drawdown", f"{max_dd:+.2f}%")
        
        # Best streak
        streaks = []
        current_streak = 0
        for p in pnls:
            if p > 0:
                current_streak += 1
            else:
                if current_streak > 0:
                    streaks.append(current_streak)
                current_streak = 0
        if current_streak > 0:
            streaks.append(current_streak)
        best_streak = max(streaks) if streaks else 0
        m9.metric("Best Win Streak", f"{best_streak}")
        
        # === EQUITY CURVE ===
        st.markdown("#### 📈 Equity Curve")
        import plotly.graph_objects as go
        
        cum_pnl = list(np.cumsum(pnls))
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            y=[0] + cum_pnl,
            mode='lines+markers',
            name='Cumulative P&L',
            line=dict(color='#00c853', width=2),
            marker=dict(size=5, color=['green' if p > 0 else 'red' for p in [0] + list(pnls)])
        ))
        fig.update_layout(
            height=300, margin=dict(l=0, r=0, t=30, b=0),
            yaxis_title="Cumulative P&L %",
            xaxis_title="Trade #",
            template="plotly_dark"
        )
        st.plotly_chart(fig, width='stretch')
        
        # === BY PAIR ANALYSIS ===
        st.markdown("#### 🪙 Статистика по парам")
        pair_stats = {}
        for t in history:
            pair = t.get('pair', f"{t.get('coin1','')}/{t.get('coin2','')}")
            if pair not in pair_stats:
                pair_stats[pair] = {'pnls': [], 'count': 0}
            pair_stats[pair]['pnls'].append(float(t.get('pnl_pct', 0)))
            pair_stats[pair]['count'] += 1
        
        pair_rows = []
        for pair, stats in sorted(pair_stats.items(), key=lambda x: sum(x[1]['pnls']), reverse=True):
            ppnls = stats['pnls']
            pair_rows.append({
                'Пара': pair,
                'Сделок': stats['count'],
                'Total P&L': f"{sum(ppnls):+.2f}%",
                'Avg P&L': f"{np.mean(ppnls):+.3f}%",
                'WR': f"{sum(1 for p in ppnls if p > 0)/len(ppnls)*100:.0f}%",
                'Best': f"{max(ppnls):+.2f}%",
                'Worst': f"{min(ppnls):+.2f}%",
            })
        if pair_rows:
            st.dataframe(pd.DataFrame(pair_rows).astype(str), width='stretch', hide_index=True)
        
        # === BY DAY ANALYSIS ===
        st.markdown("#### 📅 Статистика по дням")
        day_stats = {}
        for t in history:
            day = str(t.get('exit_time', t.get('entry_time', '')))[:10]
            if day and day != 'None':
                if day not in day_stats:
                    day_stats[day] = {'pnls': [], 'count': 0}
                day_stats[day]['pnls'].append(float(t.get('pnl_pct', 0)))
                day_stats[day]['count'] += 1
        
        day_rows = []
        for day, stats in sorted(day_stats.items()):
            dpnls = stats['pnls']
            day_rows.append({
                'Дата': day,
                'Сделок': stats['count'],
                'Total P&L': f"{sum(dpnls):+.2f}%",
                'WR': f"{sum(1 for p in dpnls if p > 0)/len(dpnls)*100:.0f}%",
                'Avg P&L': f"{np.mean(dpnls):+.3f}%",
            })
        if day_rows:
            st.dataframe(pd.DataFrame(day_rows).astype(str), width='stretch', hide_index=True)
        
        # === TRADES TABLE ===
        st.markdown("#### 📋 Все сделки")
        trade_rows = []
        for t in reversed(history):
            # v23.0: Phantom "left on table" calculation
            cut = ''
            ph_max = t.get('phantom_max_pnl')
            t_pnl = float(t.get('pnl_pct', 0) or 0)
            if ph_max is not None:
                try:
                    ph_max = float(ph_max)
                    delta_ph = ph_max - t_pnl
                    cut = f"+{delta_ph:.2f}%" if delta_ph > 0.1 else "✅"
                except (TypeError, ValueError):
                    cut = ''
            # v31: Основание сделки
            _sig = t.get('signal_type', '')
            _lbl = t.get('entry_label', '')
            _auto = t.get('auto_opened', False)
            _basis_parts = []
            if _auto:
                _basis_parts.append('🤖')
            else:
                _basis_parts.append('👤')
            if _sig:
                _basis_parts.append(f"📊{_sig}")
            if _lbl:
                _basis_parts.append(_lbl)
            # Fallback: parse from notes
            if not _sig and not _lbl:
                _notes = str(t.get('notes', ''))
                if 'SIGNAL' in _notes:
                    _basis_parts.append('📊SIGNAL')
                elif 'READY' in _notes:
                    _basis_parts.append('📊READY')
                if 'ВХОД' in _notes:
                    _basis_parts.append('🟢ВХОД')
                elif 'УСЛОВНО' in _notes:
                    _basis_parts.append('🟡УСЛОВНО')
                elif 'СЛАБЫЙ' in _notes:
                    _basis_parts.append('🟡СЛАБЫЙ')
            _basis = ' '.join(_basis_parts)
            
            trade_rows.append({
                '#': t.get('id', ''),
                'Пара': t.get('pair', ''),
                'Dir': t.get('direction', ''),
                'Основание': _basis,
                'BT': t.get('flag_bt', ''),
                'WL': t.get('flag_wl', ''),
                'NK': t.get('flag_nk', ''),
                'Size $': t.get('recommended_size', 100),
                'Entry Z': f"{float(t.get('entry_z', 0)):+.2f}",
                'Exit Z': f"{float(t.get('exit_z', 0)):+.2f}",
                'P&L': f"{t_pnl:+.2f}%",
                'Best P&L': f"{float(t.get('best_pnl_during_trade', t.get('best_pnl', 0))):+.2f}%",
                'Упущено': cut,
                'Причина': t.get('exit_reason', ''),
                'Сообщения': '; '.join(t.get('monitor_messages', [])[:3]) or '-',
                'Вход МСК': to_msk_full(t.get('entry_time', '')),
                'Выход МСК': to_msk_full(t.get('exit_time', '')),
            })
        if trade_rows:
            st.dataframe(pd.DataFrame(trade_rows).astype(str), width='stretch', hide_index=True)
        
        # === EXPORT ===
        st.markdown("#### 📥 Экспорт")
        
        # v33: Trade Journal CSV (detailed export)
        if history:
            journal_rows = []
            for t in history:
                t_pnl = float(t.get('pnl_pct', 0) or 0)
                # Calculate hold hours
                try:
                    _et = datetime.fromisoformat(str(t.get('entry_time', '')).replace('+03:00', '+03:00'))
                    _xt = datetime.fromisoformat(str(t.get('exit_time', '')).replace('+03:00', '+03:00'))
                    _hold_h = (_xt - _et).total_seconds() / 3600
                except Exception:
                    _hold_h = 0
                
                journal_rows.append({
                    'ID': t.get('id', ''),
                    'Pair': t.get('pair', f"{t.get('coin1','')}/{t.get('coin2','')}"),
                    'Direction': t.get('direction', ''),
                    'Entry_Time': t.get('entry_time', ''),
                    'Exit_Time': t.get('exit_time', ''),
                    'Hold_Hours': round(_hold_h, 1),
                    'Entry_Z': round(float(t.get('entry_z', 0) or 0), 2),
                    'Exit_Z': round(float(t.get('exit_z', 0) or 0), 2),
                    'Entry_HR': round(float(t.get('entry_hr', 0) or 0), 4),
                    'PnL_Net': round(t_pnl, 3),
                    'PnL_Gross': round(float(t.get('pnl_gross_pct', 0) or 0), 3),
                    'Best_PnL': round(float(t.get('best_pnl_during_trade', t.get('best_pnl', 0)) or 0), 3),
                    'Phantom_Max': t.get('phantom_max_pnl', ''),
                    'Exit_Reason': t.get('exit_reason', ''),
                    'Size_USD': t.get('recommended_size', 100),
                    'MBT_Verdict': t.get('mbt_verdict', ''),
                    'Signal_Type': t.get('signal_type', ''),
                    'Entry_Label': t.get('entry_label', ''),
                    'Auto_Opened': t.get('auto_opened', False),
                    'Notes': t.get('notes', ''),
                })
            df_journal = pd.DataFrame(journal_rows)
            
            jc1, jc2 = st.columns(2)
            with jc1:
                st.download_button("📥 Trade Journal (CSV)", 
                    df_journal.to_csv(index=False),
                    f"journal_{now_msk().strftime('%Y%m%d')}.csv",
                    "text/csv", key="journal_export_btn")
            with jc2:
                hist_df = pd.DataFrame(history)
                csv_hist = hist_df.to_csv(index=False)
                st.download_button("📥 Полная история (RAW CSV)", csv_hist,
                                  f"trade_history_{now_msk().strftime('%Y%m%d_%H%M')}.csv",
                                  "text/csv", key="hist_export_btn")
        
        # v33: Session Summary (итоги дня)
        st.markdown("#### 📊 Итоги дня")
        today = now_msk().strftime('%Y-%m-%d')
        today_trades = [t for t in history 
                        if str(t.get('exit_time', '')).startswith(today)]
        if today_trades:
            t_pnls = [float(t.get('pnl_pct', 0) or 0) for t in today_trades]
            t_wins = sum(1 for p in t_pnls if p > 0)
            t_wr = t_wins / len(t_pnls) * 100 if t_pnls else 0
            
            ss1, ss2, ss3, ss4 = st.columns(4)
            ss1.metric("Сделок сегодня", len(t_pnls))
            ss2.metric("WR сегодня", f"{t_wr:.0f}%", f"{t_wins}W / {len(t_pnls)-t_wins}L")
            ss3.metric("Net PnL", f"{sum(t_pnls):+.2f}%")
            ss4.metric("Avg PnL", f"{np.mean(t_pnls):+.3f}%")
        else:
            st.info("Нет закрытых сделок сегодня.")

# ═══════════════════════════════════════════════════════
# TAB DIAG: Entry Diagnostics (v38.4)
# ═══════════════════════════════════════════════════════
with tab_diag:
    st.markdown("### 🔍 Диагностика отказов открытия сделок")
    st.caption("Все отказы авто-монитора и монитора пишутся в `entry_diag_log.jsonl` и показываются здесь.")

    # ── 1. Live pending files status ──
    import glob as _dglob
    _pend_files = sorted(_dglob.glob(os.path.join(_MONITOR_IMPORT_DIR, "pending_*.json")))
    st.markdown("#### 📂 Pending файлы (monitor_import/)")
    if _pend_files:
        _pend_rows = []
        for _pf in _pend_files:
            try:
                with open(_pf, 'r', encoding='utf-8') as _pff:
                    _pd = json.load(_pff)
                if isinstance(_pd, list):
                    _pd = _pd[0] if _pd and isinstance(_pd[0], dict) else {}
                _pend_rows.append({
                    'Файл': os.path.basename(_pf),
                    'Пара': f"{_pd.get('coin1','?')}/{_pd.get('coin2','?')}",
                    'Dir': _pd.get('direction', '?'),
                    'Z': f"{float(_pd.get('entry_z', 0)):+.2f}",
                    'auto_opened': str(bool(_pd.get('auto_opened', False))),
                    'entry_label': str(_pd.get('entry_label', '')),
                    'Notes': str(_pd.get('notes', ''))[:80],
                    'Q': str(_pd.get('quality_score', 0)),
                })
            except Exception as _pe:
                _pend_rows.append({
                    'Файл': os.path.basename(_pf),
                    'Пара': '❌ JSON ERROR',
                    'Dir': '', 'Z': '', 'auto_opened': 'False',
                    'entry_label': '', 'Notes': str(_pe)[:80], 'Q': '0',
                })
        if _pend_rows:
            _pend_df = pd.DataFrame(_pend_rows).astype(str)
            st.dataframe(_pend_df, width='stretch', hide_index=True)
    else:
        st.success("✅ Нет pending файлов в очереди.")

    # ── 2. Simulate add_position for each pending file ──
    st.markdown("#### 🧪 Симуляция открытия (dry-run)")
    st.caption("Проверяет все фильтры для каждого pending файла — без реального открытия.")
    if _pend_files:
        for _pf in _pend_files:
            try:
                with open(_pf, 'r', encoding='utf-8') as _pff:
                    _pd = json.load(_pff)
                if isinstance(_pd, list):
                    _pd = _pd[0] if _pd and isinstance(_pd[0], dict) else {}
                if not isinstance(_pd, dict) or 'coin1' not in _pd:
                    st.error(f"❌ {os.path.basename(_pf)}: невалидный JSON")
                    continue
                _pn = f"{_pd['coin1']}/{_pd['coin2']}"
                _dir = _pd.get('direction', 'NONE')
                _notes = str(_pd.get('notes', ''))
                _entry_lbl = _pd.get('entry_label', '')
                _q = _pd.get('quality_score', 0)

                # Build combined label string for filter check (notes + entry_label)
                _check_str = _notes if _entry_lbl in _notes else (_notes + ' ' + _entry_lbl)

                _checks = []
                _blocked = False
                _block_reason = None

                # CHECK 1: entry_label present?
                if not _entry_lbl and '🟢' not in _notes and '🟡' not in _notes:
                    _checks.append(('⚠️', 'entry_label', f'Поле entry_label пустое, в notes нет эмодзи метки — фильтр не сможет определить уровень входа. notes={repr(_notes[:60])}'))
                else:
                    _checks.append(('✅', 'entry_label', f'entry_label={repr(_entry_lbl)}'))

                # CHECK 2: ЖДАТЬ block
                _flags_dry = parse_entry_flags(_check_str)
                _base_dry = _flags_dry['base_label']
                _is_wait_dry = '⚪' in _base_dry or 'ЖДАТЬ' in _base_dry.upper()
                _is_green_dry = '🟢' in _base_dry or 'ВХОД' in _base_dry.upper()
                _is_yellow_dry = '🟡' in _base_dry or 'УСЛОВНО' in _base_dry.upper()
                if _is_wait_dry:
                    _blocked = True
                    _block_reason = '⚪ ЖДАТЬ — системный запрет'
                    _checks.append(('🚫', 'ЖДАТЬ блок', _block_reason))
                else:
                    _checks.append(('✅', 'ЖДАТЬ блок', f'is_green={_is_green_dry} is_yellow={_is_yellow_dry}'))

                # CHECK 3: Quality gate
                _min_q = CFG('scanner', 'min_quality', 50)
                if _q > 0 and _q < _min_q:
                    _blocked = True
                    _block_reason = f'Quality gate: Q={_q} < min_q={_min_q}'
                    _checks.append(('🚫', 'Quality gate', _block_reason))
                else:
                    _checks.append(('✅', 'Quality gate', f'Q={_q} (min={_min_q})'))

                # CHECK 4: Max positions
                _open_pos_dry = [p for p in load_positions() if isinstance(p, dict) and p.get('status') == 'OPEN']
                _max_p = CFG('monitor', 'max_positions', 10)
                if len(_open_pos_dry) >= _max_p:
                    _blocked = True
                    _block_reason = f'Лимит позиций: {len(_open_pos_dry)}/{_max_p}'
                    _checks.append(('🚫', 'Max positions', _block_reason))
                else:
                    _checks.append(('✅', 'Max positions', f'{len(_open_pos_dry)}/{_max_p}'))

                # CHECK 5: Max coin positions
                _max_cp = CFG('monitor', 'max_coin_positions', 2)
                try:
                    from pairs_scanner.core.risk import check_coin_position_limit as _core_cpl_dry
                    for _coin_dry in [_pd['coin1'], _pd['coin2']]:
                        _cpl_bl, _cpl_reason = _core_cpl_dry(_coin_dry, _open_pos_dry, _max_cp)
                        if _cpl_bl:
                            _blocked = True
                            _block_reason = _cpl_reason
                            _checks.append(('🚫', f'Max coin pos ({_coin_dry})', _block_reason))
                        else:
                            _cnt = sum(1 for _op in _open_pos_dry
                                       if _coin_dry in (_op.get('coin1',''), _op.get('coin2','')))
                            _checks.append(('✅', f'Max coin pos ({_coin_dry})', f'{_cnt}/{_max_cp}'))
                except ImportError:
                    for _coin_dry in [_pd['coin1'], _pd['coin2']]:
                        _cnt = sum(1 for _op in _open_pos_dry
                                   if _coin_dry in (_op.get('coin1',''), _op.get('coin2','')))
                        if _cnt >= _max_cp:
                            _blocked = True
                            _block_reason = f'{_coin_dry} уже в {_cnt} позициях (лимит {_max_cp})'
                            _checks.append(('🚫', f'Max coin pos ({_coin_dry})', _block_reason))
                        else:
                            _checks.append(('✅', f'Max coin pos ({_coin_dry})', f'{_cnt}/{_max_cp}'))

                # B-04/N-05 FIX: загружаем cooldowns ОДИН раз для CHECK 6, 7 и 8
                _cd_data_dry = _load_cooldowns()

                # CHECK 6: Cooldown
                _is_green_for_cd = _is_green_dry
                _lbl_for_cd_dry = _check_str if _is_green_for_cd else ""
                _cd_bl, _cd_rs = check_pair_cooldown(_pn, _lbl_for_cd_dry, cd_data=_cd_data_dry)
                if not _cd_bl:
                    _pn_rev = f"{_pd['coin2']}/{_pd['coin1']}"
                    _cd_bl, _cd_rs = check_pair_cooldown(_pn_rev, _lbl_for_cd_dry, cd_data=_cd_data_dry)
                if _cd_bl:
                    _blocked = True
                    _block_reason = _cd_rs
                    _checks.append(('🚫', 'Cooldown', _cd_rs))
                else:
                    _checks.append(('✅', 'Cooldown', 'нет блока'))

                # CHECK 7: Daily loss limit
                # B-03 FIX: передаём PnL из загруженных open_positions
                _live_pnls_dry = [p.get('pnl_pct', 0) for p in _open_pos_dry if p.get('pnl_pct', 0) != 0]
                _dl_bl, _dl_rs = check_daily_loss_limit(cd_data=_cd_data_dry, live_open_pnls=_live_pnls_dry)
                if _dl_bl:
                    _blocked = True
                    _block_reason = _dl_rs
                    _checks.append(('🚫', 'Daily loss limit', _dl_rs))
                else:
                    _checks.append(('✅', 'Daily loss limit', 'нет блока'))

                # CHECK 8: Anti-repeat (SL same direction today)
                # N-05 FIX: _cd_data_dry уже загружен выше (CHECK 7)
                _today_dry = now_msk().strftime('%Y-%m-%d')
                _anti_repeat_blocked = False
                try:
                    from pairs_scanner.core.risk import check_anti_repeat as _core_ar_dry
                    for _check_pn_dry in [_pn, f"{_pd['coin2']}/{_pd['coin1']}"]:
                        _ar_bl, _ar_reason = _core_ar_dry(
                            _check_pn_dry, _dir, _cd_data_dry,
                            is_green=_is_green_dry, today_str=_today_dry)
                        if _ar_bl:
                            _anti_repeat_blocked = True
                            _block_reason = _ar_reason
                            _checks.append(('🚫', 'Anti-repeat', _block_reason))
                            _blocked = True
                            break
                except ImportError:
                    for _check_pn_dry in [_pn, f"{_pd['coin2']}/{_pd['coin1']}"]:
                        _cde = _cd_data_dry.get(_check_pn_dry, {})
                        if (_cde.get('date') == _today_dry and
                            _cde.get('sl_exit', False) and
                            _cde.get('last_dir') == _dir and
                            not _is_green_dry):
                            _anti_repeat_blocked = True
                            _block_reason = f'Anti-repeat: SL в {_dir} по {_check_pn_dry} сегодня (bypass: 🟢 ВХОД)'
                            _checks.append(('🚫', 'Anti-repeat', _block_reason))
                            _blocked = True
                            break
                if not _anti_repeat_blocked:
                    _checks.append(('✅', 'Anti-repeat', 'нет блока'))

                # CHECK 9: Already open
                _open_pairs_dry = {f"{p.get('coin1','')}/{p.get('coin2','')}" for p in _open_pos_dry}
                _open_pairs_rev_dry = {f"{p.get('coin2','')}/{p.get('coin1','')}" for p in _open_pos_dry}
                if _pn in _open_pairs_dry or _pn in _open_pairs_rev_dry:
                    _blocked = True
                    _block_reason = f'{_pn} уже открыта'
                    _checks.append(('🚫', 'Already open', _block_reason))
                else:
                    _checks.append(('✅', 'Already open', 'нет дубля'))

                # CHECK 10: BT filter
                _bt_mode_dry = CFG('strategy', 'bt_filter_mode', 'HARD')
                _bt_v = _pd.get('bt_verdict', '')
                if _bt_mode_dry == 'HARD' and _bt_v == 'FAIL':
                    _blocked = True
                    _block_reason = 'BT FAIL (bt_filter_mode=HARD)'
                    _checks.append(('🚫', 'BT filter', _block_reason))
                else:
                    _checks.append(('✅', 'BT filter', f'verdict={_bt_v or "N/A"} mode={_bt_mode_dry}'))

                # DISPLAY
                _icon = '🚫' if _blocked else '✅'
                with st.expander(f"{_icon} {_pn} {_dir} | {'ЗАБЛОКИРОВАН: ' + str(_block_reason) if _blocked else 'ПРОЙДЁТ'}", expanded=_blocked):
                    _rows = [{'Проверка': c[1], 'Статус': c[0], 'Детали': c[2]} for c in _checks]
                    st.dataframe(pd.DataFrame(_rows).astype(str), width='stretch', hide_index=True)
                    st.caption(f"notes: {repr(_notes[:100])}")
                    st.caption(f"entry_label: {repr(_entry_lbl)}")

            except Exception as _sim_ex:
                st.error(f"❌ Ошибка симуляции {os.path.basename(_pf)}: {_sim_ex}")
    else:
        st.info("Нет pending файлов для симуляции.")

    # ── 3. Persistent diag log ──
    st.markdown("#### 📋 Лог отказов (entry_diag_log.jsonl)")
    _diag_entries = _diag_load()
    if _diag_entries:
        _dcol1, _dcol2 = st.columns([3, 1])
        with _dcol1:
            st.caption(f"Всего записей: {len(_diag_entries)} (последние 200)")
        with _dcol2:
            if st.button("🗑️ Очистить лог", key="diag_clear_log"):
                _diag_save([])
                st.rerun()
        _diag_df = pd.DataFrame(reversed(_diag_entries[-100:]))
        if 'ts' in _diag_df.columns:
            _diag_df['ts'] = _diag_df['ts'].apply(lambda x: str(x)[:16])
        st.dataframe(_diag_df.astype(str), width='stretch', hide_index=True)
        st.download_button("📥 Скачать лог (JSON)",
            json.dumps(_diag_entries, ensure_ascii=False, indent=2, default=str),
            f"entry_diag_{now_msk().strftime('%Y%m%d_%H%M')}.json",
            "application/json", key="dl_diag_log")
    else:
        st.info("Лог пуст. Отказы будут появляться здесь автоматически.")

    # ── 4. Cooldowns state ──
    st.markdown("#### ⏳ Активные кулдауны")
    _cd_state = _load_cooldowns()
    _today_diag = now_msk().strftime('%Y-%m-%d')
    _cd_active = []
    for _pn_cd, _cd_e in _cd_state.items():
        if _cd_e.get('last_loss_time'):
            try:
                _loss_dt = datetime.fromisoformat(_cd_e['last_loss_time'])
                _hrs_s = (now_msk() - _loss_dt).total_seconds() / 3600
                _is_sl = _cd_e.get('sl_exit', False)
                _cons_sl = _cd_e.get('consecutive_sl', 0)
                _sess_pnl = _cd_e.get('session_pnl', 0)
                if _cons_sl >= 2: _cd_h = 12.0
                elif _is_sl: _cd_h = 12.0
                elif _sess_pnl < -0.5: _cd_h = 4.0
                else: _cd_h = 0.0
                _remaining_cd = _cd_h - _hrs_s
                _active = _remaining_cd > 0
                _cd_active.append({
                    'Пара': _pn_cd,
                    'Session PnL': f"{_sess_pnl:+.2f}%",
                    'SL exit': '🚫' if _is_sl else '–',
                    'Consec SL': _cons_sl,
                    'Cooldown ч': f"{_cd_h:.0f}h",
                    'Прошло ч': f"{_hrs_s:.1f}h",
                    'Осталось': f"⏳ {_remaining_cd:.1f}h" if _active else "✅ свободна",
                    'Last dir': _cd_e.get('last_dir', ''),
                })
            except Exception:
                pass
    if _cd_active:
        st.dataframe(pd.DataFrame(_cd_active).astype(str), width='stretch', hide_index=True)
    else:
        st.success("✅ Нет активных кулдаунов.")

    # ── 5. Summary: discovered bugs explanation ──
    st.markdown("#### ⚠️ Обнаруженные причины отказов")
    with st.expander("ℹ️ Ранее обнаруженные и исправленные баги", expanded=False):
        st.markdown("- **БАГ 1**: entry_label не мержился в notes — фильтр не видел уровень входа. Исправлено.")
        st.markdown("- **БАГ 2**: open() без encoding=utf-8 — на Windows падал UnicodeDecodeError. Исправлено.")
        st.markdown("- **БАГ 3**: pending удалялся при отказе add_position. Теперь retry до 3 раз.")

st.divider()
st.caption("""
v38.3 — Bybit Demo зеркало:
• ☑️ Галочка "Зеркалировать на Bybit Demo" в сайдбаре
• Параллельное открытие/закрытие на Demo счёте Bybit
• API ключи в config.yaml → секция bybit:
• Логи slippage + latency в tab 🔗 Bybit/Скачать

v38.2 — Полный контроль фильтров:
• 14 чекбоксов фильтров: 🟢ВХОД / 🟡УСЛОВНО / BT / WL / NK / Dir
• Per-pair TP/SL (настраивается в сайдбаре, default ±2%)
• DEEP_RALLY: ручной вкл/выкл (🟢 ВХОД bypass)
• ⚪ ЖДАТЬ — системный запрет (всегда)
• BT/WL/NK в отдельных столбцах истории

⚡ v38: Q≥50 | HL<8h | Z-exit OFF | MAD-trail | Timeout 8h
""")

# ═══════════════════════════════════════════════════════
# v39 FIX: Auto-refresh — st.rerun() only, no browser reload.
# window.parent.location.reload() was wiping session_state on every tick.
# Now: JS countdown fires window.parent.postMessage → Streamlit picks it up
# via a hidden fragment that calls st.rerun(). session_state is preserved.
# ═══════════════════════════════════════════════════════
if auto_refresh:
    _mon_wait = refresh_interval_sec
    # BUG-030 FIX: default to time.time() (not 0) so the first auto-rerun
    # fires after a full interval instead of immediately on every page load.
    _last_mon_ts = st.session_state.get('_last_monitor_ts', time.time())
    if '_last_monitor_ts' not in st.session_state:
        st.session_state['_last_monitor_ts'] = _last_mon_ts
    _elapsed = time.time() - _last_mon_ts
    # G-04 FIX: removed `if True:` dead branch and duplicate `_elapsed` assignment
    _remaining = max(0, _mon_wait - _elapsed)
    if _remaining <= 0:
        # Time's up — update timestamp and rerun inside Streamlit
        st.session_state['_last_monitor_ts'] = time.time()
        st.rerun()
    else:
        st.caption(f"⏱️ Авто-обновление через {int(_remaining)}с (интервал {_mon_wait}с)")
        # G-04 FIX: time.sleep + st.rerun вместо нерабочего JS postMessage
        time.sleep(min(_remaining, 5))  # sleep max 5s, then rerun to re-check
        st.rerun()
