"""
core/risk.py — Все risk-функции как чистые функции.

НОЛЬ импортов Streamlit. НОЛЬ вызовов CFG().
Все параметры — явные аргументы. Тестируется за миллисекунды.

Волна 1: самая ценная часть рефакторинга.
Регрессионные тесты на B-01, B-03, B-06 ловят баги до деплоя.
"""

from datetime import datetime, timedelta
from .utils import MSK, now_msk


# ═══════════════════════════════════════════════════════
# DAILY LOSS LIMIT
# ═══════════════════════════════════════════════════════

def check_daily_loss_limit(
    cd_data: dict,
    live_open_pnls: list[float],
    daily_loss_limit_pct: float = -5.0,
    today_str: str | None = None,
) -> tuple[bool, str]:
    """Проверка дневного лимита убытков.

    Чистая функция: принимает данные, возвращает решение.
    Нет st.session_state, нет CFG(), нет load_positions().

    Args:
        cd_data: cooldown dict {pair: {session_pnl, date, ...}}
        live_open_pnls: текущие PnL открытых позиций (list[float])
        daily_loss_limit_pct: порог (отрицательное число, напр. -5.0)
        today_str: дата в формате 'YYYY-MM-DD' (default: today MSK)

    Returns:
        (blocked: bool, reason: str)

    Регрессия B-03: live_open_pnls ОБЯЗАТЕЛЕН для точного расчёта.
    Регрессия BUG-016: только отрицательные open PnL учитываются.
    """
    if today_str is None:
        today_str = now_msk().date().isoformat()

    # Сумма закрытых убытков за сегодня
    closed_pnl = sum(
        e.get('session_pnl', 0)
        for e in cd_data.values()
        if e.get('date') == today_str
    )

    # Только отрицательные open PnL (BUG-016: прибыль не компенсирует убытки)
    unrealised_loss = sum(p for p in live_open_pnls if p < 0)
    total = closed_pnl + unrealised_loss

    if total <= daily_loss_limit_pct:
        return True, (
            f"🛑 ДНЕВНОЙ ЛИМИТ: потери {total:+.2f}% "
            f"(лимит {daily_loss_limit_pct}%, включая открытые)"
        )
    return False, ""


# ═══════════════════════════════════════════════════════
# PAIR COOLDOWN
# ═══════════════════════════════════════════════════════

def check_pair_cooldown(
    pair_name: str,
    cd_data: dict,
    entry_label: str = "",
    cooldown_after_sl_hours: float = 12.0,
    cooldown_after_2sl_hours: float = 12.0,
    pair_cooldown_hours: float = 4.0,
    now: datetime | None = None,
) -> tuple[bool, str]:
    """Проверка cooldown для пары.

    Graduated cooldown:
      2+ SL подряд  → cooldown_after_2sl_hours
      SL exit        → cooldown_after_sl_hours
      Loss > -0.5%   → pair_cooldown_hours
      Иначе          → 0 (нет блока)

    v38.2: 🟢 ВХОД bypasses non-SL cooldowns.
    LOG-04 FIX: 🟢 ВХОД НЕ bypasses SL cooldowns.
    """
    if now is None:
        now = now_msk()

    is_green = '🟢' in str(entry_label) or 'ВХОД' in str(entry_label).upper()
    entry = cd_data.get(pair_name, {})

    if not entry.get('last_loss_time'):
        return False, ""

    try:
        loss_dt = datetime.fromisoformat(entry['last_loss_time'])
        hours_since = (now - loss_dt).total_seconds() / 3600

        is_sl = entry.get('sl_exit', False)
        consecutive_sl = entry.get('consecutive_sl', 0)
        session_pnl = entry.get('session_pnl', 0)

        if consecutive_sl >= 2:
            cooldown_h = cooldown_after_2sl_hours
            reason_tag = "2+ SL подряд"
        elif is_sl:
            cooldown_h = cooldown_after_sl_hours
            reason_tag = "SL"
        elif session_pnl < -0.5:
            cooldown_h = pair_cooldown_hours
            reason_tag = "убыток"
        else:
            return False, ""

        # 🟢 ВХОД bypasses ONLY non-SL cooldowns (LOG-04 FIX)
        if is_green and not is_sl and consecutive_sl < 2:
            return False, ""

        if cooldown_h > 0 and hours_since < cooldown_h:
            remaining = cooldown_h - hours_since
            return True, (
                f"⏳ {pair_name}: {cooldown_h:.0f}ч блок "
                f"({reason_tag}) {remaining:.1f}ч осталось"
            )
    except (ValueError, TypeError, KeyError):
        pass

    return False, ""


# ═══════════════════════════════════════════════════════
# CASCADE SL PROTECTION
# ═══════════════════════════════════════════════════════

def check_cascade_sl(
    cd_data: dict,
    cascade_enabled: bool = True,
    window_hours: float = 6.0,
    threshold: int = 3,
    pause_hours: float = 4.0,
    cascade_state: dict | None = None,
    now: datetime | None = None,
) -> tuple[bool, str]:
    """Проверка каскадной серии SL.

    R-02 FIX: uses pause_start + pause_h (matching file format from monitor wrapper).
    """
    if not cascade_enabled:
        return False, ""
    if now is None:
        now = now_msk()

    # Check active pause (R-02 FIX: reads pause_start, not pause_until)
    if cascade_state and cascade_state.get('pause_start'):
        try:
            pause_start = datetime.fromisoformat(cascade_state['pause_start'])
            state_pause_h = cascade_state.get('pause_h', pause_hours)
            elapsed_h = (now - pause_start).total_seconds() / 3600
            if elapsed_h < state_pause_h:
                remaining = state_pause_h - elapsed_h
                sl_count = cascade_state.get('sl_count', threshold)
                return True, (
                    f"🛑 CASCADE SL: пауза до "
                    f"{(pause_start + timedelta(hours=state_pause_h)).strftime('%H:%M')} "
                    f"({remaining:.1f}ч осталось)"
                )
        except (ValueError, TypeError):
            pass

    # Count SL exits in window
    cutoff = now - timedelta(hours=window_hours)
    sl_count = 0
    for entry in cd_data.values():
        if not entry.get('sl_exit'):
            continue
        try:
            loss_dt = datetime.fromisoformat(entry.get('last_loss_time', ''))
            if loss_dt >= cutoff:
                sl_count += 1
        except (ValueError, TypeError):
            pass

    if sl_count >= threshold:
        return True, (
            f"🛑 CASCADE SL: {sl_count} SL за {window_hours:.0f}ч "
            f"(порог {threshold}). Пауза {pause_hours:.0f}ч"
        )

    return False, ""


# ═══════════════════════════════════════════════════════
# WHITELIST
# ═══════════════════════════════════════════════════════

def is_whitelisted(
    coin1: str,
    coin2: str,
    direction: str,
    wl_pairs: set[str] | None,
    config_whitelist: list[str] | None = None,
) -> bool:
    """Проверка пары в whitelist.

    Приоритет:
      1. wl_pairs (из watchlist.json) — точная проверка пары+направления
      2. config_whitelist (из config.yaml strategy.whitelist) — список монет
      3. None/empty → разрешить всё

    Регрессия B-01: :BOTH добавляется ТОЛЬКО если direction==BOTH.
    """
    c1 = coin1.upper()
    c2 = coin2.upper()
    dir_up = direction.upper() if direction else "BOTH"

    # 1. Watchlist pairs (precise pair+direction match)
    if wl_pairs is not None:
        pair_key = f"{c1}/{c2}"
        pair_key_rev = f"{c2}/{c1}"
        for key in (pair_key, pair_key_rev):
            if f"{key}:BOTH" in wl_pairs:
                return True
            if f"{key}:{dir_up}" in wl_pairs:
                return True
        return False

    # 2. Config whitelist (coin-level)
    if config_whitelist:
        wl_upper = [c.upper() for c in config_whitelist]
        return c1 in wl_upper and c2 in wl_upper

    # 3. No whitelist configured → allow all
    return True


def build_watchlist_pairs(pairs_list: list[dict]) -> set[str]:
    """Построить set из watchlist.json для is_whitelisted().

    Регрессия B-01: :BOTH добавляется ТОЛЬКО если direction=="BOTH".
    Если direction=="LONG" → добавляем только :LONG, SHORT запрещён.
    """
    result = set()
    for p in pairs_list:
        if isinstance(p, dict) and p.get("coin1") and p.get("coin2"):
            c1 = p["coin1"].upper()
            c2 = p["coin2"].upper()
            direction = p.get("direction", "BOTH").upper()
            result.add(f"{c1}/{c2}:{direction}")
            # B-01 FIX: BOTH разрешает оба направления
            if direction == "BOTH":
                result.add(f"{c1}/{c2}:LONG")
                result.add(f"{c1}/{c2}:SHORT")
    return result


# ═══════════════════════════════════════════════════════
# HEDGE RATIO SAFETY
# ═══════════════════════════════════════════════════════

def is_hr_safe(
    hr: float | None,
    min_hr: float = 0.05,
    max_hr: float = 5.0,
) -> tuple[bool, str]:
    """Проверка безопасности hedge ratio.

    BUG-014 FIX: hr==0 блокирует, отрицательный HR допустим.
    """
    if hr is None or hr == 0:
        return False, "HR=0 (нет хеджа)"
    abs_hr = abs(hr)
    if abs_hr < min_hr:
        return False, f"|HR|={abs_hr:.4f} < {min_hr} (нет хеджа)"
    if abs_hr > max_hr:
        return False, f"|HR|={abs_hr:.1f} > {max_hr} (экстремальный)"
    return True, ""


# ═══════════════════════════════════════════════════════
# PAIR MEMORY BLOCKING
# ═══════════════════════════════════════════════════════

def pair_memory_is_blocked(
    pair: str,
    memory_data: dict | None,
    min_trades: int = 2,
    ignore: bool = False,
    heavy_loss_threshold: float = -5.0,
    heavy_loss_min_trades: int = 3,
) -> tuple[bool, str]:
    """Блокировка пары по памяти.

    Two conditions (R-03 FIX: both in core):
      1. 0 wins after min_trades+ сделок
      2. Heavy cumulative loss (total_pnl < heavy_loss_threshold after heavy_loss_min_trades+)
    """
    if ignore or not memory_data:
        return False, ""

    trades = memory_data.get('trades', 0)
    wins = memory_data.get('wins', 0)
    total_pnl = memory_data.get('total_pnl', 0)

    # Check 1: zero wins
    if trades >= min_trades and wins == 0:
        return True, (
            f"🚫 Pair memory: {pair} — {trades} сделок, "
            f"0 побед, PnL={total_pnl:+.2f}%"
        )

    # Check 2: heavy cumulative loss (R-03 FIX: was only in wrapper)
    if trades >= heavy_loss_min_trades and total_pnl < heavy_loss_threshold:
        wr = wins / trades * 100 if trades > 0 else 0
        return True, (
            f"🚫 Pair memory: {pair} — "
            f"total={total_pnl:+.2f}%, WR={wr:.0f}% "
            f"за {trades} сделок"
        )

    return False, ""


# ═══════════════════════════════════════════════════════
# POSITION SIZING
# ═══════════════════════════════════════════════════════

def risk_position_size(
    ml_result: dict,
    portfolio_usdt: float = 1000.0,
    open_positions: int = 0,
    max_positions: int = 5,
    max_per_trade_pct: float = 20.0,
    min_per_trade_pct: float = 5.0,
    max_total_exposure_pct: float = 80.0,
) -> dict:
    """Размер позиции на основе ML-грейда и риск-лимитов.

    Все параметры явные — не нужен CFG() внутри функции.

    Регрессия B-06: при remaining < min_per_trade → allowed=False
    (а не max() который превышал лимит).
    """
    # Position limit
    if open_positions >= max_positions:
        return {
            'size_usdt': 0, 'size_pct': 0,
            'reason': f'⛔ Лимит позиций: {open_positions}/{max_positions}',
            'allowed': False,
        }

    # Exposure limit
    current_exposure = open_positions * max_per_trade_pct
    remaining_pct = max_total_exposure_pct - current_exposure
    if remaining_pct <= 0:
        return {
            'size_usdt': 0, 'size_pct': 0,
            'reason': f'⛔ Exposure limit: {current_exposure}%/{max_total_exposure_pct}%',
            'allowed': False,
        }

    # Size based on grade
    grade = ml_result.get('grade', 'F')
    score = ml_result.get('score', 0)

    if grade == 'A':
        size_pct = max_per_trade_pct
    elif grade == 'B':
        size_pct = max_per_trade_pct * 0.75
    elif grade == 'C':
        size_pct = max_per_trade_pct * 0.5
    elif grade == 'D':
        size_pct = min_per_trade_pct
    else:
        return {
            'size_usdt': 0, 'size_pct': 0,
            'reason': f'⛔ Grade F — не торговать',
            'allowed': False,
        }

    # B-06 FIX: cap by remaining, and if below minimum → refuse
    size_pct = min(size_pct, remaining_pct)
    if size_pct < min_per_trade_pct:
        return {
            'size_usdt': 0, 'size_pct': 0,
            'reason': (
                f'⛔ Не хватает места в портфеле: '
                f'{remaining_pct:.1f}% < min {min_per_trade_pct}%'
            ),
            'allowed': False,
        }

    size_usdt = portfolio_usdt * size_pct / 100
    return {
        'size_usdt': round(size_usdt, 1),
        'size_pct': round(size_pct, 1),
        'reason': f'Grade {grade} ({score:.0f}pt): {size_pct:.0f}% = {size_usdt:.0f} USDT',
        'allowed': True,
    }


def recommend_position_size(
    quality_score: int,
    confidence: str,
    entry_readiness: str,
    hurst: float = 0.4,
    correlation: float = 0.5,
    base_size: float = 100.0,
) -> float:
    """Рекомендуемый размер позиции $25-$base_size*1.5.

    R-01 FIX: логика ИДЕНТИЧНА оригиналу из config_loader.
    G-02 FIX: min(size, base_size*1.5) вместо min(size, base_size).
    BUG-012 FIX: Hurst < 0.35 → bonus, > 0.48 → penalty.
    """
    # Step 1: Combined quality + confidence (ORIGINAL logic)
    if quality_score >= 80 and confidence == 'HIGH':
        mult = 1.0
    elif quality_score >= 60 and confidence in ('HIGH', 'MEDIUM'):
        mult = 0.75
    else:
        mult = 0.50

    # Step 2: Entry readiness (ORIGINAL logic — handles all 🟡 variants)
    er = str(entry_readiness)
    er_upper = er.upper()
    if '🟢' in er or 'ВХОД' in er_upper:
        pass  # no change
    elif '🟡' in er and 'УСЛОВНО' in er_upper:
        mult *= 0.90
    elif '🟡' in er or 'СЛАБЫЙ' in er_upper:
        mult *= 0.75
    else:
        mult *= 0.80

    # Step 3: Hurst bonus/penalty (BUG-012 FIX)
    if hurst < 0.35:
        mult *= 1.10
    elif hurst > 0.48:
        mult *= 0.80

    # Step 4: Correlation penalty
    if correlation < 0.3:
        mult *= 0.85

    size = max(25.0, round(base_size * mult / 5) * 5)
    return min(size, int(base_size * 1.5))  # G-02 FIX


# ═══════════════════════════════════════════════════════
# ANTI-REPEAT
# ═══════════════════════════════════════════════════════

def check_anti_repeat(
    pair_name: str,
    direction: str,
    cd_data: dict,
    is_green: bool = False,
    today_str: str | None = None,
) -> tuple[bool, str]:
    """Блокировка повторного входа после SL в том же направлении за сегодня.

    🟢 ВХОД обходит anti-repeat.
    """
    if is_green:
        return False, ""
    if today_str is None:
        today_str = now_msk().strftime('%Y-%m-%d')

    entry = cd_data.get(pair_name, {})
    if (entry.get('date') == today_str
            and entry.get('sl_exit', False)
            and entry.get('last_dir') == direction):
        reason = (
            f"Anti-repeat: SL в {direction} по {pair_name} "
            f"сегодня (bypass: 🟢 ВХОД)"
        )
        return True, reason

    return False, ""


# ═══════════════════════════════════════════════════════
# COIN POSITION LIMIT
# ═══════════════════════════════════════════════════════

def check_coin_position_limit(
    coin: str,
    open_positions: list[dict],
    max_coin_positions: int = 2,
) -> tuple[bool, str]:
    """Проверка лимита позиций на одну монету.

    Returns: (blocked, reason)
    """
    count = sum(
        1 for p in open_positions
        if coin in (p.get('coin1', ''), p.get('coin2', ''))
    )
    if count >= max_coin_positions:
        return True, (
            f"{coin} уже в {count} позициях (лимит {max_coin_positions})"
        )
    return False, ""
