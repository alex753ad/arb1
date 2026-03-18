"""
core/utils.py — Общие утилиты. Единственный источник для now_msk(), MSK и т.д.

Ранее дублировались в 4+ файлах:
  - app.py:           MSK = timezone(timedelta(hours=3)); def now_msk()
  - monitor_v38_3.py: MSK = timezone(timedelta(hours=3)); def now_msk()
  - backtester:       MSK = timezone(timedelta(hours=3)); def now_msk()
  - mean_reversion:   import from datetime
  - bybit_executor:   MSK = timezone(timedelta(hours=3))
  - block_log:        MSK = timezone(timedelta(hours=3))
"""

import json
import os
import tempfile
from datetime import datetime, timezone, timedelta


# ═══════════════════════════════════════════════════════
# TIMEZONE
# ═══════════════════════════════════════════════════════

MSK = timezone(timedelta(hours=3))


def now_msk() -> datetime:
    """Текущее время в MSK. Единственный источник правды."""
    return datetime.now(MSK)


def to_msk(dt: datetime) -> str:
    """Форматировать datetime → 'HH:MM МСК'."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MSK)
    return dt.astimezone(MSK).strftime('%H:%M МСК')


def to_msk_full(dt: datetime) -> str:
    """Форматировать datetime → 'HH:MM:SS МСК DD.MM.YYYY'."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MSK)
    return dt.astimezone(MSK).strftime('%H:%M:%S МСК %d.%m.%Y')


def today_msk_str() -> str:
    """Сегодняшняя дата в MSK как строка 'YYYY-MM-DD'."""
    return now_msk().date().isoformat()


# ═══════════════════════════════════════════════════════
# ATOMIC FILE I/O
# ═══════════════════════════════════════════════════════

def atomic_json_save(path: str, data, indent: int = 2) -> bool:
    """Атомарная запись JSON через temp file + os.replace.
    
    Гарантирует, что файл либо полностью записан, либо не изменён.
    Ранее дублировалась в monitor_v38_3.py и config_loader.py.
    
    Returns: True при успешной записи.
    """
    dir_name = os.path.dirname(os.path.abspath(path))
    try:
        fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix='.tmp')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=indent, ensure_ascii=False, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
            return True
        except Exception:
            # Cleanup temp file on failure
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise
    except Exception:
        # Last resort: direct write (non-atomic)
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=indent, ensure_ascii=False, default=str)
            return True
        except Exception:
            return False


# ═══════════════════════════════════════════════════════
# COMMISSION & PnL CONSTANTS
# ═══════════════════════════════════════════════════════

# Bybit taker fee: 0.055% per leg × 2 legs × 2 (open + close) = 0.22%
# + slippage estimate: ~0.10%
# Total round-trip cost ≈ 0.32%
COMMISSION_ROUND_TRIP_PCT = 0.32


def calc_pair_pnl(
    direction: str,
    entry_price1: float, entry_price2: float,
    exit_price1: float, exit_price2: float,
    entry_hr: float,
    commission_pct: float = COMMISSION_ROUND_TRIP_PCT,
) -> float:
    """Расчёт PnL пары в процентах.
    
    LONG:  profit when spread narrows  → leg1 SHORT, leg2 LONG
    SHORT: profit when spread widens   → leg1 LONG, leg2 SHORT
    
    Returns: PnL в процентах (напр. 1.5 = +1.5%)
    """
    if entry_price1 <= 0 or entry_price2 <= 0:
        return 0.0
    
    ret1 = (exit_price1 - entry_price1) / entry_price1
    ret2 = (exit_price2 - entry_price2) / entry_price2
    
    if direction == 'LONG':
        # LONG spread: SHORT coin1, LONG coin2 (× hr)
        raw_pnl = (-ret1 + entry_hr * ret2) / (1 + abs(entry_hr))
    else:
        # SHORT spread: LONG coin1, SHORT coin2 (× hr)
        raw_pnl = (ret1 - entry_hr * ret2) / (1 + abs(entry_hr))
    
    return round((raw_pnl * 100) - commission_pct, 4)
