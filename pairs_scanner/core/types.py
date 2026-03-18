"""
core/types.py — Общие типы данных для всей системы.

Волна 0: явные dataclass'ы вместо магических dict с 30+ полями.
Предотвращает класс багов типа B-09 (dedicated columns расходятся с JSON).

Тактика миграции:
  - Новый код использует dataclass напрямую
  - Старый код продолжает работать с dict
  - Position.from_dict() / .to_dict() — мост между мирами
"""

from __future__ import annotations
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Optional

MSK = timezone(timedelta(hours=3))


# ═══════════════════════════════════════════════════════
# POSITION — жизненный цикл одной торговой позиции
# ═══════════════════════════════════════════════════════

@dataclass
class Position:
    """Полная структура позиции. Единственный источник правды о полях."""
    id: int
    coin1: str
    coin2: str
    direction: str              # 'LONG' | 'SHORT'
    status: str = 'OPEN'        # 'OPEN' | 'CLOSED'

    # Entry data
    entry_z: float = 0.0
    entry_hr: float = 0.0
    entry_price1: float = 0.0
    entry_price2: float = 0.0
    entry_intercept: float = 0.0
    entry_time: str = ''
    entry_label: str = ''
    timeframe: str = '4h'
    notes: str = ''

    # Sizing
    recommended_size: float = 100.0
    risk_size_usdt: float = 0.0

    # Scoring (from scanner)
    quality_score: int = 0
    signal_score: int = 0
    z_window: Optional[int] = None

    # BT metrics (passed from scanner)
    bt_verdict: Optional[str] = None
    bt_pnl: Optional[float] = None
    mu_bt_wr: Optional[float] = None
    v_quality: Optional[float] = None

    # Flags (entry label parsing)
    flag_bt: str = ''
    flag_wl: str = ''
    flag_nk: str = ''

    # Per-pair TP/SL overrides
    pair_tp_pct: Optional[float] = None
    pair_sl_pct: Optional[float] = None
    max_hold_hours: Optional[float] = None
    pnl_stop_pct: Optional[float] = None

    # Auto-open
    auto_opened: bool = False

    # Exit data
    exit_price1: float = 0.0
    exit_price2: float = 0.0
    exit_z: float = 0.0
    exit_time: str = ''
    exit_reason: str = ''
    pnl_pct: float = 0.0

    # Trailing state
    exit_phase: int = 1
    _z_trail_activated: bool = False
    _z_trail_peak: float = 0.0
    _tp_trail_activated: bool = False
    _tp_trail_peak: float = 0.0
    _recovery_trail_activated: bool = False
    _recovery_trail_peak: float = 0.0
    _trail_params_locked: bool = False
    _trail_act_locked: float = 0.0
    _trail_dd_locked: float = 0.0

    # Phantom tracking
    phantom_max_pnl: float = 0.0
    phantom_min_pnl: float = 0.0
    phantom_track_until: str = ''

    # Runtime
    best_pnl: float = 0.0
    best_pnl_during_trade: float = 0.0
    monitor_messages: list = field(default_factory=list)

    # Bybit fill data
    bybit_price1: float = 0.0
    bybit_price2: float = 0.0
    bybit_qty1: float = 0.0
    bybit_qty2: float = 0.0

    def to_dict(self) -> dict:
        """Конвертация в dict для обратной совместимости с JSON-хранилищем."""
        d = {}
        for k, v in asdict(self).items():
            if v is None or v == '' or v == 0.0 or v == 0 or v is False:
                # Сохраняем только непустые поля (совместимость со старым форматом)
                # Но ВСЕГДА сохраняем ключевые поля
                if k in ('id', 'coin1', 'coin2', 'direction', 'status',
                         'entry_z', 'entry_hr', 'entry_price1', 'entry_price2',
                         'entry_time', 'timeframe', 'pnl_pct', 'auto_opened'):
                    d[k] = v
            else:
                d[k] = v
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'Position':
        """Создание из dict. Игнорирует неизвестные ключи (forward compat)."""
        known = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in d.items() if k in known}
        return cls(**filtered)


# ═══════════════════════════════════════════════════════
# SCANNER TYPES
# ═══════════════════════════════════════════════════════

@dataclass
class ScanConfig:
    """Конфигурация сканера. Все параметры явные — не нужен CFG() внутри функций."""
    timeframe: str = '4h'
    lookback_days: int = 50
    min_quality: int = 65
    max_halflife_hours: float = 28.0
    zscore_threshold: float = 2.3
    min_correlation: float = 0.20
    min_hurst: float = 0.45
    max_pvalue: float = 0.15
    max_hr: float = 5.0
    min_hr: float = 0.05
    commission_pct: float = 0.10
    slippage_pct: float = 0.05
    bt_filter_mode: str = 'HARD'
    wf_filter_mode: str = 'SOFT'
    ubt_filter_mode: str = 'HARD'
    n_pca_components: int = 3
    max_same_coin_signals: int = 3
    pass_bt_metrics: bool = True

    # Dual TF
    dual_tf_enabled: bool = False
    signal_tf: str = '1h'


@dataclass
class PairResult:
    """Результат анализа одной пары. Возвращается из scanner_engine."""
    coin1: str
    coin2: str
    zscore: float
    quality_score: int
    signal_score: int
    direction: str                  # 'LONG' | 'SHORT'
    halflife_hours: float
    entry_label: str
    hedge_ratio: float
    confidence: str                 # 'HIGH' | 'MEDIUM' | 'LOW'
    hurst: float = 0.0
    pvalue: float = 1.0
    spread: Optional[object] = None  # np.array
    correlation: float = 0.0
    stability_score: float = 0.0
    adf_passed: bool = False
    crossing_density: float = 0.0
    n_bars: int = 0
    halflife_days: float = 0.0
    intercept: float = 0.0
    z_window: int = 30
    hurst_is_fallback: bool = False
    ou_score: float = 0.0
    hr_std: float = 0.0
    # BT metrics
    bt_verdict: Optional[str] = None
    bt_pnl: Optional[float] = None
    mu_bt_wr: Optional[float] = None


# ═══════════════════════════════════════════════════════
# MONITOR TYPES
# ═══════════════════════════════════════════════════════

@dataclass
class MonitorState:
    """Результат мониторинга одной позиции за один тик."""
    price1_now: float
    price2_now: float
    pnl_pct: float
    z_now: float
    hours_in: float
    z_static: float = 0.0
    best_pnl: float = 0.0
    exit_signal: str = ''
    quality_warnings: list = field(default_factory=list)
    hurst_now: float = 0.5
    pvalue_now: float = 1.0
    correlation_now: float = 0.0
    hr_now: float = 0.0


@dataclass
class MonitorDecision:
    """Решение монитора по одной позиции."""
    position_id: int
    pnl_pct: float
    should_auto_close: bool
    close_reason: Optional[str]
    warnings: list = field(default_factory=list)
    monitor_data: dict = field(default_factory=dict)


# ═══════════════════════════════════════════════════════
# RISK TYPES
# ═══════════════════════════════════════════════════════

@dataclass
class RiskSizingResult:
    """Результат расчёта размера позиции."""
    size_usdt: float
    size_pct: float
    reason: str
    allowed: bool

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CooldownEntry:
    """Запись о cooldown для одной пары."""
    session_pnl: float = 0.0
    last_loss_time: Optional[str] = None
    last_dir: Optional[str] = None
    date: Optional[str] = None
    sl_exit: bool = False
    consecutive_sl: int = 0
