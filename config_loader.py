"""
config_loader.py — Единый конфиг для всех приложений v35.
Загружает config.yaml, даёт дефолты если файл отсутствует.

v35.1 (BUG FIX):
  - BUG-006: _load() / CFG_reload() потокобезопасны через threading.Lock
             (double-checked locking — fast path без блокировки при уже загруженном конфиге)
  - BUG-014: is_hr_safe() — hr==0 блокирует, отрицательный HR допустим;
             все проверки диапазона через abs(hr)
  - BUG-020: _DEFAULTS cooldown_after_sl_hours: 4 → 12 (синхронизировано с config.yaml)
  - BUG-025: pattern_analysis() — единственный источник данных trade_history.csv,
             убрано дублирующее чтение positions.json, дедуп через set() (O(1) вместо O(N²))

v35: Position sizing (conviction), volatility regime, two-phase exit,
     phantom auto-calibration, monitor messages tracking, pattern analysis fix.
v34: Z-exit→TRAIL mode, BT hard filter, adaptive TP, trail 1.2/0.6,
     max_coin_positions=2, cooldown_after_sl, pair_memory blocking,
     pass_bt_metrics from scanner to monitor.

Использование:
    from config_loader import CFG
    entry_z = CFG('strategy', 'entry_z')         # 2.5 — единственный источник правды: _DEFAULTS['strategy']['entry_z']
    commission = CFG('strategy', 'commission_pct') # 0.10
    refresh = CFG('scanner', 'refresh_interval_min') # 10
    
    # С дефолтом:
    val = CFG('strategy', 'new_param', default=42)
"""
import os
import threading  # BUG-006 FIX

_DEFAULTS = {
    'strategy': {
        'entry_z': 2.5, 'exit_z': 0.5, 'stop_z_offset': 2.0, 'min_stop_z': 4.0,
        # F-003 FIX: take_profit_pct 1.5→2.0 (синхронизировано с config.yaml TRADE-1)
        'take_profit_pct': 2.0, 'stop_loss_pct': -5.0, 'max_hold_hours': 8,
        'micro_bt_max_bars': 6, 'min_hurst': 0.45, 'warn_hurst': 0.48,
        'min_correlation': 0.20, 'hr_naked_threshold': 0.15, 'max_pvalue': 0.15,
        # v37 Wave 1.2: HR hard block thresholds
        'max_hr_threshold': 5.0, 'min_hr_threshold': 0.05,
        'commission_pct': 0.10, 'slippage_pct': 0.05,
        'bt_filter_mode': 'HARD',
        # CONFIG-2 FIX: wf_filter_mode — управляет блокировкой по walk-forward
        # HARD = блокировать при WF FAIL, SOFT = только предупреждение, OFF = игнорировать
        'wf_filter_mode': 'SOFT',
        # Пункт 12a: uBT SKIP блокирует авто-вход (HARD = блок, INFO = только метка)
        'ubt_filter_mode': 'HARD',
        'adaptive_tp': True,
        # v37 Wave 1.5: Hurst fallback uses EMA instead of blocking
        'use_hurst_ema_fallback': True,
    },
    'scanner': {
        'coins_limit': 100, 'timeframe': '4h', 'lookback_days': 50,
        # X-003 FIX: exchange 'okx'→'bybit' (синхронизировано с config.yaml)
        'exchange': 'bybit', 'refresh_interval_min': 10,
        # v37 Wave 1.4: min_quality default = 65 (TRADE-1 [5] FIX: было 60)
        'min_quality': 65,
        'pass_bt_metrics': True, 'max_same_coin_signals': 3,
    },
    'monitor': {
        'refresh_interval_sec': 60, 'exit_z_target': 0.5, 'pnl_stop_pct': -5.0,
        'trailing_z_bounce': 0.8, 'time_warning_ratio': 1.0, 'time_exit_ratio': 1.5,
        'time_critical_ratio': 2.0, 'overshoot_deep_z': 1.0,
        'pnl_trailing_threshold': 0.5, 'pnl_trailing_fraction': 0.4,
        'hurst_critical': 0.50, 'hurst_warning': 0.48, 'hurst_border': 0.45,
        'pvalue_warning': 0.10, 'correlation_warning': 0.20,
        # F-004 FIX: auto_tp 1.5→2.0, auto_sl -2.0→-2.5 (TRADE-1 калибровка)
        'auto_exit_enabled': True, 'auto_tp_pct': 2.0, 'auto_sl_pct': -2.5,
        'auto_exit_z': 0.3, 'auto_exit_z_min_pnl': 0.6,
        # v37 Wave 1.1: Z-trail mode default = TRAIL (was DISABLED in some places)
        'auto_exit_z_mode': 'TRAIL',
        # F-005 FIX: trailing_activate 1.2→1.0, trailing_drawdown 0.6→0.5 (TRADE-1)
        'trailing_enabled': True, 'trailing_activate_pct': 1.0, 'trailing_drawdown_pct': 0.5,
        'auto_flip_enabled': True, 'pair_cooldown_hours': 4,
        # F-004 FIX: pair_loss_limit -2.0→-2.5 (синхрон с auto_sl)
        'pair_loss_limit_pct': -2.5, 'coin_loss_warn_pct': -3.0,
        'daily_loss_limit_pct': -5.0,
        'cooldown_after_sl_hours': 12,  # BUG-020 FIX: было 4, синхронизировано с config.yaml
        # CONFIG-3 FIX: 9 ключей, используемых в monitor но отсутствовавших в _DEFAULTS.
        # Без них при недоступном config.yaml inline fallback в коде мог расходиться
        # с задокументированными значениями. Особенно критично cascade_sl_enabled:
        # inline fallback был False, т.е. cascade SL молча выключался.
        'cooldown_after_2sl_hours': 12,     # 2+ consecutive SL → 12ч блок
        'cascade_sl_enabled': True,         # Cascade SL protection (3+ SL in 2h → пауза)
        'cascade_sl_window_hours': 2,       # Окно подсчёта SL
        'cascade_sl_threshold': 3,          # Порог срабатывания (кол-во SL)
        'cascade_sl_pause_hours': 1,        # Длительность паузы
        'phase2_pnl_fallback': 1.2,         # % PnL → принудительно Phase 2 (TRADE-1 [6] FIX: было 1.5)
        'phase2_hours_fallback': 6.0,       # Часов → принудительно Phase 2
        'z_trail_activate': 0.5,            # Z-trail: порог активации
        'z_trail_drawdown': 0.3,            # Z-trail: допустимый откат
        # Пункт 10: HR drift пороги (вынесены из hardcoded 15%/20%)
        'hr_drift_warn_pct': 15,            # ⚠️ предупреждение
        'hr_drift_critical_pct': 40,        # 🚨 критический
        'max_positions': 10, 'max_coin_exposure': 4,
        'max_coin_positions': 2,
        'entry_grace_minutes': 5,   # MON-1 FIX: синхронизировано с config.yaml (было 10)
        'phantom_track_hours': 12,
        # v35: two-phase exit
        'two_phase_exit': True,
        'phase1_z_threshold': 0.5,
        'phase2_trail_activate': 0.8,
        'phase2_trail_drawdown': 0.4,
        # v35: phantom auto-calibration
        'phantom_autocalibrate': True,
        'phantom_autocalibrate_min_trades': 5,
        'phantom_autocalibrate_left_threshold': 2.0,
        # F-008 FIX: recovery trailing (было захардкожено в monitor)
        'recovery_trail_threshold': 0.5,   # +0.5% PnL для активации recovery trail
        'recovery_trail_drawdown': 0.3,    # допустимый откат после recovery
    },
    # v35: position sizing
    'position_sizing': {
        'enabled': True, 'base_size': 100, 'max_multiplier': 2.0,
        'min_size': 50, 'max_size': 250,
        'signal_entry_bonus': 0.5, 'high_z_bonus': 0.3,
        'bt_pass_bonus': 0.2, 'good_volume_bonus': 0.2,
        'bt_fail_penalty': -0.5,
    },
    # v35: volatility regime
    'volatility_regime': {
        'enabled': True, 'btc_atr_window': 14, 'btc_atr_timeframe': '1h',
        'normal_atr_max': 2.5, 'elevated_atr_max': 4.0,
        'elevated_sl_multiplier': 1.3, 'elevated_size_multiplier': 0.7,
        'extreme_block_entries': True,
    },
    # E-01 FIX: risk management defaults (используются в risk_position_size)
    'risk': {
        'max_positions': 5,
        'max_per_trade_pct': 20,
        'min_per_trade_pct': 5,
        'max_total_exposure_pct': 80,
        'portfolio_usdt': 1000,
    },
    # v38.2: Bybit Demo executor
    'bybit': {
        'enabled': False,
        'api_key': '',
        'api_secret': '',
        # Всегда используется api-demo.bybit.com (Demo Trading на bybit.com)
        # Ключи создаются в Demo Trading режиме: bybit.com → Demo Trading → API Management
        'demo_mirror': False,   # Mirror internal trades to Bybit Demo
        'leverage': 1,          # Default leverage for demo orders
        'size_pct': 100,        # % of recommended_size to use on Bybit (100 = same)
        # Прокси — нужен если Bybit заблокирован (Россия и др.)
        # Примеры: "http://user:pass@host:port" | "socks5://host:port" | "" (отключён)
        'proxy': '',
    },

    'backtester': {
        'n_bars': 300, 'max_bars': 50, 'min_bars': 2,
        'n_folds_wf': 3, 'train_pct': 0.65,
    },
    'z_velocity': {
        'lookback': 5, 'excellent_min_vel': 0.1, 'decel_threshold': 0.05,
    },
    'rally_filter': {
        'warning_z': 2.0, 'block_z': 2.5, 'exit_z': 0.0, 'cooldown_bars': 2,
    },
}

_config_data = None
_config_path = None
_config_lock = threading.RLock()  # BUG-006 FIX + DEADLOCK FIX: RLock (reentrant)
# CFG_auto_reload → _config_lock → CFG_reload → _config_lock = deadlock с обычным Lock


def _load():
    """Load config once (lazy singleton). BUG-006 FIX: потокобезопасен через _config_lock."""
    global _config_data, _config_path, _config_mtime
    # Fast path без блокировки — если уже загружен
    if _config_data is not None:
        return
    with _config_lock:
        # Повторная проверка внутри блокировки (double-checked locking)
        if _config_data is not None:
            return

        _config_data = {}
        for section, vals in _DEFAULTS.items():
            _config_data[section] = dict(vals)

        # Try to load config.yaml (also try streamlit/local variants)
        _dir = os.path.dirname(os.path.abspath(__file__))
        paths = [
            'config.yaml',
            'config_streamlit.yaml',
            'config_local.yaml',
            os.path.join(_dir, 'config.yaml'),
            os.path.join(_dir, 'config_streamlit.yaml'),
            os.path.join(_dir, 'config_local.yaml'),
        ]
        for path in paths:
            if os.path.exists(path):
                # A-03 FIX: авто-исправление прав доступа (содержит API ключи)
                try:
                    import stat
                    _mode = os.stat(path).st_mode
                    if _mode & stat.S_IROTH:  # world-readable
                        try:
                            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)  # → 600
                            import logging as _cfg_log
                            _cfg_log.getLogger("config_loader").info(
                                "🔒 A-03 FIX: %s права исправлены на 600 (было %s)",
                                path, oct(_mode)[-3:])
                        except OSError:
                            import logging as _cfg_log
                            _cfg_log.getLogger("config_loader").warning(
                                "⚠️ %s доступен для чтения всем (mode=%s). "
                                "Не удалось исправить — выполните: chmod 600 %s",
                                path, oct(_mode)[-3:], path)
                except Exception:
                    pass
                try:
                    import yaml
                    with open(path, 'r', encoding='utf-8') as f:
                        user = yaml.safe_load(f) or {}
                    _merge(user)
                    _config_path = path
                    # Init mtime so CFG_auto_reload doesn't reload immediately
                    try:
                        _config_mtime = os.path.getmtime(path)
                    except OSError:
                        pass
                    return
                except ImportError:
                    _merge(_parse_simple(path))
                    _config_path = path
                    try:
                        _config_mtime = os.path.getmtime(path)
                    except OSError:
                        pass
                    return
                except Exception:
                    pass


def _merge(user_cfg):
    """Merge user config over defaults."""
    for section, vals in user_cfg.items():
        if isinstance(vals, dict) and section in _config_data:
            _config_data[section].update(vals)
        elif isinstance(vals, dict):
            _config_data[section] = vals


def _parse_simple(path):
    """Parse YAML without PyYAML (handles simple flat sections).
    
    E-02 FIX: предупреждение при обнаружении глубины >2 уровней.
    CL-3 FIX: добавлен парсинг inline YAML-списков вида ['a', 'b'] и [a, b].
    """
    result = {}
    section = None
    _depth_warned = False
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            s = line.rstrip()
            if not s or s.lstrip().startswith('#'):
                continue
            indent = len(s) - len(s.lstrip())
            s = s.strip()
            # E-02 FIX: обнаружение глубины 3+ (8+ пробелов = 3й уровень)
            if indent >= 8 and not _depth_warned and ':' in s and not s.endswith(':'):
                import logging as _ps_log
                _ps_log.getLogger("config_loader").warning(
                    "⚠️ E-02: %s содержит YAML с вложенностью >2 уровней. "
                    "Fallback-парсер поддерживает только section.key. "
                    "Установите PyYAML: pip install pyyaml", path)
                _depth_warned = True
            if indent == 0 and s.endswith(':'):
                section = s[:-1]
                result[section] = {}
            elif ':' in s and section is not None:
                k, v = s.split(':', 1)
                k, v = k.strip(), v.strip()
                if '#' in v:
                    v = v[:v.index('#')].strip()
                # CL-3 FIX: парсинг inline YAML-списков ['a', 'b'] и [a, b]
                if v.startswith('[') and v.endswith(']'):
                    inner = v[1:-1].strip()
                    if inner:
                        v = [x.strip().strip("'\"") for x in inner.split(',') if x.strip()]
                    else:
                        v = []
                elif v.startswith('"') and v.endswith('"'):
                    v = v[1:-1]
                elif v.startswith("'") and v.endswith("'"):
                    v = v[1:-1]
                elif v.lower() in ('true', 'yes'):
                    v = True
                elif v.lower() in ('false', 'no'):
                    v = False
                else:
                    try:
                        v = int(v)
                    except ValueError:
                        try:
                            v = float(v)
                        except ValueError:
                            pass
                result[section][k] = v
    return result


def CFG(section, key=None, default=None):
    """
    Get config value.
    
    CFG('strategy', 'entry_z')       → 2.5  # BUG-N10 FIX: дефолт из _DEFAULTS
    CFG('strategy', 'entry_z', 2.0)  → 2.5 (from config) or 2.0 (if missing)
    CFG('strategy')                  → dict of all strategy params
    """
    _load()
    if key is None:
        return _config_data.get(section, {})
    return _config_data.get(section, {}).get(key, default)


def CFG_path():
    """Return path to loaded config file (or None)."""
    _load()
    return _config_path


def _reload_unlocked():
    """LOG-10 FIX: Internal reload without taking lock. Caller MUST hold _config_lock."""
    global _config_data
    _config_data = None


def CFG_reload():
    """Force reload from disk. BUG-006 FIX: атомарный сброс под блокировкой."""
    with _config_lock:
        _reload_unlocked()
    _load()


# ═══════════════════════════════════════════════════════
# v37 Wave 1.7: AUTO-RELOAD — перечитывать конфиг если файл изменился
# ═══════════════════════════════════════════════════════

_config_mtime = 0

def CFG_auto_reload():
    """Reload config if file changed on disk (call at start of each scan/monitor cycle).
    
    Checks file mtime — cheap OS call, safe to call every cycle.
    Returns True if config was reloaded, False if unchanged.
    FIX CL-1: _config_mtime read/write protected by _config_lock.
    LOG-10 FIX: uses _reload_unlocked() to avoid nested lock on RLock.
    """
    global _config_mtime
    _load()  # ensure initial load
    if _config_path is None:
        return False
    try:
        current_mtime = os.path.getmtime(_config_path)
        _need_reload = False
        with _config_lock:
            if current_mtime != _config_mtime:
                _config_mtime = current_mtime
                _reload_unlocked()
                _need_reload = True
        if _need_reload:
            _load()
            return True
    except OSError:
        pass
    return False


# ═══════════════════════════════════════════════════════
# v37 Wave 1.2: is_hr_safe() — единая функция проверки HR
# Вызывается из scanner (app.py) и monitor.
# ═══════════════════════════════════════════════════════

def is_hr_safe(hr, hr_std=None):
    """Thin wrapper: reads CFG thresholds, delegates to core.risk.is_hr_safe.
    BUG-014 FIX: отрицательный HR допустим."""
    try:
        from pairs_scanner.core.risk import is_hr_safe as _core_hr_safe
        ok, reason = _core_hr_safe(
            hr,
            min_hr=CFG('strategy', 'min_hr_threshold', 0.05),
            max_hr=CFG('strategy', 'max_hr_threshold', 5.0),
        )
        # Additional check: HR uncertainty (stays here — needs hr_std)
        if ok and hr_std is not None and abs(hr) > 0 and hr_std / abs(hr) > 1.0:
            return False, f"HR uncertainty {hr_std/abs(hr):.0%} > 100%"
        return ok, reason
    except ImportError:
        # Fallback: inline logic
        max_hr = CFG('strategy', 'max_hr_threshold', 5.0)
        min_hr = CFG('strategy', 'min_hr_threshold', 0.05)
        if hr == 0:
            return False, "HR=0 — нет зависимости между парой"
        if abs(hr) < min_hr:
            return False, f"|HR|={abs(hr):.4f} < {min_hr} — naked position"
        if abs(hr) > max_hr:
            return False, f"|HR|={abs(hr):.2f} > {max_hr} — нестабильный хедж"
        if hr_std is not None and abs(hr) > 0 and hr_std / abs(hr) > 1.0:
            return False, f"HR uncertainty {hr_std/abs(hr):.0%} > 100%"
        return True, ""


# ═══════════════════════════════════════════════════════
# PAIR MEMORY — v27: Track per-pair trade history
# ═══════════════════════════════════════════════════════

PAIR_MEMORY_FILE = 'pair_memory.json'

# E-006 FIX: SQLite для pair_memory
try:
    from db_store import db_pair_memory_load, db_pair_memory_save, db_pair_memory_get, db_pair_memory_update
    _USE_SQLITE_PM = True
except ImportError:
    _USE_SQLITE_PM = False

# PERF-02 FIX: mtime кэш для pair_memory (было: полное чтение при каждом вызове)
_pair_memory_cache = None
_pair_memory_mtime = 0.0

def _invalidate_pair_memory_cache():
    """PERF-02 FIX: сброс кэша после записи."""
    global _pair_memory_cache, _pair_memory_mtime
    _pair_memory_cache = None
    _pair_memory_mtime = 0.0

def pair_memory_load():
    """Load pair memory. E-006 FIX: SQLite primary, JSON fallback.
    PERF-02 FIX: mtime cache — prevents re-reading for every ml_score() call."""
    import json
    global _pair_memory_cache, _pair_memory_mtime
    # E-006: SQLite path
    if _USE_SQLITE_PM:
        try:
            from db_store import _get_conn as _pm_get_conn
            with _pm_get_conn(readonly=True) as _vc:
                _ver = _vc.execute("PRAGMA data_version").fetchone()[0]
            if _pair_memory_cache is not None and _pair_memory_mtime == _ver:
                return _pair_memory_cache
            _pair_memory_cache = db_pair_memory_load()
            _pair_memory_mtime = _ver
            return _pair_memory_cache
        except Exception:
            pass
    # JSON fallback with mtime cache
    bak_path = PAIR_MEMORY_FILE + ".bak"
    try:
        if os.path.exists(PAIR_MEMORY_FILE):
            current_mtime = os.path.getmtime(PAIR_MEMORY_FILE)
            if _pair_memory_cache is not None and current_mtime == _pair_memory_mtime:
                return _pair_memory_cache
    except Exception:
        pass
    for path in (PAIR_MEMORY_FILE, bak_path):
        try:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if path == bak_path and data:
                    import logging as _pm_log
                    _pm_log.getLogger(__name__).warning(
                        "pair_memory: восстановлен из .bak (%d пар)", len(data))
                    pair_memory_save(data)
                _pair_memory_cache = data
                try:
                    _pair_memory_mtime = os.path.getmtime(PAIR_MEMORY_FILE)
                except Exception:
                    pass
                return data
        except Exception:
            continue
    return {}


def pair_memory_save(data):
    """Save pair memory. E-006 FIX: SQLite primary + JSON backup."""
    import json, shutil
    # E-006: SQLite primary
    if _USE_SQLITE_PM:
        try:
            db_pair_memory_save(data)
        except Exception:
            pass
    # JSON backup (always, for compatibility)
    new_path = PAIR_MEMORY_FILE + ".new"
    bak_path = PAIR_MEMORY_FILE + ".bak"
    try:
        with open(new_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        if os.path.exists(PAIR_MEMORY_FILE) and os.path.getsize(PAIR_MEMORY_FILE) > 10:
            try:
                shutil.copy2(PAIR_MEMORY_FILE, bak_path)
            except Exception:
                pass
        os.replace(new_path, PAIR_MEMORY_FILE)
    except Exception:
        try:
            with open(PAIR_MEMORY_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)
        except Exception:
            pass
    finally:
        try:
            if os.path.exists(new_path):
                os.remove(new_path)
        except Exception:
            pass
    _invalidate_pair_memory_cache()  # PERF-02 FIX


def pair_memory_update(pair, pnl_pct, hold_hours, direction, entry_z, exit_z):
    """Update pair memory with a closed trade.
    C-03 FIX: uses atomic db_pair_memory_update when SQLite available."""
    mem = pair_memory_load()
    if pair not in mem:
        mem[pair] = {'trades': 0, 'wins': 0, 'total_pnl': 0, 'pnls': [],
                     'avg_hold': 0, 'best_pnl': -999, 'worst_pnl': 999,
                     'last_trade': '', 'directions': {}}
    p = mem[pair]
    p['trades'] += 1
    if pnl_pct > 0:
        p['wins'] += 1
    p['total_pnl'] = round(p['total_pnl'] + pnl_pct, 4)
    p['pnls'] = (p.get('pnls', []) + [round(pnl_pct, 4)])[-20:]  # keep last 20
    p['avg_hold'] = round((p.get('avg_hold', 0) * (p['trades'] - 1) + hold_hours) / p['trades'], 1)
    p['best_pnl'] = max(p.get('best_pnl', -999), pnl_pct)
    p['worst_pnl'] = min(p.get('worst_pnl', 999), pnl_pct)
    from datetime import datetime, timezone, timedelta
    p['last_trade'] = datetime.now(timezone(timedelta(hours=3))).strftime('%Y-%m-%d %H:%M')
    d = p.get('directions', {})
    d[direction] = d.get(direction, 0) + 1
    p['directions'] = d
    # C-03 FIX: atomic upsert via SQLite (avoids read-modify-write race)
    if _USE_SQLITE_PM:
        try:
            from db_store import db_pair_memory_update as _db_pm_update
            _db_pm_update(pair, p)
        except Exception:
            pair_memory_save(mem)
    else:
        pair_memory_save(mem)
    return p


def pair_memory_get(pair):
    """Get pair memory stats, or None."""
    mem = pair_memory_load()
    return mem.get(pair)


def pair_memory_summary(pair):
    """One-line summary for display."""
    p = pair_memory_get(pair)
    if not p or p.get('trades', 0) == 0:
        return None
    wr = p['wins'] / p['trades'] * 100
    avg = p['total_pnl'] / p['trades']
    return (f"📝 {p['trades']} сделок, WR={wr:.0f}%, "
            f"avg={avg:+.2f}%, total={p['total_pnl']:+.2f}%, "
            f"hold={p['avg_hold']:.0f}ч")


# ═══════════════════════════════════════════════════════
# F-012 FIX: recommend_position_size — ЕДИНСТВЕННЫЙ ИСТОЧНИК
# Перенесён из monitor_v38_3.py (исправленная версия с BUG-012 FIX).
# app.py и monitor импортируют отсюда, локальные копии удалены.
# ═══════════════════════════════════════════════════════

def recommend_position_size(quality_score, confidence, entry_readiness,
                            hurst=0.4, correlation=0.5, base_size=100):
    """Thin wrapper: delegates to core.risk.recommend_position_size."""
    try:
        from pairs_scanner.core.risk import recommend_position_size as _core_rps
        return _core_rps(quality_score, confidence, entry_readiness,
                         hurst, correlation, base_size)
    except ImportError:
        # Fallback: ORIGINAL logic (R-01 FIX: must match original, not core)
        if quality_score >= 80 and confidence == 'HIGH':
            mult = 1.0
        elif quality_score >= 60 and confidence in ('HIGH', 'MEDIUM'):
            mult = 0.75
        else:
            mult = 0.50
        er = str(entry_readiness)
        er_upper = er.upper()
        if '🟢' in er or 'ВХОД' in er_upper:
            pass
        elif '🟡' in er and 'УСЛОВНО' in er_upper:
            mult *= 0.90
        elif '🟡' in er or 'СЛАБЫЙ' in er_upper:
            mult *= 0.75
        else:
            mult *= 0.80
        if hurst < 0.35: mult *= 1.10
        elif hurst > 0.48: mult *= 0.80
        if correlation < 0.3: mult *= 0.85
        size = max(25.0, round(base_size * mult / 5) * 5)
        return min(size, int(base_size * 1.5))


def pair_memory_is_blocked(pair):
    """Thin wrapper: reads CFG + pair_memory_get, delegates to core.risk."""
    try:
        from pairs_scanner.core.risk import pair_memory_is_blocked as _core_pm_blocked
        ignore = CFG('strategy', 'ignore_pair_memory', False)
        p = pair_memory_get(pair)
        # R-03 FIX: heavy-loss check now in core — no need for extra check here
        return _core_pm_blocked(pair, p, min_trades=2, ignore=ignore)
    except ImportError:
        # Fallback: inline logic
        if CFG('strategy', 'ignore_pair_memory', False):
            return False, ""
        p = pair_memory_get(pair)
        if not p or p.get('trades', 0) < 2:
            return False, ""
        if p.get('wins', 0) == 0 and p.get('trades', 0) >= 2:
            return True, (f"🚫 PAIR MEMORY BLOCK: {pair} — "
                          f"{p['trades']} сделок, 0 побед, "
                          f"total={p['total_pnl']:+.2f}%")
        if p.get('total_pnl', 0) < -5.0 and p.get('trades', 0) >= 3:
            wr = p['wins'] / p['trades'] * 100
            return True, (f"🚫 PAIR MEMORY BLOCK: {pair} — "
                          f"total={p['total_pnl']:+.2f}%, WR={wr:.0f}% "
                          f"за {p['trades']} сделок")
        return False, ""


def adaptive_tp_value(entry_z):
    """v34: Calculate adaptive TP based on entry Z-score magnitude.
    Higher |Z| = more room for reversion = higher TP target.
    F-013 FIX: результат не может быть ниже базового auto_tp_pct из конфига.
    LOG-06 FIX: мультипликативная формула вместо ступенчатой.
    Старая логика: max(adaptive_step, base_tp) = base_tp для всех Z<=3.0 при base_tp=2.0.
    Новая логика: base_tp * (1 + bonus) где bonus зависит от |Z| - entry_z.
    """
    base_tp = CFG('monitor', 'auto_tp_pct', 2.0)
    if not CFG('strategy', 'adaptive_tp', True):
        return base_tp
    az = abs(entry_z)
    threshold_z = CFG('strategy', 'entry_z', 2.5)
    # Бонус: каждые 0.5 Z сверх порога = +15% TP
    excess_z = max(0, az - threshold_z)
    bonus = min(0.5, excess_z * 0.3)  # cap at +50%
    adaptive = round(base_tp * (1.0 + bonus), 2)
    return max(adaptive, base_tp)


# ═══════════════════════════════════════════════════════
# R7 ML SCORING — weighted feature scoring model
# ═══════════════════════════════════════════════════════

def ml_score(pair_data):
    """
    R7: ML-like scoring model for trade quality prediction.
    Returns: {'score': 0-100, 'grade': A/B/C/D/F, 'factors': {...}, 'recommendation': str}
    
    Uses logistic-weighted features calibrated from real trade outcomes.
    """
    import math
    
    factors = {}
    
    # 1. Z-score strength (0-20 pts)
    z = abs(pair_data.get('zscore', 0))
    entry_z = CFG('strategy', 'entry_z', 2.5)  # BUG-N10 FIX: синхронизировано с _DEFAULTS
    z_ratio = z / entry_z if entry_z > 0 else 0
    z_pts = min(20, z_ratio * 10)  # 2x threshold = 20 pts
    factors['z_strength'] = round(z_pts, 1)
    
    # 2. μBT Quick% (0-25 pts) — strongest predictor from real results
    mbt_q = pair_data.get('mbt_quick', 0)
    mbt_pts = mbt_q / 100 * 25
    factors['mbt_quick'] = round(mbt_pts, 1)
    
    # 3. Hurst quality (0-15 pts) — lower is better
    hurst = pair_data.get('hurst', 0.5)
    if hurst < 0.1:
        h_pts = 15
    elif hurst < 0.3:
        h_pts = 12
    elif hurst < 0.45:
        h_pts = 8
    else:
        h_pts = 0
    factors['hurst'] = h_pts
    
    # 4. Correlation (0-10 pts)
    corr = abs(pair_data.get('correlation', 0))
    c_pts = min(10, corr * 12)  # ρ=0.83 → 10 pts
    factors['correlation'] = round(c_pts, 1)
    
    # 5. Statistical tests (0-10 pts)
    stat_pts = 0
    if pair_data.get('adf_passed'): stat_pts += 3
    if pair_data.get('johansen_coint'): stat_pts += 4
    if pair_data.get('fdr_passed'): stat_pts += 3
    factors['statistics'] = stat_pts
    
    # 6. Regime + MTF (0-10 pts)
    regime_pts = 0
    if pair_data.get('regime') == 'MEAN_REVERT': regime_pts += 5
    if pair_data.get('mtf_confirmed'): regime_pts += 5
    factors['regime_mtf'] = regime_pts
    
    # 7. Pair Memory bonus/penalty (-5 to +10 pts)
    mem = pair_memory_get(pair_data.get('pair', ''))
    mem_pts = 0
    if mem and mem.get('trades', 0) >= 2:
        wr = mem['wins'] / mem['trades']
        avg_pnl = mem['total_pnl'] / mem['trades']
        if wr >= 0.8 and avg_pnl > 0:
            mem_pts = min(10, avg_pnl * 10)
        elif wr < 0.3:
            mem_pts = -5
    factors['pair_memory'] = round(mem_pts, 1)
    
    # 8. Risk penalty — naked HR, high uncertainty
    risk_pen = 0
    if pair_data.get('hr_naked'): risk_pen -= 10
    if pair_data.get('hr_uncertainty', 0) > 0.3: risk_pen -= 5
    if pair_data.get('cusum_risk') in ('HIGH', 'CRITICAL'): risk_pen -= 5
    factors['risk_penalty'] = risk_pen
    
    total = max(0, min(100, sum(factors.values())))
    
    # Grade
    if total >= 80: grade = 'A'
    elif total >= 65: grade = 'B'
    elif total >= 50: grade = 'C'
    elif total >= 35: grade = 'D'
    else: grade = 'F'
    
    # Recommendation
    if grade in ('A', 'B'):
        rec = 'ВХОД — сильный сигнал'
    elif grade == 'C':
        rec = 'УСЛОВНО — уменьшить размер'
    elif grade == 'D':
        rec = 'РИСКОВАННО — минимальный размер'
    else:
        rec = 'ПРОПУСТИТЬ'
    
    return {
        'score': round(total, 1),
        'grade': grade,
        'factors': factors,
        'recommendation': rec,
    }


# ═══════════════════════════════════════════════════════
# R10 RISK MANAGER — position sizing & limits
# ═══════════════════════════════════════════════════════

def risk_position_size(ml_result, portfolio_usdt=1000, open_positions=0):
    """Thin wrapper: reads CFG risk params, delegates to core.risk.risk_position_size."""
    try:
        from pairs_scanner.core.risk import risk_position_size as _core_rps
        return _core_rps(
            ml_result=ml_result,
            portfolio_usdt=portfolio_usdt,
            open_positions=open_positions,
            max_positions=CFG('risk', 'max_positions', 5),
            max_per_trade_pct=CFG('risk', 'max_per_trade_pct', 20),
            min_per_trade_pct=CFG('risk', 'min_per_trade_pct', 5),
            max_total_exposure_pct=CFG('risk', 'max_total_exposure_pct', 80),
        )
    except ImportError:
        # Fallback: inline logic
        max_positions = CFG('risk', 'max_positions', 5)
        max_per_trade_pct = CFG('risk', 'max_per_trade_pct', 20)
        min_per_trade_pct = CFG('risk', 'min_per_trade_pct', 5)
        max_total_exposure_pct = CFG('risk', 'max_total_exposure_pct', 80)
        if open_positions >= max_positions:
            return {'size_usdt': 0, 'size_pct': 0,
                    'reason': f'⛔ Лимит позиций: {open_positions}/{max_positions}', 'allowed': False}
        current_exposure = open_positions * max_per_trade_pct
        remaining_pct = max_total_exposure_pct - current_exposure
        if remaining_pct <= 0:
            return {'size_usdt': 0, 'size_pct': 0,
                    'reason': f'⛔ Exposure limit: {current_exposure}%/{max_total_exposure_pct}%', 'allowed': False}
        grade = ml_result.get('grade', 'F')
        score = ml_result.get('score', 0)
        if grade == 'A': size_pct = max_per_trade_pct
        elif grade == 'B': size_pct = max_per_trade_pct * 0.75
        elif grade == 'C': size_pct = max_per_trade_pct * 0.5
        elif grade == 'D': size_pct = min_per_trade_pct
        else:
            return {'size_usdt': 0, 'size_pct': 0, 'reason': '⛔ Grade F', 'allowed': False}
        size_pct = min(size_pct, remaining_pct)
        if size_pct < min_per_trade_pct:
            return {'size_usdt': 0, 'size_pct': 0,
                    'reason': f'⛔ Не хватает места: {remaining_pct:.1f}% < min {min_per_trade_pct}%',
                    'allowed': False}
        size_usdt = portfolio_usdt * size_pct / 100
        return {'size_usdt': round(size_usdt, 1), 'size_pct': round(size_pct, 1),
                'reason': f'Grade {grade} ({score:.0f}pt): {size_pct:.0f}% = {size_usdt:.0f} USDT',
                'allowed': True}


# ═══════════════════════════════════════════════════════
# 3.3 PATTERN ANALYSIS — discover what works from history
# ═══════════════════════════════════════════════════════

TRADE_HISTORY_FILE = 'trade_history.csv'

def pattern_analysis():
    """
    3.3: Analyze trade history patterns.

    BUG-025 FIX: убрано дублирующее чтение positions.json.
    Единственный источник истины — trade_history.csv.
    Дедупликация по id через set() вместо O(N²) any()-поиска по списку.

    Returns dict with patterns by Z-range, direction, hold time, time of day, pair.
    """
    import csv
    from datetime import datetime

    all_trades = {}   # keyed by id → O(1) dedup
    seen_ids = set()  # BUG-025 FIX: быстрая проверка дублей

    # Единственный источник: trade_history.csv
    try:
        with open(TRADE_HISTORY_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                tid = str(row.get('id', ''))
                if tid and tid != '0' and tid not in seen_ids:
                    seen_ids.add(tid)
                    all_trades[tid] = row
    except Exception:
        pass
    
    trades = list(all_trades.values())
    
    if len(trades) < 3:
        return {'error': f'Only {len(trades)} trades - need >=3', 'n_trades': len(trades)}
    
    result = {'n_trades': len(trades)}
    
    # Parse trades
    parsed = []
    for t in trades:
        try:
            pnl = float(t.get('pnl_pct', 0) or 0)
            ez = float(t.get('entry_z', 0) or 0)
            d = t.get('direction', '')
            pair = t.get('pair', '')
            
            # Hold hours
            try:
                _et_str = str(t.get('entry_time', '')).replace('+03:00', '')
                _xt_str = str(t.get('exit_time', '')).replace('+03:00', '')
                et = datetime.fromisoformat(_et_str)
                xt = datetime.fromisoformat(_xt_str)
                hold_h = (xt - et).total_seconds() / 3600
                entry_hour = et.hour
            except Exception:
                hold_h = 0
                entry_hour = 12
            
            parsed.append({
                'pnl': pnl, 'ez': ez, 'dir': d, 'pair': pair,
                'hold_h': hold_h, 'entry_hour': entry_hour,
                'is_auto': ('AUTO' in str(t.get('notes', '')) or 
                           str(t.get('auto_opened', '')).lower() == 'true'),
            })
        except Exception:
            continue
    
    if not parsed:
        return {'error': 'No parseable trades', 'n_trades': 0}
    
    result['n_trades'] = len(parsed)
    
    # 1. Direction pattern
    longs = [t for t in parsed if t['dir'] == 'LONG']
    shorts = [t for t in parsed if t['dir'] == 'SHORT']
    result['by_direction'] = {
        'LONG': {'n': len(longs), 
                 'wr': sum(1 for t in longs if t['pnl'] > 0) / max(1, len(longs)) * 100,
                 'avg': sum(t['pnl'] for t in longs) / max(1, len(longs))},
        'SHORT': {'n': len(shorts),
                  'wr': sum(1 for t in shorts if t['pnl'] > 0) / max(1, len(shorts)) * 100,
                  'avg': sum(t['pnl'] for t in shorts) / max(1, len(shorts))},
    }
    
    # 2. Z-range pattern
    z_ranges = {'1.5-2.0': [], '2.0-2.5': [], '2.5-3.0': [], '3.0+': []}
    for t in parsed:
        az = abs(t['ez'])
        if az >= 3.0: z_ranges['3.0+'].append(t)
        elif az >= 2.5: z_ranges['2.5-3.0'].append(t)
        elif az >= 2.0: z_ranges['2.0-2.5'].append(t)
        else: z_ranges['1.5-2.0'].append(t)
    result['by_z_range'] = {}
    for rng, ts in z_ranges.items():
        if ts:
            result['by_z_range'][rng] = {
                'n': len(ts),
                'wr': sum(1 for t in ts if t['pnl'] > 0) / len(ts) * 100,
                'avg': sum(t['pnl'] for t in ts) / len(ts),
            }
    
    # 3. Hold time pattern
    quick = [t for t in parsed if t['hold_h'] <= 2]
    medium = [t for t in parsed if 2 < t['hold_h'] <= 8]
    long_ = [t for t in parsed if t['hold_h'] > 8]
    result['by_hold'] = {}
    for name, group in [('<=2h', quick), ('2-8h', medium), ('>8h', long_)]:
        if group:
            result['by_hold'][name] = {
                'n': len(group),
                'wr': sum(1 for t in group if t['pnl'] > 0) / len(group) * 100,
                'avg': sum(t['pnl'] for t in group) / len(group),
            }
    
    # 4. Time of day pattern (Moscow)
    morning = [t for t in parsed if 6 <= t['entry_hour'] < 12]
    afternoon = [t for t in parsed if 12 <= t['entry_hour'] < 18]
    evening = [t for t in parsed if 18 <= t['entry_hour'] or t['entry_hour'] < 6]
    result['by_time'] = {}
    for name, group in [('06-12 MSK', morning), ('12-18 MSK', afternoon), ('18-06 MSK', evening)]:
        if group:
            result['by_time'][name] = {
                'n': len(group),
                'wr': sum(1 for t in group if t['pnl'] > 0) / len(group) * 100,
                'avg': sum(t['pnl'] for t in group) / len(group),
            }
    
    # 5. Top pairs
    pair_stats = {}
    for t in parsed:
        if t['pair'] not in pair_stats:
            pair_stats[t['pair']] = {'pnls': [], 'n': 0}
        pair_stats[t['pair']]['pnls'].append(t['pnl'])
        pair_stats[t['pair']]['n'] += 1
    result['by_pair'] = {}
    for pair, stats in sorted(pair_stats.items(), key=lambda x: sum(x[1]['pnls']), reverse=True):
        result['by_pair'][pair] = {
            'n': stats['n'],
            'total': round(sum(stats['pnls']), 3),
            'avg': round(sum(stats['pnls']) / stats['n'], 3),
            'wr': round(sum(1 for p in stats['pnls'] if p > 0) / stats['n'] * 100, 0),
        }
    
    # 6. Auto vs Manual
    auto = [t for t in parsed if t['is_auto']]
    manual = [t for t in parsed if not t['is_auto']]
    result['auto_vs_manual'] = {
        'auto': {'n': len(auto),
                 'wr': sum(1 for t in auto if t['pnl'] > 0) / max(1, len(auto)) * 100,
                 'avg': sum(t['pnl'] for t in auto) / max(1, len(auto))},
        'manual': {'n': len(manual),
                   'wr': sum(1 for t in manual if t['pnl'] > 0) / max(1, len(manual)) * 100,
                   'avg': sum(t['pnl'] for t in manual) / max(1, len(manual))},
    }
    
    # 7. Best entry conditions
    winners = [t for t in parsed if t['pnl'] > 0]
    losers = [t for t in parsed if t['pnl'] <= 0]
    if winners and losers:
        import statistics
        result['winner_profile'] = {
            'avg_z': round(statistics.mean([abs(t['ez']) for t in winners]), 2),
            'avg_hold': round(statistics.mean([t['hold_h'] for t in winners]), 1),
        }
        result['loser_profile'] = {
            'avg_z': round(statistics.mean([abs(t['ez']) for t in losers]), 2),
            'avg_hold': round(statistics.mean([t['hold_h'] for t in losers]), 1),
        }
    
    return result


def pattern_summary():
    """One-line pattern insights for display."""
    p = pattern_analysis()
    if p.get('error'):
        return p['error']
    
    lines = [f"📊 {p['n_trades']} сделок проанализировано"]
    
    # Best direction
    bd = p.get('by_direction', {})
    if bd.get('SHORT', {}).get('avg', 0) > bd.get('LONG', {}).get('avg', 0):
        lines.append(f"📈 SHORT лучше: avg {bd['SHORT']['avg']:+.2f}% vs LONG {bd.get('LONG',{}).get('avg',0):+.2f}%")
    elif bd.get('LONG', {}).get('n', 0) > 0:
        lines.append(f"📈 LONG лучше: avg {bd['LONG']['avg']:+.2f}%")
    
    # Best Z range
    bz = p.get('by_z_range', {})
    if bz:
        best_z = max(bz.items(), key=lambda x: x[1]['avg'])
        lines.append(f"🎯 Лучший Z: {best_z[0]} (avg {best_z[1]['avg']:+.2f}%, WR={best_z[1]['wr']:.0f}%)")
    
    # Best hold time
    bh = p.get('by_hold', {})
    if bh:
        best_h = max(bh.items(), key=lambda x: x[1]['avg'])
        lines.append(f"⏱ Лучший hold: {best_h[0]} (avg {best_h[1]['avg']:+.2f}%)")
    
    return " | ".join(lines)


# ═══════════════════════════════════════════════════════
# v35: CONVICTION-BASED POSITION SIZING
# ═══════════════════════════════════════════════════════

def conviction_position_size(pair_data, bt_verdict=None, v_quality=None):
    """
    v35: Calculate position size based on conviction score.
    Uses signal quality, BT results, Z-score, volume quality.
    
    Returns: {'size_usdt': float, 'multiplier': float, 'reason': str}
    """
    if not CFG('position_sizing', 'enabled', True):
        base = CFG('position_sizing', 'base_size', 100)
        return {'size_usdt': base, 'multiplier': 1.0, 'reason': 'Sizing disabled'}
    
    base = CFG('position_sizing', 'base_size', 100)
    max_mult = CFG('position_sizing', 'max_multiplier', 2.0)
    min_size = CFG('position_sizing', 'min_size', 50)
    max_size = CFG('position_sizing', 'max_size', 250)
    
    multiplier = 1.0
    reasons = []
    
    # 1. Signal type bonus
    signal_type = pair_data.get('signal', pair_data.get('signal_type', ''))
    entry_label = pair_data.get('entry_label', '')
    if signal_type == 'SIGNAL' and bt_verdict != 'FAIL':
        bonus = CFG('position_sizing', 'signal_entry_bonus', 0.5)
        multiplier += bonus
        reasons.append(f'SIGNAL +{bonus}')
    
    # 2. High |Z| bonus
    z = abs(pair_data.get('zscore', pair_data.get('entry_z', 0)))
    if z > 2.5:
        bonus = CFG('position_sizing', 'high_z_bonus', 0.3)
        multiplier += bonus
        reasons.append(f'|Z|={z:.1f} +{bonus}')
    
    # 3. BT verdict
    if bt_verdict == 'PASS':
        bonus = CFG('position_sizing', 'bt_pass_bonus', 0.2)
        multiplier += bonus
        reasons.append(f'BT PASS +{bonus}')
    elif bt_verdict == 'FAIL':
        penalty = CFG('position_sizing', 'bt_fail_penalty', -0.5)
        multiplier += penalty
        reasons.append(f'BT FAIL {penalty}')
    
    # 4. Volume quality
    if v_quality in ('GOOD', 'EXCELLENT'):
        bonus = CFG('position_sizing', 'good_volume_bonus', 0.2)
        multiplier += bonus
        reasons.append(f'V={v_quality} +{bonus}')
    
    # 5. Pair memory bonus
    pair_name = pair_data.get('pair', '')
    mem = pair_memory_get(pair_name)
    if mem and mem.get('trades', 0) >= 3:
        wr = mem['wins'] / mem['trades']
        if wr >= 0.7 and mem.get('total_pnl', 0) > 0:
            multiplier += 0.2
            reasons.append(f'Memory WR={wr:.0%} +0.2')
        elif wr < 0.3:
            multiplier -= 0.3
            reasons.append(f'Memory WR={wr:.0%} -0.3')
    
    # Clamp multiplier
    multiplier = max(0.5, min(max_mult, multiplier))
    
    size = base * multiplier
    size = max(min_size, min(max_size, round(size / 5) * 5))  # round to $5
    
    return {
        'size_usdt': size,
        'multiplier': round(multiplier, 2),
        'reason': f'${size:.0f} (x{multiplier:.2f}): ' + ', '.join(reasons) if reasons else f'${size:.0f} (base)',
    }


# ═══════════════════════════════════════════════════════
# v35: VOLATILITY REGIME DETECTION
# ═══════════════════════════════════════════════════════

def check_volatility_regime(btc_closes):
    """
    v35: Detect BTC volatility regime.
    
    Args:
        btc_closes: numpy array of BTC close prices (1h candles)
    
    Returns: {'regime': 'NORMAL'|'ELEVATED'|'EXTREME', 
              'atr_pct': float, 'sl_mult': float, 'size_mult': float,
              'block_entries': bool}
    """
    import numpy as np
    
    if btc_closes is None or len(btc_closes) < 20:
        return {'regime': 'NORMAL', 'atr_pct': 0, 'sl_mult': 1.0, 
                'size_mult': 1.0, 'block_entries': False}
    
    closes = np.array(btc_closes, float)
    window = CFG('volatility_regime', 'btc_atr_window', 14)
    
    # ATR% calculation
    highs = closes  # simplified: use close-to-close volatility
    n = len(closes)
    if n < window + 1:
        return {'regime': 'NORMAL', 'atr_pct': 0, 'sl_mult': 1.0,
                'size_mult': 1.0, 'block_entries': False}
    
    # True range approximation from close prices
    returns = np.abs(np.diff(closes) / closes[:-1]) * 100
    atr_pct = float(np.mean(returns[-window:]))
    
    normal_max = CFG('volatility_regime', 'normal_atr_max', 2.5)
    elevated_max = CFG('volatility_regime', 'elevated_atr_max', 4.0)
    
    if atr_pct <= normal_max:
        regime = 'NORMAL'
        sl_mult = 1.0
        size_mult = 1.0
        block = False
    elif atr_pct <= elevated_max:
        regime = 'ELEVATED'
        sl_mult = CFG('volatility_regime', 'elevated_sl_multiplier', 1.3)
        size_mult = CFG('volatility_regime', 'elevated_size_multiplier', 0.7)
        block = False
    else:
        regime = 'EXTREME'
        sl_mult = CFG('volatility_regime', 'elevated_sl_multiplier', 1.3) * 1.2
        size_mult = 0.5
        block = CFG('volatility_regime', 'extreme_block_entries', True)
    
    return {
        'regime': regime,
        'atr_pct': round(atr_pct, 3),
        'sl_mult': round(sl_mult, 2),
        'size_mult': round(size_mult, 2),
        'block_entries': block,
    }


# ═══════════════════════════════════════════════════════
# v35: PHANTOM AUTO-CALIBRATION
# ═══════════════════════════════════════════════════════

def phantom_autocalibrate():
    """
    v35: Auto-calibrate trailing params based on phantom data.
    If average "left on table" > threshold, widen trail params.
    
    Returns: {'adjusted': bool, 'trail_activate': float, 'trail_drawdown': float,
              'avg_left': float, 'n_trades': int, 'reason': str}
    """
    import json
    
    if not CFG('monitor', 'phantom_autocalibrate', True):
        return {'adjusted': False, 'trail_activate': CFG('monitor', 'trailing_activate_pct', 1.0),
                'trail_drawdown': CFG('monitor', 'trailing_drawdown_pct', 0.5),
                'avg_left': 0, 'n_trades': 0, 'reason': 'Autocalibrate disabled'}
    
    min_trades = CFG('monitor', 'phantom_autocalibrate_min_trades', 5)
    left_threshold = CFG('monitor', 'phantom_autocalibrate_left_threshold', 2.0)
    
    # Collect phantom data from positions.json
    left_on_table = []
    try:
        if os.path.exists('positions.json'):
            with open('positions.json', 'r', encoding='utf-8') as f:
                positions = json.load(f)
            for p in positions:
                if p.get('status') != 'CLOSED':
                    continue
                pnl = float(p.get('pnl_pct', 0) or 0)
                ph_max = p.get('phantom_max_pnl')
                if ph_max is not None:
                    try:
                        ph_max = float(ph_max)
                        delta = ph_max - pnl
                        if delta > 0:
                            left_on_table.append(delta)
                    except (TypeError, ValueError):
                        pass
    except Exception:
        pass
    
    base_act = CFG('monitor', 'trailing_activate_pct', 1.0)
    base_dd = CFG('monitor', 'trailing_drawdown_pct', 0.5)
    
    if len(left_on_table) < min_trades:
        return {'adjusted': False, 'trail_activate': base_act,
                'trail_drawdown': base_dd,
                'avg_left': 0, 'n_trades': len(left_on_table),
                'reason': f'Need {min_trades} trades, have {len(left_on_table)}'}
    
    avg_left = sum(left_on_table) / len(left_on_table)
    
    if avg_left > left_threshold:
        # Widen trailing parameters proportionally
        widen_factor = min(1.5, 1.0 + (avg_left - left_threshold) / 5.0)
        new_act = round(base_act * widen_factor, 2)
        new_dd = round(base_dd * widen_factor, 2)
        # Cap at reasonable values
        new_act = min(2.5, new_act)
        new_dd = min(1.2, new_dd)
        return {
            'adjusted': True,
            'trail_activate': new_act,
            'trail_drawdown': new_dd,
            'avg_left': round(avg_left, 2),
            'n_trades': len(left_on_table),
            'reason': (f'Avg left on table: {avg_left:.2f}% > {left_threshold}%. '
                      f'Trail widened: act={new_act}%, dd={new_dd}%'),
        }
    
    return {
        'adjusted': False,
        'trail_activate': base_act,
        'trail_drawdown': base_dd,
        'avg_left': round(avg_left, 2),
        'n_trades': len(left_on_table),
        'reason': f'Avg left: {avg_left:.2f}% <= {left_threshold}%. No adjustment needed.',
    }


# ═══════════════════════════════════════════════════════
# v35: TWO-PHASE EXIT LOGIC
# ═══════════════════════════════════════════════════════

def determine_exit_phase(pos, z_static, pnl_pct):
    """
    v35: Determine current exit phase for a position.
    
    Phase 1 (Z-phase): Z moving toward 0. Only SL and timeout active.
    Phase 2 (Trail-phase): Z crossed threshold → trailing activated.
    
    Returns: {'phase': 1|2, 'reason': str, 'trail_params': dict|None}
    """
    if not CFG('monitor', 'two_phase_exit', True):
        return {'phase': 2, 'reason': 'Two-phase disabled, always trail', 
                'trail_params': None}
    
    entry_z = pos.get('entry_z', 0)
    direction = pos.get('direction', 'LONG')
    z_thresh = CFG('monitor', 'phase1_z_threshold', 0.5)
    
    # Check if Z has crossed the threshold (toward zero)
    if direction == 'LONG':
        # LONG: entered at Z<0, wants Z to go up to 0+
        z_crossed = z_static >= -z_thresh
    else:
        # SHORT: entered at Z>0, wants Z to go down to 0-
        z_crossed = z_static <= z_thresh
    
    # Also need positive P&L to enter phase 2
    min_pnl = CFG('monitor', 'auto_exit_z_min_pnl', 0.5)
    
    if z_crossed and pnl_pct >= min_pnl * 0.5:
        # Phase 2: Trailing
        ph2_act = CFG('monitor', 'phase2_trail_activate', 0.8)
        ph2_dd = CFG('monitor', 'phase2_trail_drawdown', 0.4)
        return {
            'phase': 2,
            'reason': f'Z crossed {z_thresh}: Z={z_static:+.2f}, P&L={pnl_pct:+.2f}%',
            'trail_params': {'activate': ph2_act, 'drawdown': ph2_dd},
        }
    
    # Phase 1: Only SL and timeout
    return {
        'phase': 1,
        'reason': f'Z still moving: Z={z_static:+.2f}, target=0',
        'trail_params': None,
    }


# v39: Whitelist function
WATCHLIST_FILE = "watchlist.json"

# CL-4 FIX: Module-level кэш watchlist с проверкой mtime.
# Без кэша: до 8385 вызовов os.path.exists() + open/read за один скан.
# С кэшем: один read при изменении файла, O(1) для повторных вызовов.
_watchlist_cache = None
_watchlist_mtime = 0.0

def _load_watchlist_pairs():
    """Загрузить пары из watchlist.json (с mtime-кэшем).
    Delegates set-building to core.risk.build_watchlist_pairs."""
    global _watchlist_cache, _watchlist_mtime
    import os, json
    
    if not os.path.exists(WATCHLIST_FILE):
        _watchlist_cache = None
        _watchlist_mtime = 0.0
        return None
    
    try:
        current_mtime = os.path.getmtime(WATCHLIST_FILE)
        if current_mtime == _watchlist_mtime and _watchlist_cache is not None:
            return _watchlist_cache
        
        with open(WATCHLIST_FILE, 'r', encoding='utf-8') as _f:
            data = json.load(_f)
        pairs = data.get("pairs", data) if isinstance(data, dict) else data
        if isinstance(pairs, list) and pairs:
            try:
                from pairs_scanner.core.risk import build_watchlist_pairs
                result = build_watchlist_pairs(pairs)
            except ImportError:
                # Fallback: inline B-01 FIX logic
                result = set()
                for p in pairs:
                    if isinstance(p, dict) and p.get("coin1") and p.get("coin2"):
                        c1 = p["coin1"].upper()
                        c2 = p["coin2"].upper()
                        direction = p.get("direction", "BOTH").upper()
                        result.add(f"{c1}/{c2}:{direction}")
                        if direction == "BOTH":
                            result.add(f"{c1}/{c2}:LONG")
                            result.add(f"{c1}/{c2}:SHORT")
            _watchlist_cache = result
            _watchlist_mtime = current_mtime
            return result
        _watchlist_cache = None
        _watchlist_mtime = current_mtime
        return None
    except Exception:
        return _watchlist_cache

def is_whitelisted(coin1, coin2, direction="BOTH"):
    """Thin wrapper: reads CFG + watchlist, delegates to core.risk.is_whitelisted."""
    if not CFG('strategy', 'whitelist_enabled', True):
        return True
    wl_pairs = _load_watchlist_pairs()
    # Resolve config whitelist
    config_wl = None
    if wl_pairs is None:
        wl = CFG('strategy', 'whitelist', None)
        if wl:
            if isinstance(wl, str):
                config_wl = [x.strip() for x in wl.split(',')]
            elif isinstance(wl, list):
                config_wl = wl
    try:
        from pairs_scanner.core.risk import is_whitelisted as _core_wl
        return _core_wl(coin1, coin2, direction, wl_pairs, config_wl)
    except ImportError:
        # Fallback: inline logic
        c1, c2 = coin1.upper(), coin2.upper()
        dir_up = direction.upper() if direction else "BOTH"
        if wl_pairs is not None:
            for key in (f"{c1}/{c2}", f"{c2}/{c1}"):
                if f"{key}:BOTH" in wl_pairs or f"{key}:{dir_up}" in wl_pairs:
                    return True
            return False
        if config_wl:
            wl_upper = [c.upper() for c in config_wl]
            return c1 in wl_upper and c2 in wl_upper
        return True
