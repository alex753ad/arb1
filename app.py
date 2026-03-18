import streamlit as st
import pandas as pd
import numpy as np
import time as _boot_time
_boot_t0 = _boot_time.time()

# v41 Wave 2.14: Block log
try:
    from block_log import log_blocks_batch, auto_cleanup as _bl_auto_cleanup  # H-04 FIX
    _BLOCK_LOG_OK = True
except ImportError:
    def log_blocks_batch(skip_reasons, source="auto_monitor", direction_map=None): pass
    def _bl_auto_cleanup(): pass
    _BLOCK_LOG_OK = False
import ccxt
import json
import os
from datetime import datetime, timedelta, timezone

# v13.0: Moscow time (UTC+3)
MSK = timezone(timedelta(hours=3))
def now_msk():
    """Current time in Moscow (UTC+3)."""
    return datetime.now(MSK)
import time
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from statsmodels.tsa.stattools import coint, adfuller
from statsmodels.regression.linear_model import OLS
import warnings
# DIAG: log import timing
# G-01 FIX: print(f"[DIAG] Base imports: {_boot_time.time()-_boot_t0:.2f}с", flush=True)
# v27: Unified config
_cfg_t0 = _boot_time.time()
try:
    from config_loader import CFG, CFG_auto_reload
    # G-01 FIX: print(f"[DIAG] config_loader import: {_boot_time.time()-_cfg_t0:.2f}с", flush=True)
except ImportError:
    # G-01 FIX: print(f"[DIAG] config_loader FAILED — using fallback", flush=True)
    def CFG(section, key=None, default=None):
        """Fallback: return defaults if config_loader not available.

        BUG-N10 FIX: Приоритет источников entry_z (от высшего к низшему):
          1. config.yaml  strategy.entry_z          — пользовательский конфиг
          2. config_loader._DEFAULTS['strategy']['entry_z'] = 2.5  — канонический дефолт
          3. Этот fallback-словарь (entry_z: 2.5)   — только при недоступном config_loader
          4. Inline CFG('strategy','entry_z', X)    — НЕ должен расходиться (см. ниже)

        Все вызовы CFG('strategy','entry_z', default=X) в app.py используют X=2.5,
        кроме mini_backtest и Entry Quality, где применяется адаптивный порог пары:
          p.get('threshold', CFG('strategy','entry_z', 2.5))
        Это намеренно: пара может иметь adaptive threshold < глобального entry_z,
        и scanner уже проверил что Z > threshold, поэтому используем threshold пары.
        """
        # F-001—F-005/X-003 FIX: fallback синхронизирован с config.yaml TRADE-1
        _d = {'strategy': {'entry_z': 2.5, 'exit_z': 0.5, 'stop_z_offset': 2.0,
              'min_stop_z': 4.0, 'take_profit_pct': 2.0, 'stop_loss_pct': -5.0,
              'max_hold_hours': 8, 'micro_bt_max_bars': 6, 'min_hurst': 0.45,
              'hr_naked_threshold': 0.15, 'max_hr_threshold': 5.0, 'min_hr_threshold': 0.05,
              'commission_pct': 0.10, 'slippage_pct': 0.05},
              'scanner': {'coins_limit': 100, 'timeframe': '4h', 'lookback_days': 50,
              'exchange': 'bybit', 'refresh_interval_min': 10, 'min_quality': 65},
              'rally_filter': {'warning_z': 2.0, 'block_z': 2.5, 'exit_z': 0.0, 'cooldown_bars': 2},
              'monitor': {'exit_z_target': 0.5, 'max_positions': 10,
              'auto_tp_pct': 2.0, 'auto_sl_pct': -2.5,
              'trailing_activate_pct': 1.0, 'trailing_drawdown_pct': 0.5,
              'phantom_track_hours': 12}}
        if key is None:
            return _d.get(section, {})
        return _d.get(section, {}).get(key, default)
    def CFG_auto_reload():
        return False
warnings.filterwarnings('ignore')

# ═══════════════════════════════════════════════════════
# v37 Wave 1.6: assess_entry_readiness — ЕДИНЫЙ ИСТОЧНИК
# Импортируем из mean_reversion_analysis.py, НЕ дублируем.
# D3-5 FIX: обёрнуто в try/except — при недоступном MRA app.py
# загружается с fallback assess_entry_readiness. Сканер покажет
# ошибку при запуске, но UI/D3 signal_scan_from_cache не упадут.
# ═══════════════════════════════════════════════════════

# Импорт модуля mean reversion analysis v10.5
_USE_MRA = False
_mra_t0 = _boot_time.time()
try:
    from mean_reversion_analysis import (
        assess_entry_readiness,
        calculate_hurst_exponent,
        calculate_rolling_zscore,
        calculate_adaptive_robust_zscore,
        calculate_crossing_density,
        calculate_rolling_correlation,
        calculate_ou_parameters,
        calculate_ou_score,
        calculate_quality_score,
        calculate_signal_score,
        calculate_trade_score,
        calculate_confidence,
        get_adaptive_signal,
        sanitize_pair,
        kalman_hedge_ratio,
        kalman_select_delta,
        apply_fdr_correction,
        check_cointegration_stability,
        adf_test_spread,
        estimate_exit_time,
        validate_ou_quality,
        detect_spread_regime,
        check_hr_magnitude,
        check_minimum_bars,
        cusum_structural_break,
        johansen_test,
        cost_aware_min_z,
        check_dollar_exposure,
        check_pnl_z_disagreement,
        calculate_hurst_ema,
        calculate_hurst_expanding,
        mini_backtest,
        walk_forward_validate,
        calculate_garch_zscore,
        pca_factor_clustering,
        pair_factor_exposure,
        micro_backtest,
        z_velocity_analysis,
        smart_exit_analysis,
    )
    _USE_MRA = True
    # G-01 FIX: print(f"[DIAG] mean_reversion_analysis import", flush=True)
except ImportError as _mra_err:
    _USE_MRA = False
    # G-01 FIX: print(f"[DIAG] mean_reversion_analysis FAILED", flush=True)
    import logging as _mra_log
    _mra_log.getLogger(__name__).warning(
        "mean_reversion_analysis НЕ ЗАГРУЖЕН: %s — сканер не сможет работать, "
        "но D3 signal_scan_from_cache и UI будут использовать fallback", _mra_err)

# D3-5 FIX: Fallback assess_entry_readiness — аналогичен monitor_v38_3.py.
# При _USE_MRA=True эта функция перезаписана импортом выше.
# При _USE_MRA=False — минимальная оценка на основе signal/zscore/quality/direction.
if not _USE_MRA:
    def assess_entry_readiness(p):
        """Minimal fallback when MRA module unavailable (D3-5 FIX)."""
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
from statsmodels.tools import add_constant
import urllib.request
import urllib.parse
import json as _json
import ssl as _ssl

# ═══════════════════════════════════════════════════════
# v31.0: RALLY FILTER (BTC-based market regime detection)
# ═══════════════════════════════════════════════════════
RALLY_STATE_FILE = "rally_state.json"

def load_rally_state():
    if os.path.exists(RALLY_STATE_FILE):
        with open(RALLY_STATE_FILE, encoding='utf-8') as f: return json.load(f)
    return {'status': 'NORMAL', 'btc_z': 0, 'last_check': ''}

def save_rally_state(state):
    with open(RALLY_STATE_FILE, 'w', encoding='utf-8') as f: json.dump(state, f, indent=2, default=str)

def check_rally_filter(exchange_obj, timeframe='4h'):
    """Check if BTC is in rally mode — blocks new LONG signals.
    v32: Thresholds from config (warning=2.0, block=2.5, exit=0.0)
    """
    state = load_rally_state()
    # v32: Configurable thresholds
    _warn_z = CFG('rally_filter', 'warning_z', 2.0)
    _block_z = CFG('rally_filter', 'block_z', 2.5)
    _exit_z = CFG('rally_filter', 'exit_z', 0.0)
    _cd_bars = CFG('rally_filter', 'cooldown_bars', 2)
    try:
        ohlcv = exchange_obj.fetch_ohlcv('BTC/USDT', timeframe, limit=200)
        closes = np.array([c[4] for c in ohlcv])
        # Simple Z-score of BTC price
        w = min(60, len(closes) // 2)
        recent = closes[-w:]
        med = np.median(recent)
        mad = np.median(np.abs(recent - med)) * 1.4826
        if mad < 1e-10:
            btc_z = 0.0
        else:
            btc_z = float((closes[-1] - med) / mad)
        
        was_rally = state.get('status', 'NORMAL') not in ('NORMAL', 'COOLDOWN')
        prev_status = state.get('status', 'NORMAL')
        
        if was_rally:
            # Exit rally only when Z drops below exit threshold
            if btc_z < _exit_z:
                state['status'] = 'COOLDOWN'
                state['cooldown_start'] = now_msk().isoformat()
                state['cooldown_bars'] = 0
                state['status_changed'] = True
            elif btc_z >= _block_z:
                state['status'] = 'DEEP_RALLY'
                state['status_changed'] = prev_status != 'DEEP_RALLY'
            else:
                state['status'] = 'RALLY'
                state['status_changed'] = False
        elif prev_status == 'COOLDOWN':
            bars_in_cd = state.get('cooldown_bars', 0) + 1
            state['cooldown_bars'] = bars_in_cd
            if bars_in_cd >= _cd_bars:
                state['status'] = 'NORMAL'
                state['status_changed'] = True
            else:
                state['status'] = 'COOLDOWN'
                state['status_changed'] = False
            if btc_z >= _warn_z:
                state['status'] = 'RALLY' if btc_z < _block_z else 'DEEP_RALLY'
                state['cooldown_bars'] = 0
                state['status_changed'] = True
        else:
            # NORMAL
            if btc_z >= _block_z:
                state['status'] = 'DEEP_RALLY'
                state['status_changed'] = True
            elif btc_z >= _warn_z:
                state['status'] = 'RALLY'
                state['status_changed'] = True
            else:
                state['status'] = 'NORMAL'
                state['status_changed'] = False
        
        state['btc_z'] = round(btc_z, 3)
        state['last_check'] = now_msk().isoformat()
        save_rally_state(state)
        return state
    except Exception:
        # E-004 FIX: если API недоступен и last_check устарел >2ч — сброс в NORMAL,
        # чтобы не застрять в DEEP_RALLY навсегда при сбое биржи.
        try:
            _last_chk = state.get('last_check', '')
            if _last_chk and state.get('status', 'NORMAL') != 'NORMAL':
                _last_dt = datetime.fromisoformat(str(_last_chk))
                _stale_hours = (now_msk() - _last_dt).total_seconds() / 3600
                if _stale_hours > 2.0:
                    state['status'] = 'NORMAL'
                    state['status_changed'] = True
                    state['btc_z'] = 0
                    save_rally_state(state)
        except Exception:
            pass
        return state


def send_rally_alert(state, tg_token, tg_chat):
    """Send Telegram alert when rally status changes."""
    if not state.get('status_changed') or not tg_token or not tg_chat:
        return
    status = state.get('status', 'NORMAL')
    btc_z = state.get('btc_z', 0)
    # U-001 FIX: пороги из конфига (было захардкожено 1.2 / 1.0 / -0.5 / 2)
    _block_z = CFG('rally_filter', 'block_z', 2.5)
    _warn_z  = CFG('rally_filter', 'warning_z', 2.0)
    _exit_z  = CFG('rally_filter', 'exit_z', 0.0)
    _cd_bars = CFG('rally_filter', 'cooldown_bars', 2)
    if status == 'DEEP_RALLY':
        msg = (f"🚨 <b>DEEP RALLY FILTER</b>\n"
               f"BTC Z={btc_z:+.2f} ≥ {_block_z}\n"
               f"⛔ Все LONG-сигналы ЗАБЛОКИРОВАНЫ\n"
               f"Только SHORT разрешены")
    elif status == 'RALLY':
        msg = (f"⚠️ <b>RALLY FILTER АКТИВИРОВАН</b>\n"
               f"BTC Z={btc_z:+.2f} ≥ {_warn_z}\n"
               f"LONG-сигналы под вопросом")
    elif status == 'COOLDOWN':
        msg = (f"⏳ <b>RALLY COOLDOWN</b>\n"
               f"BTC Z={btc_z:+.2f} вернулся < {_exit_z}\n"
               f"Ожидание {_cd_bars} бар(а) перед разблокировкой LONG")
    elif status == 'NORMAL':
        msg = (f"✅ <b>RALLY FILTER СНЯТ</b>\n"
               f"BTC Z={btc_z:+.2f}\n"
               f"LONG-сигналы разрешены")
    else:
        return
    try:
        send_telegram(tg_token, tg_chat, msg)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════
# v31.0: POSITION SIZING RECOMMENDATION
# ═══════════════════════════════════════════════════════
# F-012 FIX: recommend_position_size импортируется из config_loader (единый источник)
# Локальная копия удалена — содержала BUG-012 (Hurst logic inverted).
try:
    from config_loader import recommend_position_size
except ImportError:
    def recommend_position_size(quality_score, confidence, entry_readiness,
                                hurst=0.4, correlation=0.5, base_size=100):
        """Fallback if config_loader unavailable."""
        return base_size * 0.5


# ═══════════════════════════════════════════════════════
# v15.0: Telegram with IP fallback for DNS-blocked hosts
# ═══════════════════════════════════════════════════════
_TG_HOSTS = [
    "api.telegram.org",           # Primary DNS
    "149.154.167.220",            # Direct IP fallback (DC2)
]

def send_telegram(token, chat_id, message):
    """Send Telegram via HTTPS POST with DNS fallback."""
    if not token or not chat_id:
        return False, "Token или Chat ID не заданы"
    
    payload = _json.dumps({
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
            ctx = _ssl.create_default_context()
            if host != "api.telegram.org":
                ctx.check_hostname = False
                ctx.verify_mode = _ssl.CERT_NONE
            
            with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
                body = resp.read().decode()
                data = _json.loads(body)
                if data.get('ok'):
                    return True, f"OK (via {host})"
                return False, data.get('description', 'Unknown error')
        except Exception as e:
            last_err = f"{host}: {str(e)[:80]}"
            continue
    
    return False, f"All hosts failed. Last: {last_err}"

def send_telegram_test(token, chat_id):
    """Send test message to verify connection."""
    msg = (f"🔔 <b>Pairs Scanner v15</b>\n"
           f"✅ Telegram подключён!\n"
           f"⏰ {now_msk().strftime('%H:%M:%S МСК %d.%m.%Y')}")
    return send_telegram(token, chat_id, msg)

def format_telegram_signal(pairs_list, timeframe, exchange):
    """Format SIGNAL pairs for Telegram — v28: matches trade TXT format with ML score."""
    if not pairs_list:
        return None
    lines = [f"🔔 <b>Pairs Scanner Alert</b>"]
    lines.append(f"⏰ {now_msk().strftime('%H:%M МСК %d.%m.%Y')}")
    lines.append(f"📊 {exchange.upper()} | {timeframe} | FUTURES\n")
    
    for p in pairs_list:
        d = p.get('direction', '?')
        c1, c2 = p.get('coin1', '?'), p.get('coin2', '?')
        z = p.get('zscore', 0)
        hr = p.get('hedge_ratio', 0)
        hl = p.get('halflife_hours', 0)
        hurst = p.get('hurst', 0)
        mbt_q = p.get('mbt_quick', 0)
        mbt_pnl = p.get('mbt_pnl', 0)
        entry = p.get('_entry_label', p.get('signal', ''))
        mtf = '✅' if p.get('mtf_confirmed') else '❌'
        fr_net = p.get('funding_net', 0)
        
        # ML score
        try:
            from config_loader import ml_score
            _ml = ml_score(p)
            ml_str = f"ML:{_ml['grade']}({_ml['score']:.0f})"
        except Exception:
            ml_str = ""
        
        if d == 'SHORT':
            c1_act, c2_act = 'SELL', 'BUY'
        else:
            c1_act, c2_act = 'BUY', 'SELL'
        
        emoji = '🟢' if '🟢' in str(entry) else '🟡' if '🟡' in str(entry) else '⚪'
        fr_str = f"FR={fr_net:+.3f}%" if fr_net != 0 else ""
        
        lines.append(
            f"{'═'*20}\n"
            f"{emoji} <b>{c1}/{c2} {d}</b> {entry}\n"
            f"  {c1}/USDT:USDT → {c1_act} | {c2}/USDT:USDT → {c2_act}\n"
            f"  Z={z:+.2f} | HR={hr:.4f} | HL={hl:.0f}ч\n"
            f"  H={hurst:.3f} | uBT={mbt_q:.0f}% ({mbt_pnl:+.3f}%)\n"
            f"  MTF:{mtf} | Q={p.get('quality_score',0)} | {ml_str} {fr_str}"
        )
    return "\n".join(lines)

# Конфигурация страницы
st.set_page_config(
    page_title="Crypto Pairs Trading Scanner",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Стили
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .signal-long {
        color: #00cc00;
        font-weight: bold;
    }
    .signal-short {
        color: #ff0000;
        font-weight: bold;
    }
    .signal-neutral {
        color: #888888;
    }
    /* Исправление читаемости для темной темы */
    .stMarkdown, .stText, p, span, div {
        color: inherit !important;
    }
    /* U-003 FIX: убран forced white background — теперь совместимо с тёмной темой.
       Используем CSS переменные Streamlit для адаптивной темы. */
    .dataframe {
        background-color: var(--background-secondary-color, inherit) !important;
        color: var(--text-color, inherit) !important;
    }
    .dataframe td, .dataframe th {
        color: var(--text-color, inherit) !important;
    }
    /* Метрики - улучшенная видимость */
    [data-testid="stMetricValue"] {
        font-size: 2rem !important;
        font-weight: bold !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 1rem !important;
    }
    /* v6.0: Entry readiness */
    .entry-ready { 
        background: linear-gradient(135deg, #1b5e20 0%, #2e7d32 100%);
        color: white; padding: 12px; border-radius: 8px; 
        text-align: center; font-weight: bold; font-size: 1.1em;
        margin: 8px 0; border: 2px solid #4caf50;
    }
    .entry-conditional {
        background: linear-gradient(135deg, #e65100 0%, #f57c00 100%);
        color: white; padding: 12px; border-radius: 8px;
        text-align: center; font-weight: bold; font-size: 1.1em;
        margin: 8px 0; border: 2px solid #ff9800;
    }
    .entry-wait {
        background: #424242; color: #bdbdbd; padding: 12px; border-radius: 8px;
        text-align: center; font-size: 1.1em; margin: 8px 0;
    }
</style>
""", unsafe_allow_html=True)

# Инициализация session state
if 'running' not in st.session_state:
    st.session_state.running = False
if 'pairs_data' not in st.session_state:
    st.session_state.pairs_data = None
if 'last_update' not in st.session_state:
    st.session_state.last_update = None
if 'selected_pair_index' not in st.session_state:
    st.session_state.selected_pair_index = 0  # U-005 FIX: было int(0)
if 'settings' not in st.session_state:
    # v39: Defaults — Bybit, 4h, 28d, 130 монет, 30 пар, авто-обновление 2 мин
    st.session_state.settings = {
        'exchange':          CFG('scanner', 'exchange', 'bybit'),
        'timeframe':         CFG('scanner', 'timeframe', '4h'),
        'lookback_days':     CFG('scanner', 'lookback_days', 28),
        'top_n_coins':       CFG('scanner', 'coins_limit', 130),
        'max_pairs_display': 30,
        'pvalue_threshold':  0.03,
        'zscore_threshold':  2.3,
        'max_halflife_hours': 28,
        'hide_stablecoins':  True,
        'corr_prefilter':    0.3,
    }

# G-03 FIX: НЕ запускаем сканер автоматически при новой сессии.
# Раньше st.session_state.running = True → немедленные API-запросы при каждом Ctrl+R.
if 'auto_start_done' not in st.session_state:
    st.session_state.auto_start_done = True
    st.session_state.running = False  # G-03 FIX: ждём ручного запуска
    st.session_state['_last_scan_ts'] = 0

# v10.4: Стейблкоины, LST и wrapped-токены (торговля невыгодна из-за узкого спреда)
STABLE_LST_TOKENS = {
    'USDC', 'USDT', 'DAI', 'USDG', 'TUSD', 'BUSD', 'FDUSD', 'PYUSD',  # stablecoins
    'STETH', 'BETH', 'CBETH', 'RETH', 'WSTETH', 'METH',                 # ETH LST
    'JITOSOL', 'MSOL', 'BNSOL',                                          # SOL LST
    'WBTC', 'TBTC',                                                       # wrapped BTC
    # LOG-03 FIX: XAUT, PAXG УДАЛЕНЫ — токенизированное золото имеет волатильность
    # и активно торгуется (XAUT/ZEC session_pnl=+7.31%). Не стейблкоин.
}

class CryptoPairsScanner:
    # v7.1: Порядок fallback — OKX и KuCoin работают с HuggingFace/облачных серверов
    # Binance и Bybit блокируют CloudFront (403 Forbidden) с AWS/HF
    FALLBACK_CHAIN = ['okx', 'kucoin', 'bybit', 'binance']
    
    def __init__(self, exchange_name='binance', timeframe='1d', lookback_days=30):
        self.exchange_name = exchange_name
        self.timeframe = timeframe
        self.lookback_days = lookback_days
        
        # I-006 FIX: Единый lock для CCXT (не thread-safe)
        # CRITICAL: RLock (reentrant) — НЕ Lock! 
        # _fetch_one берёт lock → вызывает fetch_ohlcv → вызывает _ccxt_call → берёт lock.
        # С обычным Lock = deadlock. RLock позволяет тому же потоку войти повторно.
        import threading as _thr_init
        self._exchange_lock = _thr_init.RLock()
        
        # DIAG: timing
        import time as _t_init
        _t0 = _t_init.time()
        
        # v7.1: Умный fallback — пробуем запрошенную биржу, при ошибке перебираем цепочку
        tried = set()
        exchanges_to_try = [exchange_name] + [e for e in self.FALLBACK_CHAIN if e != exchange_name]
        
        last_error = None
        for exch in exchanges_to_try:
            if exch in tried:
                continue
            tried.add(exch)
            try:
                st.caption(f"🔌 Подключение к {exch.upper()}...")
                self.exchange = getattr(ccxt, exch)({'enableRateLimit': True})
                self.exchange.load_markets()
                _elapsed = _t_init.time() - _t0
                st.caption(f"✅ {exch.upper()} подключён за {_elapsed:.1f}с")
                if exch != exchange_name:
                    st.warning(f"⚠️ {exchange_name.upper()} недоступен. Переключился на **{exch.upper()}** ✅")
                self.exchange_name = exch
                return  # Успешно подключились
            except Exception as e:
                last_error = e
                _elapsed = _t_init.time() - _t0
                st.caption(f"❌ {exch.upper()} не отвечает ({_elapsed:.1f}с): {str(e)[:80]}")
                error_str = str(e).lower()
                continue  # Любая ошибка — пробуем следующую
        
        # Все биржи недоступны
        raise Exception(f"❌ Все биржи недоступны. Последняя ошибка: {last_error}")

    # I-006 FIX: все CCXT вызовы через _ccxt_call → гарантированная thread-safety.
    def _ccxt_call(self, method_name, *args, **kwargs):
        """Thread-safe CCXT вызов через _exchange_lock."""
        with self._exchange_lock:
            return getattr(self.exchange, method_name)(*args, **kwargs)
        
    def get_top_coins(self, limit=100):
        """Получить топ монет по объему торгов (FUTURES/SWAP)"""
        try:
            import time as _t_gtc
            _t0 = _t_gtc.time()
            # Рынки уже загружены в __init__.load_markets() — используем кэш
            # НЕ вызываем _ccxt_call('load_markets') повторно — это re-fetch 2000+ инструментов
            markets = self.exchange.markets
            if not markets:
                st.caption("📊 Рынки не в кэше, загружаю...")
                markets = self._ccxt_call('load_markets')
            st.caption(f"📊 Рынки: {len(markets)} инструментов, получаю тикеры...")
            
            # v32: More robust ticker fetching — try multiple methods
            tickers = {}
            methods = []
            
            # Method 1: swap tickers
            try:
                _t1 = _t_gtc.time()
                with self._exchange_lock:
                    orig_type = self.exchange.options.get('defaultType', 'spot')
                    self.exchange.options['defaultType'] = 'swap'
                    tickers = self.exchange.fetch_tickers()
                    self.exchange.options['defaultType'] = orig_type
                methods.append('swap')
                st.caption(f"📊 Swap тикеры: {len(tickers)} за {_t_gtc.time()-_t1:.1f}с")
            except Exception as _e_swap:
                st.caption(f"⚠️ Swap тикеры не получены: {str(_e_swap)[:60]}")
                with self._exchange_lock:
                    self.exchange.options['defaultType'] = 'spot'
            
            # Method 2: spot tickers (fallback or supplement)
            if len(tickers) < 10:
                try:
                    spot_tickers = self._ccxt_call('fetch_tickers')
                    tickers.update(spot_tickers)
                    methods.append('spot')
                except Exception:
                    pass
            
            if not tickers:
                raise Exception("Не удалось получить тикеры")
            
            # v28: FUTURES — collect swap perpetual (/USDT:USDT) AND spot (/USDT)
            base_currency = 'USDT'
            valid_pairs = []
            seen_coins = set()
            
            for k, v in tickers.items():
                try:
                    if v is None:
                        continue
                    coin = k.split('/')[0]
                    if coin in seen_coins:
                        continue
                    # Prefer swap format: BTC/USDT:USDT
                    is_swap = f':{base_currency}' in k
                    is_spot = f'/{base_currency}' in k and ':' not in k
                    if not is_swap and not is_spot:
                        continue
                    volume = 0
                    try:
                        volume = float(v.get('quoteVolume', 0) or v.get('baseVolume', 0) or v.get('volume', 0) or 0)
                    except (TypeError, ValueError):
                        continue
                    if volume > 0:
                        valid_pairs.append((coin, volume, is_swap))
                        seen_coins.add(coin)
                except Exception:
                    continue
            
            # Сортируем по объему
            sorted_pairs = sorted(valid_pairs, key=lambda x: x[1], reverse=True)
            
            # Берем топ монет
            top_coins = [pair[0] for pair in sorted_pairs[:limit]]
            
            if len(top_coins) > 0:
                _n_swap = sum(1 for p in sorted_pairs[:limit] if p[2])
                st.info(f"📊 {len(top_coins)} монет ({_n_swap} futures + {len(top_coins)-_n_swap} spot) с {self.exchange_name.upper()} ({', '.join(methods)})")
                return top_coins
            else:
                raise Exception("Не удалось получить данные о монетах")
            
        except Exception as e:
            st.error(f"Ошибка при получении топ монет с {self.exchange_name}: {e}")
            
            # Fallback: возвращаем популярные монеты
            st.warning("🔄 Используется fallback список популярных монет")
            return ['BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'ADA', 'AVAX', 'DOT', 
                   'MATIC', 'LINK', 'UNI', 'ATOM', 'LTC', 'ETC', 'XLM', 
                   'NEAR', 'APT', 'ARB', 'OP', 'DOGE']
    
    def fetch_ohlcv(self, symbol, limit=None):
        """Получить исторические данные с retry. v32: robust for all exchanges.
        I-006 FIX: CCXT enableRateLimit=True обеспечивает throttling.
        Lock снят — fetch_ohlcv вызывается из ThreadPoolExecutor(8) и lock
        сериализовал потоки (70 монет × 2с = 140с вместо 70/8 × 2с = 18с).
        """
        if limit is None:
            bars_per_day = {'1h': 24, '4h': 6, '1d': 1, '2h': 12, '15m': 96}.get(self.timeframe, 6)
            limit = self.lookback_days * bars_per_day
        
        # v32: Try swap (futures) first, then spot
        symbols_to_try = []
        if ':' not in symbol:
            try:
                swap_sym = symbol + ':USDT'
                if swap_sym in self.exchange.markets or self.exchange_name in ('okx', 'bybit', 'binance'):
                    symbols_to_try.append(swap_sym)
            except Exception:
                pass
        symbols_to_try.append(symbol)
        
        last_err = None
        for sym in symbols_to_try:
            for attempt in range(3):
                try:
                    # I-006: прямой вызов — enableRateLimit в CCXT уже throttles
                    ohlcv = self.exchange.fetch_ohlcv(sym, self.timeframe, limit=limit)
                    if not ohlcv or len(ohlcv) < 2:
                        break  # try next symbol
                    # v32: Validate OHLCV structure
                    if len(ohlcv[0]) < 5:
                        break
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    df.set_index('timestamp', inplace=True)
                    # v32: Drop NaN close prices
                    df = df.dropna(subset=['close'])
                    if len(df) < 20:
                        break  # not enough data, try next symbol
                    return df['close']
                except (ccxt.NetworkError, ccxt.RequestTimeout, ccxt.ExchangeNotAvailable) as e:
                    last_err = e
                    import time as _time
                    _time.sleep([2, 5, 15][attempt])
                except Exception as e:
                    last_err = e
                    break  # try next symbol
        return None
    
    def fetch_funding_rate(self, coin):
        """v27: Fetch current funding rate for perpetual swap."""
        try:
            symbol = f"{coin}/USDT:USDT"
            fr = self.exchange.fetch_funding_rate(symbol)
            return {
                'rate': float(fr.get('fundingRate', 0) or 0),
                'next_time': fr.get('fundingDatetime', ''),
                'rate_pct': float(fr.get('fundingRate', 0) or 0) * 100,
            }
        except Exception:
            return {'rate': 0, 'next_time': '', 'rate_pct': 0}
    
    def test_cointegration(self, series1, series2):
        """
        Тест на коинтеграцию v9.0:
          1. Engle-Granger → p-value (статистическая значимость)
          2. Kalman Filter → адаптивный HR + trading spread
          3. Rolling Z-score на Kalman spread
          4. Fallback на OLS если Kalman не сработал
        """
        try:
            valid_data = pd.concat([series1, series2], axis=1).dropna()
            if len(valid_data) < 20:
                return None

            s1 = valid_data.iloc[:, 0]
            s2 = valid_data.iloc[:, 1]

            # 1. Engle-Granger (p-value)
            score, pvalue, _ = coint(s1, s2)

            # 2. Kalman Filter для HR
            kf = kalman_hedge_ratio(s1.values, s2.values, delta=1e-4)

            if kf is not None and not np.isnan(kf['hr_final']) and abs(kf['hr_final']) < 1e6:
                # Kalman path
                hedge_ratio = kf['hr_final']
                intercept = kf['intercept_final']
                spread = pd.Series(kf['spread'], index=s1.index)
                hr_std = kf['hr_std']
                hr_series = kf['hedge_ratios']
                use_kalman = True
            else:
                # Fallback: OLS
                s2_const = add_constant(s2)
                model = OLS(s1, s2_const).fit()
                hedge_ratio = model.params.iloc[1] if len(model.params) > 1 else model.params.iloc[0]
                intercept = model.params.iloc[0] if len(model.params) > 1 else 0.0
                spread = s1 - hedge_ratio * s2 - intercept
                hr_std = 0.0
                hr_series = None
                use_kalman = False

            # 3. Half-life из spread
            spread_lag = spread.shift(1)
            spread_diff = spread - spread_lag
            spread_diff = spread_diff.dropna()
            spread_lag = spread_lag.dropna()
            model_hl = OLS(spread_diff, spread_lag).fit()
            halflife = -np.log(2) / model_hl.params.iloc[0] if model_hl.params.iloc[0] < 0 else np.inf

            # 4. v10: Adaptive Robust Z-score (MAD + HL-зависимое окно)
            hours_per_bar = {'1h': 1, '2h': 2, '4h': 4, '1d': 24,
                             '15m': 0.25}.get(self.timeframe, 4)
            # H-01 FIX: halflife из OLS — в БАРАХ (не в днях).
            # OLS: spread_diff ~ spread_lag, оба индексированы барами.
            # halflife = -ln(2) / beta → единица измерения = бары.
            hl_hours = halflife * hours_per_bar  # bars → hours
            hl_bars = halflife if halflife < 9999 else None  # уже в барах

            zscore, zscore_series, z_window = calculate_adaptive_robust_zscore(
                spread.values, halflife_bars=hl_bars
            )

            # v10.2: Rolling correlation — TF-aware window
            corr_windows = {'1h': 120, '2h': 60, '4h': 60, '1d': 30, '15m': 360}
            corr_w = corr_windows.get(self.timeframe, 60)
            corr_w = min(corr_w, len(s1) // 3)
            corr, corr_series = calculate_rolling_correlation(
                s1.values, s2.values, window=max(10, corr_w)
            )

            return {
                'pvalue': pvalue,
                'zscore': zscore,
                'zscore_series': zscore_series,
                'hedge_ratio': hedge_ratio,
                'intercept': intercept,
                'halflife': halflife,
                'spread': spread,
                'score': score,
                'use_kalman': use_kalman,
                'hr_std': hr_std,
                'hr_series': hr_series,
                'z_window': z_window,
                'correlation': corr,
            }
        except Exception as e:
            return None
    
    def mtf_confirm(self, coin1, coin2, primary_direction, primary_z, primary_hr):
        """
        v10.0: Multi-Timeframe Confirmation
        
        Загружает данные на младшем ТФ (4h→1h, 1d→4h) и проверяет:
          1. Z-direction: младший ТФ подтверждает направление старшего
          2. Z-velocity: Z движется к нулю (mean reversion началась)
          3. Z-magnitude: |Z| > 0.5 (ещё не вернулся к среднему)
          4. Price momentum: короткий импульс в нужную сторону
          
        Returns:
          dict с mtf_confirmed, mtf_z, mtf_velocity, mtf_details
          или None если данные недоступны
        """
        # Определяем младший TF
        confirm_tf = {
            '4h': '1h',
            '1d': '4h',
            '2h': '1h',
        }.get(self.timeframe)
        
        if confirm_tf is None:
            # Уже на младшем ТФ — нечего подтверждать
            return {'mtf_confirmed': None, 'mtf_reason': 'N/A (уже на минимальном TF)'}
        
        try:
            # Загружаем данные на младшем ТФ (последние 7 дней достаточно для Z)
            hpb = {'1h': 24, '4h': 6, '1d': 1}.get(confirm_tf, 6)
            limit = 7 * hpb  # 7 дней на младшем ТФ (168 баров для 1h)
            
            # v27: Retry wrapper for MTF data + futures
            ohlcv1, ohlcv2 = None, None
            for _sym_sfx in [':USDT', '']:
                for _attempt in range(3):
                    try:
                        # I-006 FIX: thread-safe
                        ohlcv1 = self.exchange.fetch_ohlcv(f"{coin1}/USDT{_sym_sfx}", confirm_tf, limit=limit)
                        ohlcv2 = self.exchange.fetch_ohlcv(f"{coin2}/USDT{_sym_sfx}", confirm_tf, limit=limit)
                        break
                    except (ccxt.NetworkError, ccxt.RequestTimeout, ccxt.ExchangeNotAvailable):
                        import time as _time
                        _time.sleep([2, 5, 15][_attempt])
                    except Exception:
                        break
                if ohlcv1 and ohlcv2:
                    break
            
            if not ohlcv1 or not ohlcv2:
                return {'mtf_confirmed': None, 'mtf_reason': f'Не удалось получить данные {confirm_tf}'}
            
            if len(ohlcv1) < 50 or len(ohlcv2) < 50:
                return {'mtf_confirmed': None, 'mtf_reason': f'Мало данных {confirm_tf}'}
            
            df1 = pd.DataFrame(ohlcv1, columns=['ts','o','h','l','c','v'])
            df2 = pd.DataFrame(ohlcv2, columns=['ts','o','h','l','c','v'])
            df1['ts'] = pd.to_datetime(df1['ts'], unit='ms')
            df2['ts'] = pd.to_datetime(df2['ts'], unit='ms')
            
            merged = pd.merge(df1[['ts','c']], df2[['ts','c']], on='ts', suffixes=('_1','_2'))
            if len(merged) < 50:
                return {'mtf_confirmed': None, 'mtf_reason': f'Мало общих баров {confirm_tf}'}
            
            p1 = merged['c_1'].values
            p2 = merged['c_2'].values
            
            # Строим спред на младшем ТФ с HR от старшего (для сопоставимости)
            spread_ltf = p1 - primary_hr * p2
            
            # Z-score на младшем ТФ (окно ~30 баров)
            n = len(spread_ltf)
            z_window = min(30, n // 2)
            lookback = spread_ltf[-z_window:]
            med = np.median(lookback)
            mad = np.median(np.abs(lookback - med)) * 1.4826
            
            if mad < 1e-10:
                s = np.std(lookback)
                current_z = (spread_ltf[-1] - np.mean(lookback)) / s if s > 1e-10 else 0
            else:
                current_z = (spread_ltf[-1] - med) / mad
            
            # Z-velocity: среднее изменение Z за последние 5 баров
            z_series = []
            for i in range(max(z_window, 10), n):
                lb = spread_ltf[i-z_window:i]
                m = np.median(lb)
                d = np.median(np.abs(lb - m)) * 1.4826
                if d < 1e-10:
                    s = np.std(lb)
                    z_series.append((spread_ltf[i] - np.mean(lb)) / s if s > 1e-10 else 0)
                else:
                    z_series.append((spread_ltf[i] - m) / d)
            
            if len(z_series) < 6:
                return {'mtf_confirmed': None, 'mtf_reason': 'Недостаточно Z-серии'}
            
            # Velocity: средний dZ за последние 5 баров
            recent_z = z_series[-6:]
            dz = [recent_z[i+1] - recent_z[i] for i in range(len(recent_z)-1)]
            z_velocity = np.mean(dz)
            
            # Price momentum на последних 3 барах
            p1_mom = (p1[-1] - p1[-4]) / p1[-4] * 100 if len(p1) >= 4 else 0
            p2_mom = (p2[-1] - p2[-4]) / p2[-4] * 100 if len(p2) >= 4 else 0
            
            # ═══════ CONFIRMATION LOGIC ═══════
            checks = []
            
            # Check 1: Z-direction agreement
            # Для LONG (primary_z < 0): 1h Z тоже должен быть < 0
            # Для SHORT (primary_z > 0): 1h Z тоже должен быть > 0
            z_agrees = (primary_z > 0 and current_z > 0) or (primary_z < 0 and current_z < 0)
            checks.append(('Z-direction', z_agrees, f'{self.timeframe} Z={primary_z:+.2f}, {confirm_tf} Z={current_z:+.2f}'))
            
            # Check 2: Z-velocity toward zero (mean reversion started)
            # LONG (Z<0): velocity > 0 (Z moving up toward 0)
            # SHORT (Z>0): velocity < 0 (Z moving down toward 0)
            if primary_direction == 'LONG':
                z_reverting = z_velocity > 0.02  # Z moving up
            elif primary_direction == 'SHORT':
                z_reverting = z_velocity < -0.02  # Z moving down
            else:
                z_reverting = False
            checks.append(('Z-velocity', z_reverting, f'dZ/dt={z_velocity:+.3f}/bar'))
            
            # Check 3: Z-magnitude — ещё не вернулся к нулю
            z_still_away = abs(current_z) > 0.5
            checks.append(('Z-magnitude', z_still_away, f'|Z|={abs(current_z):.2f} > 0.5'))
            
            # Check 4: Price momentum — первая монета двигается "правильно"
            if primary_direction == 'LONG':
                # LONG pair: coin1 should start outperforming coin2
                mom_ok = (p1_mom - primary_hr * p2_mom) > -0.1  # spread не ухудшается
            elif primary_direction == 'SHORT':
                mom_ok = (p1_mom - primary_hr * p2_mom) < 0.1
            else:
                mom_ok = True
            checks.append(('Momentum', mom_ok, f'Δ1={p1_mom:+.2f}%, Δ2={p2_mom:+.2f}%'))
            
            # Result
            passed = sum(1 for _, ok, _ in checks if ok)
            total = len(checks)
            
            # v10.1: STRONG DIVERGENCE OVERRIDE
            # If Z-velocity is strongly AGAINST direction, reject regardless
            # FIL/ADA case: LONG but velocity=-1.331 → Z diverging on 1h
            strong_diverge = False
            if primary_direction == 'LONG' and z_velocity < -0.3:
                strong_diverge = True
            elif primary_direction == 'SHORT' and z_velocity > 0.3:
                strong_diverge = True
            
            if strong_diverge:
                confirmed = False
                strength = 'DIVERGE'
                checks.append(('⚠️ Strong divergence', False, 
                    f'Z velocity {z_velocity:+.3f} strongly against {primary_direction}'))
            elif passed >= 3:
                confirmed = True
                strength = 'STRONG' if passed == 4 else 'OK'
            elif passed == 2 and z_agrees:
                confirmed = True
                strength = 'WEAK'
            else:
                confirmed = False
                strength = 'FAIL'
            
            return {
                'mtf_confirmed': confirmed,
                'mtf_strength': strength,
                'mtf_tf': confirm_tf,
                'mtf_z': round(current_z, 2),
                'mtf_z_velocity': round(z_velocity, 3),
                'mtf_checks': checks,
                'mtf_passed': passed,
                'mtf_total': total,
                'mtf_p1_mom': round(p1_mom, 2),
                'mtf_p2_mom': round(p2_mom, 2),
                'mtf_reason': f'{passed}/{total} checks',
            }
        
        except Exception as e:
            return {'mtf_confirmed': None, 'mtf_reason': f'Ошибка: {str(e)[:60]}'}
    
    def fetch_ohlcv_for_tf(self, coin, timeframe, lookback_days):
        """
        D2: Загрузить OHLCV для конкретного таймфрейма и периода.
        Независимо от self.timeframe / self.lookback_days.
        Используется в dual-TF скане для 1h/60d и 4h/45d.
        """
        bars_per_day = {'1h': 24, '4h': 6, '1d': 1, '2h': 12, '15m': 96}.get(timeframe, 6)
        limit = lookback_days * bars_per_day
        symbol = f"{coin}/USDT"
        symbols_to_try = []
        try:
            swap_sym = f"{coin}/USDT:USDT"
            if swap_sym in self.exchange.markets or self.exchange_name in ('okx', 'bybit', 'binance'):
                symbols_to_try.append(swap_sym)
        except Exception:
            pass
        symbols_to_try.append(symbol)

        import threading as _threading_dtf

        for sym in symbols_to_try:
            for attempt in range(3):
                try:
                    # I-006: lock — CCXT не thread-safe
                    with self._exchange_lock:
                        ohlcv = self.exchange.fetch_ohlcv(sym, timeframe, limit=limit)
                    if not ohlcv or len(ohlcv) < 2:
                        break
                    if len(ohlcv[0]) < 5:
                        break
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    df.set_index('timestamp', inplace=True)
                    df = df.dropna(subset=['close'])
                    if len(df) < 20:
                        break
                    return df['close']
                except (ccxt.NetworkError, ccxt.RequestTimeout, ccxt.ExchangeNotAvailable):
                    import time as _t
                    _t.sleep([2, 5, 15][attempt])
                except Exception:
                    break
        return None

    def scan_pairs(self, coins, max_pairs=50, progress_bar=None, max_halflife_hours=720,
                   hide_stablecoins=True, corr_prefilter=0.3,
                   dual_tf_mode=False,
                   signal_tf='1h', signal_lookback_days=60,
                   qualify_tf='4h', qualify_lookback_days=45):
        """
        Сканировать все пары.

        D2 — Dual Timeframe режим (dual_tf_mode=True):
          • qualify_tf / qualify_lookback_days (по умолчанию 4h/45d) — для расчёта
            качественных метрик: Hurst, OU, Stability, BT, WF, FDR, Q-score.
            Длинное окно → надёжная оценка mean-reversion.
          • signal_tf / signal_lookback_days (по умолчанию 1h/60d) — для определения
            актуального Z-score и направления сигнала.
            Короткое окно → чувствительный, быстрый сигнал.
          Merge-логика: качественные метрики из qualify_tf,
          Z/direction/threshold из signal_tf.
          Если dual_tf_mode=False — поведение идентично предыдущей версии
          (используется self.timeframe / self.lookback_days для всего).
        """
        import sys
        sys.stderr.write(f"[DIAG] scan_pairs: ENTERED\n")
        
        # v37 Wave 1.7: Перечитать конфиг если файл изменился
        sys.stderr.write(f"[DIAG] scan_pairs: calling CFG_auto_reload...\n")
        CFG_auto_reload()
        sys.stderr.write(f"[DIAG] scan_pairs: CFG_auto_reload done\n")
        sys.stderr.write(f"[DIAG] scan_pairs: coins={len(coins)}, dual_tf={dual_tf_mode}\n")

        # D3-5 FIX: сканер требует MRA — проверяем до начала работы
        if not _USE_MRA:
            st.error(
                "❌ **mean_reversion_analysis.py не загружен** — сканер не может работать.\n\n"
                "Убедитесь что файл `mean_reversion_analysis.py` находится рядом с `app.py`."
            )
            return []
        
        # ════════════════════════════════════════════════════════
        # D2: Dual Timeframe загрузка данных
        # dual_tf_mode=True  → два словаря price_data (qualify) + price_data_signal (signal)
        # dual_tf_mode=False → один словарь, поведение идентично предыдущей версии
        # ════════════════════════════════════════════════════════
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading

        # I-006 FIX: используем единый self._exchange_lock (было: локальный threading.Lock())

        if dual_tf_mode:
            # G-01 FIX: print(f"[DIAG] scan_pairs: DUAL TF mode — загружаю 2 таймфрейма", flush=True)
            st.info(
                f"📥 [Dual TF] Загружаю: "
                f"qualify={qualify_tf}/{qualify_lookback_days}d "
                f"+ signal={signal_tf}/{signal_lookback_days}d "
                f"× {len(coins)} монет..."
            )
            price_data = {}
            price_data_signal = {}

            def _fetch_dual(coin):
                p_q = self.fetch_ohlcv_for_tf(coin, qualify_tf, qualify_lookback_days)
                p_s = self.fetch_ohlcv_for_tf(coin, signal_tf, signal_lookback_days)
                return coin, p_q, p_s

            _t_fetch_start = time.time()
            # G-01 FIX: print(f"[DIAG] Dual TF: загрузка OHLCV для {len(coins)} монет (3 потока)...", flush=True)
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(_fetch_dual, c): c for c in coins}
                done_count = 0
                for future in as_completed(futures):
                    done_count += 1
                    if progress_bar and done_count % 5 == 0:
                        progress_bar.progress(
                            done_count / len(coins) * 0.3,
                            f"📥 [Dual TF] {done_count}/{len(coins)} монет"
                        )
                    try:
                        coin, p_q, p_s = future.result(timeout=45)
                        if p_q is not None and len(p_q) > 20:
                            price_data[coin] = p_q
                        if p_s is not None and len(p_s) > 20:
                            price_data_signal[coin] = p_s
                    except Exception:
                        pass

            both_coins = set(price_data.keys()) & set(price_data_signal.keys())
            # G-01 FIX: print(f"[DIAG] Dual TF загружен: qualify={len(price_data)}, signal={len(price_data_signal)}, оба={len(both_coins)} за {time.time()-_t_fetch_start:.1f}с", flush=True)
            st.info(
                f"📊 [Dual TF] qualify={qualify_tf}: {len(price_data)} монет | "
                f"signal={signal_tf}: {len(price_data_signal)} монет | "
                f"оба ТФ: {len(both_coins)} монет"
            )
            _dual_eligible = both_coins
        else:
            import sys as _sys_s
            _sys_s.stderr.write(f"[DIAG] scan_pairs: SINGLE TF mode — {len(coins)} монет\n")
            # G-01 FIX: print(f"[DIAG] scan_pairs: SINGLE TF mode — загружаю {len(coins)} монет", flush=True)
            st.info(f"📥 Загружаю данные для {len(coins)} монет...")
            price_data = {}
            price_data_signal = {}
            _dual_eligible = set()

            def _fetch_one(coin):
                import sys as _sys_f1
                symbol = f"{coin}/USDT"
                _sys_f1.stderr.write(f"[FETCH] {coin} start\n")
                # LOG-05 FIX: не берём exchange_lock — enableRateLimit в CCXT
                # уже обеспечивает rate limiting. Lock сериализовал потоки.
                prices = self.fetch_ohlcv(symbol)
                _ok = prices is not None and len(prices) > 20
                _sys_f1.stderr.write(f"[FETCH] {coin} {'OK' if _ok else 'FAIL'}\n")
                if _ok:
                    return coin, prices
                return coin, None

            _t_fetch_start = time.time()
            _n_workers = 4
            import sys as _sys_fetch
            _sys_fetch.stderr.write(f"[DIAG] Starting ThreadPoolExecutor({_n_workers}) for {len(coins)} coins\n")
            # G-01 FIX: print(f"[DIAG] Загрузка OHLCV для {len(coins)} монет...", flush=True)
            with ThreadPoolExecutor(max_workers=_n_workers) as executor:
                futures = {executor.submit(_fetch_one, c): c for c in coins}
                done_count = 0
                for future in as_completed(futures):
                    done_count += 1
                    if progress_bar:
                        progress_bar.progress(
                            done_count / len(coins) * 0.3,
                            f"📥 Загружено {done_count}/{len(coins)} монет"
                        )
                    if done_count % 10 == 0:
                        pass  # G-01 FIX: removed debug print
                    try:
                        coin, prices = future.result(timeout=30)
                        if prices is not None:
                            price_data[coin] = prices
                    except Exception:
                        pass
            # G-01 FIX: print(f"[DIAG] OHLCV загружен: {len(price_data)}/{len(coins)} монет за {time.time()-_t_fetch_start:.1f}с", flush=True)
        
        if len(price_data) < 2:
            st.error("❌ Недостаточно данных для анализа")
            return []
        
        # v21: Build returns_dict for correlation filter AND PCA
        coin_list = list(price_data.keys())
        min_len = min(len(price_data[c]) for c in coin_list)
        if min_len < 20:
            st.error(f"❌ Недостаточно данных: минимальная длина серии {min_len} < 20")
            return []
        returns_dict = {}
        for c in coin_list:
            try:
                p = price_data[c].values[-min_len:]
                if len(p) < 2:
                    continue
                r = np.diff(np.log(p + 1e-10))
                if len(r) > 0 and not np.all(np.isnan(r)):
                    returns_dict[c] = r
            except Exception:
                continue
        
        # Update coin_list to only include coins with valid returns
        coin_list = [c for c in coin_list if c in returns_dict]
        if len(coin_list) < 2:
            st.error("❌ Недостаточно монет с валидными данными")
            return []
        
        # P-003 FIX: Correlation pre-filter — МАТРИЧНЫЙ расчёт вместо O(N²) поштучных вызовов.
        # Было: for i,c1; for c2: np.corrcoef(r1, r2) — 2415 вызовов при 70 монетах.
        # Стало: один np.corrcoef на матрице returns → полная корр. матрица за один вызов.
        # Ускорение: ~10-50x (numpy BLAS vs Python loop).
        skip_pairs = set()
        if corr_prefilter > 0:
            try:
                # Построить матрицу returns: каждая строка — одна монета
                _returns_matrix = np.array([returns_dict[c][:min_len-1] for c in coin_list])
                # Полная корреляционная матрица NxN за один вызов
                _corr_matrix = np.corrcoef(_returns_matrix)
                # Построить skip_pairs из нижнего треугольника
                for i in range(len(coin_list)):
                    for j in range(i + 1, len(coin_list)):
                        rho = _corr_matrix[i, j]
                        if np.isnan(rho) or abs(rho) < corr_prefilter:
                            skip_pairs.add((coin_list[i], coin_list[j]))
            except Exception:
                # Fallback: поштучный расчёт если матричный упал
                for i, c1 in enumerate(coin_list):
                    for c2 in coin_list[i+1:]:
                        try:
                            r1, r2 = returns_dict[c1], returns_dict[c2]
                            min_r = min(len(r1), len(r2))
                            if min_r < 10:
                                skip_pairs.add((c1, c2))
                                continue
                            rho = np.corrcoef(r1[-min_r:], r2[-min_r:])[0, 1]
                            if np.isnan(rho) or abs(rho) < corr_prefilter:
                                skip_pairs.add((c1, c2))
                        except Exception:
                            skip_pairs.add((c1, c2))
            
            if skip_pairs:
                total_all = len(coin_list) * (len(coin_list) - 1) // 2
                st.info(f"⚡ Корр. фильтр (|ρ| < {corr_prefilter}): пропущено {len(skip_pairs)}/{total_all} пар")
        
        # v21: PCA Factor Clustering (P5) — runs regardless of corr_prefilter
        # PERF-01 FIX: skip PCA if coin set hasn't changed since last scan
        try:
            # H-02 FIX: хэш включает TF и n_components (не только список монет)
            _n_pca_comp = CFG('scanner', 'n_pca_components', 3)
            _pca_coin_hash = hash((frozenset(returns_dict.keys()), _active_tf, _n_pca_comp))
            _prev_pca_hash = st.session_state.get('_pca_coin_hash', None)
            if _prev_pca_hash == _pca_coin_hash and st.session_state.get('_pca_result') is not None:
                pca_result = st.session_state['_pca_result']
                st.info(f"🧬 PCA: кэш ({len(pca_result.get('coin_clusters', {}))} монет, "
                        f"{pca_result['total_explained']*100:.0f}% дисперсии)")
            else:
                pca_result = pca_factor_clustering(returns_dict, n_components=_n_pca_comp)
                if 'error' not in pca_result:
                    st.session_state['_pca_result'] = pca_result
                    st.session_state['_pca_coin_hash'] = _pca_coin_hash
                    st.info(
                        f"🧬 PCA: {pca_result['n_components']} факторов, "
                        f"объясняют {pca_result['total_explained']*100:.0f}% дисперсии "
                        f"({len(pca_result.get('coin_clusters', {}))} монет → "
                        f"{len(pca_result.get('cluster_summary', {}))} кластеров)"
                    )
                else:
                    st.session_state['_pca_result'] = None
        except Exception:
            st.session_state['_pca_result'] = None
        
        # v10.4: Stablecoin/LST filter  
        stable_skipped = 0
        
        total_combinations = len(price_data) * (len(price_data) - 1) // 2
        # P-003 FIX: показать реальное количество пар после фильтрации
        _after_filter = total_combinations - len(skip_pairs) - stable_skipped
        st.info(f"🔍 Фаза 1: Коинтеграция для {_after_filter} пар "
                f"(из {total_combinations} всего, {len(skip_pairs)} отсечено корр. фильтром, "
                f"{stable_skipped} стейблкоинов) | {len(price_data)} монет")

        # ═══════ ФАЗА 1: Параллельный тест коинтеграции ═══════
        # v39: Распараллелено через ThreadPoolExecutor — ускорение 4-6x
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading

        coin_keys = list(price_data.keys())
        all_pairs_list = [
            (coin_keys[i], coin_keys[j])
            for i in range(len(coin_keys))
            for j in range(i+1, len(coin_keys))
        ]

        # Предфильтр стейблкоинов и некоррелированных пар
        filtered_pairs = []
        skipped_stable = 0
        skipped_corr   = 0
        for coin1, coin2 in all_pairs_list:
            if hide_stablecoins:
                if coin1 in STABLE_LST_TOKENS and coin2 in STABLE_LST_TOKENS:
                    skipped_stable += 1
                    continue
                c1u, c2u = coin1.upper(), coin2.upper()
                if (c1u in c2u or c2u in c1u) and (
                        coin1 in STABLE_LST_TOKENS or coin2 in STABLE_LST_TOKENS):
                    skipped_stable += 1
                    continue
            if (coin1, coin2) in skip_pairs:
                skipped_corr += 1
                continue
            filtered_pairs.append((coin1, coin2))

        stable_skipped = skipped_stable
        _progress_lock = threading.Lock()
        _done_count    = [0]

        def _test_one_pair(args):
            coin1, coin2 = args
            try:
                result = self.test_cointegration(price_data[coin1], price_data[coin2])
                with _progress_lock:
                    _done_count[0] += 1
                    if progress_bar and _done_count[0] % 50 == 0:
                        pct = 0.3 + _done_count[0] / max(len(filtered_pairs), 1) * 0.35
                        progress_bar.progress(
                            min(pct, 0.65),
                            f"Фаза 1: {_done_count[0]}/{len(filtered_pairs)}"
                        )
                return coin1, coin2, result
            except Exception:
                return coin1, coin2, None

        # Bybit rate limit ~20 req/sec — но test_cointegration CPU-bound, не IO
        # Используем 8 потоков для CPU параллелизма
        _phase1_workers = 8
        _phase1_results = []
        with ThreadPoolExecutor(max_workers=_phase1_workers) as _ex1:
            _phase1_results = list(_ex1.map(_test_one_pair, filtered_pairs))

        # Собираем all_pvalues и candidates в правильном порядке
        all_pvalues = [1.0] * (skipped_stable + skipped_corr)  # скипнутые пары = p=1
        candidates  = []

        # LOG-07 FIX: halflife from OLS is in BARS, not days
        _p1_hpb = {'1h': 1, '2h': 2, '4h': 4, '1d': 24, '15m': 0.25}.get(self.timeframe, 4)

        for coin1, coin2, result in _phase1_results:
            if result:
                all_pvalues.append(result['pvalue'])
                halflife_hours = result['halflife'] * _p1_hpb  # LOG-07 FIX: bars * hours_per_bar
                if result['pvalue'] < 0.15 and halflife_hours <= max_halflife_hours:
                    candidates.append((coin1, coin2, result, len(all_pvalues) - 1))
            else:
                all_pvalues.append(1.0)

        processed = len(filtered_pairs)
        
        # ═══════ FDR на ВСЕХ p-values ═══════
        if len(all_pvalues) == 0:
            return []
        
        adj_pvalues, fdr_rejected = apply_fdr_correction(all_pvalues, alpha=0.05)
        
        total_fdr_passed = int(np.sum(fdr_rejected))
        st.info(f"🔬 FDR: {total_fdr_passed} из {len(all_pvalues)} пар прошли (α=0.05)")
        if stable_skipped > 0:
            st.info(f"🚫 Пропущено {stable_skipped} стейблкоин/LST пар")
        
        # ═══════ ФАЗА 2: Параллельный детальный анализ кандидатов ═══════
        # v39: Каждый кандидат анализируется независимо → ThreadPoolExecutor
        # D2: В dual_tf_mode dt берётся от qualify_tf (для OU параметров)
        st.info(f"🔍 Фаза 2 (параллельная): Детальный анализ {len(candidates)} кандидатов...")
        results = []
        _active_tf = qualify_tf if dual_tf_mode else self.timeframe
        dt = {'1h': 1/24, '4h': 1/6, '1d': 1}.get(_active_tf, 1/6)

        _p2_lock  = threading.Lock()
        _p2_done  = [0]
        _p2_total = max(len(candidates), 1)

        def _analyse_candidate(args):
            idx_c, (coin1, coin2, result, pval_idx) = args
            
            # v34: Robust FDR access — prevent "list index out of range"
            try:
                if pval_idx < len(fdr_rejected):
                    fdr_passed = bool(fdr_rejected[pval_idx])
                    pvalue_adj = float(adj_pvalues[pval_idx])
                else:
                    fdr_passed = False
                    pvalue_adj = result['pvalue']
            except (IndexError, TypeError):
                fdr_passed = False
                pvalue_adj = result['pvalue']
            
            # v34: Individual pair try/except (prevents >100 coins crash)
            try:
                with _p2_lock:
                    _p2_done[0] += 1
                    if progress_bar and _p2_done[0] % 5 == 0:
                        pct = 0.65 + _p2_done[0] / _p2_total * 0.35
                        progress_bar.progress(min(pct, 1.0), f"Фаза 2: {_p2_done[0]}/{_p2_total}")
                # Hurst (DFA)
                hurst_raw = calculate_hurst_exponent(result['spread'])

                # v16: Hurst EMA smoothing (рассуждение #2)
                hurst_ema_info = calculate_hurst_ema(result['spread'])
                hurst_ema    = hurst_ema_info.get('hurst_ema', hurst_raw)
                hurst_std    = hurst_ema_info.get('hurst_std', 0.0)
                hurst_stable = hurst_ema_info.get('is_stable', True)

                # BUG-024 + D1 FIX: определение fallback через hurst_std, а не hurst == 0.5.
                # Проблема: calculate_hurst_exponent возвращает 0.5 при ошибке И при
                # реальном Hurst ровно 0.5 — жёсткое сравнение давало ложные флаги.
                # Решение: hurst_std > 0.08 означает нестабильный / ненадёжный Hurst
                # (высокое std EMA-серии = большой разброс оценок = данные ненадёжны).
                # Если DFA вернул 0.5 и hurst_std == 0.0 (EMA не считалась) — тоже fallback.
                _hurst_std_val = float(hurst_std) if hurst_std else 0.0
                hurst_is_fallback = (
                    _hurst_std_val > 0.08 or
                    (hurst_raw == 0.5 and _hurst_std_val == 0.0)
                )

                # BUG-024 + D1 FIX: use_hurst_ema_fallback теперь реально управляет
                # выбором hurst для всех downstream расчётов (confidence, quality_score,
                # ou_score, _pair_row['hurst']). Раньше флаг читался из конфига, но hurst
                # всегда брался из DFA — EMA полностью игнорировалась.
                _use_ema_fallback = CFG('strategy', 'use_hurst_ema_fallback', True)
                if _use_ema_fallback and hurst_is_fallback:
                    hurst = hurst_ema   # ненадёжный DFA → стабильная EMA-оценка
                else:
                    hurst = hurst_raw   # надёжный DFA → используем напрямую

                # v19.1: Expanding Window Hurst (P3 Roadmap)
                hurst_exp_info = calculate_hurst_expanding(result['spread'])

                # OU
                ou_params = calculate_ou_parameters(result['spread'], dt=dt)
                ou_score = calculate_ou_score(ou_params, hurst)
                is_valid, reason = validate_ou_quality(ou_params, hurst)

                # Stability
                stability = check_cointegration_stability(
                    price_data[coin1].values, price_data[coin2].values
                )

                # v10: количество баров
                n_bars = len(result['spread']) if result.get('spread') is not None else 0
                hr_std_val = result.get('hr_std', 0.0)

                # [v10.1] Sanitizer — жёсткие исключения (с min_bars + HR uncertainty)
                san_ok, san_reason = sanitize_pair(
                    hedge_ratio=result['hedge_ratio'],
                    stability_passed=stability['windows_passed'],
                    stability_total=stability['total_windows'],
                    zscore=result['zscore'],
                    n_bars=n_bars,
                    hr_std=hr_std_val
                )
                if not san_ok:
                    return None

                # [NEW] ADF-тест спреда
                adf = adf_test_spread(result['spread'])

                # [v10] Crossing Density — частота пересечений нуля
                crossing_d = calculate_crossing_density(
                    result.get('zscore_series', np.array([])),
                    window=min(n_bars, 100)
                )

                # [v10.1] Confidence (с HR uncertainty)
                confidence, conf_checks, conf_total = calculate_confidence(
                    hurst=hurst,
                    stability_score=stability['stability_score'],
                    fdr_passed=fdr_passed,
                    adf_passed=adf['is_stationary'],
                    zscore=result['zscore'],
                    hedge_ratio=result['hedge_ratio'],
                    hurst_is_fallback=hurst_is_fallback,
                    hr_std=hr_std_val
                )

                # [v10.1] Quality Score (с HR uncertainty penalty)
                q_score, q_breakdown = calculate_quality_score(
                    hurst=hurst,
                    ou_params=ou_params,
                    pvalue_adj=pvalue_adj,
                    stability_score=stability['stability_score'],
                    hedge_ratio=result['hedge_ratio'],
                    adf_passed=adf['is_stationary'],
                    hurst_is_fallback=hurst_is_fallback,
                    crossing_density=crossing_d,
                    n_bars=n_bars,
                    hr_std=hr_std_val
                )

                # v41.1 FIX D2-2: halflife_hours и hl_bars нужны ДО D2 MERGE
                # LOG-07 FIX: halflife from OLS is in BARS, not days — use hours_per_bar
                hours_per_bar = {'1h': 1, '2h': 2, '4h': 4, '1d': 24, '15m': 0.25}.get(_active_tf, 4)
                halflife_hours = result['halflife'] * hours_per_bar
                hl_bars = halflife_hours / hours_per_bar if halflife_hours < 9999 else None

                # ═══════════════════════════════════════════════════════
                # v41.1 FIX D2-2: D2 MERGE перемещён ПЕРЕД signal_score
                # и get_adaptive_signal. Раньше _signal_zscore использовался
                # до определения → signal_score/state/direction считались
                # по qualify_tf Z вместо signal_tf Z.
                # ═══════════════════════════════════════════════════════
                _signal_zscore   = result['zscore']         # по умолчанию — qualify_tf
                _signal_zscore_series = result.get('zscore_series')
                _signal_tf_used  = _active_tf               # для логов
                _signal_spread   = result['spread']         # по умолчанию
                _signal_z_window = result.get('z_window', 30)

                if dual_tf_mode and coin1 in _dual_eligible and coin2 in _dual_eligible:
                    try:
                        _ps1 = price_data_signal.get(coin1)
                        _ps2 = price_data_signal.get(coin2)
                        if _ps1 is not None and _ps2 is not None and len(_ps1) >= 30 and len(_ps2) >= 30:
                            _hr_signal = result['hedge_ratio']
                            _int_signal = result.get('intercept', 0.0)
                            _arr1 = _ps1.values if hasattr(_ps1, 'values') else np.array(_ps1)
                            _arr2 = _ps2.values if hasattr(_ps2, 'values') else np.array(_ps2)
                            n_sig = min(len(_arr1), len(_arr2))
                            _arr1 = _arr1[-n_sig:]
                            _arr2 = _arr2[-n_sig:]
                            _spread_sig = _arr1 - _hr_signal * _arr2 - _int_signal

                            _hl_sig_bars_per_day = {'1h': 24, '4h': 6, '1d': 1}.get(signal_tf, 24)
                            _hl_sig_bars = (halflife_hours / (24 / _hl_sig_bars_per_day)
                                            if halflife_hours < 9999 else None)

                            _sig_z, _sig_z_series, _sig_z_win = calculate_adaptive_robust_zscore(
                                _spread_sig,
                                halflife_bars=_hl_sig_bars
                            )
                            if not np.isnan(_sig_z) and abs(_sig_z) < 20:
                                _signal_zscore       = _sig_z
                                _signal_zscore_series = _sig_z_series
                                _signal_spread       = _spread_sig
                                _signal_z_window     = _sig_z_win
                                _signal_tf_used      = signal_tf
                    except Exception:
                        pass  # fallback к qualify_tf Z уже установлен выше

                # [v8.1] Signal Score (capped by Quality)
                # v41.1: теперь _signal_zscore корректно определён из signal_tf
                s_score, s_breakdown = calculate_signal_score(
                    zscore=_signal_zscore,
                    ou_params=ou_params,
                    confidence=confidence,
                    quality_score=q_score
                )

                # [v8.0] Adaptive Signal — continuous threshold + hurst
                _signal_tf_for_adaptive = signal_tf if dual_tf_mode else self.timeframe
                stab_ratio = stability['stability_score']  # 0.0–1.0
                try:
                    state, direction, threshold = get_adaptive_signal(
                        zscore=_signal_zscore,
                        confidence=confidence,
                        quality_score=q_score,
                        timeframe=_signal_tf_for_adaptive,
                        stability_ratio=stab_ratio,
                        fdr_passed=fdr_passed,
                        hurst=hurst
                    )
                except TypeError:
                    try:
                        state, direction, threshold = get_adaptive_signal(
                            zscore=_signal_zscore,
                            confidence=confidence,
                            quality_score=q_score,
                            timeframe=_signal_tf_for_adaptive,
                            stability_ratio=stab_ratio,
                            fdr_passed=fdr_passed,
                        )
                    except TypeError:
                        state, direction, threshold = get_adaptive_signal(
                            zscore=_signal_zscore,
                            confidence=confidence,
                            quality_score=q_score,
                            timeframe=_signal_tf_for_adaptive,
                            stability_ratio=stab_ratio,
                        )

                # v18: GARCH Z-score (variance-adaptive)
                garch_info = calculate_garch_zscore(
                    result['spread'], halflife_bars=hl_bars)
                garch_z = garch_info.get('z_garch', 0)
                garch_divergence = garch_info.get('z_divergence', 0)
                garch_var_expanding = garch_info.get('variance_expanding', False)

                # v10: Z-warning (на основе signal Z — он актуальнее)
                z_warning = abs(_signal_zscore) > 4.0

                # v11.2: Regime detection (spread-based ADX)
                regime_info = detect_spread_regime(result['spread'].values if hasattr(result['spread'], 'values') else result['spread'])

                # v12.0: CUSUM structural break test (v13: +Z-magnitude)
                cusum_info = cusum_structural_break(
                    result['spread'].values if hasattr(result['spread'], 'values') else result['spread'],
                    min_tail=min(30, n_bars // 5),
                    zscore=_signal_zscore  # D2: используем signal Z
                )

                # v13.0: Johansen test (symmetric cointegration)
                johansen_info = johansen_test(
                    price_data.get(coin1, pd.Series()).values if hasattr(price_data.get(coin1, pd.Series()), 'values') else np.array([]),
                    price_data.get(coin2, pd.Series()).values if hasattr(price_data.get(coin2, pd.Series()), 'values') else np.array([])
                )

                # v11.2: HR magnitude warning
                hr_warning = check_hr_magnitude(result['hedge_ratio'])

                # v11.2: Min bars gate
                bars_warning = check_minimum_bars(n_bars, _active_tf)

                # v17: Mini-backtest gate (P1 Roadmap)
                # BT/WF работают на qualify_tf данных — более длинная история надёжнее
                _cfg_entry_z = CFG('strategy', 'entry_z', 2.5)
                _cfg_tp = CFG('strategy', 'take_profit_pct', 2.0)
                _cfg_sl = CFG('strategy', 'stop_loss_pct', -5.0)
                _cfg_mbt_bars = CFG('strategy', 'micro_bt_max_bars', 6)
                _cfg_comm = CFG('strategy', 'commission_pct', 0.10)
                _cfg_slip = CFG('strategy', 'slippage_pct', 0.05)
                _cfg_naked = CFG('strategy', 'hr_naked_threshold', 0.15)

                bt_result = {'verdict': 'SKIP', 'n_trades': 0}
                try:
                    p1_arr = price_data[coin1].values if hasattr(price_data.get(coin1, pd.Series()), 'values') else np.array([])
                    p2_arr = price_data[coin2].values if hasattr(price_data.get(coin2, pd.Series()), 'values') else np.array([])
                    if len(p1_arr) >= 80 and len(p2_arr) >= 80:
                        bt_result = mini_backtest(
                            result['spread'], p1_arr, p2_arr,
                            result.get('hedge_ratios', np.full(len(result['spread']), result['hedge_ratio'])),
                            entry_z=max(_cfg_entry_z, threshold),
                            halflife_bars=hl_bars if hl_bars and hl_bars > 0 else None,
                            commission_pct=_cfg_comm,
                            slippage_pct=_cfg_slip,
                        )
                except Exception:
                    bt_result = {'verdict': 'SKIP', 'n_trades': 0}

                # v19: Walk-Forward Validation (P1 Roadmap)
                wf_result = {'verdict': 'SKIP', 'folds_passed': 0}
                try:
                    if len(p1_arr) >= 120 and len(p2_arr) >= 120:
                        wf_result = walk_forward_validate(
                            result['spread'], p1_arr, p2_arr,
                            result.get('hedge_ratios', np.full(len(result['spread']), result['hedge_ratio'])),
                            entry_z=max(_cfg_entry_z, threshold),
                            halflife_bars=hl_bars if hl_bars and hl_bars > 0 else None,
                        )
                except Exception:
                    wf_result = {'verdict': 'SKIP', 'folds_passed': 0}

                # v23: R2 Micro-Backtest (1-6 bar horizon)
                mbt_result = {'verdict': 'SKIP', 'n_trades': 0}
                try:
                    if len(p1_arr) >= 80 and len(p2_arr) >= 80:
                        mbt_result = micro_backtest(
                            result['spread'], p1_arr, p2_arr,
                            result.get('hedge_ratios', np.full(len(result['spread']), result['hedge_ratio'])),
                            entry_z=max(_cfg_entry_z, threshold),
                            max_hold_bars=_cfg_mbt_bars,
                            take_profit_pct=_cfg_tp,
                            stop_loss_pct=_cfg_sl,
                            commission_pct=_cfg_comm,
                            slippage_pct=_cfg_slip,
                        )
                except Exception:
                    mbt_result = {'verdict': 'SKIP', 'n_trades': 0}

                # v19: Combined BT verdict — use worst of mini-BT and WF
                # BUG-N04 NOTE: combined_verdict используется только для UI-отображения (bt_verdict в _pair_row).
                # Фильтрация в auto_monitor разделена: mbt_verdict (micro-BT) и wf_verdict (walk-forward) —
                # каждый управляется своим конфиг-ключом (bt_filter_mode / wf_filter_mode).
                # WF FAIL → WARN (не FAIL): «NEVER block entry on WF FAIL alone» — intentional.
                combined_verdict = bt_result.get('verdict', 'SKIP')
                if wf_result.get('verdict') == 'FAIL' and combined_verdict != 'FAIL':
                    combined_verdict = 'WARN'  # WF fail downgrades but doesn't hard-block

                # Пункт 12b FIX: пересчёт Q-score с учётом mbt_verdict (uBT penalty).
                # N-01 FIX: calculate_quality_score v45 принимает ubt_passed (bool),
                # а не mbt_verdict (str). WARN/SKIP → ubt_passed=False → penalty -5.
                _mbt_v = mbt_result.get('verdict', 'SKIP')
                if _mbt_v in ('WARN', 'SKIP'):
                    q_score, q_breakdown = calculate_quality_score(
                        hurst=hurst, ou_params=ou_params,
                        pvalue_adj=pvalue_adj,
                        stability_score=stability['stability_score'],
                        hedge_ratio=result['hedge_ratio'],
                        adf_passed=adf['is_stationary'],
                        hurst_is_fallback=hurst_is_fallback,
                        crossing_density=crossing_d,
                        n_bars=n_bars, hr_std=hr_std_val,
                        ubt_passed=False,
                    )

                # v24: R4 Z-Velocity analysis
                # D2: используем signal_tf zscore_series (актуальнее для timing)
                zvel_result = {'velocity': 0, 'entry_quality': 'UNKNOWN'}
                try:
                    zs_series = _signal_zscore_series if _signal_zscore_series is not None else result.get('zscore_series')
                    if zs_series is not None and len(zs_series) >= 7:
                        zvel_result = z_velocity_analysis(zs_series, lookback=5)
                except Exception:
                    pass

                _pair_row = {
                    'pair': f"{coin1}/{coin2}",
                    'coin1': coin1,
                    'coin2': coin2,
                    'price1_last': float(price_data[coin1].values[-1]) if coin1 in price_data else 0,
                    'price2_last': float(price_data[coin2].values[-1]) if coin2 in price_data else 0,
                    'pvalue': result['pvalue'],
                    'pvalue_adj': pvalue_adj,
                    'fdr_passed': fdr_passed,
                    # D2: zscore и zscore_series из signal_tf (1h) если dual_tf_mode
                    'zscore': _signal_zscore,
                    'zscore_qualify': result['zscore'],        # D2: сохраняем qualify Z для диагностики
                    'zscore_series': _signal_zscore_series,
                    'hedge_ratio': result['hedge_ratio'],
                    'intercept': result.get('intercept', 0.0),
                    'halflife_days': halflife_hours / 24,  # H-01 FIX: actual days (was storing bars)
                    'halflife_hours': halflife_hours,
                    'spread': result['spread'],                # D2: qualify_tf спред (для графиков)
                    'spread_signal': _signal_spread,           # D2: signal_tf спред
                    'signal': state,
                    'direction': direction,
                    'threshold': threshold,
                    # D2 metadata
                    'dual_tf_mode': dual_tf_mode,
                    'qualify_tf': qualify_tf if dual_tf_mode else self.timeframe,
                    'signal_tf_used': _signal_tf_used,
                    'hurst': hurst,
                    'hurst_is_fallback': hurst_is_fallback,
                    'theta': ou_params['theta'] if ou_params else 0,
                    'mu': ou_params['mu'] if ou_params else 0,
                    'sigma': ou_params['sigma'] if ou_params else 0,
                    'halflife_ou': ou_params['halflife_ou'] * 24 if ou_params else 999,
                    'ou_score': ou_score,
                    'ou_valid': is_valid,
                    'ou_reason': reason,
                    'stability_score': stability['stability_score'],
                    'stability_passed': stability['windows_passed'],
                    'stability_total': stability['total_windows'],
                    'is_stable': stability['is_stable'],
                    'adf_pvalue': adf['adf_pvalue'],
                    'adf_passed': adf['is_stationary'],
                    'quality_score': q_score,
                    'quality_breakdown': q_breakdown,
                    'signal_score': s_score,
                    'signal_breakdown': s_breakdown,
                    'trade_score': q_score,
                    'trade_breakdown': q_breakdown,
                    'confidence': confidence,
                    'conf_checks': conf_checks,
                    'conf_total': conf_total,
                    # v9: Kalman
                    'use_kalman': result.get('use_kalman', False),
                    'hr_std': result.get('hr_std', 0.0),
                    'hr_series': result.get('hr_series'),
                    # v10: new metrics
                    'n_bars': n_bars,
                    'z_warning': z_warning,
                    'z_window': _signal_z_window,              # D2: окно из signal TF
                    'crossing_density': crossing_d,
                    'correlation': result.get('correlation', 0.0),
                    # v10.1: HR uncertainty ratio
                    'hr_uncertainty': (hr_std_val / result['hedge_ratio']
                                       if result['hedge_ratio'] > 0 and hr_std_val > 0
                                       else 0.0),
                    # v22: R1 HR Naked Position Filter — v27: from config
                    'hr_naked': abs(result['hedge_ratio']) < _cfg_naked,
                    'hr_naked_warning': (
                        f"⚠️ HR={result['hedge_ratio']:.3f} < {_cfg_naked} — почти naked position! "
                        f"Хедж {abs(result['hedge_ratio'])*100:.0f}%, "
                        f"фактически направленная ставка."
                    ) if abs(result['hedge_ratio']) < _cfg_naked else '',
                    # v11.2: Regime detection
                    'regime': regime_info.get('regime', 'UNKNOWN'),
                    'regime_adx': regime_info.get('adx', 0),
                    'regime_vr': regime_info.get('variance_ratio', 1.0),
                    'regime_trend_pct': regime_info.get('trend_pct', 0.5),
                    # v12.0: CUSUM structural break
                    'cusum_break': cusum_info.get('has_break', False),
                    'cusum_score': cusum_info.get('cusum_score', 0.0),
                    'cusum_drift': cusum_info.get('tail_drift', 0.0),
                    'cusum_warning': cusum_info.get('warning'),
                    'cusum_risk': cusum_info.get('risk_level', 'LOW'),
                    'cusum_advice': cusum_info.get('position_advice', ''),
                    # v13.0: Johansen test
                    'johansen_coint': johansen_info.get('is_cointegrated', False) if johansen_info else False,
                    'johansen_trace': johansen_info.get('trace_stat', 0) if johansen_info else 0,
                    'johansen_cv': johansen_info.get('trace_cv_5pct', 0) if johansen_info else 0,
                    'johansen_hr': johansen_info.get('hedge_ratio', 0) if johansen_info else 0,
                    # v21: PCA factor exposure (P5)
                    'pca_cluster_c1': -1,
                    'pca_cluster_c2': -1,
                    'pca_same_cluster': False,
                    'pca_market_neutral': 0.0,
                    'pca_net_pc1': 0.0,
                    # v16: Hurst EMA smoothing
                    'hurst_ema': hurst_ema,
                    'hurst_exp_slope': hurst_exp_info.get('hurst_slope', 0),
                    'hurst_exp_assessment': hurst_exp_info.get('assessment', 'N/A'),
                    'hurst_exp_short': hurst_exp_info.get('hurst_short', hurst),
                    'hurst_exp_long': hurst_exp_info.get('hurst_long', hurst),
                    'hurst_mr_strengthening': hurst_exp_info.get('mr_strengthening', False),
                    'hurst_mr_weakening': hurst_exp_info.get('mr_weakening', False),
                    'hurst_raw': hurst_raw,  # BUG-024 FIX: всегда сохраняем DFA-значение
                    'hurst_std': hurst_ema_info.get('hurst_std', 0),
                    'hurst_stable': hurst_stable,
                    'hurst_series': hurst_ema_info.get('hurst_series', []),
                    # v18: GARCH Z-score
                    'garch_z': garch_z,
                    'garch_divergence': garch_divergence,
                    'garch_var_expanding': garch_var_expanding,
                    'garch_vol_ratio': garch_info.get('vol_ratio', 1.0),
                    # v17: Mini-backtest results
                    'bt_verdict': combined_verdict,
                    'bt_pnl': bt_result.get('total_pnl', 0),
                    'bt_sharpe': bt_result.get('sharpe', 0),
                    'bt_wr': bt_result.get('win_rate', 0),
                    'bt_pf': bt_result.get('pf', 0),
                    'bt_trades': bt_result.get('n_trades', 0),
                    # v23: R2 Micro-Backtest results
                    'mbt_verdict': mbt_result.get('verdict', 'SKIP'),
                    'mbt_pnl': mbt_result.get('avg_pnl', 0),
                    'mbt_wr': mbt_result.get('win_rate', 0),
                    'mbt_quick': mbt_result.get('quick_reversion_rate', 0),
                    'mbt_trades': mbt_result.get('n_trades', 0),
                    'mbt_z_vel': mbt_result.get('avg_z_velocity', 0),
                    'mbt_avg_bars': mbt_result.get('avg_bars_held', 0),
                    'mbt_pf': mbt_result.get('pf', 0),
                    # v24: R4 Z-Velocity
                    'z_velocity': zvel_result.get('velocity', 0),
                    'z_acceleration': zvel_result.get('acceleration', 0),
                    'z_entry_quality': zvel_result.get('entry_quality', 'UNKNOWN'),
                    'z_toward_zero': zvel_result.get('z_toward_zero', False),
                    'z_vel_description': zvel_result.get('description', ''),
                    # v19: Walk-Forward results
                    'wf_verdict': wf_result.get('verdict', 'SKIP'),
                    'wf_oos_pnl': wf_result.get('total_oos_pnl', 0),
                    'wf_folds_passed': wf_result.get('folds_passed', 0),
                    'wf_n_folds': wf_result.get('n_folds', 0),
                    # v11.2: Warnings
                    'hr_warning': hr_warning,
                    'bars_warning': bars_warning,
                    # v27: Funding rate (populated later for SIGNAL pairs)
                    'funding_rate_1': 0.0,
                    'funding_rate_2': 0.0,
                    'funding_net': 0.0,
                }
            except Exception as _pair_err:
                # v34: Skip broken pair, don't kill entire scan
                return None
            return _pair_row

        # Запускаем параллельный анализ кандидатов
        _p2_workers = 6  # меньше чем фаза 1 — задачи CPU-тяжёлые
        with ThreadPoolExecutor(max_workers=_p2_workers) as _ex2:
            _p2_raw = list(_ex2.map(_analyse_candidate, enumerate(candidates)))

        results = [r for r in _p2_raw if r is not None]

        # Сортируем: v6.0 — сначала по entry readiness, потом по Signal, потом по Quality
        signal_order = {'SIGNAL': 0, 'READY': 1, 'WATCH': 2, 'NEUTRAL': 3}
        entry_order = {'ENTRY': 0, 'CONDITIONAL': 1, 'WAIT': 2}
        
        for r in results:
            ea = assess_entry_readiness(r)
            r['_entry_level'] = ea['level']
            r['_entry_label'] = ea['label']
            r['_fdr_bypass'] = ea['fdr_bypass']
            r['_opt_count'] = ea['opt_count']
            r['_all_mandatory'] = ea['all_mandatory']
            
            # Пункт 11 FIX: BT — КОНФИГУРИРУЕМЫЙ режим (HARD/SOFT/OFF)
            # Управляется bt_filter_mode в config.yaml/strategy.
            # HARD = блокирует авто-вход при mbt_verdict=FAIL (в auto_monitor ниже)
            # SOFT = только метка в UI, не блокирует
            # OFF  = ни метки, ни блокировки
            # Лейбл ❌BT/⚠️BT всегда добавляется для информирования пользователя.
            bt_v = r.get('bt_verdict', 'SKIP')
            if bt_v == 'FAIL':
                lbl = r.get('_entry_label', '')
                if '❌BT' not in lbl and '⚠️BT' not in lbl:
                    r['_entry_label'] = lbl + ' ❌BT'
            elif bt_v == 'WARN':
                lbl = r.get('_entry_label', '')
                if '❌BT' not in lbl and '⚠️BT' not in lbl:
                    r['_entry_label'] = lbl + ' ⚠️BT'
            
            # v22: R1 HR Naked Position Filter
            if r.get('hr_naked', False):
                lbl = r.get('_entry_label', '')
                if '⚠️NK' not in lbl:
                    r['_entry_label'] = lbl + ' ⚠️NK'
                # Downgrade ENTRY to CONDITIONAL for naked HR
                if r.get('_entry_level') == 'ENTRY':
                    r['_entry_level'] = 'CONDITIONAL'
        
        results.sort(key=lambda x: (
            entry_order.get(x.get('_entry_level', 'WAIT'), 3),
            signal_order.get(x['signal'], 4),
            -x['quality_score']
        ))
        
        # v10.2: Cluster detection — найти активы, повторяющиеся в 3+ SIGNAL-парах
        signal_pairs = [r for r in results if r['signal'] == 'SIGNAL']
        if signal_pairs:
            from collections import Counter
            coin_counts = Counter()
            for r in signal_pairs:
                coin_counts[r['coin1']] += 1
                coin_counts[r['coin2']] += 1
            # Кластеры: актив в 3+ SIGNAL-парах
            clusters = {coin: count for coin, count in coin_counts.items() if count >= 3}
            # Пометить каждую пару кластером
            for r in results:
                cluster_coins = []
                if r['coin1'] in clusters:
                    cluster_coins.append(f"{r['coin1']}({clusters[r['coin1']]})")
                if r['coin2'] in clusters:
                    cluster_coins.append(f"{r['coin2']}({clusters[r['coin2']]})")
                r['cluster'] = ', '.join(cluster_coins) if cluster_coins else ''
            
            if clusters:
                sorted_clusters = sorted(clusters.items(), key=lambda x: -x[1])
                cluster_msg = ', '.join(f"**{c}** ({n} пар)" for c, n in sorted_clusters)
                st.warning(f"🔗 Кластеры в SIGNAL: {cluster_msg} — это не {sum(clusters.values())} независимых сделок!")
        else:
            for r in results:
                r['cluster'] = ''
        
        if len(results) > 0:
            entry_ready = sum(1 for r in results if r.get('_entry_level') == 'ENTRY')
            entry_cond = sum(1 for r in results if r.get('_entry_level') == 'CONDITIONAL')
            st.success(f"✅ Найдено {len(results)} пар (FDR: {total_fdr_passed}) | 🟢 ВХОД: {entry_ready} | 🟡 УСЛОВНО: {entry_cond}")
        
        # v21: Enrich with PCA factor exposure (P5)
        pca_r = st.session_state.get('_pca_result')
        if pca_r and 'error' not in pca_r:
            for r in results:
                try:
                    fe = pair_factor_exposure(pca_r, r['coin1'], r['coin2'], r['hedge_ratio'])
                    if fe:
                        r['pca_cluster_c1'] = fe.get('cluster_coin1', -1)
                        r['pca_cluster_c2'] = fe.get('cluster_coin2', -1)
                        r['pca_same_cluster'] = fe.get('same_cluster', False)
                        r['pca_market_neutral'] = fe.get('market_neutrality', 0)
                        r['pca_net_pc1'] = fe.get('net_exposure', {}).get('PC1', 0)
                except Exception:
                    pass
        
        # v27: Fetch funding rates for SIGNAL/READY pairs (futures)
        try:
            _funding_cache = {}
            for r in results:
                if r.get('signal') in ('SIGNAL', 'READY'):
                    for coin_key in ('coin1', 'coin2'):
                        c = r[coin_key]
                        if c not in _funding_cache:
                            _funding_cache[c] = self.fetch_funding_rate(c)
                    fr1 = _funding_cache.get(r['coin1'], {})
                    fr2 = _funding_cache.get(r['coin2'], {})
                    r['funding_rate_1'] = fr1.get('rate_pct', 0)
                    r['funding_rate_2'] = fr2.get('rate_pct', 0)
                    # Net funding: what we pay/receive per 8h
                    d = r.get('direction', 'LONG')
                    if d == 'SHORT':
                        r['funding_net'] = -r['funding_rate_1'] + r['funding_rate_2']
                    else:
                        r['funding_net'] = r['funding_rate_1'] - r['funding_rate_2']
        except Exception:
            pass
        
        return results[:max_pairs]
    
    def get_signal(self, zscore, threshold=2):
        """Определить торговый сигнал"""
        if zscore > threshold:
            return "SHORT"
        elif zscore < -threshold:
            return "LONG"
        else:
            return "NEUTRAL"


# ═══════════════════════════════════════════════════════════════════════════
# D3: ДВУХУРОВНЕВЫЙ СКАН — Кэш квалификаций
#
# Архитектура:
#   Уровень 1 (КВАЛИФИКАЦИЯ, медленно ~30 мин):
#     • Полный scan_pairs() — Hurst, OU, Stability, BT, WF, FDR, Q-score
#     • Результат пишется в qualification_cache.json
#     • Запускается по истечении qual_interval или при пустом кэше
#
#   Уровень 2 (СИГНАЛ, быстро ~2-5 мин):
#     • Только пары из кэша — загружаем свежие цены
#     • Пересчитываем Z-score с Kalman HR из кэша
#     • Обновляем state/direction без полной квалификации
#     • ~10-20x быстрее полного скана
#
# Файл кэша: qualification_cache.json
#   {'timestamp': ISO, 'exchange': str, 'timeframe': str,
#    'pairs': [{...pair dict без spread/zscore_series...}, ...]}
# ═══════════════════════════════════════════════════════════════════════════

QUAL_CACHE_FILE = "qualification_cache.json"
_QUAL_CACHE_STRIP_KEYS = {
    # Не сохраняем в кэш — слишком большие numpy-массивы
    'spread', 'zscore_series', 'hr_series', 'hurst_series',
    'spread_signal', 'signal_breakdown',
    'trade_breakdown', 'conf_checks',
    # v44: quality_breakdown СОХРАНЯЕТСЯ в кэше (убран из strip) для симуляции Q-score
}


def qual_cache_save(pairs_results, exchange, timeframe, qualify_tf='', signal_tf=''):
    """D3: Сохранить результаты квалификации в кэш."""
    try:
        cache_pairs = []
        for p in pairs_results:
            row = {}
            for k, v in p.items():
                if k in _QUAL_CACHE_STRIP_KEYS:
                    continue
                # Конвертируем numpy-типы в python
                try:
                    if isinstance(v, np.ndarray):
                        continue  # пропускаем массивы
                    elif hasattr(v, 'item'):
                        row[k] = v.item()
                    elif isinstance(v, (np.bool_,)):
                        row[k] = bool(v)
                    else:
                        row[k] = v
                except Exception:
                    row[k] = str(v)
            cache_pairs.append(row)

        cache_data = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'exchange': exchange,
            'timeframe': timeframe,
            'qualify_tf': qualify_tf,
            'signal_tf': signal_tf,
            'n_pairs': len(cache_pairs),
            'pairs': cache_pairs,
        }
        import tempfile as _tf_d3
        _dir = os.path.dirname(os.path.abspath(QUAL_CACHE_FILE)) or '.'
        with _tf_d3.NamedTemporaryFile('w', dir=_dir, suffix='.tmp',
                                       delete=False, encoding='utf-8') as _f:
            json.dump(cache_data, _f, indent=2, ensure_ascii=False, default=str)
            _tmp = _f.name
        os.replace(_tmp, QUAL_CACHE_FILE)
        return True
    except Exception as _e:
        return False


def qual_cache_load():
    """D3: Загрузить кэш квалификаций. Возвращает (data_dict | None, age_minutes)."""
    if not os.path.exists(QUAL_CACHE_FILE):
        return None, 9999
    try:
        if os.path.getsize(QUAL_CACHE_FILE) < 10:
            return None, 9999
        with open(QUAL_CACHE_FILE, encoding='utf-8') as f:
            data = json.load(f)
        ts_str = data.get('timestamp', '')
        if ts_str:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age_min = (datetime.now(timezone.utc) - ts).total_seconds() / 60
        else:
            age_min = 9999
        return data, age_min
    except Exception:
        return None, 9999


def qual_cache_is_valid(exchange, timeframe, max_age_min=30,
                        qualify_tf=None, signal_tf=None):
    """D3: Проверить актуальность кэша.
    v41.1 FIX D3-6: Дополнительно проверяет qualify_tf и signal_tf.
    """
    data, age_min = qual_cache_load()
    if data is None:
        return False, 9999, "кэш отсутствует"
    if data.get('exchange') != exchange:
        return False, age_min, f"биржа изменилась ({data.get('exchange')} → {exchange})"
    if data.get('timeframe') != timeframe:
        return False, age_min, f"таймфрейм изменился ({data.get('timeframe')} → {timeframe})"
    if qualify_tf and data.get('qualify_tf') and data.get('qualify_tf') != qualify_tf:
        return False, age_min, f"qualify_tf изменился ({data.get('qualify_tf')} → {qualify_tf})"
    if signal_tf and data.get('signal_tf') and data.get('signal_tf') != signal_tf:
        return False, age_min, f"signal_tf изменился ({data.get('signal_tf')} → {signal_tf})"
    if age_min > max_age_min:
        return False, age_min, f"кэш устарел ({age_min:.0f} мин > {max_age_min} мин)"
    return True, age_min, f"актуален ({age_min:.1f} мин)"


def signal_scan_from_cache(scanner, cache_data, progress_bar=None,
                           dual_tf_mode=False,
                           signal_tf='1h', signal_lookback_days=60):
    """
    D3 Уровень 2: Быстрый сигнальный скан по кэшу квалификаций.

    Загружает ТОЛЬКО свежие цены для пар из кэша.
    Пересчитывает Z-score с сохранённым HR (Kalman).
    Не запускает: Hurst, OU, BT, WF, FDR, PCA — всё берётся из кэша.

    Возвращает список обновлённых пар (тот же формат что scan_pairs).
    """
    import threading as _thr_d3
    from concurrent.futures import ThreadPoolExecutor as _TPE_d3, as_completed as _ac_d3

    cached_pairs = cache_data.get('pairs', [])
    if not cached_pairs:
        return []

    # Собираем уникальные монеты
    coins_needed = set()
    for p in cached_pairs:
        coins_needed.add(p.get('coin1', ''))
        coins_needed.add(p.get('coin2', ''))
    coins_needed.discard('')

    # Определяем ТФ для сигнала
    fetch_tf = signal_tf if dual_tf_mode else scanner.timeframe
    fetch_days = signal_lookback_days if dual_tf_mode else scanner.lookback_days

    if progress_bar:
        progress_bar.progress(0.05, f"📡 [D3 Signal] Загружаю цены для {len(coins_needed)} монет ({fetch_tf})...")

    # Параллельная загрузка свежих цен
    _lock_d3 = _thr_d3.Lock()
    fresh_prices = {}

    def _fetch_fresh(coin):
        p = scanner.fetch_ohlcv_for_tf(coin, fetch_tf, fetch_days)
        return coin, p

    with _TPE_d3(max_workers=8) as ex:
        futs = {ex.submit(_fetch_fresh, c): c for c in coins_needed}
        done = 0
        for fut in _ac_d3(futs):
            done += 1
            if progress_bar and done % 5 == 0:
                progress_bar.progress(
                    0.05 + done / len(coins_needed) * 0.5,
                    f"📡 [D3 Signal] {done}/{len(coins_needed)} монет"
                )
            try:
                coin, prices = fut.result(timeout=30)
                if prices is not None and len(prices) > 20:
                    fresh_prices[coin] = prices
            except Exception:
                pass

    updated_pairs = []
    total = max(len(cached_pairs), 1)

    for idx, cached_p in enumerate(cached_pairs):
        if progress_bar and idx % 5 == 0:
            pct = 0.55 + idx / total * 0.40
            progress_bar.progress(min(pct, 0.95), f"⚡ [D3 Signal] Z-пересчёт {idx}/{total} пар")

        c1 = cached_p.get('coin1', '')
        c2 = cached_p.get('coin2', '')
        if not c1 or not c2:
            continue
        if c1 not in fresh_prices or c2 not in fresh_prices:
            # Нет свежих цен — оставляем кэшированный сигнал
            p_copy = dict(cached_p)
            p_copy['_signal_stale'] = True
            updated_pairs.append(p_copy)
            continue

        try:
            hr = float(cached_p.get('hedge_ratio', 1.0))
            intercept = float(cached_p.get('intercept', 0.0))

            arr1 = fresh_prices[c1].values if hasattr(fresh_prices[c1], 'values') else np.array(fresh_prices[c1])
            arr2 = fresh_prices[c2].values if hasattr(fresh_prices[c2], 'values') else np.array(fresh_prices[c2])
            n = min(len(arr1), len(arr2))
            arr1, arr2 = arr1[-n:], arr2[-n:]

            spread_new = arr1 - hr * arr2 - intercept

            # Адаптивный Z-score
            hl_hours = float(cached_p.get('halflife_hours', 24))
            hours_per_bar = {'1h': 1, '4h': 4, '1d': 24}.get(fetch_tf, 1)
            hl_bars = hl_hours / hours_per_bar if hl_hours < 9999 else None

            new_z, new_z_series, new_z_win = calculate_adaptive_robust_zscore(
                spread_new, halflife_bars=hl_bars
            )

            # Определяем новый сигнал по новому Z + кэшированному threshold
            threshold = float(cached_p.get('threshold', 2.0))
            q_score   = int(cached_p.get('quality_score', 0))
            confidence = cached_p.get('confidence', 'LOW')
            stab_ratio = float(cached_p.get('stability_score', 0.5))
            hurst      = float(cached_p.get('hurst', 0.5))
            fdr_passed = bool(cached_p.get('fdr_passed', False))

            try:
                state, direction, threshold_new = get_adaptive_signal(
                    zscore=new_z,
                    confidence=confidence,
                    quality_score=q_score,
                    timeframe=fetch_tf,
                    stability_ratio=stab_ratio,
                    fdr_passed=fdr_passed,
                    hurst=hurst
                )
            except TypeError:
                try:
                    state, direction, threshold_new = get_adaptive_signal(
                        zscore=new_z, confidence=confidence,
                        quality_score=q_score, timeframe=fetch_tf,
                        stability_ratio=stab_ratio, fdr_passed=fdr_passed
                    )
                except TypeError:
                    state, direction, threshold_new = get_adaptive_signal(
                        zscore=new_z, confidence=confidence,
                        quality_score=q_score, timeframe=fetch_tf,
                        stability_ratio=stab_ratio
                    )

            # Z-velocity
            zvel_result = {'velocity': 0, 'entry_quality': 'UNKNOWN'}
            try:
                if new_z_series is not None and len(new_z_series) >= 7:
                    zvel_result = z_velocity_analysis(new_z_series, lookback=5)
            except Exception:
                pass

            # Строим обновлённую пару
            p_copy = dict(cached_p)
            p_copy.update({
                'zscore': new_z,
                'zscore_series': new_z_series,
                'signal': state,
                'direction': direction,
                'threshold': threshold_new,
                'z_window': new_z_win,
                'z_warning': abs(new_z) > 4.0,
                'z_velocity': zvel_result.get('velocity', 0),
                'z_acceleration': zvel_result.get('acceleration', 0),
                'z_entry_quality': zvel_result.get('entry_quality', 'UNKNOWN'),
                'z_toward_zero': zvel_result.get('z_toward_zero', False),
                'z_vel_description': zvel_result.get('description', ''),
                # Обновлённые цены
                'price1_last': float(arr1[-1]),
                'price2_last': float(arr2[-1]),
                'spread': spread_new,
                # D3 метаданные
                '_signal_stale': False,
                '_d3_signal_refresh': datetime.now(timezone.utc).isoformat(),
                'signal_tf_used': fetch_tf,
            })
            # entry readiness обновляем на основе нового Z
            try:
                ea = assess_entry_readiness(p_copy)
                p_copy['_entry_level'] = ea['level']
                p_copy['_entry_label'] = ea['label']
                p_copy['_fdr_bypass'] = ea['fdr_bypass']
                p_copy['_opt_count'] = ea['opt_count']
                p_copy['_all_mandatory'] = ea['all_mandatory']
            except Exception:
                pass

            updated_pairs.append(p_copy)

        except Exception:
            # fallback — кэшированный результат
            p_copy = dict(cached_p)
            p_copy['_signal_stale'] = True
            updated_pairs.append(p_copy)

    if progress_bar:
        progress_bar.progress(1.0, f"✅ [D3 Signal] {len(updated_pairs)} пар обновлено")

    # Сортировка аналогична scan_pairs
    signal_order = {'SIGNAL': 0, 'READY': 1, 'WATCH': 2, 'NEUTRAL': 3}
    entry_order  = {'ENTRY': 0, 'CONDITIONAL': 1, 'WAIT': 2}
    updated_pairs.sort(key=lambda x: (
        entry_order.get(x.get('_entry_level', 'WAIT'), 3),
        signal_order.get(x.get('signal', 'NEUTRAL'), 4),
        -x.get('quality_score', 0)
    ))

    return updated_pairs


def plot_spread_chart(spread_data, pair_name, zscore, threshold=2.0, direction='NONE',
                      z_window=30, halflife_bars=None):
    """v10.3: Chart with actual adaptive Z-score and signal marker."""
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=(f'Спред: {pair_name}', f'Z-Score (адаптивный, окно={z_window})'),
        vertical_spacing=0.15,
        row_heights=[0.5, 0.5]
    )
    
    spread = np.array(spread_data)
    n = len(spread)
    x_axis = list(range(n))
    
    # 1. Spread chart
    fig.add_trace(
        go.Scatter(x=x_axis, y=spread, name='Spread', 
                   line=dict(color='#42A5F5', width=1.5)),
        row=1, col=1
    )
    
    # Spread mean + ±2σ bands
    w = min(z_window, n // 2)
    if w > 5:
        rolling_mean = pd.Series(spread).rolling(w, min_periods=1).median().values
        rolling_std = pd.Series(spread).rolling(w, min_periods=1).std().values
        fig.add_trace(go.Scatter(x=x_axis, y=rolling_mean, name='Median',
                                 line=dict(color='gray', dash='dash', width=1)), row=1, col=1)
        fig.add_trace(go.Scatter(x=x_axis, y=rolling_mean + 2*rolling_std, name='+2σ',
                                 line=dict(color='#EF5350', dash='dot', width=0.8)), row=1, col=1)
        fig.add_trace(go.Scatter(x=x_axis, y=rolling_mean - 2*rolling_std, name='-2σ',
                                 line=dict(color='#66BB6A', dash='dot', width=0.8)), row=1, col=1)
    
    # Mark current bar (signal moment)
    fig.add_trace(go.Scatter(x=[n-1], y=[spread[-1]], name='📍 Сейчас',
                             mode='markers', marker=dict(size=12, color='yellow', 
                             symbol='star', line=dict(width=1, color='black'))),
                 row=1, col=1)
    
    # 2. Actual adaptive Z-score (MAD-based, matching scanner)
    zs = np.full(n, np.nan)
    w_z = max(10, min(z_window, n // 2))
    for i in range(w_z, n):
        lb = spread[i - w_z:i]
        med = np.median(lb)
        mad = np.median(np.abs(lb - med)) * 1.4826
        if mad < 1e-10:
            s = np.std(lb)
            zs[i] = (spread[i] - np.mean(lb)) / s if s > 1e-10 else 0
        else:
            zs[i] = (spread[i] - med) / mad
    
    # Color Z-score by signal zone
    fig.add_trace(
        go.Scatter(x=x_axis, y=zs, name='Z-Score', 
                   line=dict(color='#AB47BC', width=1.5)),
        row=2, col=1
    )
    
    # Threshold lines (adaptive, not fixed ±2)
    fig.add_hline(y=0, line_dash="dash", line_color="gray", row=2, col=1)
    fig.add_hline(y=threshold, line_dash="dot", line_color="red", row=2, col=1,
                  annotation_text=f"+{threshold}", annotation_position="right")
    fig.add_hline(y=-threshold, line_dash="dot", line_color="green", row=2, col=1,
                  annotation_text=f"-{threshold}", annotation_position="right")
    
    # Signal zone shading
    fig.add_hrect(y0=threshold, y1=max(threshold+3, np.nanmax(zs) if np.any(~np.isnan(zs)) else threshold+1), 
                  fillcolor="red", opacity=0.08, line_width=0, row=2, col=1)
    fig.add_hrect(y0=-threshold-3, y1=-threshold,
                  fillcolor="green", opacity=0.08, line_width=0, row=2, col=1)
    
    # Mark current Z (signal moment) with star
    current_z = zs[-1] if not np.isnan(zs[-1]) else zscore
    signal_color = 'red' if direction == 'SHORT' else 'green' if direction == 'LONG' else 'yellow'
    fig.add_trace(go.Scatter(x=[n-1], y=[current_z], name=f'📍 Z={current_z:+.2f}',
                             mode='markers+text', text=[f'Z={current_z:+.2f}'],
                             textposition='top center', textfont=dict(size=11, color=signal_color),
                             marker=dict(size=14, color=signal_color, symbol='star',
                                        line=dict(width=1, color='black'))),
                 row=2, col=1)
    
    fig.update_xaxes(title_text="Бар #", row=2, col=1)
    fig.update_yaxes(title_text="Спред", row=1, col=1)
    fig.update_yaxes(title_text="Z-Score", row=2, col=1)
    
    fig.update_layout(
        height=650, showlegend=True, hovermode='x unified',
        template='plotly_dark',
        margin=dict(l=60, r=30, t=40, b=30),
        legend=dict(orientation='h', yanchor='bottom', y=1.02)
    )
    
    return fig

# === ИНТЕРФЕЙС ===

st.markdown('<p class="main-header">🔍 Crypto Pairs Trading Scanner</p>', unsafe_allow_html=True)
st.caption("Версия 41.2 | 16.03.2026 | D2+D3 FIX | MON-9: z_mode TRAIL | BYBIT-2: close side from position")

# v27: Config info panel
try:
    from config_loader import CFG_path
    _cp = CFG_path()
    if _cp:
        with st.expander("⚙️ Config", expanded=False):
            st.caption(f"📁 {_cp}")
            st.code(
                f"entry_z: {CFG('strategy', 'entry_z')}\n"
                f"exit_z: {CFG('strategy', 'exit_z')}\n"
                f"commission: {CFG('strategy', 'commission_pct')}%\n"
                f"slippage: {CFG('strategy', 'slippage_pct')}%\n"
                f"take_profit: {CFG('strategy', 'take_profit_pct')}%\n"
                f"hr_naked: {CFG('strategy', 'hr_naked_threshold')}\n"
                f"min_hurst: {CFG('strategy', 'min_hurst')}",
                language='yaml'
            )
except Exception:
    pass
st.markdown("---")

# Sidebar - настройки
with st.sidebar:
    st.header("⚙️ Настройки")
    
    _exch_list = ['bybit', 'okx', 'kucoin', 'binance']
    _exch_val  = st.session_state.settings['exchange']
    if _exch_val not in _exch_list:
        _exch_val = 'bybit'
    exchange = st.selectbox(
        "Биржа",
        _exch_list,
        index=_exch_list.index(_exch_val),
        help="Локальный запуск: Bybit работает напрямую. На облачных серверах используйте OKX/KuCoin.",
        key='exchange_select'
    )
    st.session_state.settings['exchange'] = exchange
    
    timeframe = st.selectbox(
        "Таймфрейм",
        ['1h', '4h', '1d'],
        index=['1h', '4h', '1d'].index(st.session_state.settings['timeframe']),
        key='timeframe_select'
    )
    st.session_state.settings['timeframe'] = timeframe
    
    lookback_days = st.slider(
        "Период анализа (дней)",
        min_value=7,
        max_value=90,
        value=int(st.session_state.settings.get('lookback_days', 28)),
        step=7,
        key='lookback_slider'
    )
    st.session_state.settings['lookback_days'] = lookback_days
    
    top_n_coins = st.slider(
        "Количество монет для анализа",
        min_value=20,
        max_value=200,
        value=int(st.session_state.settings.get('top_n_coins', 130)),
        step=10,
        help="Больше монет = больше пар. 100 монет ≈ 4950 пар, 130 ≈ 8385 пар, 150 ≈ 11000+",
        key='coins_slider'
    )
    st.session_state.settings['top_n_coins'] = top_n_coins
    
    max_pairs_display = st.slider(
        "Максимум пар в результатах",
        min_value=10,
        max_value=100,
        value=st.session_state.settings['max_pairs_display'],
        step=10,
        key='max_pairs_slider'
    )
    st.session_state.settings['max_pairs_display'] = max_pairs_display
    
    st.markdown("---")
    st.subheader("🎯 Фильтры качества")
    
    pvalue_threshold = st.slider(
        "P-value порог",
        min_value=0.01,
        max_value=0.10,
        value=st.session_state.settings['pvalue_threshold'],
        step=0.01,
        key='pvalue_slider'
    )
    st.session_state.settings['pvalue_threshold'] = pvalue_threshold
    
    zscore_threshold = st.slider(
        "Z-score порог для сигнала",
        min_value=1.5,
        max_value=3.0,
        value=st.session_state.settings['zscore_threshold'],
        step=0.1,
        key='zscore_slider'
    )
    st.session_state.settings['zscore_threshold'] = zscore_threshold
    
    st.markdown("---")
    st.subheader("⏱️ Фильтр по времени возврата")
    
    max_halflife_hours = st.slider(
        "Максимальный Half-life (часы)",
        min_value=6,
        max_value=50,  # 50 часов максимум
        value=min(st.session_state.settings['max_halflife_hours'], 50),
        step=2,
        help="Время возврата к среднему. Для 4h: 12-28ч быстрые, 28-50ч стандарт",
        key='halflife_slider'
    )
    st.session_state.settings['max_halflife_hours'] = max_halflife_hours
    
    st.info(f"📊 Текущий фильтр: до {max_halflife_hours} часов ({max_halflife_hours/24:.1f} дней)")
    
    # v10.4: Фильтры мусорных пар
    st.markdown("---")
    st.subheader("🚫 Фильтры пар")
    
    hide_stablecoins = st.checkbox(
        "Скрыть стейблкоины / LST / wrapped",
        value=st.session_state.settings['hide_stablecoins'],
        help="USDC/DAI, ETH/STETH, XAUT/PAXG — идеальная коинтеграция, но спред < 0.5% → убыточно",
        key='hide_stable_chk'
    )
    st.session_state.settings['hide_stablecoins'] = hide_stablecoins
    
    corr_prefilter = st.slider(
        "Корреляционный пре-фильтр",
        min_value=0.0, max_value=0.6, 
        value=st.session_state.settings['corr_prefilter'],
        step=0.05,
        help="Пропускать пары с |ρ| < порога. 0.3 = ускорение 3-5×. 0 = выкл.",
        key='corr_prefilter_slider'
    )
    st.session_state.settings['corr_prefilter'] = corr_prefilter
    
    # НОВОЕ: Фильтры Hurst + OU Process
    st.markdown("---")
    st.subheader("🔬 Mean Reversion Analysis")
    
    st.info("""
    **DFA Hurst** (v6.0):
    • H < 0.35 → Strong mean-reversion ✅
    • H < 0.48 → Mean-reverting ✅
    • H ≈ 0.50 → Random walk ⚪
    • H > 0.55 → Trending ❌
    """)
    
    # Hurst фильтр
    max_hurst = st.slider(
        "Максимальный Hurst",
        min_value=0.0,
        max_value=1.0,
        value=0.55,  # Обновлено для нового метода
        step=0.05,
        help="H < 0.40 = отлично, H < 0.50 = хорошо, H > 0.60 = избегать",
        key='max_hurst'
    )
    
    # OU theta фильтр
    min_theta = st.slider(
        "Минимальная скорость возврата (θ)",
        min_value=0.0,
        max_value=3.0,
        value=0.0,  # Выключен по умолчанию!
        step=0.1,
        help="θ > 1.0 = быстрый возврат. 0.0 = показать все",
        key='min_theta'
    )
    
    # Quality Score фильтр (v8.0)
    min_quality = st.slider(
        "Мин. Quality Score",
        min_value=0, max_value=100, value=0, step=5,
        help="Качество пары (FDR + Stability + Hurst + ADF + HR). 0 = все",
        key='min_quality'
    )
    
    # Signal state фильтр
    signal_filter = st.multiselect(
        "Показывать статусы",
        options=["SIGNAL", "READY", "WATCH", "NEUTRAL"],
        default=["SIGNAL", "READY", "WATCH", "NEUTRAL"],
        help="SIGNAL=вход, READY=почти, WATCH=мониторинг",
        key='signal_filter'
    )
    
    # FDR фильтр
    fdr_only = st.checkbox(
        "Только FDR-подтверждённые",
        value=False,
        help="Только пары, прошедшие Benjamini-Hochberg",
        key='fdr_only'
    )
    
    # Stability фильтр
    stable_only = st.checkbox(
        "Только стабильные пары",
        value=False,
        help="Коинтеграция ≥3/4 подокон",
        key='stable_only'
    )
    
    # v6.0: Entry readiness filter
    st.markdown("---")
    st.subheader("🟢 Готовность к входу")
    entry_filter = st.multiselect(
        "Показывать уровни",
        ["🟢 ВХОД", "🟡 УСЛОВНО", "🟡 СЛАБЫЙ", "⚪ ЖДАТЬ"],
        default=["🟢 ВХОД", "🟡 УСЛОВНО", "🟡 СЛАБЫЙ", "⚪ ЖДАТЬ"],
        key='entry_filter'
    )
    # v41 FIX: auto_entry_filter — отдельный список для АВТО-открытия.
    # entry_filter выше управляет только ОТОБРАЖЕНИЕМ в таблице.
    # Авто-открытие — только 🟢 ВХОД по умолчанию.
    auto_entry_filter = st.multiselect(
        "🤖 Авто-открытие: разрешённые уровни",
        ["🟢 ВХОД", "🟡 УСЛОВНО", "🟡 СЛАБЫЙ"],
        default=["🟢 ВХОД"],
        key='auto_entry_filter',
        help="Какие уровни готовности разрешены для авто-открытия. По умолчанию — только 🟢 ВХОД."
    )
    
    auto_refresh = st.checkbox("Автообновление", value=True, key='auto_refresh_check',
                               help="Автоматически пересканирует пары каждые N минут")

    refresh_interval = 2  # default 2 мин
    if auto_refresh:
        refresh_interval = st.slider(
            "Интервал обновления (минуты)",
            min_value=2,
            max_value=30,
            value=st.session_state.get('_refresh_interval_val', 2),
            step=1,
            key='refresh_interval_slider',
            help="2-5 мин для скальпинга, 10-15 для 4h, 15-30 для 1d"
        )
        st.session_state['_refresh_interval_val'] = refresh_interval
    st.markdown("---")
    st.subheader("🔄 Multi-Timeframe")
    mtf_enabled = st.checkbox(
        "MTF подтверждение",
        value=True,
        help="Проверяет сигнал на младшем ТФ (4h→1h, 1d→4h). Добавляет ~30сек к скану.",
        key='mtf_enabled'
    )
    if mtf_enabled:
        confirm_tf_map = {'4h': '1h', '1d': '4h', '2h': '1h', '1h': None}
        ctf = confirm_tf_map.get(timeframe)
        if ctf:
            st.caption(f"📊 {timeframe} сигнал → проверка на {ctf}")
        else:
            st.caption(f"⚪ {timeframe} — уже минимальный ТФ, MTF недоступен")
    
    # v10.0: Multi-Timeframe Confirmation
    st.markdown("---")

    # ══════════════════════════════════════════════════
    # D2: Dual Timeframe режим
    # ══════════════════════════════════════════════════
    st.subheader("🔀 Dual Timeframe (D2)")
    dual_tf_enabled = st.checkbox(
        "Dual TF режим (1h сигнал + 4h качество)",
        value=st.session_state.get('dual_tf_enabled', False),
        key='dual_tf_enabled',
        help=(
            "D2: Двойной таймфрейм.\n"
            "• qualify_tf (4h/45d) — Hurst, OU, BT, WF, Q-score\n"
            "• signal_tf  (1h/60d) — Z-score, direction\n"
            "Качественная оценка пары + чувствительный вход."
        )
    )
    if dual_tf_enabled:
        _dtf_col1, _dtf_col2 = st.columns(2)
        qualify_tf = _dtf_col1.selectbox(
            "Qualify TF",
            ['4h', '1d', '2h'],
            index=0,
            key='qualify_tf_select',
            help="Качественный анализ (Hurst, BT, WF)"
        )
        signal_tf_d2 = _dtf_col2.selectbox(
            "Signal TF",
            ['1h', '4h', '15m'],
            index=0,
            key='signal_tf_select',
            help="Z-score и direction входа"
        )
        qualify_lookback = st.slider(
            "Qualify lookback (дней)",
            min_value=20, max_value=90, value=45, step=5,
            key='qualify_lookback_slider',
            help="Период для качественного анализа (4h)"
        )
        signal_lookback = st.slider(
            "Signal lookback (дней)",
            min_value=20, max_value=90, value=60, step=5,
            key='signal_lookback_slider',
            help="Период для сигнального Z-score (1h)"
        )
        st.caption(
            f"📊 Qualify: {qualify_tf}/{qualify_lookback}d "
            f"→ Q-score | Signal: {signal_tf_d2}/{signal_lookback}d → Z"
        )
    else:
        qualify_tf = timeframe
        signal_tf_d2 = timeframe
        qualify_lookback = lookback_days
        signal_lookback = lookback_days

    st.markdown("---")

    # ══════════════════════════════════════════════════
    # D3: Двухуровневый скан — настройки кэша
    # ══════════════════════════════════════════════════
    st.subheader("⚡ Двухуровневый скан (D3)")
    d3_enabled = st.checkbox(
        "D3: Кэш квалификаций",
        value=st.session_state.get('d3_enabled', False),
        key='d3_enabled',
        help=(
            "D3: Разделяет скан на 2 уровня:\n"
            "• Полный скан (~30 мин) — квалификация + кэш\n"
            "• Быстрый скан (~2 мин) — только Z-score по кэшу\n"
            "Ускоряет обновление в 10-20 раз."
        )
    )
    if d3_enabled:
        d3_qual_interval = st.slider(
            "Интервал переквалификации (мин)",
            min_value=10, max_value=120, value=30, step=5,
            key='d3_qual_interval_slider',
            help="Как часто запускать полный скан. 30 мин — оптимально для 4h."
        )
        # Статус кэша
        _cache_data, _cache_age = qual_cache_load()
        if _cache_data is not None:
            _cn = _cache_data.get('n_pairs', 0)
            _cex = _cache_data.get('exchange', '?')
            _ctf = _cache_data.get('timeframe', '?')
            if _cache_age < d3_qual_interval:
                st.success(
                    f"✅ Кэш актуален: {_cn} пар, "
                    f"{_cex}/{_ctf}, {_cache_age:.0f} мин назад"
                )
            else:
                st.warning(
                    f"⚠️ Кэш устарел: {_cache_age:.0f} мин "
                    f"(лимит {d3_qual_interval} мин)"
                )
            if st.button("🗑️ Сбросить кэш квалификаций (D3 вкл.)", key='d3_reset_cache'):
                try:
                    os.remove(QUAL_CACHE_FILE)
                    st.success("✅ Кэш сброшен — следующий скан будет полным")
                except Exception:
                    st.error("❌ Не удалось удалить кэш")
        else:
            st.info("📭 Кэш пуст — первый скан будет полным")
    else:
        d3_qual_interval = 30

    # FIX UX-1: Кнопка сброса кэша доступна всегда (даже при d3_enabled=False),
    # чтобы пользователь мог удалить устаревший кэш перед повторным включением D3.
    if os.path.exists(QUAL_CACHE_FILE):
        _ux1_data, _ux1_age = qual_cache_load()
        _ux1_n = _ux1_data.get('n_pairs', 0) if _ux1_data else 0
        if not d3_enabled:
            st.caption(f"📦 Кэш D3: {_ux1_n} пар, {_ux1_age:.0f} мин назад")
        if st.button("🗑️ Сбросить кэш D3", key='d3_reset_cache_global'):
            try:
                os.remove(QUAL_CACHE_FILE)
                st.success("✅ Кэш D3 сброшен")
            except Exception:
                st.error("❌ Не удалось удалить кэш")

    st.markdown("---")
    st.subheader("🤖 Авто-Монитор")
    auto_monitor = st.checkbox(
        "Авто-открытие позиций (SIGNAL/READY)",
        value=True, key='auto_monitor',
        help="Автоматически отслеживать SIGNAL и READY пары в мониторе для наработки истории"
    )
    st.caption("📍 Пары из сканера автоматически отслеживаются в мониторе. "
              "Открытие реальных сделок — вручную на ваше усмотрение.")
    
    st.markdown("---")
    st.subheader("📱 Telegram уведомления")
    tg_enabled = st.checkbox("Включить Telegram", value=False, key='tg_enabled',
                             help="Получайте пуш при новом 🟢 SIGNAL")
    
    # v30: Alert types
    tg_alert_signals = st.checkbox("🔔 Новые сигналы", value=True, key='tg_alert_signals')
    tg_alert_exits = st.checkbox("📤 Сигналы выхода", value=True, key='tg_alert_exits')
    tg_alert_quality = st.checkbox("⚠️ Деградация качества", value=False, key='tg_alert_quality')
    
    # S-003 FIX: credentials из env vars, не хардкод. type=password для обоих.
    import os as _tg_os
    tg_token = st.text_input("Bot Token", 
                             value=_tg_os.environ.get("TG_TOKEN", ""),
                             type="password", key='tg_token',
                             help="Создайте бота через @BotFather. Или задайте TG_TOKEN env var.")
    tg_chat_id = st.text_input("Chat ID",
                               value=_tg_os.environ.get("TG_CHAT_ID", ""),
                               type="password", key='tg_chat_id',
                               help="Узнайте через @userinfobot. Или задайте TG_CHAT_ID env var.")
    
    if tg_token and tg_chat_id:
        if st.button("🔔 Тест Telegram", key='tg_test_btn'):
            ok, msg = send_telegram_test(tg_token, tg_chat_id)
            if ok:
                st.success("✅ Telegram работает!")
            else:
                st.error(f"❌ {msg}")
                if "address" in msg.lower() or "hostname" in msg.lower() or "network" in msg.lower() or "timed out" in msg.lower() or "hosts failed" in msg.lower():
                    st.warning(
                        "⚠️ **Хостинг блокирует ВСЕ подключения к Telegram** "
                        "(DNS + IP + timeout).\n\n"
                        "Это ограничение HuggingFace/Railway/Render и др.\n\n"
                        "**Единственное решение — запуск ЛОКАЛЬНО:**\n"
                        "```\npip install -r requirements.txt\nstreamlit run app.py\n```\n\n"
                        "Локально Telegram будет работать без проблем."
                    )
    
    st.markdown("---")
    st.markdown("### 📖 Как использовать:")
    st.markdown("""
    1. **Нажмите "Запустить сканер"**
    2. **Дождитесь результатов** (1-3 минуты)
    3. **Найдите пары с сигналами:**
       - 🟢 LONG - покупать первую монету
       - 🔴 SHORT - продавать первую монету
    4. **Проверьте графики** для подтверждения
    5. **Кликните на строку** → откроется анализ
    6. **Добавьте в отслеживание** для мониторинга
    """)
    
    st.markdown("---")

# Основная область
col1, col2, col3 = st.columns([2, 2, 1])

with col1:
    if st.button("🚀 Запустить сканер", type="primary", width='stretch'):
        st.session_state.running = True

with col2:
    if st.button("⏹️ Остановить", width='stretch'):
        st.session_state.running = False

with col3:
    if st.session_state.last_update:
        st.metric("Последнее обновление", 
                 st.session_state.last_update.strftime("%H:%M:%S"))

# Запуск сканера
# v27: Clean auto-refresh logic
# 1. Manual button → running=True → scan
# 2. Auto-refresh: timestamp check → _do_scan=True if interval elapsed
_do_scan = st.session_state.running
if not _do_scan and auto_refresh and st.session_state.pairs_data is not None:
    _last_ts = st.session_state.get('_last_scan_ts', 0)
    if _last_ts > 0:
        _elapsed = (time.time() - _last_ts) / 60
        if _elapsed >= refresh_interval:
            _do_scan = True
        # Countdown shown at bottom of page (after results display)

if _do_scan:
    # Set timestamp IMMEDIATELY to prevent re-trigger on error
    st.session_state['_last_scan_ts'] = time.time()
    st.session_state['_scan_ts_str'] = now_msk().strftime('%Y%m%d_%H%M%S')  # v43 FIX
    try:
        scanner = CryptoPairsScanner(
            exchange_name=exchange,
            timeframe=qualify_tf if dual_tf_enabled else timeframe,
            lookback_days=qualify_lookback if dual_tf_enabled else lookback_days
        )
        
        # Прогресс бар
        progress_placeholder = st.empty()
        progress_bar = progress_placeholder.progress(0, "Инициализация...")

        # ══════════════════════════════════════════════════════════════
        # D3: Двухуровневый скан
        # Решение: если D3 включён И кэш актуален → быстрый сигнальный скан
        #          если D3 включён И кэш устарел  → полный скан + сохранить кэш
        #          если D3 выключен               → полный скан (поведение до D3)
        # ══════════════════════════════════════════════════════════════
        _d3_mode = st.session_state.get('d3_enabled', False)
        _d3_qual_interval = st.session_state.get('d3_qual_interval_slider', 30)
        # G-01 FIX: print(f"[DIAG] D3 mode={_d3_mode}, dual_tf={dual_tf_enabled}", flush=True)

        _d3_cache_valid, _d3_cache_age_min, _d3_cache_reason = (
            qual_cache_is_valid(exchange, timeframe, max_age_min=_d3_qual_interval)
            if _d3_mode else (False, 9999, 'D3 выключен')
        )

        if _d3_mode and _d3_cache_valid:
            # ─── D3 Уровень 2: БЫСТРЫЙ СИГНАЛЬНЫЙ СКАН ───────────────
            _cache_data_d3, _ = qual_cache_load()
            st.info(
                f"⚡ [D3] Быстрый сигнальный скан "
                f"(кэш {_d3_cache_age_min:.0f} мин, лимит {_d3_qual_interval} мин). "
                f"Пересчёт Z-score без переквалификации..."
            )
            pairs_results = signal_scan_from_cache(
                scanner,
                _cache_data_d3,
                progress_bar=progress_bar,
                dual_tf_mode=dual_tf_enabled,
                signal_tf=signal_tf_d2,
                signal_lookback_days=signal_lookback,
            )
            top_coins = []  # не нужны — уже загрузили внутри signal_scan_from_cache

        else:
            # ─── D3 Уровень 1 (или D3 выключен): ПОЛНЫЙ СКАН ─────────
            if _d3_mode and not _d3_cache_valid:
                st.info(
                    f"🔄 [D3] Полный скан квалификаций "
                    f"({_d3_cache_reason}). После завершения — кэш сохранится на {_d3_qual_interval} мин."
                )

            top_coins = scanner.get_top_coins(limit=top_n_coins)
            # G-01 FIX: print(f"[DIAG] get_top_coins вернул {len(top_coins) if top_coins else 0} монет", flush=True)

            if not top_coins:
                st.error("❌ Не удалось получить список монет. Проверьте подключение к интернету или попробуйте другую биржу.")
                st.session_state.running = False
                pairs_results = []
            else:
                import sys as _sys_pre
                _sys_pre.stderr.write(f"[DIAG] Calling scan_pairs NOW...\n")
                # G-01 FIX: print(f"[DIAG] Вызываю scan_pairs(coins={len(top_coins)}, dual_tf={dual_tf_enabled})...", flush=True)
                pairs_results = scanner.scan_pairs(
                    top_coins,
                    max_pairs=max_pairs_display,
                    progress_bar=progress_bar,
                    max_halflife_hours=max_halflife_hours,
                    hide_stablecoins=st.session_state.settings['hide_stablecoins'],
                    corr_prefilter=st.session_state.settings['corr_prefilter'],
                    # D2 параметры
                    dual_tf_mode=dual_tf_enabled,
                    signal_tf=signal_tf_d2,
                    signal_lookback_days=signal_lookback,
                    qualify_tf=qualify_tf,
                    qualify_lookback_days=qualify_lookback,
                )

                # D3: сохраняем кэш квалификаций после полного скана
                if _d3_mode and pairs_results:
                    _saved = qual_cache_save(
                        pairs_results, exchange, timeframe,
                        qualify_tf=qualify_tf if dual_tf_enabled else timeframe,
                        signal_tf=signal_tf_d2 if dual_tf_enabled else timeframe,
                    )
                    if _saved:
                        st.toast(
                            f"💾 [D3] Кэш квалификаций сохранён "
                            f"({len(pairs_results)} пар, актуален {_d3_qual_interval} мин)",
                            icon="✅"
                        )
        
        progress_placeholder.empty()
        
        # ═══════ v10.0: MULTI-TIMEFRAME CONFIRMATION ═══════
        mtf_enabled = st.session_state.get('mtf_enabled', True)
        # В dual_tf_mode MTF подтверждение выполнять на signal_tf (уже сделано через D2),
        # поэтому пропускаем дополнительный MTF-проход чтобы не дублировать
        confirm_tf = {'4h': '1h', '1d': '4h', '2h': '1h'}.get(timeframe)
        
        if mtf_enabled and confirm_tf and pairs_results and not dual_tf_enabled:
            # Только для SIGNAL и READY пар (не тратим время на WATCH/NEUTRAL)
            mtf_candidates = [p for p in pairs_results 
                              if p.get('signal') in ('SIGNAL', 'READY') 
                              and p.get('direction', 'NONE') != 'NONE']
            
            if mtf_candidates:
                mtf_bar = st.progress(0, f"🔄 MTF подтверждение ({confirm_tf}) для {len(mtf_candidates)} пар...")
                
                for idx, p in enumerate(mtf_candidates):
                    mtf_bar.progress((idx + 1) / len(mtf_candidates), 
                                    f"🔄 MTF: {p['coin1']}/{p['coin2']} ({idx+1}/{len(mtf_candidates)})")
                    
                    mtf = scanner.mtf_confirm(
                        p['coin1'], p['coin2'],
                        primary_direction=p.get('direction', 'NONE'),
                        primary_z=p.get('zscore', 0),
                        primary_hr=p.get('hedge_ratio', 1.0)
                    )
                    
                    # Добавляем MTF данные к результату пары
                    if mtf:
                        p.update({
                            'mtf_confirmed': mtf.get('mtf_confirmed'),
                            'mtf_strength': mtf.get('mtf_strength', ''),
                            'mtf_tf': mtf.get('mtf_tf', confirm_tf),
                            'mtf_z': mtf.get('mtf_z', None),
                            'mtf_z_velocity': mtf.get('mtf_z_velocity', None),
                            'mtf_checks': mtf.get('mtf_checks', []),
                            'mtf_passed': mtf.get('mtf_passed', 0),
                            'mtf_total': mtf.get('mtf_total', 0),
                            'mtf_reason': mtf.get('mtf_reason', ''),
                        })
                    else:
                        p['mtf_confirmed'] = None
                    
                    import time as _time
                    _time.sleep(0.15)  # Rate limit protection
                
                mtf_bar.empty()
                
                confirmed_count = sum(1 for p in mtf_candidates if p.get('mtf_confirmed') == True)
                st.info(f"✅ MTF ({confirm_tf}): {confirmed_count}/{len(mtf_candidates)} пар подтверждены")
        
        # v15.0: Detect directional conflicts (same coin LONG+SHORT)
        coin_directions = {}  # coin → set of directions
        for p in pairs_results:
            d = p.get('direction', 'NONE')
            if d == 'NONE': continue
            c1 = p.get('pair', '/').split('/')[0]
            c2 = p.get('pair', '/').split('/')[1] if '/' in p.get('pair','') else ''
            # Coin1 action = direction, Coin2 action = opposite
            c1_dir = d
            c2_dir = 'SHORT' if d == 'LONG' else 'LONG'
            coin_directions.setdefault(c1, set()).add(c1_dir)
            coin_directions.setdefault(c2, set()).add(c2_dir)
        
        conflict_coins = {c for c, dirs in coin_directions.items() 
                          if len(dirs) > 1}
        
        if conflict_coins:
            for p in pairs_results:
                c1 = p.get('pair', '/').split('/')[0]
                c2 = p.get('pair', '/').split('/')[1] if '/' in p.get('pair','') else ''
                p['coin_conflict'] = (c1 in conflict_coins or c2 in conflict_coins)
                if p['coin_conflict']:
                    confl = [c for c in [c1, c2] if c in conflict_coins]
                    p['conflict_coins'] = ','.join(confl)
            
            st.warning(
                f"🚨 **Конфликт направлений:** {', '.join(sorted(conflict_coins))} "
                f"торгуются LONG+SHORT одновременно в разных парах. "
                f"Выбирайте пары где монета торгуется в ОДНОМ направлении."
            )
        
        # v20: Progress indicator so user knows table is building
        try:
            progress_bar.progress(95, "📊 Формирую таблицу результатов...")
        except Exception:
            pass
        
        # v19: Portfolio concentration check
        signal_pairs = [p for p in pairs_results 
                       if p.get('_entry_level') in ('ENTRY', 'CONDITIONAL')]
        if signal_pairs:
            coin_count = {}
            for p in signal_pairs:
                c1 = p.get('pair', '/').split('/')[0]
                c2 = p.get('pair', '/').split('/')[1] if '/' in p.get('pair','') else ''
                coin_count[c1] = coin_count.get(c1, 0) + 1
                coin_count[c2] = coin_count.get(c2, 0) + 1
            concentrated = {c: n for c, n in coin_count.items() if n >= 3}
            if concentrated:
                coins_str = ', '.join(f"{c}({n}×)" for c, n in sorted(concentrated.items(), key=lambda x: -x[1]))
                st.warning(
                    f"⚠️ **Концентрация:** {coins_str} — слишком много пар с одной монетой. "
                    f"Диверсифицируйте: выберите 1-2 лучшие пары на монету."
                )
        
        # Store
        st.session_state.pairs_data = pairs_results
        st.session_state.last_update = now_msk()
        st.session_state.running = False
        
        # v44: Логирование quality breakdown для симуляции Q-score (Волна 3, п.15)
        # Append-only JSONL: одна строка на пару, сохраняет все компоненты Q-score.
        try:
            _qbd_file = os.path.join("scan_exports", "quality_breakdown_log.jsonl")
            os.makedirs("scan_exports", exist_ok=True)
            _qbd_ts = now_msk().isoformat()
            with open(_qbd_file, 'a', encoding='utf-8') as _qf:
                for _p in pairs_results:
                    _qbd = _p.get('quality_breakdown')
                    if _qbd:
                        _qf.write(json.dumps({
                            'ts': _qbd_ts,
                            'pair': _p.get('pair', ''),
                            'quality': _p.get('quality_score', 0),
                            'quality_bd': _qbd,
                            'signal': _p.get('signal', ''),
                            'direction': _p.get('direction', ''),
                            'zscore': round(_p.get('zscore', 0), 4),
                            'hurst': round(_p.get('hurst', 0.5), 4),
                            'pvalue_adj': round(_p.get('pvalue_adj', 1.0), 6),
                            'hedge_ratio': round(_p.get('hedge_ratio', 0), 6),
                            'entry_label': _p.get('_entry_label', ''),
                        }, ensure_ascii=False) + '\n')
        except Exception:
            pass
        
        # v20.1: Auto-export CSV of scan results + SIGNAL details
        try:
            import os
            export_dir = "scan_exports"
            os.makedirs(export_dir, exist_ok=True)
            ts = now_msk().strftime('%Y%m%d_%H%M%S')
            
            # 1. Full scan CSV
            exp_rows = []
            for p in pairs_results:
                exp_rows.append({
                    'Пара': p['pair'], 'Coin1': p['coin1'], 'Coin2': p['coin2'],
                    'Вход': p.get('_entry_label', '⚪'), 'Статус': p['signal'],
                    'Направление': p.get('direction', ''),
                    'Q': p.get('quality_score', 0), 'S': p.get('signal_score', 0),
                    'Z': round(p['zscore'], 4), 'Thr': p.get('threshold', 2.0),
                    'P-val': round(p['pvalue'], 6), 'Hurst': round(p.get('hurst', 0.5), 4),
                    'HL_h': round(p.get('halflife_hours', 0), 2),
                    'HR': round(p['hedge_ratio'], 6),
                    'ρ': round(p.get('correlation', 0), 4),
                    'BT': p.get('bt_verdict', ''), 'BT_PnL': p.get('bt_pnl', ''),
                    'WF': p.get('wf_verdict', ''), 'H_slope': p.get('hurst_exp_slope', ''),
                    'uBT': p.get('mbt_verdict', ''), 'uBT_PnL': p.get('mbt_pnl', ''),
                    'uBT_WR': p.get('mbt_wr', ''), 'uBT_Quick': p.get('mbt_quick', ''),
                    'V↕': p.get('z_velocity', ''), 'V_Quality': p.get('z_entry_quality', ''),
                })
            scan_df = pd.DataFrame(exp_rows)
            scan_path = f"{export_dir}/scan_{exchange}_{timeframe}_{ts}.csv"
            scan_df.to_csv(scan_path, index=False)
            
            # 2. Detail CSVs for SIGNAL pairs
            signal_results = [p for p in pairs_results if p['signal'] == 'SIGNAL']
            detail_paths = []
            for p in signal_results:
                pair_name = p['pair'].replace('/', '_')
                detail_data = {
                    'Параметр': ['Пара', 'Направление', 'Статус', 'Вход', 'Z-score',
                                 'Threshold', 'Quality Score', 'Signal Score', 'Confidence',
                                 'P-value (adj)', 'FDR', 'ADF', 'Hurst (DFA)',
                                 'Half-life (ч)', 'Theta', 'Hedge Ratio', 'HR uncertainty',
                                 'Корреляция ρ', 'Stability', 'Crossing Density', 'Z-window',
                                 'Kalman HR', 'N баров', 'Regime', 'Regime ADX',
                                 'Johansen', 'Johansen Trace', 'Johansen CV 5%', 'Johansen HR',
                                 'BT Verdict', 'BT P&L', 'WF Verdict', 'WF OOS P&L',
                                 'H Slope', 'H Assessment',
                                 'uBT Verdict', 'uBT Avg P&L', 'uBT WR', 'uBT Quick%',
                                 'uBT Trades', 'uBT Z-velocity', 'uBT Avg Bars'],
                    'Значение': [p['pair'], p.get('direction', ''), p['signal'],
                                 p.get('_entry_label', ''), round(p['zscore'], 4),
                                 p.get('threshold', 2.0), p.get('quality_score', 0),
                                 p.get('signal_score', 0), p.get('confidence', ''),
                                 round(p.get('pvalue_adj', p['pvalue']), 6),
                                 '✅' if p.get('fdr_passed') else '❌',
                                 '✅' if p.get('adf_passed') else '❌',
                                 round(p.get('hurst', 0.5), 4),
                                 round(p.get('halflife_hours', 0), 2),
                                 round(p.get('theta', 0), 4),
                                 round(p['hedge_ratio'], 6),
                                 round(p.get('hr_uncertainty', 0), 4),
                                 round(p.get('correlation', 0), 4),
                                 f"{p.get('stability_passed', 0)}/{p.get('stability_total', 4)}",
                                 round(p.get('crossing_density', 0), 4),
                                 p.get('z_window', 30),
                                 '✅' if p.get('use_kalman') else '❌',
                                 p.get('n_bars', 0),
                                 p.get('regime', ''), p.get('regime_adx', 0),
                                 '✅' if p.get('johansen_coint') else '❌',
                                 round(p.get('johansen_trace', 0), 1),
                                 round(p.get('johansen_cv', 0), 1),
                                 round(p.get('johansen_hr', 0), 4),
                                 p.get('bt_verdict', ''), p.get('bt_pnl', ''),
                                 p.get('wf_verdict', ''), p.get('wf_oos_pnl', ''),
                                 p.get('hurst_exp_slope', ''),
                                 p.get('hurst_exp_assessment', ''),
                                 p.get('mbt_verdict', ''), p.get('mbt_pnl', ''),
                                 p.get('mbt_wr', ''), p.get('mbt_quick', ''),
                                 p.get('mbt_trades', ''), p.get('mbt_z_vel', ''),
                                 p.get('mbt_avg_bars', '')]
                }
                d_path = f"{export_dir}/detail_{pair_name}_{ts}.csv"
                pd.DataFrame(detail_data).to_csv(d_path, index=False)
                detail_paths.append(d_path)
            
            st.session_state['_last_scan_csv'] = scan_path
            st.session_state['_last_detail_csvs'] = detail_paths
            st.toast(f"💾 Экспорт: {scan_path} + {len(detail_paths)} detail файлов")
        except Exception as ex:
            st.toast(f"⚠️ Auto-export: {ex}", icon="⚠️")  # v7.1: КРИТИЧНО — без этого выбор пары перезапускает скан
        
        # v30: Enhanced Telegram — multiple alert types
        tg_token = st.session_state.get('tg_token', '')
        tg_chat = st.session_state.get('tg_chat_id', '')
        if st.session_state.get('tg_enabled') and tg_token and tg_chat and pairs_results:
            # Signal alerts
            if st.session_state.get('tg_alert_signals', True):
                signal_pairs = [p for p in pairs_results 
                               if p.get('signal') in ('SIGNAL', 'READY')
                               and p.get('direction', 'NONE') != 'NONE']
                prev_signals = st.session_state.get('_prev_signal_pairs', set())
                new_signals = [p for p in signal_pairs 
                              if p.get('pair') not in prev_signals]
                st.session_state['_prev_signal_pairs'] = {
                    p.get('pair') for p in signal_pairs}
                
                if new_signals:
                    msg = format_telegram_signal(new_signals, timeframe, exchange)
                    if msg:
                        ok, err = send_telegram(tg_token, tg_chat, msg)
                        if ok:
                            st.toast(f"📱 TG: {len(new_signals)} сигналов")
            
            # Vanished signals (was SIGNAL → now gone)
            _vanished = prev_signals - {p.get('pair') for p in signal_pairs}
            if _vanished and st.session_state.get('tg_alert_exits', True):
                _van_msg = (f"📤 <b>Сигналы исчезли</b>\n"
                           f"⏰ {now_msk().strftime('%H:%M МСК')}\n\n"
                           + "\n".join(f"❌ {v}" for v in _vanished))
                send_telegram(tg_token, tg_chat, _van_msg)
        
        # v20: auto-refresh moved to END of script (after display)
        # Old code had st.rerun() here — table NEVER rendered on refresh!
        # v25: Set timestamp AFTER scan completes (not before!)
        st.session_state['_last_scan_ts'] = time.time()
        # v43 FIX: стабильный timestamp для file_name в download_button.
        # Фиксируется один раз при завершении скана. При rerun не меняется,
        # поэтому download_button не генерирует новый хеш media файла.
        st.session_state['_scan_ts_str'] = now_msk().strftime('%Y%m%d_%H%M%S')
        
        # ═══ v39: AUTO-OPEN positions for SIGNAL/READY pairs ═══
        # v39 FIX: Respect entry_filter checkboxes + add missing safety checks
        if st.session_state.get('auto_monitor', True) and pairs_results:
            try:
                import json as _json
                _pos_file = "positions.json"
                _existing = []
                # BUG-N11 FIX: атомарное чтение с fallback на .bak и .new,
                # идентично load_positions() монитора — защита от гонки состояний.
                for _fpath in [_pos_file, _pos_file + ".bak", _pos_file + ".new"]:
                    if not os.path.exists(_fpath):
                        continue
                    try:
                        if os.path.getsize(_fpath) < 3:
                            continue
                        with open(_fpath, encoding='utf-8') as f:
                            _src = f.read()
                        if _src.startswith('\x00') or _src[:10].count('\x00') > 3:
                            continue
                        _raw = _json.loads(_src)
                        # v35.1 FIX: Filter out corrupted non-dict entries
                        if isinstance(_raw, list):
                            _existing = [p for p in _raw if isinstance(p, dict)]
                        elif isinstance(_raw, dict):
                            # v39 FIX: Handle dict format (single position)
                            _existing = [_raw] if _raw else []
                        else:
                            _existing = []
                        break  # успешно прочитали — выходим из цикла
                    except Exception:
                        continue
                _open_pairs = {f"{p.get('coin1','')}/{p.get('coin2','')}" 
                               for p in _existing if p.get('status') == 'OPEN'}
                _open_pairs_rev = {f"{p.get('coin2','')}/{p.get('coin1','')}" 
                                   for p in _existing if p.get('status') == 'OPEN'}
                
                # v41 FIX: Авто-открытие использует auto_entry_filter (только 🟢 по умолчанию),
                # НЕ entry_filter (который управляет отображением таблицы).
                _ef = st.session_state.get('auto_entry_filter', ["🟢 ВХОД"])
                
                # v39: Read rally state for LONG blocking
                try:
                    _rally_st = load_rally_state()
                except Exception:
                    _rally_st = {'status': 'NORMAL'}
                
                # v39: Daily loss limit check
                # v40 FIX: Inline check — importing monitor kills auto-monitor (st.set_page_config crash)
                # BUG-N08 FIX: добавляем unrealized PnL открытых убыточных позиций,
                # идентично check_daily_loss_limit() монитора (BUG-016 там уже исправлен).
                # Без этого можно открыть новые позиции при уже превышенном фактическом убытке.
                _daily_blocked = False
                _daily_reason = ""
                try:
                    _cd_file = "pair_cooldowns.json"
                    if os.path.exists(_cd_file):
                        with open(_cd_file, 'r', encoding='utf-8') as _cdf:
                            _cd_all = _json.load(_cdf)
                        _today_str = now_msk().strftime('%Y-%m-%d')
                        _daily_pnl = sum(e.get('session_pnl', 0) for e in _cd_all.values()
                                       if e.get('date') == _today_str)
                        # BUG-N08 FIX: прибавляем unrealized losses открытых позиций.
                        # _existing уже загружен выше (с атомарной защитой BUG-N11).
                        # Учитываем только отрицательный PnL (прибыльные позиции не считаем).
                        try:
                            _unrealized_loss = sum(
                                p.get('pnl_pct', 0) for p in _existing
                                if p.get('status') == 'OPEN' and p.get('pnl_pct', 0) < 0
                            )
                            _daily_pnl += _unrealized_loss
                        except Exception:
                            pass
                        # Берём лимит из session_state (сайдбар монитора) или CFG —
                        # аналогично check_daily_loss_limit() в monitor_v38_3.py
                        try:
                            _daily_limit = float(st.session_state.get(
                                'daily_loss_limit_pct',
                                CFG('monitor', 'daily_loss_limit_pct', -5.0)
                            ))
                        except Exception:
                            _daily_limit = float(CFG('monitor', 'daily_loss_limit_pct', -5.0))
                        if _daily_pnl <= _daily_limit:
                            _daily_blocked = True
                            _daily_reason = (
                                f"🛑 ДНЕВНОЙ ЛИМИТ: {_daily_pnl:+.2f}% "
                                f"(лимит {_daily_limit}%, включая открытые)"
                            )
                except Exception:
                    _daily_blocked = False

                # BUG-N14 FIX: Cascade SL inline check для app.py auto_monitor.
                # Не импортируем monitor — используем ту же логику inline
                # (импорт monitor убивает auto-monitor через st.set_page_config crash).
                _cascade_blocked = False
                _cascade_reason = ""
                try:
                    if CFG('monitor', 'cascade_sl_enabled', False):
                        import time as _time_mod
                        from datetime import timedelta as _td_casc
                        _casc_w  = float(CFG('monitor', 'cascade_sl_window_hours', 2))
                        _casc_th = int(CFG('monitor', 'cascade_sl_threshold', 3))
                        _casc_ph = float(CFG('monitor', 'cascade_sl_pause_hours', 1))
                        _casc_state_file = "cascade_sl_state.json"

                        # Проверяем текущую активную паузу
                        _casc_state = {}
                        if os.path.exists(_casc_state_file):
                            try:
                                with open(_casc_state_file, 'r', encoding='utf-8') as _csf:
                                    _casc_state = _json.load(_csf)
                            except Exception:
                                pass

                        _casc_now = now_msk()
                        if _casc_state.get('pause_start'):
                            from datetime import datetime as _dt_casc
                            _casc_ps = _dt_casc.fromisoformat(_casc_state['pause_start'])
                            _casc_elapsed = (_casc_now - _casc_ps).total_seconds() / 3600
                            if _casc_elapsed < _casc_ph:
                                _casc_rem = _casc_ph - _casc_elapsed
                                _casc_cnt = _casc_state.get('sl_count', _casc_th)
                                _cascade_blocked = True
                                _cascade_reason = (
                                    f"🛑 CASCADE SL ПАУЗА: {_casc_cnt} стоп-лоссов "
                                    f"за {_casc_w:.0f}ч → пауза {_casc_ph:.0f}ч "
                                    f"({_casc_rem:.1f}ч осталось)"
                                )
                        # Если пауза не активна — считаем свежие SL для определения новой паузы
                        if not _cascade_blocked:
                            _cd_file2 = "pair_cooldowns.json"
                            if os.path.exists(_cd_file2):
                                with open(_cd_file2, 'r', encoding='utf-8') as _cdf2:
                                    _cd2 = _json.load(_cdf2)
                                _casc_cutoff = _casc_now - _td_casc(hours=_casc_w)
                                from datetime import datetime as _dt_casc2
                                _sl_cnt = sum(
                                    1 for _e in _cd2.values()
                                    if _e.get('sl_exit') and _e.get('last_loss_time') and
                                    _dt_casc2.fromisoformat(_e['last_loss_time']) >= _casc_cutoff
                                )
                                if _sl_cnt >= _casc_th:
                                    # Фиксируем паузу в файле (monitor подхватит тоже)
                                    _new_state = {
                                        'pause_start': _casc_now.isoformat(),
                                        'sl_count': _sl_cnt,
                                        'window_h': _casc_w,
                                        'pause_h': _casc_ph,
                                    }
                                    try:
                                        with open(_casc_state_file, 'w', encoding='utf-8') as _csfw:
                                            _json.dump(_new_state, _csfw, indent=2)
                                    except Exception:
                                        pass
                                    _cascade_blocked = True
                                    _cascade_reason = (
                                        f"🛑 CASCADE SL ПАУЗА: {_sl_cnt} стоп-лоссов "
                                        f"за {_casc_w:.0f}ч → пауза {_casc_ph:.0f}ч "
                                        f"({_casc_ph:.1f}ч осталось)"
                                    )
                except Exception:
                    _cascade_blocked = False
                
                _auto_count = 0
                _skip_reasons = []

                # BUG-N01 FIX: проверяем глобальный лимит max_positions ДО цикла.
                # Если уже открыто >= max_positions позиций — пропускаем весь цикл,
                # не создаём мусорных pending-файлов и лишних логов.
                _max_positions = CFG('monitor', 'max_positions', 10)
                _open_count = sum(1 for _p in _existing if _p.get('status') == 'OPEN')
                if _open_count >= _max_positions:
                    _skip_reasons.append(
                        f"🛑 max_positions={_max_positions} достигнут "
                        f"({_open_count} открытых) — авто-открытие заблокировано"
                    )

                # BUG-N09 FIX: подгружаем pending-файлы из monitor_import/,
                # чтобы coin_conflict и max_coin_positions учитывали позиции,
                # ещё не принятые монитором (иначе за один цикл может создаться
                # несколько конфликтующих pending по одной монете).
                _pending_positions = []
                try:
                    _imp_dir = "monitor_import"
                    if os.path.isdir(_imp_dir):
                        for _pf_name in os.listdir(_imp_dir):
                            if not _pf_name.startswith("pending_") or not _pf_name.endswith(".json"):
                                continue
                            _pf_path = os.path.join(_imp_dir, _pf_name)
                            try:
                                with open(_pf_path, 'r', encoding='utf-8') as _pff:
                                    _pd = _json.load(_pff)
                                if isinstance(_pd, dict) and _pd.get('coin1') and _pd.get('coin2'):
                                    # Представляем pending как псевдо-позицию OPEN
                                    # для проверок coin_conflict / max_coin_positions
                                    _pending_positions.append({
                                        'coin1': _pd['coin1'],
                                        'coin2': _pd['coin2'],
                                        'direction': _pd.get('direction', 'LONG'),
                                        'status': 'OPEN',
                                        '_is_pending': True,
                                    })
                            except Exception:
                                pass
                except Exception:
                    pass
                # Объединяем реальные открытые позиции + pending для всех проверок ниже
                _existing_with_pending = _existing + _pending_positions

                for p in pairs_results:
                    # BUG-N01 FIX: повторная проверка лимита внутри цикла,
                    # т.к. _auto_count растёт по мере создания pending-файлов.
                    if (_open_count + _auto_count) >= _max_positions:
                        _skip_reasons.append(
                            f"{p.get('pair', p.get('coin1','') + '/' + p.get('coin2',''))}: "
                            f"max_positions={_max_positions} достигнут"
                        )
                        continue
                    _pair_name = p.get('pair', f"{p.get('coin1','')}/{p.get('coin2','')}")
                    
                    if p.get('signal') not in ('SIGNAL', 'READY'):
                        continue
                    if p.get('direction', 'NONE') == 'NONE':
                        continue
                    
                    # v39 FIX: Check entry_label against entry_filter checkboxes
                    # ⚪ ЖДАТЬ is ALWAYS blocked (user requirement)
                    _entry_lbl = p.get('_entry_label', '⚪ ЖДАТЬ')
                    if _entry_lbl.startswith('⚪'):
                        continue  # WAIT always blocked
                    
                    # Check if entry level is in allowed filter list
                    _entry_allowed = False
                    for _f in _ef:
                        if _f.startswith('⚪'):
                            continue  # Skip WAIT from filter check
                        if _entry_lbl.startswith(_f[:2]):  # Match by emoji prefix
                            _entry_allowed = True
                            break
                    # If no non-WAIT filters selected, allow all non-WAIT entries
                    _non_wait_filters = [f for f in _ef if not f.startswith('⚪')]
                    if not _non_wait_filters:
                        _entry_allowed = True  # All filters off → allow everything except WAIT
                    
                    if not _entry_allowed:
                        _skip_reasons.append(f"{_pair_name}: entry_filter ({_entry_lbl})")
                        continue
                    
                    # v39: Daily loss limit
                    if _daily_blocked:
                        _skip_reasons.append(f"{_pair_name}: {_daily_reason}")
                        continue

                    # BUG-N14 FIX: Cascade SL block
                    if _cascade_blocked:
                        _skip_reasons.append(f"{_pair_name}: {_cascade_reason}")
                        continue
                    
                    # v39: SHORT-only mode check (default=False, включается вручную)
                    if CFG('strategy', 'short_only', False) and p.get('direction') == 'LONG':
                        _skip_reasons.append(f"{_pair_name}: SHORT-only mode")
                        continue
                    
                    # v39: Rally filter — block LONG in DEEP_RALLY
                    if (_rally_st.get('status') == 'DEEP_RALLY' and 
                        p.get('direction') == 'LONG'):
                        _skip_reasons.append(f"{_pair_name}: DEEP_RALLY blocks LONG")
                        continue
                    
                    # v37 Wave 1.2: HR hard block — единая функция is_hr_safe()
                    try:
                        from config_loader import is_hr_safe as _is_hr_safe_app
                        _p_hr = p.get('hedge_ratio', p.get('hr', 1.0))
                        _hr_ok_app, _hr_reason_app = _is_hr_safe_app(_p_hr)
                        if not _hr_ok_app:
                            _skip_reasons.append(f"{_pair_name}: {_hr_reason_app}")
                            continue
                    except ImportError:
                        pass

                    # v40 FIX: Min Z guard — используем адаптивный порог ПАРЫ, не глобальный
                    # Сканер уже определил что Z > threshold и пометил как SIGNAL.
                    # Глобальный entry_z=2.5 блокировал пары с adaptive threshold 1.8
                    # (напр. WLD/FIL Z=2.02 при threshold=1.8 → SIGNAL → но 2.02 < 2.5 → blocked!)
                    _pair_threshold = p.get('threshold', CFG('strategy', 'entry_z', 2.5))
                    _p_z = abs(p.get('zscore', 0))
                    if _p_z < _pair_threshold:
                        _skip_reasons.append(f"{_pair_name}: |Z|={_p_z:.2f} < threshold {_pair_threshold}")
                        continue

                    # v34: Hard BT filter — block ❌BT from auto-entry
                    # BUG-N04 FIX: разделяем micro-BT и walk-forward на два независимых конфиг-ключа.
                    #
                    # Поля пары:
                    #   bt_verdict  = combined_verdict = worst(micro-BT result, WF→WARN если WF FAIL)
                    #   mbt_verdict = micro-BT (uBT) отдельно
                    #   wf_verdict  = walk-forward отдельно
                    #
                    # Конфиг-ключи (config.yaml → strategy):
                    #   bt_filter_mode:  HARD | SOFT | OFF  — управляет блокировкой по micro-BT (mbt_verdict)
                    #   wf_filter_mode:  HARD | SOFT | OFF  — управляет блокировкой по walk-forward (wf_verdict)
                    #
                    # Старый bt_filter_mode проверял combined_verdict (bt_verdict),
                    # который при WF FAIL получал WARN, а не FAIL — вход не блокировался
                    # вопреки ожиданиям. Теперь каждый источник проверяется отдельно.

                    # 1. Micro-BT (uBT) фильтр — по mbt_verdict
                    _bt_mode = CFG('strategy', 'bt_filter_mode', 'HARD')
                    if _bt_mode == 'HARD' and p.get('mbt_verdict') == 'FAIL':
                        _skip_reasons.append(f"{_pair_name}: micro-BT FAIL (bt_filter_mode=HARD)")
                        continue

                    # 2. Walk-Forward фильтр — по wf_verdict (дефолт SOFT = не блокирует, только предупреждает)
                    # "NEVER block entry on WF FAIL alone" — поэтому дефолт SOFT, не HARD
                    _wf_mode = CFG('strategy', 'wf_filter_mode', 'SOFT')
                    if _wf_mode == 'HARD' and p.get('wf_verdict') == 'FAIL':
                        _skip_reasons.append(f"{_pair_name}: Walk-Forward FAIL (wf_filter_mode=HARD)")
                        continue

                    # 3. Пункт 12a FIX: uBT SKIP блокирует авто-вход
                    # mbt_verdict='SKIP' означает micro-backtest не нашёл достаточно сделок
                    # для оценки — пара статистически ненадёжна для автоматического входа.
                    # HARD = блокировать SKIP, INFO = только метка в UI
                    _ubt_mode = CFG('strategy', 'ubt_filter_mode', 'HARD')
                    if _ubt_mode == 'HARD' and p.get('mbt_verdict') == 'SKIP':
                        _skip_reasons.append(f"{_pair_name}: uBT SKIP (ubt_filter_mode=HARD)")
                        continue
                    
                    # v39: Pair memory blocking check
                    try:
                        from config_loader import pair_memory_is_blocked
                        _mem_blocked, _mem_reason = pair_memory_is_blocked(_pair_name)
                        if not _mem_blocked:
                            _pair_rev = f"{p['coin2']}/{p['coin1']}"
                            _mem_blocked, _mem_reason = pair_memory_is_blocked(_pair_rev)
                        if _mem_blocked:
                            # v39: Allow override via config
                            if not CFG('strategy', 'ignore_pair_memory', False):
                                _skip_reasons.append(f"{_pair_name}: {_mem_reason}")
                                continue
                    except ImportError:
                        pass
                    
                    # v40 FIX: Inline cooldown check (importing monitor crashes auto-monitor)
                    try:
                        _cd_file = "pair_cooldowns.json"
                        _cd_blocked = False
                        if os.path.exists(_cd_file):
                            with open(_cd_file, 'r', encoding='utf-8') as _cdf:
                                _cd_all = _json.load(_cdf)
                            for _check_pn in [_pair_name, f"{p['coin2']}/{p['coin1']}"]:
                                _cd_entry = _cd_all.get(_check_pn, {})
                                if _cd_entry.get('last_loss_time'):
                                    from datetime import datetime as _dt
                                    _loss_dt = _dt.fromisoformat(_cd_entry['last_loss_time'])
                                    _hrs_since = (now_msk() - _loss_dt).total_seconds() / 3600
                                    _is_sl = _cd_entry.get('sl_exit', False)
                                    _cons_sl = _cd_entry.get('consecutive_sl', 0)
                                    _sess_pnl = _cd_entry.get('session_pnl', 0)
                                    if _cons_sl >= 2:
                                        _cd_h = 12.0
                                    elif _is_sl:
                                        _cd_h = 12.0
                                    elif _sess_pnl < -0.5:
                                        _cd_h = 4.0
                                    else:
                                        _cd_h = 0
                                    if _cd_h > 0 and _hrs_since < _cd_h:
                                        _cd_blocked = True
                                        _skip_reasons.append(f"{_pair_name}: cooldown {_cd_h:.0f}ч ({_cd_h - _hrs_since:.1f}ч осталось)")
                                        break
                        if _cd_blocked:
                            continue
                    except Exception:
                        pass
                    
                    # v34: Max coin positions check
                    _max_coin_pos = CFG('monitor', 'max_coin_positions', 2)
                    _skip_coin = False
                    for _chk_coin in [p['coin1'], p['coin2']]:
                        _coin_cnt = sum(1 for _op in _existing_with_pending
                                      if _op.get('status') == 'OPEN'
                                      and _chk_coin in (_op.get('coin1',''), _op.get('coin2','')))
                        if _coin_cnt >= _max_coin_pos:
                            _skip_coin = True
                            _skip_reasons.append(f"{_pair_name}: {_chk_coin} в {_coin_cnt} позициях")
                            break
                    if _skip_coin:
                        continue
                    
                    # Already open check (both directions)
                    if _pair_name in _open_pairs or _pair_name in _open_pairs_rev:
                        continue
                    _pair_rev = f"{p['coin2']}/{p['coin1']}"
                    if _pair_rev in _open_pairs or _pair_rev in _open_pairs_rev:
                        continue
                    
                    # v39: Coin conflict check — prevent hedging same coin
                    _coin_conflict = False
                    for _op in _existing_with_pending:
                        if _op.get('status') != 'OPEN':
                            continue
                        _overlap = {p['coin1'], p['coin2']} & {_op.get('coin1',''), _op.get('coin2','')}
                        if _overlap and {_op.get('coin1',''), _op.get('coin2','')} != {p['coin1'], p['coin2']}:
                            # Different pair with shared coin — check direction conflict
                            for _sc in _overlap:
                                _ex_long = _op['coin1'] if _op.get('direction') == 'LONG' else _op.get('coin2','')
                                _new_long = p['coin1'] if p.get('direction') == 'LONG' else p['coin2']
                                if (_sc == _ex_long) != (_sc == _new_long):
                                    _coin_conflict = True
                                    _skip_reasons.append(f"{_pair_name}: coin conflict ({_sc})")
                                    break
                        if _coin_conflict:
                            break
                    if _coin_conflict:
                        continue
                    
                    # Get prices
                    _p1 = p.get('price1_last', 0)
                    _p2 = p.get('price2_last', 0)
                    if _p1 <= 0 or _p2 <= 0:
                        continue
                    
                    # ML score for notes
                    try:
                        from config_loader import ml_score as _ml_fn
                        _ml_r = _ml_fn(p)
                        _ml_str = f"ML:{_ml_r['grade']}{_ml_r['score']:.0f}"
                    except:
                        _ml_str = ""
                    
                    _stop_offset = CFG('strategy', 'stop_z_offset', 2.0)
                    _min_stop = CFG('strategy', 'min_stop_z', 4.0)
                    _ez = p['zscore']
                    _adaptive_stop = max(abs(_ez) + _stop_offset, _min_stop)
                    
                    _new_pos = {
                        'id': len(_existing) + 1,
                        'coin1': p['coin1'], 'coin2': p['coin2'],
                        'direction': p.get('direction'),
                        'entry_z': round(_ez, 4),
                        'entry_hr': round(p['hedge_ratio'], 6),
                        'entry_price1': round(_p1, 6),
                        'entry_price2': round(_p2, 6),
                        'entry_time': now_msk().isoformat(),
                        'timeframe': p.get('signal_tf_used', timeframe),
                        'status': 'OPEN',
                        'notes': (f"AUTO | {p.get('signal','')} | "
                                 f"{p.get('_entry_label','')} | "
                                 f"Q={p.get('quality_score',0)} "
                                 f"H={p.get('hurst',0):.3f} "
                                 f"uBT={p.get('mbt_quick',0):.0f}% "
                                 f"{_ml_str}"),                            'exit_z_target': CFG('monitor', 'exit_z_target', 0.5),
                        'stop_z': _adaptive_stop,
                        'max_hold_hours': CFG('strategy', 'max_hold_hours', 8),
                        'pnl_stop_pct': CFG('monitor', 'pnl_stop_pct', -5.0),
                        # v39: Store per-pair TP/SL from config
                        'pair_tp_pct': CFG('monitor', 'auto_tp_pct', 2.0),
                        'pair_sl_pct': CFG('monitor', 'auto_sl_pct', -2.5),
                        # v30: Auto-open metadata
                        'auto_opened': True,
                        'signal_type': p.get('signal', ''),
                        'entry_label': p.get('_entry_label', ''),
                        'ml_grade': _ml_str,
                        # v34: BT metrics from scanner
                        'bt_verdict': p.get('bt_verdict', ''),
                        'bt_pnl': p.get('bt_pnl', 0),
                        'mu_bt_wr': p.get('mbt_wr', 0),
                        'v_quality': p.get('z_entry_quality', ''),
                        '_z_trail_activated': False,
                        '_z_trail_peak': 0.0,
                    }
                    # v39 FIX: Вместо прямой записи в positions.json
                    # пишем pending JSON — монитор его подхватит,
                    # откроет позицию И отправит ордер на Bybit Demo.
                    # Это единственный путь где вызывается _bybit_open().
                    try:
                        _rec_size = float(CFG('bybit', 'default_size_usdt', 100))
                        _pending_data = {
                            'coin1':        p['coin1'],
                            'coin2':        p['coin2'],
                            'direction':    p.get('direction'),
                            'entry_z':      round(_ez, 4),
                            'entry_hr':     round(p['hedge_ratio'], 6),
                            'entry_price1': round(_p1, 6),
                            'entry_price2': round(_p2, 6),
                            'entry_intercept': p.get('intercept', 0.0),
                            # v41.2 FIX D2-10: монитор должен мониторить позицию
                            # на том же ТФ, на котором был сформирован сигнал входа.
                            # Раньше здесь было timeframe (UI=4h), но при dual_tf_mode
                            # сигнал приходит с signal_tf (1h) — пороги выхода не совпадали.
                            'timeframe':    p.get('signal_tf_used', timeframe),
                            # D2: сохраняем из какого ТФ взят сигнал и квалификация
                            'signal_tf':    p.get('signal_tf_used', timeframe),
                            'qualify_tf':   p.get('qualify_tf', timeframe),
                            'dual_tf_mode': p.get('dual_tf_mode', False),
                            # D3: пометка что позиция открыта после D3 signal scan
                            'd3_signal_scan': bool(p.get('_d3_signal_refresh')),
                            'risk_size_usdt': _rec_size,
                            'recommended_size': _rec_size,
                            'z_window':     p.get('z_window', 30),
                            # v38.4 FIX: entry_label всегда встроен в notes
                            # чтобы monitor/add_position мог распознать уровень входа
                            'notes':        _new_pos['notes'],
                            'auto_opened':  True,
                            'signal_type':  p.get('signal', ''),
                            'entry_label':  p.get('_entry_label', ''),
                            'quality_score': p.get('quality_score', 0),
                            'hurst':        p.get('hurst', 0),
                            'bt_verdict':   p.get('bt_verdict', ''),
                        }
                        import tempfile as _tempfile
                        os.makedirs("monitor_import", exist_ok=True)
                        _pnd_path = f"monitor_import/pending_{p['coin1']}_{p['coin2']}.json"

                        # BUG-N03 FIX: не перезаписываем pending-файл, если он свежее 5 минут.
                        # Это сохраняет счётчик _attempts монитора и защищает от
                        # бесконечного сброса при медленной обработке.
                        _pnd_skip = False
                        if os.path.exists(_pnd_path):
                            _pnd_age = time.time() - os.path.getmtime(_pnd_path)
                            if _pnd_age < 300:  # 5 минут
                                _pnd_skip = True

                        if not _pnd_skip:
                            # BUG-N02 FIX: атомарная запись через tempfile + os.replace().
                            # При краше в середине записи монитор не получит повреждённый JSON —
                            # .tmp либо цел, либо отсутствует; оригинал не тронут.
                            _pnd_dir = os.path.dirname(os.path.abspath(_pnd_path))
                            with _tempfile.NamedTemporaryFile(
                                'w', dir=_pnd_dir, suffix='.tmp',
                                delete=False, encoding='utf-8'
                            ) as _pf_tmp:
                                _json.dump(_pending_data, _pf_tmp, indent=2,
                                           ensure_ascii=False, default=str)
                                _pf_tmp_name = _pf_tmp.name
                            os.replace(_pf_tmp_name, _pnd_path)

                        _open_pairs.add(_pair_name)
                        _auto_count += 1
                    except Exception as _pe:
                        st.toast(f"⚠️ pending write: {_pe}", icon="⚠️")
                
                if _auto_count > 0:
                    st.toast(
                        f"🤖 Auto-monitor: {_auto_count} пар → pending (откроются в мониторе)",
                        icon="📍"
                    )
                
                # v39: Show skip reasons for debugging
                if _skip_reasons:
                    with st.expander(f"🔍 Auto-monitor: {len(_skip_reasons)} пар пропущено", expanded=False):
                        for _sr in _skip_reasons[:20]:
                            st.caption(f"⏭️ {_sr}")
                    # v41 Wave 2.14: Log blocks
                    try:
                        _dir_map = {}
                        for _p in st.session_state.get('pairs_data', []):
                            _pn = f"{_p.get('coin1','')}/{_p.get('coin2','')}"
                            _dir_map[_pn] = _p.get('direction', '')
                        log_blocks_batch(_skip_reasons, source="auto_monitor", direction_map=_dir_map)
                        _bl_auto_cleanup()  # H-04 FIX: periodic cleanup of old block_log entries
                    except Exception:
                        pass
            except Exception as _ae:
                st.toast(f"⚠️ Auto-monitor: {_ae}", icon="⚠️")
        
    except Exception as e:
        import traceback
        _tb = traceback.format_exc()
        st.error(f"❌ Ошибка: {e}")
        with st.expander("🔍 Детали ошибки", expanded=False):
            st.code(_tb)
        st.info("💡 Попробуйте: уменьшить количество монет, изменить таймфрейм или выбрать другую биржу")
        st.session_state.running = False

# Отображение результатов
if st.session_state.pairs_data is not None:
    pairs = st.session_state.pairs_data
    
    # Фильтрация v8.0
    if 'max_hurst' in st.session_state and 'min_theta' in st.session_state:
        filtered_pairs = []
        for p in pairs:
            if p.get('hurst', 0.5) > st.session_state.max_hurst:
                continue
            if p.get('theta', 0) < st.session_state.min_theta:
                continue
            if st.session_state.get('min_quality', 0) > 0 and p.get('quality_score', 0) < st.session_state.min_quality:
                continue
            if st.session_state.get('signal_filter') and p.get('signal', 'NEUTRAL') not in st.session_state.signal_filter:
                continue
            if st.session_state.get('fdr_only', False) and not p.get('fdr_passed', False):
                continue
            if st.session_state.get('stable_only', False) and not p.get('is_stable', False):
                continue
            # v6.0: Entry readiness filter (v10.3: startswith match for ⚠️ labels)
            entry_label = p.get('_entry_label', '⚪ ЖДАТЬ')
            ef = st.session_state.get('entry_filter', [])
            if ef and not any(entry_label.startswith(f) for f in ef):
                continue
            filtered_pairs.append(p)
        
        if len(filtered_pairs) < len(pairs):
            st.info(f"🔬 Фильтры: {len(pairs)} → {len(filtered_pairs)} пар")
        
        pairs = filtered_pairs
    
    if len(pairs) == 0:
        st.warning("⚠️ Коинтегрированных пар не найдено с текущими параметрами. Попробуйте ослабить фильтры.")
    
    # ═══════ v10.3: FIXED DISPLAY — all sections always visible ═══════
    scan_time = st.session_state.get('last_update', now_msk())
    
    # v31.0: Rally Filter — check BTC market regime
    rally_state = {'status': 'NORMAL', 'btc_z': 0}
    try:
        # v40 FIX: Используем ту же биржу что и сканер, не hardcoded binance
        # ccxt.binance() зависал на 10-30с при гео-блокировке
        _tmp_ex = getattr(ccxt, exchange)({'enableRateLimit': True})
        rally_state = check_rally_filter(_tmp_ex, timeframe)
        # v31: Send Telegram alert on status change
        if rally_state.get('status_changed') and st.session_state.get('tg_enabled'):
            _tg_tok = st.session_state.get('tg_token', '')
            _tg_cid = st.session_state.get('tg_chat_id', '')
            send_rally_alert(rally_state, _tg_tok, _tg_cid)
    except Exception:
        pass
    
    if rally_state.get('status') in ('RALLY', 'DEEP_RALLY', 'COOLDOWN'):
        _rl_z = rally_state.get('btc_z', 0)
        _rl_warn = CFG('rally_filter', 'warning_z', 2.0)
        _rl_block = CFG('rally_filter', 'block_z', 2.5)
        _rl_exit = CFG('rally_filter', 'exit_z', 0.0)
        _rl_cd = CFG('rally_filter', 'cooldown_bars', 2)
        if rally_state['status'] == 'DEEP_RALLY':
            st.error(f"🚨 **DEEP RALLY FILTER** | BTC Z={_rl_z:+.2f} ≥ {_rl_block} | "
                     f"Все новые LONG-сигналы **ЗАБЛОКИРОВАНЫ**. Только SHORT разрешены.")
        elif rally_state['status'] == 'COOLDOWN':
            _cd_bars = rally_state.get('cooldown_bars', 0)
            st.warning(f"⏳ **RALLY COOLDOWN** | BTC Z={_rl_z:+.2f} | "
                       f"Ожидание {_rl_cd - _cd_bars} бар(ов) перед разблокировкой LONG.")
        else:
            # X-004 FIX: пояснение гистерезиса — RALLY снимается только при Z < exit_z
            _hyst_note = f" (гистерезис: снятие при Z < {_rl_exit})" if _rl_z < _rl_warn else ""
            st.warning(f"⚠️ **RALLY FILTER** | BTC Z={_rl_z:+.2f}{_hyst_note} | "
                       f"LONG-сигналы под вопросом. Будьте осторожны.")
    
    # Separate by entry level
    entry_pairs = [p for p in pairs if p.get('_entry_level') == 'ENTRY']
    cond_pairs = [p for p in pairs if p.get('_entry_level') == 'CONDITIONAL']
    wait_pairs = [p for p in pairs if p.get('_entry_level') == 'WAIT']
    
    # ═══ 0. SUMMARY METRICS (always visible) ═══
    # v20.1: Auto-download buttons (prominent at top)
    last_csv = st.session_state.get('_last_scan_csv', '')
    last_details = st.session_state.get('_last_detail_csvs', [])
    if last_csv or last_details:
        with st.expander("📥 **Авто-экспорт** (скачать результаты скана)", expanded=True):
            dl1, dl2, dl3 = st.columns(3)
            if last_csv:
                try:
                    with open(last_csv, 'r', encoding='utf-8') as f:
                        dl1.download_button("📥 Полная таблица CSV", f.read(),
                                           last_csv.split('/')[-1], "text/csv",
                                           key="auto_dl_scan")
                except Exception:
                    pass
            for i, dp in enumerate(last_details[:3]):
                try:
                    with open(dp, 'r', encoding='utf-8') as f:
                        col = dl2 if i == 0 else dl3
                        col.download_button(f"📥 Detail: {dp.split('detail_')[1][:15]}",
                                           f.read(), dp.split('/')[-1], "text/csv",
                                           key=f"auto_dl_detail_{i}")
                except Exception:
                    pass
    
    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("🟢 ВХОД", len(entry_pairs))
    mc2.metric("🟡 УСЛОВНО", len(cond_pairs))
    mc3.metric("⚪ ЖДАТЬ", len(wait_pairs))
    mc4.metric("📊 Всего пар", len(pairs))

    # D2/D3 режим — индикатор в шапке результатов
    _mode_parts = []
    _any_dual = any(p.get('dual_tf_mode') for p in pairs[:3]) if pairs else False
    _any_d3   = any(p.get('_d3_signal_refresh') for p in pairs[:3]) if pairs else False
    if _any_dual or st.session_state.get('dual_tf_enabled', False):
        _qtf = st.session_state.get('qualify_tf_select', '4h')
        _stf = st.session_state.get('signal_tf_select', '1h')
        _mode_parts.append(f"🔀 **Dual TF**: qualify={_qtf} / signal={_stf}")
    if _any_d3 or st.session_state.get('d3_enabled', False):
        _cache_data_ui, _cache_age_ui = qual_cache_load()
        if _cache_data_ui:
            _mode_parts.append(f"⚡ **D3 Signal scan** (кэш {_cache_age_ui:.0f} мин)")
            # D3-8 FIX: предупреждение если BT/WF данные из кэша старше 30 мин
            if _cache_age_ui > 30:
                _mode_parts.append(f"⚠️ **BT/WF из кэша {_cache_age_ui:.0f} мин** — "
                                   f"запустите полный скан для обновления")
        else:
            _mode_parts.append("🔄 **D3 Full qualify scan**")
    if _mode_parts:
        st.info(" | ".join(_mode_parts))
    
    # ═══ 1. ACTION PANEL — READY TO TRADE ═══
    try:
        if entry_pairs:
            st.markdown("## 🟢 ГОТОВЫ К ВХОДУ")
        for p in entry_pairs:
            d = p.get('direction', 'NONE')
            c1, c2 = p['coin1'], p['coin2']
            if d == 'LONG':
                c1_act, c2_act = '🟢 КУПИТЬ', '🔴 ПРОДАТЬ'
            elif d == 'SHORT':
                c1_act, c2_act = '🔴 ПРОДАТЬ', '🟢 КУПИТЬ'
            else:
                c1_act, c2_act = '⚪', '⚪'
            
            # MTF badge
            mtf_conf = p.get('mtf_confirmed')
            if mtf_conf is True:
                mtf_str = p.get('mtf_strength', 'OK')
                mtf_badge = f"✅ MTF {p.get('mtf_tf', '1h')} ({mtf_str})"
            elif mtf_conf is False:
                mtf_badge = f"❌ MTF {p.get('mtf_tf', '1h')} не подтв."
            else:
                mtf_badge = ""
            
            with st.container():
                ac1, ac2, ac3, ac4, ac5 = st.columns([3, 2, 2, 2, 2])
                dir_arrow = '🟢↑' if d == 'LONG' else '🔴↓'
                # D2: показываем TF сигнала если dual mode
                _d2_label = f"🔀{p.get('signal_tf_used','')}" if p.get('dual_tf_mode') else ""
                ac1.markdown(f"### **{p['pair']}** {dir_arrow} {_d2_label}")
                # D2: в дельте показываем qualify Z для сравнения
                _z_qualify = p.get('zscore_qualify')
                _z_delta = (f"Qualify Z={_z_qualify:+.2f}" if _z_qualify is not None and p.get('dual_tf_mode')
                            else f"Порог: {p.get('threshold', 2.0)}")
                ac2.metric("Z-Score", f"{p['zscore']:+.2f}", _z_delta)
                ac3.metric("Quality", f"{p.get('quality_score', 0)}/100")
                ac4.metric("Hurst", f"{p.get('hurst', 0.5):.3f}")
                ac5.metric("HR", f"{p['hedge_ratio']:.4f}")
                
                hl_val = p.get('halflife_hours', p.get('halflife_days', 1)*24)
                info_line = f"**{c1}**: {c1_act} | **{c2}**: {c2_act} | **HR:** 1:{p['hedge_ratio']:.4f} | **HL:** {hl_val:.0f}ч | **ρ:** {p.get('correlation', 0):.2f}"
                
                if mtf_badge:
                    info_line += f" | **{mtf_badge}**"
                    if mtf_conf is True:
                        mtf_z = p.get('mtf_z')
                        mtf_vel = p.get('mtf_z_velocity')
                        if mtf_z is not None:
                            info_line += f" (Z={mtf_z:+.2f}, dZ={mtf_vel:+.3f}/bar)"
                
                st.markdown(info_line)
                
                # v31.0: Position sizing recommendation
                _rec_size = recommend_position_size(
                    p.get('quality_score', 50),
                    p.get('confidence', 'MEDIUM'),
                    p.get('_entry_level', 'CONDITIONAL'),
                    p.get('hurst', 0.5),
                    p.get('correlation', 0.5)
                )
                _intercept_val = p.get('intercept', 0.0)
                st.markdown(
                    f"💰 **Рекомендуемый объём:** ${_rec_size:.0f} | "
                    f"📋 **Для монитора:** Intercept = `{_intercept_val:.6f}` | HR = `{p['hedge_ratio']:.4f}`"
                )
                
                # v31.0: Rally filter warning for LONG signals
                if rally_state.get('status') in ('RALLY', 'DEEP_RALLY', 'COOLDOWN') and d == 'LONG':
                    _rl_z = rally_state.get('btc_z', 0)
                    if rally_state['status'] == 'DEEP_RALLY':
                        st.error(f"🚨 RALLY FILTER: BTC Z={_rl_z:+.2f}. LONG-сигнал ЗАБЛОКИРОВАН!")
                    elif rally_state['status'] == 'COOLDOWN':
                        st.warning(f"⏳ RALLY COOLDOWN: BTC Z={_rl_z:+.2f}. LONG ещё заблокирован (ожидание).")
                    else:
                        st.warning(f"⚠️ RALLY FILTER: BTC Z={_rl_z:+.2f}. LONG-сигнал под вопросом.")
                
                # MTF warning
                if mtf_conf is False:
                    st.warning(f"⚠️ {p.get('mtf_tf', '1h')} не подтверждает: {p.get('mtf_reason', '')}. Рассмотрите отложенный вход.")
                
                # Regime warning
                if p.get('regime') == 'TRENDING':
                    st.error(f"🚨 TRENDING (ADX={p.get('regime_adx', 0):.0f}, VR={p.get('regime_vr', 1):.1f}) — спред в тренде!")
                
                # HR magnitude warning
                if p.get('hr_warning'):
                    st.warning(p['hr_warning'])
                
                # Bars warning
                if p.get('bars_warning'):
                    st.warning(p['bars_warning'])
                
                # v12.0: CUSUM structural break warning
                if p.get('cusum_warning'):
                    st.error(p['cusum_warning'])
                
                st.markdown("---")
        if not entry_pairs:
            st.info("⚪ Нет пар готовых к входу (🟢 ВХОД). Дождитесь сигнала или ослабьте фильтры.")
    except Exception as e:
        st.warning(f"⚠️ Ошибка отображения панели входа: {e}")
    
    # ═══ 2. CLUSTER + CONFLICT WARNINGS ═══
    try:
        signal_pairs_list = [p for p in pairs if p.get('signal') in ('SIGNAL', 'READY')]
        if signal_pairs_list:
            from collections import Counter
            coin_count = Counter()
            coin_dirs = {}
            for p in signal_pairs_list:
                c1 = p.get('coin1', p.get('pair', '/').split('/')[0])
                c2 = p.get('coin2', p.get('pair', '/').split('/')[1] if '/' in p.get('pair','') else '')
                d = p.get('direction', 'NONE')
                coin_count[c1] += 1
                coin_count[c2] += 1
                if d == 'LONG':
                    coin_dirs.setdefault(c1, set()).add('LONG')
                    coin_dirs.setdefault(c2, set()).add('SHORT')
                elif d == 'SHORT':
                    coin_dirs.setdefault(c1, set()).add('SHORT')
                    coin_dirs.setdefault(c2, set()).add('LONG')
            
            clusters = [(c, n) for c, n in coin_count.most_common() if n >= 3]
            if clusters:
                st.warning("⚠️ **Кластеры:** " + ", ".join([f"**{c}** ({n} пар)" for c, n in clusters]) + " — НЕ независимые сделки!")
            
            conflicts = [(c, dirs) for c, dirs in coin_dirs.items() if len(dirs) > 1]
            if conflicts:
                st.error("🚨 **Конфликт:** " + ", ".join([f"**{c}** (LONG+SHORT)" for c, _ in conflicts]))
    except Exception as e:
        st.warning(f"⚠️ Ошибка анализа кластеров: {e}")
    
    # ═══ 3. FULL TABLE (always visible) ═══
    try:
        st.subheader(f"📊 Все коинтегрированные пары ({len(pairs)}) | Скан: {scan_time.strftime('%H:%M:%S')}")
    except Exception:
        st.subheader(f"📊 Все коинтегрированные пары ({len(pairs)})")
    st.caption("🟢 ВХОД = все обязательные ОК | 🟡 УСЛОВНО = обяз. ОК но мало желательных | ⚪ ЖДАТЬ = не входить")
    
    # Проверка что есть пары для отображения
    if len(pairs) > 0:
        df_rows = []
        for p in pairs:
            try:
                hl_h = p.get('halflife_hours', p.get('halflife_days', 1) * 24)
                # v28: ML scoring for table
                try:
                    from config_loader import ml_score as _ml_fn
                    _ml_r = _ml_fn(p)
                    _ml_grade_for_table = f"{_ml_r['grade']}{_ml_r['score']:.0f}"
                except Exception:
                    _ml_grade_for_table = '—'
                # D3-8 FIX: вычисляем возраст кэша BT/WF для D3-пар.
                # При D3 signal scan BT/WF берутся из кэша и могут быть устаревшими.
                # Суффикс ⏳Nм показывает пользователю возраст данных.
                _d3_bt_suffix = ''
                if p.get('_d3_signal_refresh'):
                    try:
                        _d3_refresh_dt = datetime.fromisoformat(p['_d3_signal_refresh'])
                        if _d3_refresh_dt.tzinfo is None:
                            _d3_refresh_dt = _d3_refresh_dt.replace(tzinfo=timezone.utc)
                        _d3_age_min = (datetime.now(timezone.utc) - _d3_refresh_dt).total_seconds() / 60
                        if _d3_age_min > 5:
                            _d3_bt_suffix = f' ⏳{_d3_age_min:.0f}м'
                    except Exception:
                        _d3_bt_suffix = ' ⏳?'
                df_rows.append({
                    'Пара': (p.get('pair', '?')
                             + (' 🔀' if p.get('dual_tf_mode') else '')
                             + (' ⚡' if p.get('_d3_signal_refresh') else '')),
                    'Вход': p.get('_entry_label', '⚪ ЖДАТЬ'),
                    'Статус': p.get('signal', '?'),
                    'Dir': p.get('direction', ''),
                    'MTF': ('✅' if p.get('mtf_confirmed') is True 
                            else '❌' if p.get('mtf_confirmed') is False 
                            else '—'),
                    'TF': (f"{p.get('signal_tf_used','?')}/{p.get('qualify_tf','?')}"
                           if p.get('dual_tf_mode') else p.get('signal_tf_used', '—')),
                    'Q': p.get('quality_score', 0),
                    'S': p.get('signal_score', 0),
                    'Conf': p.get('confidence', '?'),
                    'Z': round(p.get('zscore', 0), 2),
                    'Thr': round(p.get('threshold', 2.0), 2),
                    'FDR': ('✅' if p.get('fdr_passed', False) 
                            else ('🟡' if p.get('_fdr_bypass', False) else '❌')),
                    'Hurst': round(p.get('hurst', 0.5), 3),
                    'H↕': ('🟢' if p.get('hurst_mr_strengthening') else 
                           '🔴' if p.get('hurst_mr_weakening') else '—'),
                    'Stab': f"{p.get('stability_passed', 0)}/{p.get('stability_total', 4)}",
                    'HL': f"{hl_h:.1f}ч" if hl_h < 48 else '∞',
                    'HR': round(p.get('hedge_ratio', 0), 4),
                    'ρ': round(p.get('correlation', 0), 2),
                    'Opt': f"{p.get('_opt_count', 0)}/6",
                    'Regime': ('🟢' if p.get('regime') == 'MEAN_REVERT' 
                               else '🔴' if p.get('regime') == 'TRENDING' 
                               else '🟡') + f" {p.get('regime_adx', 0):.0f}",
                    'CUSUM': ('🚫' if p.get('cusum_risk') == 'CRITICAL' else
                              '🔴' if p.get('cusum_risk') == 'HIGH' else
                              '🟡' if p.get('cusum_risk') == 'MEDIUM' else '✅')
                             + f" {p.get('cusum_drift', 0):+.1f}",
                    'Joh': '✅' if p.get('johansen_coint') else '❌',
                    'BT': ('✅' if p.get('bt_verdict') == 'PASS' else
                           '⚠️' if p.get('bt_verdict') == 'WARN' else
                           '❌' if p.get('bt_verdict') == 'FAIL' else '—') +
                          (f" {p.get('bt_pnl',0):+.0f}%" if p.get('bt_trades',0) > 0 else '') +
                          _d3_bt_suffix,  # D3-8 FIX
                    'uBT': ('✅' if p.get('mbt_verdict') == 'PASS' else
                            '⚠️' if p.get('mbt_verdict') == 'WARN' else
                            '❌' if p.get('mbt_verdict') == 'FAIL' else '—') +
                           (f" {p.get('mbt_pnl',0):+.1f}%" if p.get('mbt_trades',0) > 0 else '') +
                           _d3_bt_suffix,  # D3-8 FIX
                    'V↕': (('🟢' if p.get('z_entry_quality') == 'EXCELLENT' else
                            '✅' if p.get('z_entry_quality') == 'GOOD' else
                            '🟡' if p.get('z_entry_quality') == 'FAIR' else
                            '🔴' if p.get('z_entry_quality') == 'POOR' else '—') +
                           f" {p.get('z_velocity', 0):+.2f}"
                           if p.get('z_entry_quality', 'UNKNOWN') != 'UNKNOWN' else '—'),
                    'WF': (f"{p.get('wf_folds_passed',0)}/{p.get('wf_n_folds',0)}"
                           if p.get('wf_n_folds', 0) > 0 else '—') +
                          _d3_bt_suffix,  # D3-8 FIX
                    'Конфл': '🚨' if p.get('coin_conflict') else '',
                    'PCA': (f"{'✅' if p.get('pca_same_cluster') else '⚠️'}"
                            f" {p.get('pca_market_neutral', 0):.0%}"
                            if p.get('pca_market_neutral', 0) > 0 else '—'),
                    # v28: Funding rate net
                    'FR': (f"{p.get('funding_net', 0):+.3f}%"
                           if p.get('funding_net', 0) != 0 else '—'),
                    # v28: ML Score
                    'ML': _ml_grade_for_table,
                    # v31: Rally filter status for this pair
                    'Rally': ('🚨' if rally_state.get('status') == 'DEEP_RALLY' and p.get('direction') == 'LONG'
                              else '⚠️' if rally_state.get('status') in ('RALLY', 'COOLDOWN') and p.get('direction') == 'LONG'
                              else ''),
                    # v31: Position sizing
                    '$': recommend_position_size(
                        p.get('quality_score', 50), p.get('confidence', 'MEDIUM'),
                        p.get('_entry_level', 'CONDITIONAL'), p.get('hurst', 0.5), p.get('correlation', 0.5)),
                })
            except Exception as e:
                # Log error instead of silently dropping row
                df_rows.append({
                    'Пара': p.get('pair', '?'), 'Вход': '⚠️ERR',
                    'Статус': str(e)[:20], 'Dir': '', 'MTF': '', 'TF': '',
                    'Q': 0, 'S': 0, 'Conf': '', 'Z': 0, 'Thr': 0,
                    'FDR': '', 'Hurst': 0, 'H↕': '', 'Stab': '', 'HL': '',
                    'HR': 0, 'ρ': 0, 'Opt': '', 'Regime': '',
                    'CUSUM': '', 'Joh': '', 'BT': '', 'uBT': '', 'V↕': '', 'WF': '', 'Конфл': '', 'PCA': '', 'FR': '', 'ML': '', 'Rally': '', '$': 0,
                })
        df_display = pd.DataFrame(df_rows) if df_rows else pd.DataFrame()
        # v40 FIX: Cast numeric columns to proper types (prevents Arrow serialization crash)
        for _num_col in ['Q', 'S', 'Z', 'Thr', 'Hurst', 'HR', 'ρ', '$']:
            if _num_col in df_display.columns:
                df_display[_num_col] = pd.to_numeric(df_display[_num_col], errors='coerce').fillna(0)
    else:
        df_display = pd.DataFrame(columns=[
            'Пара', 'Вход', 'Статус', 'Dir', 'MTF', 'Q', 'S', 'Conf', 'Z', 'Thr',
            'FDR', 'Hurst', 'Stab', 'HL', 'HR', 'ρ', 'Opt', 'FR', 'ML'
        ])
    
    # Функция для отображения таблицы
    def show_pairs_table(df):
        """Robust table display — no data_editor crash risk"""
        try:
            st.dataframe(
                df,
                hide_index=True,
                width='stretch',
                height=min(400, 35 * (len(df) + 1))
            )
        except Exception as e:
            st.error(f"Ошибка таблицы: {e}")
    
    try:
        show_pairs_table(df_display)
    except Exception as e:
        st.warning(f"⚠️ Ошибка отображения таблицы: {e}")
        import traceback
        st.code(traceback.format_exc())
    
    # Детальный анализ выбранной пары
    if len(pairs) > 0:
        st.markdown("---")
        st.subheader("📈 Детальный анализ пары")
        
        pair_options = [p.get('pair', '?') for p in pairs]
        
        # Ограничиваем индекс
        safe_index = int(st.session_state.selected_pair_index)
        if safe_index >= len(pair_options):
            safe_index = 0
        
        # Selectbox с index из session_state (обновляется по checkbox)
        selected_pair = st.selectbox(
            "Выберите пару для анализа:",
            pair_options,
            index=safe_index,
            key='pair_selector_main'
        )
        
        # Синхронизируем обратно
        try:
            st.session_state.selected_pair_index = int(pair_options.index(selected_pair))
        except ValueError:
            st.session_state.selected_pair_index = 0
        
        selected_data = next((p for p in pairs if p.get('pair') == selected_pair), pairs[0])
    else:
        # Нет пар — не показываем детальный анализ
        st.info("📊 Запустите сканер для получения результатов")
        st.stop()
    
    # ═══════ v6.0: ENTRY READINESS PANEL ═══════
    ea = assess_entry_readiness(selected_data)
    
    if ea['level'] == 'ENTRY':
        st.markdown(f'<div class="entry-ready">🟢 ГОТОВ К ВХОДУ — все обязательные ОК + {ea["opt_count"]}/6 желательных</div>', unsafe_allow_html=True)
    elif ea['level'] == 'CONDITIONAL':
        st.markdown(f'<div class="entry-conditional">🟡 УСЛОВНЫЙ — {ea["opt_count"]}/6 желательных</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="entry-wait">⚪ НЕ ВХОДИТЬ</div>', unsafe_allow_html=True)
    
    # v9.0: Compact key metrics
    state = selected_data.get('signal', 'NEUTRAL')
    direction = selected_data.get('direction', 'NONE')
    conf = selected_data.get('confidence', '?')
    threshold = selected_data.get('threshold', 2.0)
    dir_emoji = {'LONG': '🟢↑', 'SHORT': '🔴↓', 'NONE': ''}.get(direction, '')
    
    # v27: Pair Memory display
    try:
        from config_loader import pair_memory_summary
        _pm = pair_memory_summary(selected_pair)
        if _pm:
            st.info(_pm)
    except Exception:
        pass
    
    # v27: Funding rate display
    _fr1 = selected_data.get('funding_rate_1', 0)
    _fr2 = selected_data.get('funding_rate_2', 0)
    _frn = selected_data.get('funding_net', 0)
    if _fr1 != 0 or _fr2 != 0:
        _fr_color = "🟢" if _frn > 0 else "🔴" if _frn < -0.01 else "⚪"
        st.caption(f"💰 Funding: {selected_data.get('coin1','')}={_fr1:+.4f}% | "
                  f"{selected_data.get('coin2','')}={_fr2:+.4f}% | "
                  f"Net={_fr_color} {_frn:+.4f}%/8h")
    
    km1, km2, km3, km4, km5 = st.columns(5)
    km1.metric("Z-Score", f"{selected_data['zscore']:+.2f}", f"Порог: ±{threshold}")
    km2.metric("Quality", f"{selected_data.get('quality_score', 0)}/100", f"{conf}")
    km3.metric("Hurst", f"{selected_data.get('hurst', 0.5):.3f}", 
               "✅ MR" if selected_data.get('hurst', 0.5) < 0.35 else "⚠️" if selected_data.get('hurst', 0.5) < 0.45 else "❌ No MR")
    km4.metric("Half-life", f"{selected_data.get('halflife_hours', selected_data['halflife_days']*24):.0f}ч")
    km5.metric("Корреляция", f"{selected_data.get('correlation', 0):.2f}")
    
    # v9.0: Entry/Exit info in expander
    with st.expander("📋 Критерии входа", expanded=ea['level'] == 'ENTRY'):
        chk1, chk2 = st.columns(2)
        with chk1:
            st.markdown("**🟢 Обязательные (все = ✅):**")
            for name, met, val in ea['mandatory']:
                st.markdown(f"  {'✅' if met else '❌'} **{name}** → `{val}`")
        with chk2:
            st.markdown("**🔵 Желательные (больше = лучше):**")
            for name, met, val in ea['optional']:
                st.markdown(f"  {'✅' if met else '⬜'} {name} → `{val}`")
            if ea['fdr_bypass']:
                st.info("🟡 **FDR bypass активен**")
    
    # ⚠️ Предупреждения (keep visible)
    warnings_list = []
    if selected_data.get('hurst_is_fallback', False):
        warnings_list.append("⚠️ Hurst = 0.5 (DFA fallback — данных недостаточно)")
    if abs(selected_data.get('zscore', 0)) > 5:
        warnings_list.append(f"⚠️ |Z| > 5 — аномалия")
    if not selected_data.get('fdr_passed', False) and not ea.get('fdr_bypass', False):
        warnings_list.append("⚠️ FDR не пройден")
    # v11.2: Regime warning
    if selected_data.get('regime') == 'TRENDING':
        warnings_list.append(f"🚨 TRENDING (ADX={selected_data.get('regime_adx', 0):.0f})")
    # v11.2: HR warning
    if selected_data.get('hr_warning'):
        warnings_list.append(selected_data['hr_warning'])
    # v11.2: Bars warning
    if selected_data.get('bars_warning'):
        warnings_list.append(selected_data['bars_warning'])
    
    if selected_data.get('cusum_warning'):
        warnings_list.append(selected_data['cusum_warning'])
    if warnings_list:
        st.warning(" | ".join(warnings_list))
    
    # ═══════ v10.0: MTF CONFIRMATION PANEL ═══════
    mtf_conf = selected_data.get('mtf_confirmed')
    if mtf_conf is not None:
        st.markdown("---")
        mtf_tf = selected_data.get('mtf_tf', '1h')
        mtf_strength = selected_data.get('mtf_strength', '')
        mtf_z = selected_data.get('mtf_z')
        mtf_vel = selected_data.get('mtf_z_velocity')
        mtf_passed = selected_data.get('mtf_passed', 0)
        mtf_total = selected_data.get('mtf_total', 0)
        
        if mtf_conf:
            badge_color = 'entry-ready' if mtf_strength in ('STRONG', 'OK') else 'entry-conditional'
            st.markdown(f'<div class="{badge_color}">✅ MTF ПОДТВЕРЖДЕНО ({mtf_tf}) — {mtf_strength} ({mtf_passed}/{mtf_total})</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="entry-wait">❌ MTF НЕ ПОДТВЕРЖДЕНО ({mtf_tf}) — {mtf_passed}/{mtf_total} проверок</div>', unsafe_allow_html=True)
        
        mtf_checks = selected_data.get('mtf_checks', [])
        if mtf_checks:
            mc1, mc2 = st.columns(2)
            with mc1:
                if mtf_z is not None:
                    st.metric(f"Z-Score ({mtf_tf})", f"{mtf_z:+.2f}")
            with mc2:
                if mtf_vel is not None:
                    vel_dir = '↑к0' if mtf_vel > 0 else '↓к0' if mtf_vel < 0 else '→'
                    st.metric(f"Z-Velocity ({mtf_tf})", f"{mtf_vel:+.3f}/bar", vel_dir)
            
            with st.expander(f"🔄 MTF Проверки ({mtf_tf})", expanded=False):
                for name, passed, detail in mtf_checks:
                    st.markdown(f"{'✅' if passed else '❌'} **{name}** — {detail}")
                
                if not mtf_conf:
                    st.warning(f"💡 Рассмотрите отложенный вход. Дождитесь когда {mtf_tf} Z начнёт двигаться к нулю.")
    
    # ═══════ MEAN REVERSION ANALYSIS ═══════
    if 'hurst' in selected_data and 'theta' in selected_data:
        st.markdown("---")
        st.subheader("🔬 Детальная статистика")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            hurst = selected_data['hurst']
            if selected_data.get('hurst_is_fallback', False):
                h_st = "⚠️ Fallback"
            elif hurst < 0.35:
                h_st = "🟢 Strong MR"
            elif hurst < 0.48:
                h_st = "🟢 Reverting"
            elif hurst < 0.52:
                h_st = "⚪ Random"
            else:
                h_st = "🔴 Trending"
            
            # v16: Show both raw and EMA Hurst
            h_ema = selected_data.get('hurst_ema', hurst)
            h_std = selected_data.get('hurst_std', 0)
            h_stable = selected_data.get('hurst_stable', True)
            
            if abs(h_ema - hurst) > 0.05:
                st.metric("Hurst (DFA)", f"{hurst:.3f}", 
                          f"EMA={h_ema:.3f} {'✅' if h_stable else '⚠️неустойч'}")
            else:
                st.metric("Hurst (DFA)", f"{hurst:.3f}", h_st)
        
        with col2:
            theta = selected_data['theta']
            t_st = "✅ Быстрый" if theta > 1.0 else "⚠️ Средний" if theta > 0.5 else "❌ Медленный"
            st.metric("θ (Скорость)", f"{theta:.3f}", t_st)
        
        with col3:
            hr = selected_data['hedge_ratio']
            hr_unc = selected_data.get('hr_uncertainty', 0)
            if hr_unc > 0.5:
                hr_st = f"⚠️ ±{hr_unc:.0%}"
            elif hr_unc > 0.2:
                hr_st = f"🟡 ±{hr_unc:.0%}"
            elif hr_unc > 0:
                hr_st = f"✅ ±{hr_unc:.0%}"
            elif 0.2 <= abs(hr) <= 5.0:
                hr_st = "✅ OK"
            else:
                hr_st = "⚠️ Экстрем."
            st.metric("Hedge Ratio", f"{hr:.4f}", hr_st)
        
        with col4:
            if theta > 0:
                exit_time = estimate_exit_time(
                    current_z=selected_data['zscore'], theta=theta, target_z=0.5
                )
                st.metric("Прогноз", f"{exit_time * 24:.1f}ч", "до Z=0.5")
            else:
                st.metric("Прогноз", "∞", "Нет возврата")
        
        # Проверки
        checks_col1, checks_col2 = st.columns(2)
        with checks_col1:
            fdr_s = "✅" if selected_data.get('fdr_passed', False) else "❌"
            adf_s = "✅" if selected_data.get('adf_passed', False) else "❌"
            stab = f"{selected_data.get('stability_passed', 0)}/{selected_data.get('stability_total', 4)}"
            stab_e = "✅" if selected_data.get('is_stable', False) else "⚠️"
            kf_s = "🔷 Kalman" if selected_data.get('use_kalman', False) else "○ OLS"
            hr_unc = selected_data.get('hr_std', 0)
            st.info(f"""
            **Проверки:**
            {fdr_s} FDR (p-adj={selected_data.get('pvalue_adj', 0):.4f})
            {adf_s} ADF (p={selected_data.get('adf_pvalue', 1.0):.4f})
            {stab_e} Стабильность: {stab} окон
            **HR метод:** {kf_s} (±{hr_unc:.4f})
            """)
        
        with checks_col2:
            if theta > 2.0:
                t_msg = "🟢 Очень быстрый (~{:.1f}ч)".format(-np.log(0.5)/theta * 24)
            elif theta > 1.0:
                t_msg = "🟢 Быстрый (~{:.1f}ч)".format(-np.log(0.5)/theta * 24)
            elif theta > 0.5:
                t_msg = "🟡 Средний (~{:.1f}ч)".format(-np.log(0.5)/theta * 24)
            else:
                t_msg = "🔴 Медленный"
            st.info(f"""
            **OU Process:** {t_msg}
            
            **Adaptive порог:** |Z| ≥ {threshold}
            ({conf} confidence → {'сниженный' if threshold < 2.0 else 'стандартный'} порог)
            """)
        
        # v10: дополнительные метрики
        v10_col1, v10_col2, v10_col3 = st.columns(3)
        with v10_col1:
            zw = selected_data.get('z_window', 30)
            st.metric("Z-окно", f"{zw} баров", "адаптивное (HL×2.5)")
        with v10_col2:
            cd = selected_data.get('crossing_density', 0)
            cd_emoji = "🟢" if cd >= 0.05 else "🟡" if cd >= 0.03 else "🔴"
            st.metric("Crossing Density", f"{cd:.3f} {cd_emoji}",
                       "активный" if cd >= 0.03 else "застрял")
        with v10_col3:
            corr = selected_data.get('correlation', 0)
            corr_emoji = "🟢" if corr >= 0.7 else "🟡" if corr >= 0.4 else "⚪"
            st.metric("Корреляция (ρ)", f"{corr:.3f} {corr_emoji}")
        
        # v11.2: Regime detection row
        regime = selected_data.get('regime', 'UNKNOWN')
        reg_adx = selected_data.get('regime_adx', 0)
        reg_vr = selected_data.get('regime_vr', 1.0)
        
        if regime != 'UNKNOWN':
            reg_col1, reg_col2, reg_col3 = st.columns(3)
            with reg_col1:
                reg_emoji = '🟢' if regime == 'MEAN_REVERT' else '🟡' if regime == 'NEUTRAL' else '🔴'
                st.metric("Режим рынка", f"{reg_emoji} {regime}")
            with reg_col2:
                adx_emoji = '🟢' if reg_adx < 20 else '🟡' if reg_adx < 30 else '🔴'
                st.metric("Spread ADX", f"{reg_adx:.0f} {adx_emoji}", 
                          "<25 = MR" if reg_adx < 25 else ">25 = TREND")
            with reg_col3:
                vr_emoji = '🟢' if reg_vr < 1.3 else '🟡' if reg_vr < 2.0 else '🔴'
                st.metric("Variance Ratio", f"{reg_vr:.2f} {vr_emoji}",
                          "<1.3 = MR" if reg_vr < 1.3 else ">1.5 = TREND")
        
        # v13.0: CUSUM structural break + Risk level + Position advice
        cusum_score = selected_data.get('cusum_score', 0)
        cusum_drift = selected_data.get('cusum_drift', 0)
        cusum_break = selected_data.get('cusum_break', False)
        cusum_risk = selected_data.get('cusum_risk', 'LOW')
        cusum_advice = selected_data.get('cusum_advice', '')
        
        if cusum_score > 0:
            st.markdown("#### 🔬 CUSUM Structural Break Test")
            cu_col1, cu_col2, cu_col3, cu_col4 = st.columns(4)
            with cu_col1:
                cu_emoji = '🚨' if cusum_break else '⚠️' if cusum_score > 2.0 else '✅'
                st.metric("CUSUM Test", f"{cu_emoji} {'BREAK' if cusum_break else 'OK'}")
            with cu_col2:
                st.metric("CUSUM Score", f"{cusum_score:.1f}σ", 
                          "< 2σ = ОК" if cusum_score < 2 else "> 2σ = Риск")
            with cu_col3:
                st.metric("Tail Drift", f"{cusum_drift:+.2f}σ",
                          "Стабильно" if abs(cusum_drift) < 1.0 else "Дрейф!")
            with cu_col4:
                risk_colors = {'LOW': '🟢', 'MEDIUM': '🟡', 'HIGH': '🔴', 'CRITICAL': '🚫'}
                st.metric("Риск", f"{risk_colors.get(cusum_risk, '?')} {cusum_risk}")
            
            # Position advice box
            if cusum_risk != 'LOW':
                advice_colors = {'MEDIUM': 'warning', 'HIGH': 'error', 'CRITICAL': 'error'}
                getattr(st, advice_colors.get(cusum_risk, 'info'))(
                    f"**Рекомендация по позиции:** {cusum_advice}\n\n"
                    f"{'💡 **Что значит CUSUM «Возможный сдвиг»?** Историческая связь пары ОСЛАБЛА. Спред может продолжить тренд вместо возврата к среднему. Входите ЧАСТИЧНО и зарезервируйте капитал для усреднения если Z вернётся.' if cusum_risk == 'MEDIUM' else ''}"
                    f"{'⛔ **CUSUM HIGH/CRITICAL:** Коинтеграция вероятно РАЗРУШЕНА. Бэктест таких пар обычно убыточный (FIL/CRV=-18%). НЕ ВХОДИТЕ даже при идеальных метриках.' if cusum_risk in ('HIGH','CRITICAL') else ''}"
                )
        
        # v13.0: Johansen test results
        joh_coint = selected_data.get('johansen_coint', False)
        joh_trace = selected_data.get('johansen_trace', 0)
        joh_cv = selected_data.get('johansen_cv', 0)
        if joh_trace > 0:
            st.markdown("#### 🔬 Johansen Test (симметричный)")
            jo_col1, jo_col2, jo_col3 = st.columns(3)
            with jo_col1:
                jo_emoji = '✅' if joh_coint else '❌'
                st.metric("Johansen", f"{jo_emoji} {'COINT' if joh_coint else 'НЕТ'}")
            with jo_col2:
                st.metric("Trace Stat", f"{joh_trace:.1f}", f"CV₅%={joh_cv:.1f}")
            with jo_col3:
                joh_hr = selected_data.get('johansen_hr', 0)
                eg_hr = selected_data.get('hedge_ratio', 0)
                hr_diff = abs(joh_hr - eg_hr) / max(abs(eg_hr), 0.01) * 100
                st.metric("Johansen HR", f"{joh_hr:.4f}", 
                          f"vs EG: {hr_diff:.0f}% разница")
            if not joh_coint:
                st.warning("⚠️ Johansen НЕ подтверждает коинтеграцию — Engle-Granger может быть ложным")
        
        # v16: Hurst EMA stability panel
        h_series = selected_data.get('hurst_series', [])
        if len(h_series) > 1:
            st.markdown("#### 📊 Hurst EMA (сглаженный)")
            he_col1, he_col2, he_col3, he_col4 = st.columns(4)
            with he_col1:
                st.metric("Raw Hurst", f"{selected_data.get('hurst', 0):.4f}")
            with he_col2:
                st.metric("EMA Hurst", f"{selected_data.get('hurst_ema', 0):.4f}")
            with he_col3:
                h_std = selected_data.get('hurst_std', 0)
                st.metric("σ (разброс)", f"{h_std:.4f}", 
                          "✅ стабильно" if h_std < 0.08 else "⚠️ нестабильно")
            with he_col4:
                st.metric("Окна", f"{len(h_series)}", 
                          f"[{min(h_series):.3f}...{max(h_series):.3f}]")
            if not selected_data.get('hurst_stable', True):
                st.warning(
                    f"⚠️ **Hurst нестабилен** (σ={h_std:.3f}): значения прыгают от "
                    f"{min(h_series):.3f} до {max(h_series):.3f}. "
                    f"Mean reversion может быть временным. Используйте EMA={selected_data.get('hurst_ema',0):.3f}."
                )
        
        # v17: Mini-Backtest results panel
        bt_v = selected_data.get('bt_verdict', 'SKIP')
        bt_trades = selected_data.get('bt_trades', 0)
        if bt_trades > 0:
            st.markdown("#### 📈 Mini-Backtest (300 баров)")
            bt_col1, bt_col2, bt_col3, bt_col4 = st.columns(4)
            with bt_col1:
                v_emoji = '✅' if bt_v == 'PASS' else '⚠️' if bt_v == 'WARN' else '❌'
                st.metric("Verdict", f"{v_emoji} {bt_v}")
            with bt_col2:
                st.metric("Total P&L", f"{selected_data.get('bt_pnl',0):+.1f}%")
            with bt_col3:
                st.metric("Sharpe", f"{selected_data.get('bt_sharpe',0):.1f}")
            with bt_col4:
                st.metric("WR / PF", 
                          f"{selected_data.get('bt_wr',0):.0f}% / {selected_data.get('bt_pf',0):.1f}")
            
            if bt_v == 'FAIL':
                st.error(
                    f"❌ **BACKTEST FAIL**: P&L={selected_data.get('bt_pnl',0):+.1f}%, "
                    f"Sharpe={selected_data.get('bt_sharpe',0):.1f}. "
                    f"Историческая торговля убыточна. Вход заблокирован до 🟡 УСЛОВНО."
                )
        
        # v19: Walk-Forward panel
        wf_v = selected_data.get('wf_verdict', 'SKIP')
        wf_folds = selected_data.get('wf_folds_passed', 0)
        wf_total = selected_data.get('wf_n_folds', 0)
        if wf_total > 0:
            st.markdown("#### 🔄 Walk-Forward Validation (OOS)")
            wf_col1, wf_col2, wf_col3 = st.columns(3)
            with wf_col1:
                wf_emoji = '✅' if wf_v == 'PASS' else '⚠️' if wf_v == 'WARN' else '❌'
                st.metric("WF Verdict", f"{wf_emoji} {wf_v}")
            with wf_col2:
                st.metric("OOS P&L", f"{selected_data.get('wf_oos_pnl',0):+.1f}%")
            with wf_col3:
                st.metric("Folds passed", f"{wf_folds}/{wf_total}")
        
        # v24: R4 Z-Velocity panel
        zvel_q = selected_data.get('z_entry_quality', 'UNKNOWN')
        if zvel_q != 'UNKNOWN':
            st.markdown("#### ⚡ Z-Velocity (R4)")
            zv1, zv2, zv3, zv4 = st.columns(4)
            with zv1:
                q_emoji = {'EXCELLENT': '🟢', 'GOOD': '✅', 'FAIR': '🟡', 'POOR': '🔴'}.get(zvel_q, '❓')
                st.metric("Качество входа", f"{q_emoji} {zvel_q}")
            with zv2:
                st.metric("Velocity", f"{selected_data.get('z_velocity', 0):+.3f}/бар",
                         help="dZ/dt — скорость изменения Z. Отрицательная для Z>0 = ревертирует")
            with zv3:
                st.metric("Acceleration", f"{selected_data.get('z_acceleration', 0):+.3f}/бар²",
                         help="Ускорение. Отрицательная при Z>0 = замедляется")
            with zv4:
                toward = selected_data.get('z_toward_zero', False)
                st.metric("К нулю?", "✅ ДА" if toward else "❌ НЕТ")
            
            desc = selected_data.get('z_vel_description', '')
            if desc:
                if zvel_q in ('EXCELLENT', 'GOOD'):
                    st.success(desc)
                elif zvel_q == 'POOR':
                    st.error(desc)
                else:
                    st.info(desc)
        
        # v23: R2 Micro-Backtest panel
        mbt_v = selected_data.get('mbt_verdict', 'SKIP')
        mbt_trades = selected_data.get('mbt_trades', 0)
        if mbt_trades > 0:
            st.markdown("#### ⚡ Micro-Backtest (1-6 баров, R2)")
            mb1, mb2, mb3, mb4, mb5 = st.columns(5)
            with mb1:
                v_emoji = '✅' if mbt_v == 'PASS' else '⚠️' if mbt_v == 'WARN' else '❌'
                st.metric("Verdict", f"{v_emoji} {mbt_v}")
            with mb2:
                st.metric("Avg P&L", f"{selected_data.get('mbt_pnl', 0):+.2f}%",
                         delta=f"{mbt_trades} сделок")
            with mb3:
                st.metric("WR / PF", 
                         f"{selected_data.get('mbt_wr', 0):.0f}% / {selected_data.get('mbt_pf', 0):.1f}")
            with mb4:
                qr = selected_data.get('mbt_quick', 0)
                qr_emoji = '🟢' if qr > 60 else '🟡' if qr > 40 else '🔴'
                st.metric("Quick Exit%", f"{qr_emoji} {qr:.0f}%",
                         help="% сделок закрытых по MEAN_REVERT или TAKE_PROFIT")
            with mb5:
                zv = selected_data.get('mbt_z_vel', 0)
                ab = selected_data.get('mbt_avg_bars', 0)
                st.metric("Z скорость", f"{zv:.3f}/бар",
                         delta=f"avg {ab:.1f} баров")
            
            if mbt_v == 'PASS':
                st.success(
                    f"✅ **MICRO-BT PASS**: Avg P&L={selected_data.get('mbt_pnl',0):+.2f}%, "
                    f"WR={selected_data.get('mbt_wr',0):.0f}%, "
                    f"Quick exits={selected_data.get('mbt_quick',0):.0f}%. "
                    f"Пара хорошо ревертирует на коротком горизонте!")
            elif mbt_v == 'FAIL':
                st.warning(
                    f"⚠️ **MICRO-BT FAIL**: Avg P&L={selected_data.get('mbt_pnl',0):+.2f}%. "
                    f"Пара плохо ревертирует за 1-6 баров. Осторожно!")
        
        # v18: GARCH Z-score panel
        g_z = selected_data.get('garch_z', 0)
        g_div = selected_data.get('garch_divergence', 0)
        g_var = selected_data.get('garch_var_expanding', False)
        g_vr = selected_data.get('garch_vol_ratio', 1.0)
        std_z = selected_data.get('zscore', 0)
        
        if g_z != 0:
            st.markdown("#### 📐 GARCH Z-score (волатильность-адаптивный)")
            gz_col1, gz_col2, gz_col3, gz_col4 = st.columns(4)
            with gz_col1:
                st.metric("Z standard", f"{std_z:+.2f}")
            with gz_col2:
                st.metric("Z GARCH", f"{g_z:+.2f}", 
                          f"Δ={g_div:.2f}" if g_div > 0.5 else "≈ совпадает")
            with gz_col3:
                vr_emoji = '✅' if g_vr < 1.3 else '🟡' if g_vr < 1.8 else '🔴'
                st.metric("σ ratio", f"{vr_emoji} {g_vr:.2f}x")
            with gz_col4:
                st.metric("σ растёт?", "🔴 ДА" if g_var else "✅ НЕТ")
            
            if g_div > 1.5:
                st.error(
                    f"🚨 **Z-score расхождение {g_div:.1f}**: стандартный Z={std_z:+.2f}, "
                    f"GARCH Z={g_z:+.2f}. Волатильность спреда изменилась в {g_vr:.1f}x. "
                    f"{'Вероятно ЛОЖНОЕ СХОЖДЕНИЕ — σ выросла!' if abs(g_z) > abs(std_z) else 'Сигнал СИЛЬНЕЕ чем кажется.'}"
                )
            elif g_div > 0.8:
                st.warning(
                    f"⚠️ Z расходятся: стандартный={std_z:+.2f}, GARCH={g_z:+.2f}. "
                    f"Волатильность спреда нестабильна (σ ratio={g_vr:.2f}x)."
                )
        
        # v19.1: Expanding Window Hurst panel (P3 Roadmap)
        h_slope = selected_data.get('hurst_exp_slope', 0)
        h_assess = selected_data.get('hurst_exp_assessment', 'N/A')
        h_short = selected_data.get('hurst_exp_short', 0)
        h_long = selected_data.get('hurst_exp_long', 0)
        
        if h_assess != 'N/A':
            st.markdown("#### 📏 Expanding Hurst (многомасштабный)")
            eh_col1, eh_col2, eh_col3, eh_col4 = st.columns(4)
            with eh_col1:
                a_emoji = {'STABLE': '✅', 'MR_STRENGTHENING': '🟢', 
                          'MR_WEAKENING': '🔴', 'TRENDING_SHIFT': '🚨',
                          'MIXED': '🟡'}.get(h_assess, '❓')
                a_label = {'STABLE': 'Стабильно', 'MR_STRENGTHENING': 'MR усиливается',
                          'MR_WEAKENING': 'MR ослабевает', 'TRENDING_SHIFT': 'В тренд!',
                          'MIXED': 'Смешанное'}.get(h_assess, h_assess)
                st.metric("Режим", f"{a_emoji} {a_label}")
            with eh_col2:
                st.metric("H short (60 баров)", f"{h_short:.3f}")
            with eh_col3:
                st.metric("H long (300 баров)", f"{h_long:.3f}")
            with eh_col4:
                sl_emoji = '🟢' if h_slope < -0.03 else '🔴' if h_slope > 0.05 else '🟡'
                st.metric("Slope", f"{sl_emoji} {h_slope:+.3f}")
            
            if selected_data.get('hurst_mr_weakening'):
                st.warning(
                    f"⚠️ **MR ослабевает**: H(short)={h_short:.3f} > H(long)={h_long:.3f}. "
                    f"Недавняя динамика менее mean-reverting. Рассмотрите отложенный вход.")
            elif selected_data.get('hurst_mr_strengthening'):
                st.success(
                    f"🟢 **MR усиливается**: H(short)={h_short:.3f} < H(long)={h_long:.3f}. "
                    f"Идеальный момент для входа — пара становится более mean-reverting.")
        
        # v21: PCA Factor Exposure panel (P5)
        pca_r = st.session_state.get('_pca_result')
        if pca_r and 'error' not in pca_r:
            c1 = selected_data.get('coin1', '')
            c2 = selected_data.get('coin2', '')
            fe = pair_factor_exposure(pca_r, c1, c2, selected_data.get('hedge_ratio', 1.0))
            
            if fe:
                st.markdown("#### 🧬 PCA Factor Exposure (P5)")
                pc1, pc2, pc3, pc4 = st.columns(4)
                
                with pc1:
                    mn = fe.get('market_neutrality', 0)
                    mn_emoji = '🟢' if mn > 0.7 else '🟡' if mn > 0.4 else '🔴'
                    st.metric("Рыночная нейтральность", f"{mn_emoji} {mn:.0%}")
                with pc2:
                    sc = fe.get('same_cluster', False)
                    st.metric("Один кластер", "✅ Да" if sc else "⚠️ Нет",
                             f"C1:{fe.get('cluster_coin1', '?')}, C2:{fe.get('cluster_coin2', '?')}")
                with pc3:
                    net_pc1 = fe.get('net_exposure', {}).get('PC1', 0)
                    st.metric("Net PC1 (Market)", f"{net_pc1:+.3f}",
                             "нейтрально" if abs(net_pc1) < 0.3 else "экспозиция!")
                with pc4:
                    te = fe.get('total_exposure', 0)
                    st.metric("Total Exposure", f"{te:.3f}")
                
                if not fe.get('same_cluster'):
                    st.warning(
                        f"⚠️ **Разные кластеры**: {c1} (кластер {fe.get('cluster_coin1')}) "
                        f"vs {c2} (кластер {fe.get('cluster_coin2')}). "
                        f"Монеты движутся разными факторами → пара может быть менее стабильной.")
                elif fe.get('market_neutrality', 0) < 0.4:
                    st.warning(
                        f"⚠️ **Высокая рыночная экспозиция**: Net PC1={net_pc1:+.3f}. "
                        f"При обвале рынка эта пара пострадает.")
                elif fe.get('market_neutrality', 0) > 0.7:
                    st.success(
                        f"🟢 **Рыночно-нейтральная пара**: Net PC1={net_pc1:+.3f}. "
                        f"Хорошая защита от рыночных шоков.")
        
        # v16: Dollar Exposure panel
        st.markdown("#### 💵 Dollar Exposure (баланс ног)")
        hr = selected_data.get('hedge_ratio', 1.0)
        # Use last known prices from spread if available
        p1_last = selected_data.get('price1_last', 1.0)
        p2_last = selected_data.get('price2_last', 1.0)
        
        # Calculate exposure for $1000 notional
        notional = 1000
        leg1 = notional / (1 + abs(hr))
        leg2 = notional - leg1
        exposure_pct = abs(leg1 - leg2) / max(leg1, leg2) * 100
        
        de_col1, de_col2, de_col3 = st.columns(3)
        with de_col1:
            st.metric(f"Нога 1 ({selected_data.get('coin1','')})", 
                      f"${leg1:.0f}", f"{leg1/notional*100:.0f}%")
        with de_col2:
            st.metric(f"Нога 2 ({selected_data.get('coin2','')})", 
                      f"${leg2:.0f}", f"{leg2/notional*100:.0f}%")
        with de_col3:
            exp_emoji = '✅' if exposure_pct < 30 else '🟡' if exposure_pct < 60 else '🔴'
            st.metric("Дисбаланс", f"{exp_emoji} {exposure_pct:.0f}%",
                      "Нейтрально" if exposure_pct < 30 else "Перекос!")
        
        if exposure_pct > 60:
            st.error(
                f"🚨 **Dollar Exposure {exposure_pct:.0f}%**: на {notional} USD капитала ноги "
                f"{leg1:.0f} USD vs {leg2:.0f} USD. При обвале рынка на 10% чистый убыток "
                f"~{abs(leg1-leg2)*0.10:.0f} USD от дисбаланса. Рассмотрите уменьшение позиции."
            )
        elif exposure_pct > 30:
            st.warning(
                f"⚠️ Перекос {abs(leg1-leg2):.0f} USD ({exposure_pct:.0f}%). "
                f"Позиция не полностью доллар-нейтральна."
            )
    
    # График спреда
    if selected_data['spread'] is not None:
        fig = plot_spread_chart(
            selected_data['spread'], selected_pair, selected_data['zscore'],
            threshold=selected_data.get('threshold', 2.0),
            direction=selected_data.get('direction', 'NONE'),
            z_window=selected_data.get('z_window', 30),
            halflife_bars=selected_data.get('halflife_hours', 30) / ({'1h':1,'4h':4,'1d':24}.get(st.session_state.settings.get('timeframe','4h'), 4))
        )
        st.plotly_chart(fig, width='stretch')
    
    # Калькулятор размера позиции
    st.markdown("---")
    st.subheader("💰 Калькулятор размера позиции")
    
    col1, col2 = st.columns(2)
    
    with col1:
        total_capital = st.number_input(
            "💵 Общая сумма для входа (USD)",
            min_value=10.0,
            max_value=1000000.0,
            value=100.0,  # $100 по умолчанию
            step=10.0,
            help="Сколько всего хотите вложить в эту пару",
            key=f"capital_{selected_pair}"
        )
        
        commission_rate = st.number_input(
            "💸 Комиссия биржи (%)",
            min_value=0.0,
            max_value=1.0,
            value=0.1,
            step=0.01,
            help="Обычно 0.1% для мейкеров, 0.075% на Binance с BNB",
            key=f"commission_{selected_pair}"
        )
    
    with col2:
        hedge_ratio = selected_data['hedge_ratio']
        
        st.markdown("### 📊 Распределение капитала:")
        
        # Расчет позиций с учетом hedge ratio
        position1 = total_capital / (1 + hedge_ratio)
        position2 = position1 * hedge_ratio
        
        # Учет комиссий (вход + выход, обе стороны)
        commission_total = (position1 + position2) * (commission_rate / 100) * 2
        effective_capital = total_capital - commission_total
        
        coin1, coin2 = selected_data['coin1'], selected_data['coin2']
        signal = selected_data['signal']
        direction = selected_data.get('direction', 'NONE')
        
        if direction == 'LONG' or (direction == 'NONE' and signal == 'LONG'):
            st.success(f"""
            **🟢 LONG позиция:**
            
            **{coin1}:** 🟢 LONG (КУПИТЬ) — ${position1:.2f}
            **{coin2}:** 🔴 SHORT (ПРОДАТЬ) — ${position2:.2f}
            
            💸 Комиссии: ${commission_total:.2f}
            💰 Эффективно: ${effective_capital:.2f}
            """)
        elif direction == 'SHORT' or (direction == 'NONE' and signal == 'SHORT'):
            st.error(f"""
            **🔴 SHORT позиция:**
            
            **{coin1}:** 🔴 SHORT (ПРОДАТЬ) — ${position1:.2f}
            **{coin2}:** 🟢 LONG (КУПИТЬ) — ${position2:.2f}
            
            💸 Комиссии: ${commission_total:.2f}
            💰 Эффективно: ${effective_capital:.2f}
            """)
        else:
            st.info(f"""
            **⚪ Нет сигнала:**
            
            **{coin1}:** ${position1:.2f}
            **{coin2}:** ${position2:.2f}
            
            ⏳ Ждите сигнал (|Z| > порога)
            """)
    
    # Детальная разбивка
    st.markdown("### 📝 Детальная разбивка позиции")
    
    breakdown_col1, breakdown_col2, breakdown_col3 = st.columns(3)
    
    # v7.1: Определяем направление для каждой монеты
    dir_label = selected_data.get('direction', 'NONE')
    if dir_label == 'LONG':
        coin1_dir, coin2_dir = "🟢 LONG", "🔴 SHORT"
    elif dir_label == 'SHORT':
        coin1_dir, coin2_dir = "🔴 SHORT", "🟢 LONG"
    else:
        coin1_dir, coin2_dir = "⚪", "⚪"
    
    with breakdown_col1:
        st.metric(f"{coin1} {coin1_dir}", f"${position1:.2f}", 
                 f"{(position1/total_capital)*100:.1f}% от капитала")
    
    with breakdown_col2:
        st.metric(f"{coin2} {coin2_dir}", f"${position2:.2f}",
                 f"{(position2/total_capital)*100:.1f}% от капитала")
    
    with breakdown_col3:
        st.metric("Hedge Ratio", f"{hedge_ratio:.4f}",
                 f"1:{hedge_ratio:.4f}")
    
    # v16: Dollar Exposure warning in calculator
    calc_exposure = abs(position1 - position2)
    calc_exp_pct = calc_exposure / max(position1, position2) * 100 if max(position1, position2) > 0 else 0
    if calc_exp_pct > 30:
        st.warning(
            f"⚠️ **Перекос ног:** {position1:.2f} USD vs {position2:.2f} USD "
            f"(разница {calc_exposure:.2f} USD, {calc_exp_pct:.0f}%). "
            f"При обвале рынка на 10% чистый убыток ~{calc_exposure*0.10:.2f} USD от дисбаланса."
        )
    
    # Калькулятор прибыли/убытков
    st.markdown("---")
    st.subheader("🎯 Расчет прибыли и стоп-лосса")
    
    entry_z = selected_data['zscore']
    
    # Стоп-лосс и цели
    if abs(entry_z) > 0:
        if entry_z < 0:  # LONG
            stop_z = entry_z - 1.0
            tp1_z = entry_z + (abs(entry_z) * 0.4)
            target_z = 0.0
        else:  # SHORT
            stop_z = entry_z + 1.0
            tp1_z = entry_z - (abs(entry_z) * 0.4)
            target_z = 0.0
        
        # Процент изменения Z-score
        stop_loss_pct = ((abs(stop_z - entry_z) / abs(entry_z)) * 100)
        tp1_pct = ((abs(tp1_z - entry_z) / abs(entry_z)) * 100)
        target_pct = 100.0
        
        # Реалистичная прибыль для парного арбитража (~6% при полном цикле)
        # Формула: (движение_Z / 100) × капитал × 0.06
        hedge_efficiency = 0.06  # 6% типичная прибыль при полном движении к Z=0
        
        stop_loss_usd = -total_capital * (stop_loss_pct / 100) * hedge_efficiency
        tp1_usd = total_capital * (tp1_pct / 100) * hedge_efficiency
        target_usd = total_capital * (target_pct / 100) * hedge_efficiency
        
        pnl_col1, pnl_col2, pnl_col3 = st.columns(3)
        
        with pnl_col1:
            st.markdown("**🛡️ Стоп-лосс**")
            st.metric("Z-score", f"{stop_z:.2f}")
            st.error(f"Убыток: **${abs(stop_loss_usd):.2f}**")
            st.caption(f"(-{stop_loss_pct:.1f}% от входа)")
        
        with pnl_col2:
            st.markdown("**💰 Take Profit 1**")
            st.metric("Z-score", f"{tp1_z:.2f}")
            st.success(f"Прибыль: **${tp1_usd:.2f}**")
            st.caption(f"(+{tp1_pct:.1f}%, закрыть 50%)")
        
        with pnl_col3:
            st.markdown("**🎯 Полная цель**")
            st.metric("Z-score", "0.00")
            st.success(f"Прибыль: **${target_usd:.2f}**")
            st.caption(f"(+{target_pct:.0f}%, полный выход)")
        
        # Risk/Reward
        risk_reward = abs(target_usd / stop_loss_usd) if stop_loss_usd != 0 else 0
        
        st.markdown("---")
        
        rr_col1, rr_col2, rr_col3 = st.columns(3)
        
        with rr_col1:
            st.metric("💎 Потенциал прибыли", f"${target_usd:.2f}")
        
        with rr_col2:
            st.metric("⚠️ Максимальный риск", f"${abs(stop_loss_usd):.2f}")
        
        with rr_col3:
            if risk_reward >= 2:
                emoji = "🟢"
                assessment = "Отлично!"
            elif risk_reward >= 1.5:
                emoji = "🟡"
                assessment = "Приемлемо"
            else:
                emoji = "🔴"
                assessment = "Слабо"
            
            st.metric(f"{emoji} Risk/Reward", f"{risk_reward:.2f}:1")
            st.caption(assessment)
    
    # Рекомендации по торговле
    st.markdown("---")
    st.markdown("### 💡 Торговая рекомендация")
    
    rec_direction = selected_data.get('direction', 'NONE')
    rec_thr = selected_data.get('threshold', 2.0)
    adaptive_stop = max(rec_thr + 2.0, 4.0)
    
    # v10.0: MTF status for recommendation
    mtf_rec = selected_data.get('mtf_confirmed')
    mtf_line = ""
    if mtf_rec is True:
        mtf_line = f"\n        - ✅ **MTF ({selected_data.get('mtf_tf', '1h')}):** подтверждено ({selected_data.get('mtf_strength', '')})"
    elif mtf_rec is False:
        mtf_line = f"\n        - ⚠️ **MTF ({selected_data.get('mtf_tf', '1h')}):** НЕ подтверждено — рассмотрите отложенный вход"
    
    if rec_direction == 'LONG':
        st.success(f"""
        **Стратегия: 🟢 LONG**
        - 🟢 **КУПИТЬ** {selected_data['coin1']}
        - 🔴 **ПРОДАТЬ** {selected_data['coin2']} (шорт)
        - **Соотношение:** 1:{selected_data['hedge_ratio']:.4f}
        - **Таргет:** Z-score → 0 (mean revert)
        - **Стоп-лосс:** Z < -{adaptive_stop:.1f} (адаптивный: порог {rec_thr} + 2.0){mtf_line}
        """)
    elif rec_direction == 'SHORT':
        st.error(f"""
        **Стратегия: 🔴 SHORT**
        - 🔴 **ПРОДАТЬ** {selected_data['coin1']} (шорт)
        - 🟢 **КУПИТЬ** {selected_data['coin2']}
        - **Соотношение:** 1:{selected_data['hedge_ratio']:.4f}
        - **Таргет:** Z-score → 0 (mean revert)
        - **Стоп-лосс:** Z > +{adaptive_stop:.1f} (адаптивный: порог {rec_thr} + 2.0){mtf_line}
        """)
    else:
        st.info("⚪ Нет активного сигнала. Дождитесь |Z-score| > порога")
    
    # v8.0: Детальный анализ пары — CSV export
    st.markdown("---")
    st.markdown("### 📥 Экспорт детального анализа пары")
    
    # v10.0: MTF data for detail export
    mtf_params = []
    mtf_values = []
    if selected_data.get('mtf_confirmed') is not None:
        mtf_params.extend(['MTF Confirmed', 'MTF Strength', 'MTF TF', 'MTF Z-Score', 'MTF Z-Velocity', 'MTF Checks'])
        mtf_values.extend([
            '✅ YES' if selected_data.get('mtf_confirmed') else '❌ NO',
            selected_data.get('mtf_strength', ''),
            selected_data.get('mtf_tf', ''),
            selected_data.get('mtf_z', ''),
            selected_data.get('mtf_z_velocity', ''),
            f"{selected_data.get('mtf_passed', 0)}/{selected_data.get('mtf_total', 0)}",
        ])
    
    detail_data = {
        'Параметр': [
            'Пара', 'Направление', 'Статус', 'Вход',
            'Z-score', 'Threshold', 'Quality Score', 'Signal Score',
            'Confidence', 'P-value (adj)', 'FDR', 'ADF',
            'Hurst (DFA)', 'Hurst fallback?', 'Half-life (ч)', 'Theta',
            'Hedge Ratio', 'HR uncertainty', 'Корреляция ρ',
            'Stability', 'Crossing Density', 'Z-window',
            'Kalman HR', 'N баров',
            f'{selected_data["coin1"]} Action', f'{selected_data["coin2"]} Action',
        ] + mtf_params + [
            'CUSUM Break', 'CUSUM Score', 'CUSUM Drift', 'CUSUM Risk',
            'Regime', 'Regime ADX',
            'Johansen', 'Johansen Trace', 'Johansen CV 5%', 'Johansen HR',
        ],
        'Значение': [
            selected_data['pair'],
            selected_data.get('direction', 'NONE'),
            selected_data.get('signal', 'NEUTRAL'),
            selected_data.get('_entry_label', '⚪ ЖДАТЬ'),
            round(selected_data['zscore'], 4),
            selected_data.get('threshold', 2.0),
            selected_data.get('quality_score', 0),
            selected_data.get('signal_score', 0),
            selected_data.get('confidence', '?'),
            round(selected_data.get('pvalue_adj', selected_data['pvalue']), 6),
            '✅' if selected_data.get('fdr_passed') else '❌',
            '✅' if selected_data.get('adf_passed') else '❌',
            round(selected_data.get('hurst', 0.5), 4),
            '⚠️ YES' if selected_data.get('hurst_is_fallback') else 'NO',
            round(selected_data.get('halflife_hours', selected_data['halflife_days']*24), 2),
            round(selected_data.get('theta', 0), 4),
            round(selected_data['hedge_ratio'], 6),
            round(selected_data.get('hr_uncertainty', 0), 4),
            round(selected_data.get('correlation', 0), 4),
            f"{selected_data.get('stability_passed', 0)}/{selected_data.get('stability_total', 4)}",
            round(selected_data.get('crossing_density', 0), 4),
            selected_data.get('z_window', 30),
            '✅' if selected_data.get('use_kalman') else '❌ OLS',
            selected_data.get('n_bars', 0),
            'LONG (КУПИТЬ)' if rec_direction == 'LONG' else ('SHORT (ПРОДАТЬ)' if rec_direction == 'SHORT' else '-'),
            'SHORT (ПРОДАТЬ)' if rec_direction == 'LONG' else ('LONG (КУПИТЬ)' if rec_direction == 'SHORT' else '-'),
        ] + mtf_values + [
            '🚨 YES' if selected_data.get('cusum_break') else '✅ NO',
            f"{selected_data.get('cusum_score', 0):.1f}σ",
            f"{selected_data.get('cusum_drift', 0):+.2f}σ",
            selected_data.get('cusum_risk', 'LOW'),
            selected_data.get('regime', 'UNKNOWN'),
            f"{selected_data.get('regime_adx', 0):.0f}",
            '✅' if selected_data.get('johansen_coint') else '❌',
            f"{selected_data.get('johansen_trace', 0):.1f}",
            f"{selected_data.get('johansen_cv', 0):.1f}",
            f"{selected_data.get('johansen_hr', 0):.4f}",
        ]
    }
    # v24: Add Micro-BT results to detail CSV
    if selected_data.get('mbt_trades', 0) > 0:
        detail_data['Параметр'].extend([
            'uBT Verdict', 'uBT Avg P&L', 'uBT Win Rate', 'uBT Quick%',
            'uBT Trades', 'uBT Z-velocity', 'uBT Avg Bars',
        ])
        detail_data['Значение'].extend([
            selected_data.get('mbt_verdict', 'SKIP'),
            f"{selected_data.get('mbt_pnl', 0):+.3f}%",
            f"{selected_data.get('mbt_wr', 0):.1f}%",
            f"{selected_data.get('mbt_quick', 0):.1f}%",
            selected_data.get('mbt_trades', 0),
            f"{selected_data.get('mbt_z_vel', 0):.4f}",
            f"{selected_data.get('mbt_avg_bars', 0):.1f}",
        ])
    df_detail = pd.DataFrame(detail_data)
    csv_detail = df_detail.to_csv(index=False)
    st.download_button(
        "📥 Скачать детальный анализ пары (CSV)",
        csv_detail,
        f"detail_{selected_pair.replace('/', '_')}_{st.session_state.get('_scan_ts_str', now_msk().strftime('%Y%m%d_%H%M%S'))}.csv",
        "text/csv",
        key="detail_csv_btn"
    )
    
    # v27: One-Click Entry (2.3) + R3 Auto-Import
    if selected_data.get('signal') in ('SIGNAL', 'READY'):
        st.markdown("---")
        st.markdown("#### 🚀 One-Click Entry (R3 + 2.3)")
        
        _c1 = selected_data.get('coin1', '')
        _c2 = selected_data.get('coin2', '')
        _dir = selected_data.get('direction', 'LONG')
        _z = selected_data.get('zscore', 0)
        _hr = selected_data.get('hedge_ratio', 1.0)
        _p1 = selected_data.get('price1_last', 0)
        _p2 = selected_data.get('price2_last', 0)
        _tf = selected_data.get('signal_tf_used', timeframe if 'timeframe' in dir() else '4h')
        _mbt = selected_data.get('mbt_quick', 0)
        _hurst = selected_data.get('hurst', 0.5)
        _hl = selected_data.get('halflife_hours', 24)
        _vel = selected_data.get('z_entry_quality', '')
        
        # Warnings panel
        _warnings = []
        if selected_data.get('hr_naked'):
            _warnings.append(f"🔴 NAKED: HR={_hr:.4f} < {CFG('strategy', 'hr_naked_threshold', 0.15)}")
        if _hurst >= CFG('strategy', 'min_hurst', 0.45):
            _warnings.append(f"🟡 Hurst={_hurst:.3f} ≥ {CFG('strategy', 'min_hurst', 0.45)}")
        if selected_data.get('hr_uncertainty', 0) > 0.3:
            _warnings.append(f"🟡 HR uncertainty={selected_data.get('hr_uncertainty',0):.1%}")
        if not selected_data.get('mtf_confirmed'):
            _warnings.append("🟡 MTF не подтверждён")
        
        if _warnings:
            st.warning(" | ".join(_warnings))
        
        # === R7 ML SCORING ===
        try:
            from config_loader import ml_score, risk_position_size
            _ml = ml_score(selected_data)
            _grade_emoji = {'A': '🟢', 'B': '🔵', 'C': '🟡', 'D': '🟠', 'F': '🔴'}
            
            _ml_col1, _ml_col2 = st.columns([1, 2])
            with _ml_col1:
                st.metric("ML Score", f"{_ml['score']:.0f}/100",
                         f"Grade {_ml['grade']} — {_ml['recommendation']}")
            with _ml_col2:
                _factors_str = " | ".join(f"{k}={v}" for k, v in _ml['factors'].items() if v != 0)
                st.caption(f"📊 {_factors_str}")
            
            # === R10 RISK MANAGER ===
            _portfolio = CFG('risk', 'portfolio_usdt', 1000)
            _n_open = len([p for p in (st.session_state.get('_all_open_pairs', []))]) if '_all_open_pairs' in st.session_state else 0
            _risk = risk_position_size(_ml, _portfolio, _n_open)
            
            if not _risk['allowed']:
                st.error(_risk['reason'])
            
            _suggested_size = int(_risk['size_usdt']) if _risk['allowed'] else 100
        except Exception:
            _ml = {'score': 0, 'grade': '?', 'recommendation': ''}
            _suggested_size = 100
        
        # v35: Conviction-based position sizing
        try:
            from config_loader import conviction_position_size
            _conv = conviction_position_size(
                selected_data, 
                bt_verdict=selected_data.get('mbt_verdict'),
                v_quality=selected_data.get('v_quality')
            )
            if _conv['size_usdt'] > 0:
                _suggested_size = int(_conv['size_usdt'])
                st.caption(f"📐 v35 Sizing: {_conv['reason']}")
        except Exception:
            pass
        
        # === EXCHANGE INSTRUCTIONS ===
        _size_usdt = st.number_input("💰 Размер позиции (USDT)", 
                                      min_value=10, max_value=10000, value=_suggested_size, step=10,
                                      key="one_click_size")
        
        if _dir == 'SHORT':
            _c1_action = f"SELL (SHORT)"
            _c2_action = f"BUY (LONG)"
            _c1_size = _size_usdt / (1 + abs(_hr))
            _c2_size = _size_usdt * abs(_hr) / (1 + abs(_hr))
        else:
            _c1_action = f"BUY (LONG)"
            _c2_action = f"SELL (SHORT)"
            _c1_size = _size_usdt / (1 + abs(_hr))
            _c2_size = _size_usdt * abs(_hr) / (1 + abs(_hr))
        
        _c1_qty = _c1_size / _p1 if _p1 > 0 else 0
        _c2_qty = _c2_size / _p2 if _p2 > 0 else 0
        
        # Clipboard-ready text
        _fr_net = selected_data.get('funding_net', 0)
        _fr_str = f"FR Net={_fr_net:+.3f}%/8h" if _fr_net != 0 else "FR=N/A"
        _ml_grade = _ml.get('grade', '?') if '_ml' in dir() else '?'
        _ml_pts = _ml.get('score', 0) if '_ml' in dir() else 0
        _exchange_text = (
            f"═══ {_c1}/{_c2} {_dir} ═══\n"
            f"ML: Grade {_ml_grade} ({_ml_pts:.0f}pt) | {_fr_str}\n"
            f"\n"
            f"Leg 1: {_c1}/USDT:USDT → {_c1_action}\n"
            f"  Размер: ~{_c1_size:.1f} USDT ({_c1_qty:.4f} {_c1})\n"
            f"  Цена: {_p1:.6g} USDT\n"
            f"\n"
            f"Leg 2: {_c2}/USDT:USDT → {_c2_action}\n"
            f"  Размер: ~{_c2_size:.1f} USDT ({_c2_qty:.4f} {_c2})\n"
            f"  Цена: {_p2:.6g} USDT\n"
            f"\n"
            f"HR = {_hr:.6f} | Z = {_z:+.2f} | HL = {_hl:.0f}ч\n"
            f"uBT Quick = {_mbt:.0f}% | Hurst = {_hurst:.3f}\n"
            f"Total: {_size_usdt} USDT"
        )
        
        st.code(_exchange_text, language=None)
        
        # === ENTRY QUALITY SUMMARY ===
        _checks = []
        # BUG-N10 FIX: дефолт 2.5 синхронизирован с config_loader._DEFAULTS
        _checks.append(("Z > entry_z", abs(_z) >= CFG('strategy', 'entry_z', 2.5), f"|Z|={abs(_z):.2f}"))
        _checks.append(("uBT Quick ≥ 50%", _mbt >= 50, f"{_mbt:.0f}%"))
        _checks.append(("Hurst < 0.45", _hurst < CFG('strategy', 'min_hurst', 0.45), f"{_hurst:.3f}"))
        _checks.append(("MTF OK", bool(selected_data.get('mtf_confirmed')), str(selected_data.get('mtf_strength', '?'))))
        _checks.append(("HR > naked", not selected_data.get('hr_naked'), f"HR={_hr:.4f}"))
        _checks.append(("V↕ OK", _vel in ('EXCELLENT', 'GOOD', ''), str(_vel or 'N/A')))
        
        _passed = sum(1 for _, ok, _ in _checks if ok)
        _total = len(_checks)
        _color = "🟢" if _passed >= 5 else "🟡" if _passed >= 3 else "🔴"
        st.markdown(f"**{_color} Entry Score: {_passed}/{_total}** — " + 
                   ", ".join(f"{'✅' if ok else '❌'} {name}" for name, ok, _ in _checks))
        
        # === BUTTONS ===
        b1, b2, b3 = st.columns(3)
        
        import json
        monitor_data = {
            'coin1': _c1, 'coin2': _c2,
            'direction': _dir,
            'entry_z': round(_z, 4),
            'entry_hr': round(_hr, 6),
            'entry_price1': round(_p1, 6) if _p1 else 0,
            'entry_price2': round(_p2, 6) if _p2 else 0,
            'timeframe': _tf,
            'quality_score': selected_data.get('quality_score', 0),
            'hurst': _hurst,
            'halflife_hours': _hl,
            'mbt_quick': _mbt,
            'ml_grade': _ml.get('grade', '?') if '_ml' in dir() else '?',
            'ml_score': _ml.get('score', 0) if '_ml' in dir() else 0,
            'risk_size_usdt': _size_usdt,
            'intercept': round(selected_data.get('intercept', 0.0), 6),
            'z_window': selected_data.get('z_window', 30),
            'notes': f"Q={selected_data.get('quality_score',0)} "
                     f"H={_hurst:.3f} HL={_hl:.0f}h "
                     f"uBT={_mbt:.0f}% "
                     f"ML={_ml.get('grade','?')}{_ml.get('score',0):.0f} "
                     f"Size={_size_usdt}$ "
                     f"{'NAKED!' if selected_data.get('hr_naked') else ''}",
        }
        json_str = json.dumps(monitor_data, indent=2, ensure_ascii=False)
        
        with b1:
            # Save pending file for monitor to auto-import
            try:
                import os
                os.makedirs("monitor_import", exist_ok=True)
                imp_path = f"monitor_import/pending_{_c1}_{_c2}.json"
                with open(imp_path, 'w', encoding='utf-8') as f:
                    json.dump(monitor_data, f, ensure_ascii=False)
                st.success(f"✅ Готово к импорту в Монитор")
            except Exception as ex:
                st.warning(f"⚠️ {ex}")
        
        with b2:
            st.download_button(
                f"📥 JSON → Монитор",
                json_str,
                f"monitor_import_{_c1}_{_c2}.json",
                "application/json",
                key="monitor_export_btn"
            )
        
        with b3:
            st.download_button(
                f"📋 Инструкции (TXT)",
                _exchange_text,
                f"trade_{_c1}_{_c2}_{st.session_state.get('_scan_ts_str', now_msk().strftime('%H%M'))}.txt",
                "text/plain",
                key="exchange_txt_btn"
            )
    
    # Экспорт данных — расширенный CSV (v7.1)
    st.markdown("---")
    
    # v7.1: Расширенный CSV с направлением и всеми метриками для бэктеста
    export_rows = []
    for p in pairs:
        export_rows.append({
            'Пара': p['pair'],
            'Coin1': p['coin1'],
            'Coin2': p['coin2'],
            'Вход': p.get('_entry_label', '⚪ ЖДАТЬ'),
            'Статус': p['signal'],
            'Направление': p.get('direction', 'NONE'),
            'Coin1_Action': ('LONG' if p.get('direction') == 'LONG' else 'SHORT' if p.get('direction') == 'SHORT' else ''),
            'Coin2_Action': ('SHORT' if p.get('direction') == 'LONG' else 'LONG' if p.get('direction') == 'SHORT' else ''),
            'Quality': p.get('quality_score', 0),
            'Signal_Score': p.get('signal_score', 0),
            'Confidence': p.get('confidence', '?'),
            'Z-score': round(p['zscore'], 4),
            'Threshold': p.get('threshold', 2.0),
            'P-value': round(p['pvalue'], 6),
            'P-value_adj': round(p.get('pvalue_adj', p['pvalue']), 6),
            'FDR': p.get('fdr_passed', False),
            'Hurst': round(p.get('hurst', 0.5), 4),
            'Half-life_hours': round(p.get('halflife_hours', p['halflife_days']*24), 2),
            'Hedge_Ratio': round(p['hedge_ratio'], 6),
            'HR_uncertainty': round(p.get('hr_uncertainty', 0), 4),
            'Correlation': round(p.get('correlation', 0), 4),
            'Stability': f"{p.get('stability_passed', 0)}/{p.get('stability_total', 4)}",
            'ADF_passed': p.get('adf_passed', False),
            'Theta': round(p.get('theta', 0), 4),
            'Crossing_Density': round(p.get('crossing_density', 0), 4),
            'Z_window': p.get('z_window', 30),
            'Kalman': p.get('use_kalman', False),
            'N_bars': p.get('n_bars', 0),
            'Opt_criteria': f"{p.get('_opt_count', 0)}/6",
            'FDR_bypass': p.get('_fdr_bypass', False),
            'Cluster': p.get('cluster', ''),
            'MTF_confirmed': p.get('mtf_confirmed', ''),
            'MTF_strength': p.get('mtf_strength', ''),
            'MTF_Z': p.get('mtf_z', ''),
            'MTF_velocity': p.get('mtf_z_velocity', ''),
            'MTF_checks': f"{p.get('mtf_passed', '')}/{p.get('mtf_total', '')}",
            'uBT_verdict': p.get('mbt_verdict', ''),
            'uBT_avg_pnl': p.get('mbt_pnl', ''),
            'uBT_wr': p.get('mbt_wr', ''),
            'uBT_quick': p.get('mbt_quick', ''),
            'uBT_trades': p.get('mbt_trades', ''),
        })
    
    df_export = pd.DataFrame(export_rows)
    csv_data = df_export.to_csv(index=False)
    
    # v43 FIX: Стабильный file_name — используем timestamp последнего скана,
    # а не now_msk(). При rerun now_msk() меняется → новый хеш → старый
    # файл исчезает из media storage → MediaFileStorageError.
    _scan_ts_str = st.session_state.get('_scan_ts_str', now_msk().strftime('%Y%m%d_%H%M%S'))
    
    dl_col1, dl_col2 = st.columns(2)
    with dl_col1:
        st.download_button(
            label="📥 Скачать результаты (CSV)",
            data=csv_data,
            file_name=f"pairs_scan_{exchange}_{timeframe}_{_scan_ts_str}.csv",
            mime="text/csv",
            key="dl_results_csv"
        )
    with dl_col2:
        # Краткая таблица (как раньше)
        csv_short = df_display.to_csv(index=False)
        st.download_button(
            label="📥 Краткая таблица (CSV)",
            data=csv_short,
            file_name=f"pairs_short_{_scan_ts_str}.csv",
            mime="text/csv",
            key="dl_short_csv"
        )

    # v21: PCA Factor Clustering Dashboard
    pca_r = st.session_state.get('_pca_result')
    if pca_r and 'error' not in pca_r:
        with st.expander("🧬 PCA Factor Clustering — Кластеры монет", expanded=False):
            # Factor summary
            ev = pca_r.get('explained_variance', [])
            fn = pca_r.get('factor_names', [])
            
            st.markdown(f"**Всего факторов:** {pca_r['n_components']} | "
                       f"**Объяснено:** {pca_r['total_explained']*100:.1f}% дисперсии")
            
            for i, (name, var) in enumerate(zip(fn, ev)):
                st.progress(var, text=f"**{name}**: {var*100:.1f}%")
            
            # Cluster table
            cs = pca_r.get('cluster_summary', {})
            if cs:
                st.markdown("**Кластеры монет:**")
                for cl_id, info in sorted(cs.items()):
                    members = ', '.join(sorted(info['members']))
                    avg_l = info.get('avg_loadings', {})
                    pc1_avg = avg_l.get('PC1', 0)
                    emoji = '🟢' if abs(pc1_avg) < 0.15 else '🟡' if abs(pc1_avg) < 0.3 else '🔴'
                    st.markdown(
                        f"  {emoji} **Кластер {cl_id}** ({info['n']} монет): "
                        f"`{members}` | PC1={pc1_avg:+.3f}")
            
            # Pair neutrality ranking for SIGNAL pairs
            signal_p = [p for p in pairs if p.get('signal') in ('SIGNAL', 'READY')]
            if signal_p:
                st.markdown("**Рыночная нейтральность сигнальных пар:**")
                for p in sorted(signal_p, key=lambda x: -x.get('pca_market_neutral', 0)):
                    mn = p.get('pca_market_neutral', 0)
                    sc = '✅' if p.get('pca_same_cluster') else '⚠️'
                    mn_bar = '🟢' if mn > 0.7 else '🟡' if mn > 0.4 else '🔴'
                    st.markdown(
                        f"  {mn_bar} **{p['pair']}** — neutrality={mn:.0%} {sc} "
                        f"| PC1={p.get('pca_net_pc1', 0):+.3f}")

else:
    st.info("👆 Нажмите 'Запустить сканер' для начала анализа")
    
    # Инструкция
    # U-004 FIX: динамические значения из конфига (было: Binance, топ-100)
    _info_exchange = st.session_state.get('settings', {}).get('exchange', CFG('scanner', 'exchange', 'bybit')).upper()
    _info_coins = st.session_state.get('settings', {}).get('top_n_coins', CFG('scanner', 'top_n_coins', 70))
    st.markdown(f"""
    ### 🎯 Что делает этот скринер:
    
    1. **Загружает данные** топ-{_info_coins} криптовалют с {_info_exchange}
    2. **Тестирует все пары** на статистическую коинтеграцию
    3. **Находит возможности** для парного арбитража
    4. **Показывает сигналы** на основе Z-score
    
    ### 📚 Как торговать:
    
    - **Z-score > +2**: Пара переоценена → SHORT первая монета, LONG вторая
    - **Z-score < -2**: Пара недооценена → LONG первая монета, SHORT вторая
    - **Z-score → 0**: Закрытие позиции (возврат к среднему)
    
    ### ⚠️ Важно:
    - Используйте стоп-лоссы
    - Учитывайте комиссии биржи
    - Проверяйте ликвидность пар
    - Это не финансовая рекомендация
    """)

# Footer
st.markdown("---")
st.caption("⚠️ Disclaimer: Этот инструмент предназначен только для образовательных целей. Не является финансовой рекомендацией.")

# v27: Auto-refresh — st.rerun() MUST be outside try/except!
# v43 FIX: window.parent.location.reload() УДАЛЁН — он уничтожал Streamlit
# in-memory media storage, из-за чего download_button CSV-файлы теряли хеши
# и вызывали MediaFileStorageError: "Missing file ...csv".
# Теперь используем ТОЛЬКО st.rerun() (как в monitor v39 FIX).
# st.rerun() сохраняет session_state И media storage.
_needs_rerun = False
if auto_refresh and st.session_state.pairs_data is not None:
    _last_ts = st.session_state.get('_last_scan_ts', 0)
    if _last_ts > 0:
        _elapsed_sec = time.time() - _last_ts
        _remaining_sec = refresh_interval * 60 - _elapsed_sec

        if _remaining_sec <= 0:
            _needs_rerun = True
        else:
            _remaining_min = int(_remaining_sec / 60)
            _remaining_s   = int(_remaining_sec % 60)
            st.caption(f"⏱️ Авто-обновление через {_remaining_min}м {_remaining_s}с (интервал {refresh_interval} мин)")
            # v43 FIX: JS-таймер вызывает st.rerun() через Streamlit API,
            # а НЕ browser reload. Media storage и session_state сохраняются.
            # U-002 FIX: toast за 5 секунд до обновления + отложенный rerun.
            try:
                import streamlit.components.v1 as _comp
                _warn_ms = max(0, int((_remaining_sec - 5) * 1000))
                _rerun_ms = int(_remaining_sec * 1000)
                _comp.html(
                    f"""<script>
                    // U-002 FIX: предупреждение за 5 секунд
                    if ({_remaining_sec} > 6) {{
                        setTimeout(function() {{
                            try {{
                                var toast = document.createElement('div');
                                toast.textContent = '🔄 Обновление через 5 секунд...';
                                toast.style.cssText = 'position:fixed;top:20px;right:20px;background:#1f77b4;color:#fff;padding:10px 20px;border-radius:8px;z-index:99999;font-size:14px;opacity:0.95;';
                                window.parent.document.body.appendChild(toast);
                                setTimeout(function(){{ toast.remove(); }}, 5000);
                            }} catch(e) {{}}
                        }}, {_warn_ms});
                    }}
                    // Rerun
                    setTimeout(function() {{
                        try {{
                            var url = new URL(window.parent.location.href);
                            url.searchParams.set('_t', Date.now());
                            window.parent.history.replaceState(null, '', url.toString());
                            window.parent.dispatchEvent(new Event('popstate'));
                        }} catch(e) {{}}
                    }}, {_rerun_ms});
                    </script>""",
                    height=0
                )
            except Exception:
                pass

# st.rerun() MUST be at top level (not inside try/except!)
if _needs_rerun:
    st.rerun()
# VERSION: 7.3 (v35 update)
# LAST UPDATED: 2026-02-27
# FIXES v7.2:
#   [FIX] Unicode μ → uBT — fixes 'charmap' codec error on Windows
#   [NEW] v35: Conviction-based position sizing
#   [FIX] Pattern analysis reads from both CSV and positions.json
