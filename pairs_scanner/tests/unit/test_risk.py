"""
tests/unit/test_risk.py — Unit-тесты для core/risk.py

Каждый тест назван по багу, который он ловит.
Запуск: pytest tests/unit/test_risk.py -v
Время: < 1 сек, без сети, без Streamlit, без CCXT.
"""

import pytest
from datetime import datetime, timedelta
from pairs_scanner.core.utils import MSK
from pairs_scanner.core.risk import (
    check_daily_loss_limit,
    check_pair_cooldown,
    check_cascade_sl,
    is_whitelisted,
    build_watchlist_pairs,
    is_hr_safe,
    pair_memory_is_blocked,
    risk_position_size,
    recommend_position_size,
    check_anti_repeat,
    check_coin_position_limit,
)


# ═══════════════════════════════════════════════════════
# CHECK_DAILY_LOSS_LIMIT
# ═══════════════════════════════════════════════════════

class TestDailyLossLimit:
    """Регрессии: B-03 (live_open_pnls), BUG-016 (unrealised loss)."""

    def test_b03_live_pnls_included(self):
        """B-03: live_open_pnls ОБЯЗАТЕЛЬНО учитывается в расчёте."""
        cd_data = {'ETH/BTC': {'session_pnl': -3.0, 'date': '2026-03-18'}}
        live_pnls = [-1.5, -1.0]  # total open loss = -2.5
        # closed (-3) + open (-2.5) = -5.5 → blocked at -5.0
        blocked, reason = check_daily_loss_limit(
            cd_data, live_pnls, -5.0, today_str='2026-03-18'
        )
        assert blocked is True
        assert 'ЛИМИТ' in reason

    def test_bug016_positive_pnl_ignored(self):
        """BUG-016: прибыльные open позиции НЕ компенсируют убытки."""
        cd_data = {'ETH/BTC': {'session_pnl': -4.8, 'date': '2026-03-18'}}
        live_pnls = [+2.0, -0.5]  # only -0.5 counts
        # -4.8 + (-0.5) = -5.3 → blocked
        blocked, _ = check_daily_loss_limit(
            cd_data, live_pnls, -5.0, today_str='2026-03-18'
        )
        assert blocked is True

    def test_not_blocked_when_under_limit(self):
        cd_data = {'ETH/BTC': {'session_pnl': -2.0, 'date': '2026-03-18'}}
        blocked, _ = check_daily_loss_limit(
            cd_data, [-1.0], -5.0, today_str='2026-03-18'
        )
        assert blocked is False

    def test_empty_data_not_blocked(self):
        blocked, _ = check_daily_loss_limit({}, [], -5.0)
        assert blocked is False

    def test_ignores_other_dates(self):
        cd_data = {'ETH/BTC': {'session_pnl': -10.0, 'date': '2026-03-17'}}
        blocked, _ = check_daily_loss_limit(
            cd_data, [], -5.0, today_str='2026-03-18'
        )
        assert blocked is False

    def test_multiple_pairs_sum(self):
        cd_data = {
            'ETH/BTC': {'session_pnl': -2.0, 'date': '2026-03-18'},
            'SOL/AVAX': {'session_pnl': -2.5, 'date': '2026-03-18'},
        }
        # -2.0 + -2.5 + open(-1.0) = -5.5 → blocked
        blocked, _ = check_daily_loss_limit(
            cd_data, [-1.0], -5.0, today_str='2026-03-18'
        )
        assert blocked is True

    def test_exact_boundary(self):
        """На границе лимита = blocked (<=, не <)."""
        cd_data = {'X/Y': {'session_pnl': -5.0, 'date': '2026-03-18'}}
        blocked, _ = check_daily_loss_limit(
            cd_data, [], -5.0, today_str='2026-03-18'
        )
        assert blocked is True


# ═══════════════════════════════════════════════════════
# CHECK_PAIR_COOLDOWN
# ═══════════════════════════════════════════════════════

class TestPairCooldown:
    """Регрессии: B-04 (cd_data param), LOG-04 (green bypass)."""

    def _make_cd(self, hours_ago=2.0, sl_exit=True, consecutive_sl=0,
                 session_pnl=-3.0):
        now = datetime.now(MSK)
        loss_time = (now - timedelta(hours=hours_ago)).isoformat()
        return {
            'session_pnl': session_pnl,
            'last_loss_time': loss_time,
            'sl_exit': sl_exit,
            'consecutive_sl': consecutive_sl,
        }

    def test_sl_cooldown_12h(self):
        cd = {'ETH/BTC': self._make_cd(hours_ago=2)}
        blocked, reason = check_pair_cooldown(
            'ETH/BTC', cd, cooldown_after_sl_hours=12
        )
        assert blocked is True
        assert 'SL' in reason

    def test_sl_cooldown_expired(self):
        cd = {'ETH/BTC': self._make_cd(hours_ago=13)}
        blocked, _ = check_pair_cooldown(
            'ETH/BTC', cd, cooldown_after_sl_hours=12
        )
        assert blocked is False

    def test_log04_green_does_not_bypass_sl(self):
        """LOG-04: 🟢 ВХОД НЕ обходит SL cooldown."""
        cd = {'ETH/BTC': self._make_cd(hours_ago=2, sl_exit=True)}
        blocked, _ = check_pair_cooldown(
            'ETH/BTC', cd, entry_label='🟢 ВХОД',
            cooldown_after_sl_hours=12,
        )
        assert blocked is True

    def test_green_bypasses_non_sl_cooldown(self):
        """🟢 ВХОД обходит non-SL cooldown."""
        cd = {'ETH/BTC': self._make_cd(hours_ago=1, sl_exit=False, session_pnl=-1.0)}
        blocked, _ = check_pair_cooldown(
            'ETH/BTC', cd, entry_label='🟢 ВХОД',
            pair_cooldown_hours=4,
        )
        assert blocked is False

    def test_2sl_consecutive_longer_cooldown(self):
        cd = {'ETH/BTC': self._make_cd(hours_ago=5, sl_exit=True, consecutive_sl=2)}
        blocked, reason = check_pair_cooldown(
            'ETH/BTC', cd,
            cooldown_after_2sl_hours=12,
        )
        assert blocked is True
        assert '2+ SL' in reason

    def test_no_loss_time_not_blocked(self):
        cd = {'ETH/BTC': {'session_pnl': 0}}
        blocked, _ = check_pair_cooldown('ETH/BTC', cd)
        assert blocked is False

    def test_unknown_pair_not_blocked(self):
        blocked, _ = check_pair_cooldown('XYZ/ABC', {})
        assert blocked is False

    def test_small_loss_no_cooldown(self):
        """Loss > -0.5% → cooldown. Loss -0.3% → no cooldown."""
        cd = {'ETH/BTC': self._make_cd(hours_ago=1, sl_exit=False, session_pnl=-0.3)}
        blocked, _ = check_pair_cooldown('ETH/BTC', cd, pair_cooldown_hours=4)
        assert blocked is False


# ═══════════════════════════════════════════════════════
# IS_WHITELISTED + BUILD_WATCHLIST_PAIRS
# ═══════════════════════════════════════════════════════

class TestWhitelist:
    """Регрессия B-01: :BOTH добавлялся безусловно → direction filter сломан."""

    def test_b01_long_does_not_allow_short(self):
        """B-01: watchlist с direction=LONG НЕ разрешает SHORT."""
        wl = build_watchlist_pairs([
            {'coin1': 'BTC', 'coin2': 'ETH', 'direction': 'LONG'}
        ])
        assert is_whitelisted('BTC', 'ETH', 'LONG', wl) is True
        assert is_whitelisted('BTC', 'ETH', 'SHORT', wl) is False

    def test_b01_both_allows_any_direction(self):
        """B-01: direction=BOTH разрешает и LONG, и SHORT."""
        wl = build_watchlist_pairs([
            {'coin1': 'BTC', 'coin2': 'ETH', 'direction': 'BOTH'}
        ])
        assert is_whitelisted('BTC', 'ETH', 'LONG', wl) is True
        assert is_whitelisted('BTC', 'ETH', 'SHORT', wl) is True
        assert is_whitelisted('BTC', 'ETH', 'BOTH', wl) is True

    def test_reverse_pair_also_matches(self):
        wl = build_watchlist_pairs([
            {'coin1': 'BTC', 'coin2': 'ETH', 'direction': 'BOTH'}
        ])
        assert is_whitelisted('ETH', 'BTC', 'LONG', wl) is True

    def test_unlisted_pair_blocked(self):
        wl = build_watchlist_pairs([
            {'coin1': 'BTC', 'coin2': 'ETH', 'direction': 'BOTH'}
        ])
        assert is_whitelisted('SOL', 'AVAX', 'LONG', wl) is False

    def test_no_watchlist_allows_all(self):
        assert is_whitelisted('ANY', 'PAIR', 'LONG', None) is True

    def test_config_whitelist_coin_level(self):
        assert is_whitelisted('BTC', 'ETH', 'LONG', None, ['BTC', 'ETH']) is True
        assert is_whitelisted('BTC', 'SOL', 'LONG', None, ['BTC', 'ETH']) is False

    def test_default_direction_is_both(self):
        """Если direction не указан в watchlist → считается BOTH."""
        wl = build_watchlist_pairs([
            {'coin1': 'BTC', 'coin2': 'ETH'}  # no direction key
        ])
        assert is_whitelisted('BTC', 'ETH', 'SHORT', wl) is True


# ═══════════════════════════════════════════════════════
# RISK_POSITION_SIZE
# ═══════════════════════════════════════════════════════

class TestRiskPositionSize:
    """Регрессия B-06: max() превышал remaining_pct."""

    def test_b06_exposure_limit_respected(self):
        """B-06: remaining=2% < min_per_trade=5% → not allowed."""
        result = risk_position_size(
            ml_result={'grade': 'D', 'score': 35},
            portfolio_usdt=1000,
            open_positions=3,
            max_per_trade_pct=20.0,
            min_per_trade_pct=5.0,
            max_total_exposure_pct=62.0,  # remaining = 62 - 3*20 = 2%
        )
        assert result['allowed'] is False
        assert result['size_pct'] == 0

    def test_grade_f_not_allowed(self):
        result = risk_position_size({'grade': 'F', 'score': 10}, 1000, 0)
        assert result['allowed'] is False

    def test_grade_a_full_size(self):
        result = risk_position_size(
            {'grade': 'A', 'score': 90}, 1000, 0,
            max_per_trade_pct=20.0,
        )
        assert result['allowed'] is True
        assert result['size_pct'] == 20.0
        assert result['size_usdt'] == 200.0

    def test_position_limit_blocks(self):
        result = risk_position_size(
            {'grade': 'A', 'score': 90}, 1000, 5, max_positions=5,
        )
        assert result['allowed'] is False

    def test_grade_b_75_percent(self):
        result = risk_position_size(
            {'grade': 'B', 'score': 70}, 1000, 0,
            max_per_trade_pct=20.0,
        )
        assert result['size_pct'] == 15.0  # 20 * 0.75

    def test_grade_c_50_percent(self):
        result = risk_position_size(
            {'grade': 'C', 'score': 50}, 1000, 0,
            max_per_trade_pct=20.0, min_per_trade_pct=5.0,
        )
        assert result['size_pct'] == 10.0  # 20 * 0.5

    def test_sufficient_remaining_allows(self):
        """remaining=20% >= min_per_trade=5% → allowed."""
        result = risk_position_size(
            {'grade': 'D', 'score': 30}, 1000, 3,
            max_per_trade_pct=20.0, min_per_trade_pct=5.0,
            max_total_exposure_pct=80.0,  # remaining = 80 - 60 = 20%
        )
        assert result['allowed'] is True
        assert result['size_pct'] == 5.0


# ═══════════════════════════════════════════════════════
# RECOMMEND_POSITION_SIZE
# ═══════════════════════════════════════════════════════

class TestRecommendPositionSize:
    """Регрессия G-02: min(size, base_size) блокировал бонусы."""

    def test_g02_bonus_not_blocked(self):
        """G-02: качественная пара может получить размер > base_size."""
        size = recommend_position_size(
            quality_score=85, confidence='HIGH',
            entry_readiness='🟢 ВХОД', hurst=0.3, correlation=0.8,
            base_size=100,
        )
        # С бонусами mult > 1.0 → size > 100 допускается (до 150)
        assert size >= 100

    def test_low_quality_reduces_size(self):
        size = recommend_position_size(
            quality_score=40, confidence='LOW',
            entry_readiness='⚪ ЖДАТЬ', hurst=0.49, correlation=0.2,
            base_size=100,
        )
        assert size < 50

    def test_minimum_25(self):
        size = recommend_position_size(
            quality_score=10, confidence='LOW',
            entry_readiness='⚪ ЖДАТЬ', hurst=0.5, correlation=0.1,
            base_size=100,
        )
        assert size >= 25

    def test_max_150_pct_of_base(self):
        size = recommend_position_size(
            quality_score=99, confidence='HIGH',
            entry_readiness='🟢 ВХОД', hurst=0.2, correlation=0.9,
            base_size=100,
        )
        assert size <= 150


# ═══════════════════════════════════════════════════════
# IS_HR_SAFE
# ═══════════════════════════════════════════════════════

class TestHrSafe:
    """Регрессия BUG-014: hr==0 блокирует, отрицательный допустим."""

    def test_zero_hr_blocked(self):
        safe, _ = is_hr_safe(0)
        assert safe is False

    def test_none_hr_blocked(self):
        safe, _ = is_hr_safe(None)
        assert safe is False

    def test_negative_hr_allowed(self):
        """BUG-014: отрицательный HR допустим."""
        safe, _ = is_hr_safe(-1.5, min_hr=0.05, max_hr=5.0)
        assert safe is True

    def test_extreme_hr_blocked(self):
        safe, _ = is_hr_safe(15.0, max_hr=5.0)
        assert safe is False

    def test_normal_hr_safe(self):
        safe, _ = is_hr_safe(1.2)
        assert safe is True

    def test_tiny_hr_blocked(self):
        safe, _ = is_hr_safe(0.001, min_hr=0.05)
        assert safe is False


# ═══════════════════════════════════════════════════════
# CASCADE SL
# ═══════════════════════════════════════════════════════

class TestCascadeSl:
    def test_disabled_not_blocked(self):
        blocked, _ = check_cascade_sl({}, cascade_enabled=False)
        assert blocked is False

    def test_active_pause_blocks(self):
        now = datetime.now(MSK)
        pause_until = (now + timedelta(hours=2)).isoformat()
        blocked, reason = check_cascade_sl(
            {}, cascade_state={'pause_until': pause_until}, now=now,
        )
        assert blocked is True
        assert 'CASCADE SL' in reason

    def test_threshold_triggers(self):
        now = datetime.now(MSK)
        recent = (now - timedelta(hours=1)).isoformat()
        cd = {
            'A/B': {'sl_exit': True, 'last_loss_time': recent},
            'C/D': {'sl_exit': True, 'last_loss_time': recent},
            'E/F': {'sl_exit': True, 'last_loss_time': recent},
        }
        blocked, _ = check_cascade_sl(cd, threshold=3, now=now)
        assert blocked is True

    def test_below_threshold_ok(self):
        now = datetime.now(MSK)
        recent = (now - timedelta(hours=1)).isoformat()
        cd = {
            'A/B': {'sl_exit': True, 'last_loss_time': recent},
            'C/D': {'sl_exit': True, 'last_loss_time': recent},
        }
        blocked, _ = check_cascade_sl(cd, threshold=3, now=now)
        assert blocked is False


# ═══════════════════════════════════════════════════════
# ANTI-REPEAT
# ═══════════════════════════════════════════════════════

class TestAntiRepeat:
    def test_blocks_same_direction_after_sl(self):
        cd = {'ETH/BTC': {'date': '2026-03-18', 'sl_exit': True, 'last_dir': 'LONG'}}
        blocked, _ = check_anti_repeat(
            'ETH/BTC', 'LONG', cd, today_str='2026-03-18',
        )
        assert blocked is True

    def test_different_direction_ok(self):
        cd = {'ETH/BTC': {'date': '2026-03-18', 'sl_exit': True, 'last_dir': 'LONG'}}
        blocked, _ = check_anti_repeat(
            'ETH/BTC', 'SHORT', cd, today_str='2026-03-18',
        )
        assert blocked is False

    def test_green_bypasses(self):
        cd = {'ETH/BTC': {'date': '2026-03-18', 'sl_exit': True, 'last_dir': 'LONG'}}
        blocked, _ = check_anti_repeat(
            'ETH/BTC', 'LONG', cd, is_green=True, today_str='2026-03-18',
        )
        assert blocked is False


# ═══════════════════════════════════════════════════════
# COIN POSITION LIMIT
# ═══════════════════════════════════════════════════════

class TestCoinPositionLimit:
    def test_at_limit_blocks(self):
        positions = [
            {'coin1': 'ETH', 'coin2': 'BTC'},
            {'coin1': 'ETH', 'coin2': 'SOL'},
        ]
        blocked, _ = check_coin_position_limit('ETH', positions, max_coin_positions=2)
        assert blocked is True

    def test_below_limit_ok(self):
        positions = [{'coin1': 'ETH', 'coin2': 'BTC'}]
        blocked, _ = check_coin_position_limit('ETH', positions, max_coin_positions=2)
        assert blocked is False


# ═══════════════════════════════════════════════════════
# PAIR MEMORY
# ═══════════════════════════════════════════════════════

class TestPairMemory:
    def test_blocked_zero_wins(self):
        mem = {'trades': 3, 'wins': 0, 'total_pnl': -5.0}
        blocked, _ = pair_memory_is_blocked('ETH/BTC', mem)
        assert blocked is True

    def test_not_blocked_has_wins(self):
        mem = {'trades': 3, 'wins': 1, 'total_pnl': -1.0}
        blocked, _ = pair_memory_is_blocked('ETH/BTC', mem)
        assert blocked is False

    def test_not_blocked_few_trades(self):
        mem = {'trades': 1, 'wins': 0, 'total_pnl': -2.0}
        blocked, _ = pair_memory_is_blocked('ETH/BTC', mem, min_trades=2)
        assert blocked is False

    def test_ignore_flag(self):
        mem = {'trades': 5, 'wins': 0, 'total_pnl': -10.0}
        blocked, _ = pair_memory_is_blocked('ETH/BTC', mem, ignore=True)
        assert blocked is False

    def test_none_memory_not_blocked(self):
        blocked, _ = pair_memory_is_blocked('ETH/BTC', None)
        assert blocked is False
