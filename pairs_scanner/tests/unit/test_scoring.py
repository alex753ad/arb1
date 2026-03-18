"""
tests/unit/test_scoring.py — Unit-тесты для core/scoring.py

Каждый тест назван по багу или функции, которую он проверяет.
Запуск: pytest tests/unit/test_scoring.py -v
Время: < 1 сек, без сети, без Streamlit, без CCXT.
"""

import pytest
from pairs_scanner.core.scoring import (
    calculate_confidence,
    calculate_quality_score,
    calculate_signal_score,
    get_adaptive_signal,
    calculate_ou_score,
    validate_ou_quality,
    estimate_exit_time,
    cost_aware_min_z,
    sanitize_pair,
    assess_entry_readiness,
    calculate_trade_score,
)

# Вспомогательные OU-параметры для тестов
OU_GOOD = {'halflife_ou': 0.5, 'theta': 0.3, 'r_squared': 0.20}   # HL=12h
OU_SLOW = {'halflife_ou': 3.0, 'theta': 0.1, 'r_squared': 0.06}   # HL=72h


# ═══════════════════════════════════════════════════════
# CALCULATE_CONFIDENCE
# ═══════════════════════════════════════════════════════

class TestCalculateConfidence:
    """Регрессии: N-03 (abs(hedge_ratio)), v11 (Hurst HARD GATE)."""

    def test_n03_negative_hr_counts(self):
        """N-03: отрицательный hedge_ratio должен проходить HR-чек (abs)."""
        conf, checks, _ = calculate_confidence(
            hurst=0.30, stability_score=0.80, fdr_passed=True,
            adf_passed=True, zscore=2.5, hedge_ratio=-1.5,
        )
        assert conf in ('HIGH', 'MEDIUM')
        # HR-чек должен пройти (abs(-1.5) = 1.5 ∈ [0.1, 10.0])
        assert checks >= 4

    def test_high_confidence_all_checks_pass(self):
        conf, checks, total = calculate_confidence(
            hurst=0.30, stability_score=0.80, fdr_passed=True,
            adf_passed=True, zscore=2.5, hedge_ratio=1.5,
        )
        assert conf == 'HIGH'
        assert checks >= 6
        assert total == 7

    def test_v11_hurst_gate_caps_at_medium(self):
        """v11.0: Hurst >= 0.45 → максимум MEDIUM, даже если всё идеально."""
        conf, checks, _ = calculate_confidence(
            hurst=0.47, stability_score=0.90, fdr_passed=True,
            adf_passed=True, zscore=2.5, hedge_ratio=1.5,
            hurst_is_fallback=False,
        )
        assert conf != 'HIGH'

    def test_hurst_fallback_caps_at_medium(self):
        """hurst_is_fallback=True → не выше MEDIUM."""
        conf, _, _ = calculate_confidence(
            hurst=0.30, stability_score=0.90, fdr_passed=True,
            adf_passed=True, zscore=2.5, hedge_ratio=1.5,
            hurst_is_fallback=True,
        )
        assert conf in ('MEDIUM', 'LOW')

    def test_low_confidence_most_checks_fail(self):
        conf, checks, _ = calculate_confidence(
            hurst=0.55, stability_score=0.30, fdr_passed=False,
            adf_passed=False, zscore=0.5, hedge_ratio=0.001,
        )
        assert conf == 'LOW'
        assert checks < 4

    def test_hr_uncertainty_reduces_checks(self):
        """Высокая hr_std снижает кол-во чеков."""
        _, checks_low_unc, _ = calculate_confidence(
            hurst=0.30, stability_score=0.80, fdr_passed=True,
            adf_passed=True, zscore=2.5, hedge_ratio=1.5,
            hr_std=0.1,
        )
        _, checks_high_unc, _ = calculate_confidence(
            hurst=0.30, stability_score=0.80, fdr_passed=True,
            adf_passed=True, zscore=2.5, hedge_ratio=1.5,
            hr_std=1.0,  # unc = 67% > 50%
        )
        assert checks_low_unc > checks_high_unc

    def test_zero_hr_not_counted(self):
        """HR = 0 → чек не засчитывается."""
        _, checks_zero, _ = calculate_confidence(
            hurst=0.30, stability_score=0.80, fdr_passed=True,
            adf_passed=True, zscore=2.5, hedge_ratio=0,
        )
        _, checks_normal, _ = calculate_confidence(
            hurst=0.30, stability_score=0.80, fdr_passed=True,
            adf_passed=True, zscore=2.5, hedge_ratio=1.5,
        )
        assert checks_zero < checks_normal


# ═══════════════════════════════════════════════════════
# CALCULATE_QUALITY_SCORE
# ═══════════════════════════════════════════════════════

class TestCalculateQualityScore:
    """Регрессии: N-02 (hedge_ratio=None), R-07 (отрицательный HR)."""

    def test_n02_none_hr_does_not_raise(self):
        """N-02: hedge_ratio=None не должен вызывать исключение."""
        score, bd = calculate_quality_score(
            hurst=0.35, ou_params=OU_GOOD, pvalue_adj=0.05,
            stability_score=0.75, hedge_ratio=None,
        )
        assert 0 <= score <= 100
        assert bd['hedge_ratio'] == 0

    def test_r07_negative_hr_gets_score(self):
        """R-07: отрицательный hedge_ratio должен получить ненулевой балл."""
        score_neg, bd_neg = calculate_quality_score(
            hurst=0.35, ou_params=OU_GOOD, pvalue_adj=0.05,
            stability_score=0.75, hedge_ratio=-1.5,
        )
        score_pos, bd_pos = calculate_quality_score(
            hurst=0.35, ou_params=OU_GOOD, pvalue_adj=0.05,
            stability_score=0.75, hedge_ratio=1.5,
        )
        assert bd_neg['hedge_ratio'] == bd_pos['hedge_ratio']

    def test_perfect_params_high_score(self):
        score, _ = calculate_quality_score(
            hurst=0.28, ou_params=OU_GOOD, pvalue_adj=0.02,
            stability_score=0.95, hedge_ratio=1.2,
            adf_passed=True,
        )
        assert score >= 75

    def test_bad_params_low_score(self):
        score, _ = calculate_quality_score(
            hurst=0.60, ou_params=None, pvalue_adj=0.14,
            stability_score=0.20, hedge_ratio=0.001,
            adf_passed=False,
        )
        assert score < 30

    def test_hurst_fallback_penalty(self):
        score_real, _ = calculate_quality_score(
            hurst=0.35, ou_params=OU_GOOD, pvalue_adj=0.05,
            stability_score=0.75, hedge_ratio=1.5,
            hurst_is_fallback=False,
        )
        score_fallback, _ = calculate_quality_score(
            hurst=0.35, ou_params=OU_GOOD, pvalue_adj=0.05,
            stability_score=0.75, hedge_ratio=1.5,
            hurst_is_fallback=True,
        )
        assert score_fallback < score_real

    def test_few_bars_penalty(self):
        score_many, _ = calculate_quality_score(
            hurst=0.35, ou_params=OU_GOOD, pvalue_adj=0.05,
            stability_score=0.75, hedge_ratio=1.5, n_bars=300,
        )
        score_few, _ = calculate_quality_score(
            hurst=0.35, ou_params=OU_GOOD, pvalue_adj=0.05,
            stability_score=0.75, hedge_ratio=1.5, n_bars=80,
        )
        assert score_few < score_many

    def test_high_hr_uncertainty_penalty(self):
        score_low_unc, _ = calculate_quality_score(
            hurst=0.35, ou_params=OU_GOOD, pvalue_adj=0.05,
            stability_score=0.75, hedge_ratio=1.5, hr_std=0.1,
        )
        score_high_unc, _ = calculate_quality_score(
            hurst=0.35, ou_params=OU_GOOD, pvalue_adj=0.05,
            stability_score=0.75, hedge_ratio=1.5, hr_std=1.2,  # >0.7 ratio
        )
        assert score_high_unc <= score_low_unc - 20

    def test_score_bounded_0_to_100(self):
        """Итоговый балл всегда в диапазоне [0, 100]."""
        for hr in [None, 0, -0.5, 50.0]:
            score, _ = calculate_quality_score(
                hurst=0.5, ou_params=None, pvalue_adj=0.99,
                stability_score=0.0, hedge_ratio=hr,
            )
            assert 0 <= score <= 100

    def test_ubt_penalty(self):
        score_pass, _ = calculate_quality_score(
            hurst=0.35, ou_params=OU_GOOD, pvalue_adj=0.05,
            stability_score=0.75, hedge_ratio=1.5, ubt_passed=True,
        )
        score_fail, _ = calculate_quality_score(
            hurst=0.35, ou_params=OU_GOOD, pvalue_adj=0.05,
            stability_score=0.75, hedge_ratio=1.5, ubt_passed=False,
        )
        assert score_fail == score_pass - 5


# ═══════════════════════════════════════════════════════
# CALCULATE_SIGNAL_SCORE
# ═══════════════════════════════════════════════════════

class TestCalculateSignalScore:
    def test_high_z_score_gets_points(self):
        score, bd = calculate_signal_score(
            zscore=3.5, ou_params=OU_GOOD, confidence='HIGH',
        )
        assert bd['zscore'] == 40

    def test_extreme_z_penalized(self):
        score_normal, _ = calculate_signal_score(3.5, OU_GOOD, 'HIGH')
        score_extreme, _ = calculate_signal_score(6.0, OU_GOOD, 'HIGH')
        assert score_extreme < score_normal

    def test_high_confidence_bonus(self):
        score_high, bd_high = calculate_signal_score(2.5, OU_GOOD, 'HIGH')
        score_low, bd_low = calculate_signal_score(2.5, OU_GOOD, 'LOW')
        assert bd_high['confidence'] == 30
        assert bd_low['confidence'] == 0

    def test_quality_cap_applied(self):
        """Signal score не превышает quality_score * 1.2."""
        score, bd = calculate_signal_score(
            zscore=3.5, ou_params=OU_GOOD, confidence='HIGH',
            quality_score=40,
        )
        assert score <= 40 * 1.2

    def test_none_ou_params_zero_speed(self):
        score, bd = calculate_signal_score(2.5, ou_params=None, confidence='MEDIUM')
        assert bd['ou_speed'] == 0

    def test_fast_halflife_max_ou_score(self):
        """HL = 12h (halflife_ou=0.5 дня) → максимальный балл OU."""
        _, bd_fast = calculate_signal_score(2.5, ou_params=OU_GOOD, confidence='HIGH')
        _, bd_slow = calculate_signal_score(2.5, ou_params=OU_SLOW, confidence='HIGH')
        assert bd_fast['ou_speed'] > bd_slow['ou_speed']

    def test_score_bounded_0_to_100(self):
        score, _ = calculate_signal_score(-6.0, OU_GOOD, 'HIGH', quality_score=100)
        assert 0 <= score <= 100


# ═══════════════════════════════════════════════════════
# GET_ADAPTIVE_SIGNAL
# ═══════════════════════════════════════════════════════

class TestGetAdaptiveSignal:
    def test_returns_signal_high_z(self):
        state, direction, thr = get_adaptive_signal(
            zscore=3.0, confidence='HIGH', quality_score=80,
        )
        assert state == 'SIGNAL'
        assert direction == 'SHORT'  # zscore > 0 → SHORT

    def test_long_direction_negative_z(self):
        state, direction, thr = get_adaptive_signal(
            zscore=-3.0, confidence='HIGH', quality_score=80,
        )
        assert direction == 'LONG'

    def test_neutral_when_quality_too_low(self):
        state, direction, _ = get_adaptive_signal(
            zscore=3.0, confidence='HIGH', quality_score=20,
        )
        assert state == 'NEUTRAL'
        assert direction == 'NONE'

    def test_extreme_z_blocked_without_quality(self):
        """|Z| > 4.5 без достаточного качества → NEUTRAL."""
        state, direction, _ = get_adaptive_signal(
            zscore=5.0, confidence='MEDIUM', quality_score=50,
            stability_ratio=0.5,
        )
        assert state == 'NEUTRAL'

    def test_ready_state_for_intermediate_z(self):
        state, _, _ = get_adaptive_signal(
            zscore=2.2, confidence='HIGH', quality_score=80,
            stability_ratio=1.0,
        )
        assert state in ('READY', 'SIGNAL')

    def test_watch_state_low_z(self):
        state, _, _ = get_adaptive_signal(
            zscore=1.2, confidence='HIGH', quality_score=80,
        )
        assert state in ('WATCH', 'NEUTRAL')

    def test_bad_hurst_raises_threshold(self):
        _, _, thr_bad = get_adaptive_signal(
            zscore=2.0, confidence='HIGH', quality_score=80, hurst=0.47,
        )
        _, _, thr_good = get_adaptive_signal(
            zscore=2.0, confidence='HIGH', quality_score=80, hurst=0.25,
        )
        assert thr_bad > thr_good

    def test_threshold_bounded(self):
        _, _, thr = get_adaptive_signal(2.0, 'LOW', quality_score=10)
        assert 1.5 <= thr <= 3.5


# ═══════════════════════════════════════════════════════
# CALCULATE_OU_SCORE
# ═══════════════════════════════════════════════════════

class TestCalculateOuScore:
    def test_none_params_returns_zero(self):
        assert calculate_ou_score(None, hurst=0.35) == 0

    def test_good_params_high_score(self):
        score = calculate_ou_score(OU_GOOD, hurst=0.35)
        assert score >= 80

    def test_slow_ou_lower_score(self):
        score_fast = calculate_ou_score(OU_GOOD, hurst=0.35)
        score_slow = calculate_ou_score(OU_SLOW, hurst=0.55)
        assert score_fast > score_slow

    def test_bounded_0_to_100(self):
        for ou in [OU_GOOD, OU_SLOW, None]:
            score = calculate_ou_score(ou, hurst=0.30)
            assert 0 <= score <= 100


# ═══════════════════════════════════════════════════════
# VALIDATE_OU_QUALITY
# ═══════════════════════════════════════════════════════

class TestValidateOuQuality:
    def test_none_params_fails(self):
        ok, reason = validate_ou_quality(None)
        assert ok is False
        assert 'No OU' in reason

    def test_good_params_ok(self):
        ok, reason = validate_ou_quality(OU_GOOD)
        assert ok is True
        assert reason == 'OK'

    def test_low_theta_fails(self):
        ok, reason = validate_ou_quality({'halflife_ou': 0.5, 'theta': 0.05, 'r_squared': 0.1})
        assert ok is False
        assert 'theta' in reason.lower()

    def test_high_halflife_fails(self):
        ok, reason = validate_ou_quality(
            {'halflife_ou': 10.0, 'theta': 0.3, 'r_squared': 0.1},  # HL=240h
            max_halflife=100,
        )
        assert ok is False
        assert 'HL' in reason

    def test_high_hurst_fails(self):
        ok, reason = validate_ou_quality(OU_GOOD, hurst=0.75)
        assert ok is False
        assert 'Hurst' in reason


# ═══════════════════════════════════════════════════════
# ESTIMATE_EXIT_TIME
# ═══════════════════════════════════════════════════════

class TestEstimateExitTime:
    def test_returns_finite_for_valid_input(self):
        t = estimate_exit_time(current_z=2.5, theta=0.3, target_z=0.5)
        assert 0 < t < 999

    def test_zero_theta_returns_999(self):
        t = estimate_exit_time(current_z=2.5, theta=0.0)
        assert t == 999.0

    def test_larger_z_longer_exit(self):
        t_small = estimate_exit_time(current_z=2.0, theta=0.3)
        t_large = estimate_exit_time(current_z=4.0, theta=0.3)
        assert t_large > t_small

    def test_faster_theta_shorter_exit(self):
        t_slow = estimate_exit_time(current_z=2.5, theta=0.1)
        t_fast = estimate_exit_time(current_z=2.5, theta=0.5)
        assert t_fast < t_slow


# ═══════════════════════════════════════════════════════
# COST_AWARE_MIN_Z
# ═══════════════════════════════════════════════════════

class TestCostAwareMinZ:
    def test_minimum_1_5(self):
        z = cost_aware_min_z(spread_std=10.0, commission_pct=0.01, slippage_pct=0.01)
        assert z >= 1.5

    def test_higher_costs_raise_threshold(self):
        z_cheap = cost_aware_min_z(spread_std=0.3, commission_pct=0.05, slippage_pct=0.02)
        z_expensive = cost_aware_min_z(spread_std=0.3, commission_pct=0.20, slippage_pct=0.10)
        assert z_expensive >= z_cheap

    def test_zero_spread_uses_default(self):
        z = cost_aware_min_z(spread_std=0.0)
        assert z >= 1.5


# ═══════════════════════════════════════════════════════
# SANITIZE_PAIR
# ═══════════════════════════════════════════════════════

class TestSanitizePair:
    def test_valid_pair_passes(self):
        ok, reason = sanitize_pair(
            hedge_ratio=1.2, stability_passed=3, stability_total=4,
            zscore=2.5, n_bars=200,
        )
        assert ok is True
        assert reason == 'OK'

    def test_zero_hr_fails(self):
        ok, reason = sanitize_pair(
            hedge_ratio=0, stability_passed=3, stability_total=4, zscore=2.5,
        )
        assert ok is False

    def test_none_hr_fails(self):
        ok, _ = sanitize_pair(
            hedge_ratio=None, stability_passed=3, stability_total=4, zscore=2.5,
        )
        assert ok is False

    def test_extreme_z_fails(self):
        ok, reason = sanitize_pair(
            hedge_ratio=1.2, stability_passed=3, stability_total=4, zscore=11.0,
        )
        assert ok is False
        assert '> 10' in reason

    def test_zero_stability_fails(self):
        ok, reason = sanitize_pair(
            hedge_ratio=1.2, stability_passed=0, stability_total=4, zscore=2.5,
        )
        assert ok is False

    def test_few_bars_fails(self):
        ok, reason = sanitize_pair(
            hedge_ratio=1.2, stability_passed=3, stability_total=4,
            zscore=2.5, n_bars=30,
        )
        assert ok is False
        assert 'N=' in reason

    def test_high_hr_uncertainty_fails(self):
        """hr_std / abs(hr) > 1.0 → отклоняем."""
        ok, reason = sanitize_pair(
            hedge_ratio=1.0, stability_passed=3, stability_total=4,
            zscore=2.5, hr_std=1.5,
        )
        assert ok is False
        assert 'uncertainty' in reason.lower() or 'HR' in reason

    def test_negative_hr_allowed(self):
        """Отрицательный HR в допустимых пределах → проходит."""
        ok, _ = sanitize_pair(
            hedge_ratio=-1.2, stability_passed=3, stability_total=4,
            zscore=2.5, n_bars=200,
        )
        assert ok is True


# ═══════════════════════════════════════════════════════
# ASSESS_ENTRY_READINESS
# ═══════════════════════════════════════════════════════

class TestAssessEntryReadiness:
    def _make_good_pair(self, **overrides):
        p = {
            'signal': 'SIGNAL',
            'zscore': -2.8,
            'threshold': 2.0,
            'quality_score': 75,
            'direction': 'LONG',
            'fdr_passed': True,
            'confidence': 'HIGH',
            'signal_score': 70,
            'correlation': 0.65,
            'hurst': 0.30,
            'stability_passed': 3,
            'stability_total': 4,
        }
        p.update(overrides)
        return p

    def test_good_pair_entry_level(self):
        result = assess_entry_readiness(self._make_good_pair())
        assert result['level'] == 'ENTRY'
        assert result['all_mandatory'] is True

    def test_wait_when_signal_neutral(self):
        p = self._make_good_pair(signal='NEUTRAL')
        result = assess_entry_readiness(p)
        assert result['level'] == 'WAIT'

    def test_wait_when_quality_low(self):
        p = self._make_good_pair(quality_score=40)
        result = assess_entry_readiness(p, q_entry_min=50)
        assert result['level'] == 'WAIT'

    def test_wait_when_direction_none(self):
        p = self._make_good_pair(direction='NONE')
        result = assess_entry_readiness(p)
        assert result['level'] == 'WAIT'

    def test_conditional_high_hurst(self):
        p = self._make_good_pair(hurst=0.47)
        result = assess_entry_readiness(p, min_hurst=0.45)
        assert result['level'] == 'CONDITIONAL'

    def test_conditional_hurst_fallback(self):
        p = self._make_good_pair(hurst=0.50)  # fallback value
        result = assess_entry_readiness(p)
        assert result['level'] == 'CONDITIONAL'

    def test_conditional_cusum_break(self):
        p = self._make_good_pair(cusum_break=True)
        result = assess_entry_readiness(p)
        assert result['level'] == 'CONDITIONAL'

    def test_opt_count_below_3_conditional(self):
        """< 3 опциональных чека → CONDITIONAL, не ENTRY."""
        p = self._make_good_pair(
            fdr_passed=False, confidence='LOW', signal_score=30,
            correlation=0.2,
        )
        result = assess_entry_readiness(p)
        assert result['level'] in ('CONDITIONAL', 'WAIT')

    def test_fdr_bypass_flag(self):
        """≥ 4 optional checks, fdr=False → fdr_bypass=True."""
        p = self._make_good_pair(
            fdr_passed=False,
            confidence='HIGH',
            signal_score=70,
            correlation=0.65,
            hurst=0.28,
            stability_passed=4,
        )
        result = assess_entry_readiness(p)
        if result['all_mandatory']:
            assert result['fdr_bypass'] is True

    def test_mandatory_and_optional_lists_present(self):
        result = assess_entry_readiness(self._make_good_pair())
        assert 'mandatory' in result
        assert 'optional' in result
        assert len(result['mandatory']) == 4
        assert len(result['optional']) == 6


# ═══════════════════════════════════════════════════════
# CALCULATE_TRADE_SCORE (legacy shim)
# ═══════════════════════════════════════════════════════

class TestCalculateTradeScore:
    def test_returns_same_as_quality_score(self):
        """calculate_trade_score — thin wrapper вокруг calculate_quality_score."""
        score1, bd1 = calculate_trade_score(
            hurst=0.35, ou_params=OU_GOOD, pvalue_adj=0.05, zscore=2.5,
            stability_score=0.75, hedge_ratio=1.5, adf_passed=True,
        )
        score2, bd2 = calculate_quality_score(
            hurst=0.35, ou_params=OU_GOOD, pvalue_adj=0.05,
            stability_score=0.75, hedge_ratio=1.5, adf_passed=True,
        )
        assert score1 == score2
