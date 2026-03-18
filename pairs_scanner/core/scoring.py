"""
core/scoring.py — Scoring functions: quality, signal, confidence, entry readiness.

Pure functions — no Streamlit, no CFG(), no file I/O.
All parameters are explicit arguments.

Extracted from mean_reversion_analysis.py (Волна 2).
Includes fixes: N-02 (hedge_ratio=None), N-03 (abs(hedge_ratio) in confidence).
"""

from __future__ import annotations
import numpy as np


# ═══════════════════════════════════════════════════════
# CONFIDENCE — HIGH / MEDIUM / LOW
# ═══════════════════════════════════════════════════════

def calculate_confidence(hurst, stability_score, fdr_passed, adf_passed,
                         zscore, hedge_ratio, hurst_is_fallback=False,
                         hr_std=None):
    """HIGH / MEDIUM / LOW based on 7 criteria.
    N-03 FIX: abs(hedge_ratio) for negative HR support.
    v11.0: HARD GATE — Hurst>=0.45 or fallback -> max MEDIUM."""
    checks = 0
    total_checks = 7
    if fdr_passed:
        checks += 1
    if adf_passed:
        checks += 1
    if not hurst_is_fallback and hurst != 0.5 and hurst < 0.48:
        checks += 1
    if stability_score >= 0.75:
        checks += 1
    if hedge_ratio and hedge_ratio != 0 and 0.1 <= abs(hedge_ratio) <= 10.0:
        checks += 1
    if 1.5 <= abs(zscore) <= 5.0:
        checks += 1
    if hr_std is not None and hedge_ratio and hedge_ratio != 0:
        hr_unc = hr_std / abs(hedge_ratio)
        if hr_unc < 0.5:
            checks += 1
    else:
        checks += 1

    hurst_is_bad = hurst_is_fallback or hurst >= 0.45
    if checks >= 6 and not hurst_is_bad:
        return "HIGH", checks, total_checks
    elif checks >= 4:
        return "MEDIUM", checks, total_checks
    else:
        return "LOW", checks, total_checks


# ═══════════════════════════════════════════════════════
# QUALITY SCORE v45 HYBRID (0-100)
# ═══════════════════════════════════════════════════════

def calculate_quality_score(hurst, ou_params, pvalue_adj, stability_score,
                            hedge_ratio, adf_passed=None,
                            hurst_is_fallback=False,
                            crossing_density=None, n_bars=None,
                            hr_std=None, ubt_passed=None):
    """Quality Score v45 HYBRID (0-100). N-02 FIX: hedge_ratio=None safe.
    Components: FDR(25) + Stability(25) + Hurst(20) + ADF(15) + HR(15) = 100."""
    bd = {}
    bd['fdr'] = min(25, int(max(0.0, (0.15 - pvalue_adj) / 0.15) * 25))
    bd['stability'] = max(0, min(25, int(stability_score * 25)))

    if hurst_is_fallback or hurst == 0.5:
        bd['hurst'] = 5
    elif hurst <= 0.30:
        bd['hurst'] = 20
    elif hurst <= 0.40:
        bd['hurst'] = 15
    elif hurst <= 0.48:
        bd['hurst'] = 10
    elif hurst < 0.50:
        bd['hurst'] = 4
    else:
        bd['hurst'] = 0

    bd['adf'] = 15 if adf_passed else 0

    abs_hr = abs(hedge_ratio) if hedge_ratio else 0
    if not hedge_ratio or hedge_ratio <= 0 or abs_hr > 30:
        bd['hedge_ratio'] = 0
    elif 0.2 <= abs_hr <= 3.5:
        bd['hedge_ratio'] = 15
    elif 0.1 <= abs_hr <= 7.0:
        bd['hedge_ratio'] = 10
    elif 0.05 <= abs_hr <= 15.0:
        bd['hedge_ratio'] = 5
    else:
        bd['hedge_ratio'] = 0

    bd['crossing_penalty'] = -10 if (crossing_density is not None and crossing_density < 0.03) else 0
    bd['data_penalty'] = -15 if (n_bars is not None and n_bars < 100) else 0

    bd['hr_unc_penalty'] = 0
    if hr_std is not None and hedge_ratio and hedge_ratio != 0:  # R-07 FIX: negative HR too
        unc_ratio = hr_std / abs(hedge_ratio)
        if unc_ratio > 0.7:
            bd['hr_unc_penalty'] = -25
        elif unc_ratio > 0.5:
            bd['hr_unc_penalty'] = -15
        elif unc_ratio > 0.3:
            bd['hr_unc_penalty'] = -8

    bd['ubt_penalty'] = -5 if (ubt_passed is not None and not ubt_passed) else 0

    total = max(0, min(100, sum(bd.values())))
    return int(total), bd


# ═══════════════════════════════════════════════════════
# SIGNAL SCORE (0-100)
# ═══════════════════════════════════════════════════════

def calculate_signal_score(zscore, ou_params, confidence, quality_score=100):
    """Signal Score — moment quality. Capped at quality_score * 1.2."""
    bd = {}
    az = abs(zscore)
    if az > 5.0: bd['zscore'] = 10
    elif az >= 3.0: bd['zscore'] = 40
    elif az >= 2.5: bd['zscore'] = 35
    elif az >= 2.0: bd['zscore'] = 30
    elif az >= 1.5: bd['zscore'] = 20
    elif az >= 1.0: bd['zscore'] = 10
    else: bd['zscore'] = 0

    if ou_params is not None:
        hl = ou_params['halflife_ou'] * 24
        bd['ou_speed'] = 30 if hl <= 12 else 25 if hl <= 20 else 15 if hl <= 28 else 8 if hl <= 48 else 0
    else:
        bd['ou_speed'] = 0

    bd['confidence'] = 30 if confidence == "HIGH" else 15 if confidence == "MEDIUM" else 0

    raw = max(0, min(100, sum(bd.values())))
    cap = int(quality_score * 1.2)
    total = min(raw, cap)
    bd['_cap'] = cap
    return int(total), bd


# ═══════════════════════════════════════════════════════
# ADAPTIVE SIGNAL
# ═══════════════════════════════════════════════════════

def get_adaptive_signal(zscore, confidence, quality_score, timeframe='4h',
                        stability_ratio=1.0, fdr_passed=True, hurst=None):
    """Adaptive entry signal. Returns (state, direction, threshold)."""
    az = abs(zscore)
    direction = "LONG" if zscore < 0 else "SHORT" if zscore > 0 else "NONE"

    if az > 4.5 and not (quality_score >= 70 and stability_ratio >= 0.75):
        return "NEUTRAL", "NONE", 4.5
    if quality_score < 30:
        return "NEUTRAL", "NONE", 99.0

    base_map = {'1h': {'HIGH': 1.8, 'MEDIUM': 2.3, 'LOW': 2.8},
                '1d': {'HIGH': 1.3, 'MEDIUM': 1.8, 'LOW': 2.3}
                }.get(timeframe, {'HIGH': 1.5, 'MEDIUM': 2.0, 'LOW': 2.5})
    base = base_map.get(confidence, base_map['LOW'])
    q_adj = max(0, (quality_score - 50)) / 250.0

    h_adj = 0.0
    if hurst is not None:
        if hurst >= 0.45: h_adj = 0.50
        elif hurst >= 0.35: h_adj = 0.0
        elif hurst >= 0.20: h_adj = -0.05
        else: h_adj = -0.10

    threshold = round(max(1.5, min(3.5, base - q_adj + h_adj)), 2)
    cost_floor = {'1h': 2.0, '4h': 1.8, '1d': 1.5}.get(timeframe, 1.8)
    if threshold < cost_floor:
        threshold = cost_floor

    t_ready = round(threshold * 0.80, 2)
    t_watch = round(threshold * 0.55, 2)

    if az >= threshold:
        if stability_ratio < 0.5: return "WATCH", direction, threshold
        if quality_score < 40: return "READY", direction, threshold
        if not fdr_passed and quality_score < 60: return "READY", direction, threshold
        return "SIGNAL", direction, threshold
    elif az >= t_ready:
        return ("WATCH" if stability_ratio < 0.5 else "READY"), direction, threshold
    elif az >= t_watch:
        return "WATCH", direction, threshold
    else:
        return "NEUTRAL", "NONE", threshold


# ═══════════════════════════════════════════════════════
# OU SCORING & VALIDATION
# ═══════════════════════════════════════════════════════

def calculate_ou_score(ou_params, hurst):
    """OU Score (0-100)."""
    if ou_params is None: return 0
    score = 0
    if 0.30 <= hurst <= 0.48: score += 50
    elif 0.48 < hurst <= 0.52: score += 30
    elif 0.25 <= hurst < 0.30: score += 40
    elif hurst < 0.25: score += 25
    elif 0.52 < hurst <= 0.60: score += 15
    hl = ou_params['halflife_ou'] * 24
    if 4 <= hl <= 24: score += 30
    elif 24 < hl <= 48: score += 20
    elif 2 <= hl < 4: score += 15
    elif hl < 2: score += 5
    if ou_params['r_squared'] > 0.15: score += 20
    elif ou_params['r_squared'] > 0.08: score += 15
    elif ou_params['r_squared'] > 0.05: score += 10
    return int(min(100, max(0, score)))


def validate_ou_quality(ou_params, hurst=None, min_theta=0.1, max_halflife=100):
    if ou_params is None: return False, "No OU"
    if ou_params['theta'] < min_theta: return False, "Low theta"
    if ou_params['halflife_ou'] * 24 > max_halflife: return False, "High HL"
    if hurst is not None and hurst > 0.70: return False, "High Hurst"
    return True, "OK"


def estimate_exit_time(current_z, theta, mu=0.0, target_z=0.5):
    if theta <= 0.001: return 999.0
    try:
        ratio = abs(target_z - mu) / abs(current_z - mu)
        ratio = max(0.001, min(0.999, ratio))
        return -np.log(ratio) / theta
    except Exception:
        return 999.0


def calculate_trade_score(hurst, ou_params, pvalue_adj, zscore,
                          stability_score, hedge_ratio,
                          adf_passed=None, hurst_is_fallback=False):
    """Legacy — calls quality_score."""
    return calculate_quality_score(hurst, ou_params, pvalue_adj,
                                    stability_score, hedge_ratio,
                                    adf_passed, hurst_is_fallback)


# ═══════════════════════════════════════════════════════
# COST-AWARE & SANITIZE
# ═══════════════════════════════════════════════════════

def cost_aware_min_z(spread_std, commission_pct=0.10, slippage_pct=0.05,
                     min_profit_ratio=3.0):
    total_costs_pct = (commission_pct + slippage_pct) * 2
    pnl_per_z = max(0.15, min(0.8, spread_std * 100)) if spread_std > 0 else 0.3
    min_z = total_costs_pct * min_profit_ratio / pnl_per_z
    return max(1.5, round(min_z, 2))


def sanitize_pair(hedge_ratio, stability_passed, stability_total, zscore,
                  n_bars=None, hr_std=None, min_hr=0.05, max_hr=5.0):
    """Hard filter: exclude pair if it doesn't pass basic checks."""
    from .risk import is_hr_safe
    hr_ok, hr_reason = is_hr_safe(hedge_ratio, min_hr=min_hr, max_hr=max_hr)
    if not hr_ok:
        return False, hr_reason
    if hr_std is not None and hedge_ratio and abs(hedge_ratio) > 0:
        if hr_std / abs(hedge_ratio) > 1.0:
            return False, f"HR uncertainty {hr_std/abs(hedge_ratio):.0%} > 100%"
    if stability_total > 0 and stability_passed == 0:
        return False, f"Stab=0/{stability_total}"
    if abs(zscore) > 10:
        return False, f"|Z|={abs(zscore):.1f} > 10"
    if n_bars is not None and n_bars < 50:
        return False, f"N={n_bars} < 50 баров"
    return True, "OK"


# ═══════════════════════════════════════════════════════
# ENTRY READINESS
# ═══════════════════════════════════════════════════════

def assess_entry_readiness(p, min_hurst=0.45, q_entry_min=50):
    """Unified entry readiness. All thresholds explicit."""
    mandatory = [
        ('Статус ≥ READY', p.get('signal', 'NEUTRAL') in ('SIGNAL', 'READY'),
         p.get('signal', 'NEUTRAL')),
        ('|Z| ≥ Thr', abs(p.get('zscore', 0)) >= p.get('threshold', 2.0),
         f"|{p.get('zscore',0):.2f}| vs {p.get('threshold',2.0)}"),
        (f'Q ≥ {q_entry_min}', p.get('quality_score', 0) >= q_entry_min,
         f"Q={p.get('quality_score', 0)}"),
        ('Dir ≠ NONE', p.get('direction', 'NONE') != 'NONE',
         p.get('direction', 'NONE')),
    ]
    all_mandatory = all(m[1] for m in mandatory)

    hurst_val = p.get('hurst', 0.5)
    hurst_is_fallback = (hurst_val == 0.5)
    if hurst_is_fallback:
        hurst_ema = p.get('hurst_ema', None)
        if hurst_ema is not None and hurst_ema != 0.5:
            hurst_val = hurst_ema
            hurst_is_fallback = False

    optional = [
        ('FDR ✅', p.get('fdr_passed', False), '✅' if p.get('fdr_passed') else '❌'),
        ('Conf=HIGH', p.get('confidence', 'LOW') == 'HIGH', p.get('confidence', 'LOW')),
        ('S ≥ 60', p.get('signal_score', 0) >= 60, f"S={p.get('signal_score', 0)}"),
        ('ρ ≥ 0.5', p.get('correlation', 0) >= 0.5, f"ρ={p.get('correlation', 0):.2f}"),
        ('Hurst < 0.35', hurst_val < 0.35, f"H={hurst_val:.3f}"),
        ('Stability ≥ 3/4', p.get('stability_passed', 0) >= 3,
         f"{p.get('stability_passed', 0)}/{p.get('stability_total', 4)}"),
    ]
    opt_count = sum(1 for o in optional if o[1])

    if not all_mandatory:
        level, label = 'WAIT', '⚪ ЖДАТЬ'
    elif hurst_is_fallback:
        level, label = 'CONDITIONAL', '🟡 СЛАБЫЙ ⚠️H=0.5'
    elif p.get('cusum_break', False):
        level, label = 'CONDITIONAL', '🟡 УСЛОВНО ⚠️CUSUM'
    elif hurst_val >= min_hurst:
        level, label = 'CONDITIONAL', f'🟡 УСЛОВНО ⚠️H≥{min_hurst}'
    elif opt_count >= 3:
        level, label = 'ENTRY', '🟢 ВХОД'
    else:
        level, label = 'CONDITIONAL', '🟡 УСЛОВНО'

    return {
        'level': level, 'label': label,
        'mandatory': mandatory, 'optional': optional,
        'all_mandatory': all_mandatory, 'opt_count': opt_count,
        'fdr_bypass': opt_count >= 4 and not p.get('fdr_passed', False),
    }
