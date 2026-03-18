#!/usr/bin/env python3
"""
run_tests.py — Standalone test runner for pairs_scanner.core

Runs all 96 unit tests without pytest dependency.
Usage: python3 run_tests.py

Tests cover:
  core/risk.py    — 57 tests (regressions: B-01, B-03, B-06, G-02, BUG-014, BUG-016, LOG-04)
  core/scoring.py — 39 tests (regressions: N-01, N-02, N-03)
  core/types.py   — 2 tests (Position round-trip)
  core/utils.py   — 2 tests (PnL calc)
"""

import sys
import os
import time

# Ensure pairs_scanner package is importable
_this_dir = os.path.dirname(os.path.abspath(__file__))
_parent_dir = os.path.dirname(_this_dir)  # parent of pairs_scanner/
sys.path.insert(0, _parent_dir)

from datetime import datetime, timedelta
from pairs_scanner.core.utils import MSK, calc_pair_pnl
from pairs_scanner.core.types import Position
from pairs_scanner.core.risk import (
    check_daily_loss_limit, check_pair_cooldown, check_cascade_sl,
    is_whitelisted, build_watchlist_pairs, is_hr_safe,
    pair_memory_is_blocked, risk_position_size, recommend_position_size,
    check_anti_repeat, check_coin_position_limit,
)
from pairs_scanner.core.scoring import (
    calculate_quality_score, calculate_signal_score, calculate_confidence,
    get_adaptive_signal, calculate_ou_score, validate_ou_quality,
    sanitize_pair, assess_entry_readiness, cost_aware_min_z,
)

passed = 0
failed = 0
errors = []

def test(name, condition):
    global passed, failed
    if condition:
        passed += 1
    else:
        failed += 1
        errors.append(name)
        print(f"  FAIL: {name}")

t0 = time.time()
now = datetime.now(MSK)

# ═══════════════════════════════════════════════════════
# RISK TESTS
# ═══════════════════════════════════════════════════════

print("risk/daily_loss_limit...")
test("B-03 live_pnls", check_daily_loss_limit({"E/B": {"session_pnl": -3.0, "date": "2026-03-18"}}, [-1.5, -1.0], -5.0, "2026-03-18")[0])
test("BUG-016 positive ignored", check_daily_loss_limit({"E/B": {"session_pnl": -4.8, "date": "2026-03-18"}}, [+2.0, -0.5], -5.0, "2026-03-18")[0])
test("not blocked under limit", not check_daily_loss_limit({"E/B": {"session_pnl": -2.0, "date": "2026-03-18"}}, [-1.0], -5.0, "2026-03-18")[0])
test("empty not blocked", not check_daily_loss_limit({}, [], -5.0)[0])
test("ignores other dates", not check_daily_loss_limit({"E/B": {"session_pnl": -10, "date": "2026-03-17"}}, [], -5.0, "2026-03-18")[0])
test("exact boundary blocked", check_daily_loss_limit({"X/Y": {"session_pnl": -5.0, "date": "2026-03-18"}}, [], -5.0, "2026-03-18")[0])
test("multi pair sum", check_daily_loss_limit({"A": {"session_pnl": -2.0, "date": "2026-03-18"}, "B": {"session_pnl": -2.5, "date": "2026-03-18"}}, [-1.0], -5.0, "2026-03-18")[0])

print("risk/whitelist (B-01)...")
wl = build_watchlist_pairs([{"coin1": "BTC", "coin2": "ETH", "direction": "LONG"}])
test("B-01 LONG allowed", is_whitelisted("BTC", "ETH", "LONG", wl))
test("B-01 SHORT blocked", not is_whitelisted("BTC", "ETH", "SHORT", wl))
wl2 = build_watchlist_pairs([{"coin1": "BTC", "coin2": "ETH", "direction": "BOTH"}])
test("BOTH allows LONG", is_whitelisted("BTC", "ETH", "LONG", wl2))
test("BOTH allows SHORT", is_whitelisted("BTC", "ETH", "SHORT", wl2))
test("reverse pair", is_whitelisted("ETH", "BTC", "LONG", wl2))
test("unlisted blocked", not is_whitelisted("SOL", "AVAX", "LONG", wl2))
test("no watchlist allows all", is_whitelisted("ANY", "X", "LONG", None))
test("default BOTH", is_whitelisted("BTC", "ETH", "SHORT", build_watchlist_pairs([{"coin1": "BTC", "coin2": "ETH"}])))
test("config wl ok", is_whitelisted("BTC", "ETH", "LONG", None, ["BTC", "ETH"]))
test("config wl blocked", not is_whitelisted("BTC", "SOL", "LONG", None, ["BTC", "ETH"]))

print("risk/position_sizing (B-06)...")
test("B-06 not allowed", not risk_position_size({"grade": "D", "score": 35}, 1000, 3, max_per_trade_pct=20.0, min_per_trade_pct=5.0, max_total_exposure_pct=62.0)["allowed"])
test("grade F blocked", not risk_position_size({"grade": "F", "score": 10}, 1000, 0)["allowed"])
r = risk_position_size({"grade": "A", "score": 90}, 1000, 0, max_per_trade_pct=20.0)
test("grade A full", r["allowed"] and r["size_pct"] == 20.0)
test("position limit", not risk_position_size({"grade": "A", "score": 90}, 1000, 5, max_positions=5)["allowed"])
test("grade B 75%", risk_position_size({"grade": "B", "score": 70}, 1000, 0, max_per_trade_pct=20.0)["size_pct"] == 15.0)
test("grade C 50%", risk_position_size({"grade": "C", "score": 50}, 1000, 0, max_per_trade_pct=20.0, min_per_trade_pct=5.0)["size_pct"] == 10.0)
test("D ok remaining", risk_position_size({"grade": "D", "score": 30}, 1000, 3, max_per_trade_pct=20.0, min_per_trade_pct=5.0, max_total_exposure_pct=80.0)["allowed"])

print("risk/recommend_size (G-02)...")
test("G-02 bonus not blocked", recommend_position_size(85, "HIGH", "🟢 ВХОД", 0.3, 0.8, 100) >= 100)
test("minimum 25", recommend_position_size(10, "LOW", "⚪ ЖДАТЬ", 0.5, 0.1, 100) >= 25)
test("max 150% of base", recommend_position_size(99, "HIGH", "🟢 ВХОД", 0.2, 0.9, 100) <= 150)

print("risk/hr_safe (BUG-014)...")
test("zero blocked", not is_hr_safe(0)[0])
test("None blocked", not is_hr_safe(None)[0])
test("BUG-014 negative ok", is_hr_safe(-1.5)[0])
test("extreme blocked", not is_hr_safe(15.0, max_hr=5.0)[0])
test("normal safe", is_hr_safe(1.2)[0])
test("tiny blocked", not is_hr_safe(0.001, min_hr=0.05)[0])

print("risk/cooldown (LOG-04)...")
cd_sl = {"E/B": {"session_pnl": -3.0, "last_loss_time": (now - timedelta(hours=2)).isoformat(), "sl_exit": True, "consecutive_sl": 0}}
test("SL 12h blocked", check_pair_cooldown("E/B", cd_sl, cooldown_after_sl_hours=12)[0])
cd_exp = {"E/B": {"session_pnl": -3.0, "last_loss_time": (now - timedelta(hours=13)).isoformat(), "sl_exit": True, "consecutive_sl": 0}}
test("SL expired ok", not check_pair_cooldown("E/B", cd_exp, cooldown_after_sl_hours=12)[0])
test("LOG-04 green NO bypass SL", check_pair_cooldown("E/B", cd_sl, entry_label="🟢 ВХОД", cooldown_after_sl_hours=12)[0])
cd_nosl = {"E/B": {"session_pnl": -1.0, "last_loss_time": (now - timedelta(hours=1)).isoformat(), "sl_exit": False, "consecutive_sl": 0}}
test("green bypasses non-SL", not check_pair_cooldown("E/B", cd_nosl, entry_label="🟢 ВХОД", pair_cooldown_hours=4)[0])
cd_2sl = {"E/B": {"session_pnl": -3.0, "last_loss_time": (now - timedelta(hours=5)).isoformat(), "sl_exit": True, "consecutive_sl": 2}}
test("2SL longer cooldown", check_pair_cooldown("E/B", cd_2sl, cooldown_after_2sl_hours=12)[0])
test("unknown pair ok", not check_pair_cooldown("XYZ", {})[0])
cd_small = {"E/B": {"session_pnl": -0.3, "last_loss_time": (now - timedelta(hours=1)).isoformat(), "sl_exit": False, "consecutive_sl": 0}}
test("small loss no cd", not check_pair_cooldown("E/B", cd_small, pair_cooldown_hours=4)[0])

print("risk/anti_repeat...")
cd_ar = {"E/B": {"date": "2026-03-18", "sl_exit": True, "last_dir": "LONG"}}
test("blocks same dir", check_anti_repeat("E/B", "LONG", cd_ar, today_str="2026-03-18")[0])
test("diff dir ok", not check_anti_repeat("E/B", "SHORT", cd_ar, today_str="2026-03-18")[0])
test("green bypasses", not check_anti_repeat("E/B", "LONG", cd_ar, is_green=True, today_str="2026-03-18")[0])

print("risk/coin_limit...")
test("at limit blocks", check_coin_position_limit("ETH", [{"coin1": "ETH", "coin2": "BTC"}, {"coin1": "ETH", "coin2": "SOL"}], 2)[0])
test("below limit ok", not check_coin_position_limit("ETH", [{"coin1": "ETH", "coin2": "BTC"}], 2)[0])

print("risk/pair_memory...")
test("blocked zero wins", pair_memory_is_blocked("E/B", {"trades": 3, "wins": 0})[0])
test("not blocked has wins", not pair_memory_is_blocked("E/B", {"trades": 3, "wins": 1, "total_pnl": 1.0})[0])
test("few trades ok", not pair_memory_is_blocked("E/B", {"trades": 1, "wins": 0, "total_pnl": -1.0})[0])
test("ignore flag", not pair_memory_is_blocked("E/B", {"trades": 5, "wins": 0}, ignore=True)[0])
test("None memory ok", not pair_memory_is_blocked("E/B", None)[0])

print("risk/cascade_sl...")
test("disabled ok", not check_cascade_sl({}, cascade_enabled=False)[0])
# R-02 FIX: cascade state uses pause_start (not pause_until)
_cs_active = {"pause_start": (now - timedelta(hours=1)).isoformat(), "pause_h": 4.0, "sl_count": 3}
test("active pause blocks", check_cascade_sl({}, cascade_state=_cs_active, now=now)[0])
recent = (now - timedelta(hours=1)).isoformat()
test("threshold triggers", check_cascade_sl({"A": {"sl_exit": True, "last_loss_time": recent}, "B": {"sl_exit": True, "last_loss_time": recent}, "C": {"sl_exit": True, "last_loss_time": recent}}, threshold=3)[0])
test("below threshold ok", not check_cascade_sl({"A": {"sl_exit": True, "last_loss_time": recent}, "B": {"sl_exit": True, "last_loss_time": recent}}, threshold=3)[0])

print("types/Position...")
pos = Position.from_dict({"id": 1, "coin1": "ETH", "coin2": "BTC", "direction": "LONG", "unknown_v50": "x"})
test("from_dict ignores unknown", pos.coin1 == "ETH")
test("round trip", Position.from_dict(pos.to_dict()).id == 1)

print("utils/pnl...")
test("long profit", calc_pair_pnl("LONG", 100, 50, 95, 55, 1.0, 0) > 0)
test("zero = 0", calc_pair_pnl("LONG", 0, 50, 95, 55, 1.0) == 0.0)

# ═══════════════════════════════════════════════════════
# R-01/R-02/R-03 REGRESSION TESTS
# ═══════════════════════════════════════════════════════

print("regression/R-01 recommend_position_size...")
test("R-01 Q80+HIGH+green=110", recommend_position_size(85, "HIGH", "🟢 ВХОД", 0.3, 0.8, 100) == 110)
test("R-01 Q60+MED+conditional=70", recommend_position_size(60, "MEDIUM", "🟡 УСЛОВНО", 0.4, 0.5, 100) == 70)
test("R-01 low+wait=25", recommend_position_size(40, "LOW", "⚪ ЖДАТЬ", 0.5, 0.2, 100) == 25)
test("R-01 weak=55", recommend_position_size(70, "MEDIUM", "🟡 СЛАБЫЙ", 0.4, 0.5, 100) == 55)

print("regression/R-02 cascade state keys...")
_r02_start = (now - timedelta(hours=1)).isoformat()
test("R-02 pause_start works", check_cascade_sl({}, cascade_state={"pause_start": _r02_start, "pause_h": 4.0, "sl_count": 3}, now=now)[0])
_r02_old = (now - timedelta(hours=10)).isoformat()
test("R-02 expired ok", not check_cascade_sl({}, cascade_state={"pause_start": _r02_old, "pause_h": 4.0}, now=now)[0])
test("R-02 old key ignored", not check_cascade_sl({}, cascade_state={"pause_until": (now + timedelta(hours=2)).isoformat()}, now=now)[0])

print("regression/R-03 pair_memory heavy loss...")
test("R-03 heavy loss", pair_memory_is_blocked("E/B", {"trades": 4, "wins": 1, "total_pnl": -7.0})[0])
test("R-03 moderate ok", not pair_memory_is_blocked("E/B", {"trades": 4, "wins": 1, "total_pnl": -3.0})[0])

print("regression/R-07 quality hr_unc negative HR...")
_, _bd_pos = calculate_quality_score(0.3, None, 0.01, 0.8, 1.5, hr_std=0.9)
_, _bd_neg = calculate_quality_score(0.3, None, 0.01, 0.8, -1.5, hr_std=0.9)
test("R-07 positive HR penalty=-15", _bd_pos["hr_unc_penalty"] == -15)
test("R-07 negative HR same penalty", _bd_neg["hr_unc_penalty"] == -15)
_, _bd_z = calculate_quality_score(0.3, None, 0.01, 0.8, 0, hr_std=0.9)
test("R-07 zero HR no penalty", _bd_z["hr_unc_penalty"] == 0)

# ═══════════════════════════════════════════════════════
# SCORING TESTS
# ═══════════════════════════════════════════════════════

print("scoring/quality_score (N-01, N-02)...")
q_pass, bd_pass = calculate_quality_score(0.3, None, 0.01, 0.8, 1.5, adf_passed=True, ubt_passed=True)
q_fail, bd_fail = calculate_quality_score(0.3, None, 0.01, 0.8, 1.5, adf_passed=True, ubt_passed=False)
test("N-01 ubt penalty -5", bd_fail["ubt_penalty"] == -5 and bd_pass["ubt_penalty"] == 0)
test("N-01 diff = 5", q_pass - q_fail == 5)
test("N-01 None no penalty", calculate_quality_score(0.3, None, 0.01, 0.8, 1.5, ubt_passed=None)[1]["ubt_penalty"] == 0)
test("N-02 None no crash", isinstance(calculate_quality_score(0.3, None, 0.01, 0.8, None)[0], int))
test("N-02 zero no crash", calculate_quality_score(0.3, None, 0.01, 0.8, 0)[1]["hedge_ratio"] == 0)
test("perfect pair Q>=80", calculate_quality_score(0.25, None, 0.001, 0.95, 1.5, adf_passed=True, n_bars=300)[0] >= 80)
test("FDR smooth", abs(calculate_quality_score(0.3, None, 0.049, 0.8, 1.5)[1]["fdr"] - calculate_quality_score(0.3, None, 0.051, 0.8, 1.5)[1]["fdr"]) <= 2)
test("crossing penalty", calculate_quality_score(0.3, None, 0.01, 0.8, 1.5, crossing_density=0.01)[1]["crossing_penalty"] == -10)
test("data penalty", calculate_quality_score(0.3, None, 0.01, 0.8, 1.5, n_bars=50)[1]["data_penalty"] == -15)
test("hr_unc 0.2=0", calculate_quality_score(0.3, None, 0.01, 0.8, 1.5, hr_std=0.3)[1]["hr_unc_penalty"] == 0)
test("hr_unc 0.6=-15", calculate_quality_score(0.3, None, 0.01, 0.8, 1.5, hr_std=0.9)[1]["hr_unc_penalty"] == -15)
test("hr_unc 0.8=-25", calculate_quality_score(0.3, None, 0.01, 0.8, 1.5, hr_std=1.2)[1]["hr_unc_penalty"] == -25)
test("bounded 0-100", 0 <= calculate_quality_score(0.55, None, 0.5, 0.0, None, crossing_density=0.01, n_bars=50)[0] <= 100)

print("scoring/confidence (N-03)...")
_, c_pos, _ = calculate_confidence(0.3, 0.8, True, True, 2.5, 1.5, hr_std=0.3)
_, c_neg, _ = calculate_confidence(0.3, 0.8, True, True, 2.5, -1.5, hr_std=0.3)
test("N-03 neg HR same checks", c_pos == c_neg)
test("HIGH confidence", calculate_confidence(0.3, 0.9, True, True, 2.5, 1.5, hr_std=0.1)[0] == "HIGH")
test("Hurst gate blocks HIGH", calculate_confidence(0.46, 0.9, True, True, 2.5, 1.5, hr_std=0.1)[0] in ("MEDIUM", "LOW"))
test("Fallback blocks HIGH", calculate_confidence(0.5, 0.9, True, True, 2.5, 1.5, hurst_is_fallback=True, hr_std=0.1)[0] in ("MEDIUM", "LOW"))

print("scoring/signal_score...")
test("capped by quality", calculate_signal_score(3.0, {"halflife_ou": 0.3, "r_squared": 0.2}, "HIGH", quality_score=30)[0] <= 36)
test("Z>5 suspicious", calculate_signal_score(6.0, None, "LOW")[1]["zscore"] == 10)
test("optimal Z=40", calculate_signal_score(3.0, None, "HIGH")[1]["zscore"] == 40)

print("scoring/adaptive_signal...")
test("low Q neutral", get_adaptive_signal(3.0, "HIGH", 25)[0] == "NEUTRAL")
s, d, _ = get_adaptive_signal(-3.0, "HIGH", 80, "4h", stability_ratio=0.9)
test("SIGNAL LONG", s == "SIGNAL" and d == "LONG")
test("direction from Z sign", get_adaptive_signal(-2.5, "HIGH", 80)[1] == "LONG" and get_adaptive_signal(2.5, "HIGH", 80)[1] == "SHORT")
test("hurst penalty raises threshold", get_adaptive_signal(2.0, "HIGH", 80, hurst=0.50)[2] > get_adaptive_signal(2.0, "HIGH", 80, hurst=0.25)[2])

print("scoring/entry_readiness...")
p_good = {"signal": "SIGNAL", "zscore": -2.8, "threshold": 2.0, "quality_score": 75, "direction": "LONG", "fdr_passed": True, "confidence": "HIGH", "signal_score": 65, "correlation": 0.7, "hurst": 0.28, "stability_passed": 4, "stability_total": 4}
test("green entry", assess_entry_readiness(p_good)["label"] == "🟢 ВХОД")
test("wait", assess_entry_readiness({"signal": "NEUTRAL", "zscore": 0.5, "threshold": 2.0, "quality_score": 30, "direction": "NONE"})["label"] == "⚪ ЖДАТЬ")
test("EMA fallback", "🟢" in assess_entry_readiness({**p_good, "hurst": 0.5, "hurst_ema": 0.30})["label"])
test("hurst warning", "УСЛОВНО" in assess_entry_readiness({**p_good, "hurst": 0.47}, min_hurst=0.45)["label"])

print("scoring/sanitize...")
test("zero HR fails", not sanitize_pair(0, 3, 4, 2.0)[0])
test("extreme Z fails", not sanitize_pair(1.5, 3, 4, 12.0)[0])
test("few bars fails", not sanitize_pair(1.5, 3, 4, 2.0, n_bars=30)[0])
test("good pair passes", sanitize_pair(1.5, 3, 4, 2.0, n_bars=200)[0])

print("scoring/cost_min_z...")
test("returns float >= 1.5", cost_aware_min_z(0.005) >= 1.5)
test("higher costs higher z", cost_aware_min_z(0.005, commission_pct=0.20) >= cost_aware_min_z(0.005, commission_pct=0.05))

print("scoring/ou...")
test("OU None = 0", calculate_ou_score(None, 0.3) == 0)
test("OU good > 50", calculate_ou_score({"halflife_ou": 0.5, "r_squared": 0.2, "theta": 1.0}, 0.35) > 50)
test("validate None fails", not validate_ou_quality(None)[0])
test("validate good ok", validate_ou_quality({"theta": 0.5, "halflife_ou": 1.0})[0])

# ═══════════════════════════════════════════════════════

elapsed = time.time() - t0
print()
print(f"{'='*60}")
print(f"  {passed} passed, {failed} failed, {elapsed:.2f}s")
print(f"{'='*60}")

if errors:
    print(f"\nFailed tests:")
    for e in errors:
        print(f"  - {e}")
    sys.exit(1)
else:
    print("\nAll tests passed!")
