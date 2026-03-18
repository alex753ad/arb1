"""
q_score_simulation.py — Симуляция новой формулы Q-score (Волна 3, п.15)

Читает quality_breakdown_log.jsonl и trade_history.csv,
пересчитывает Q по старой и новой формуле, сравнивает.

Запуск: python q_score_simulation.py
  или:  python q_score_simulation.py --log scan_exports/quality_breakdown_log.jsonl

Вывод:
  1. Таблица: pair | Q_old | Q_new | delta | signal | PnL (если торговалась)
  2. Статистика: WR по квантилям Q, корреляция Q-PnL
  3. Проверка 3 критериев принятия
"""

import json
import csv
import os
import sys
from collections import defaultdict


# ═══════════════════════════════════════════════════════
# СТАРАЯ ФОРМУЛА (для сравнения)
# ═══════════════════════════════════════════════════════

def q_score_old(bd_raw):
    """Пересчитать Q по СТАРОЙ формуле из компонентов breakdown."""
    # Старая формула просто суммировала компоненты как есть
    return max(0, min(100, sum(bd_raw.values())))


# ═══════════════════════════════════════════════════════
# НОВАЯ ФОРМУЛА (v44)
# ═══════════════════════════════════════════════════════

def q_score_new_from_breakdown(bd_raw, pvalue_adj=None, hedge_ratio=None,
                                hr_std=None, hurst_is_fallback=False,
                                stability_score=None, hurst=None, adf_passed=None):
    """Пересчитать Q по НОВОЙ формуле.
    
    Если переданы сырые параметры (pvalue_adj, etc.) — считает с нуля.
    Если только bd_raw — реконструирует из старого breakdown.
    """
    bd = {}
    
    # FDR (20) — непрерывная шкала
    if pvalue_adj is not None:
        bd['fdr'] = int(max(0.0, (0.15 - pvalue_adj) / 0.15) * 20)
    else:
        # Реконструкция: старый fdr был ступенчатым 25/20/12/0
        # Примерная обратная оценка p-value
        old_fdr = bd_raw.get('fdr', 0)
        if old_fdr >= 25:
            est_p = 0.005
        elif old_fdr >= 20:
            est_p = 0.02
        elif old_fdr >= 12:
            est_p = 0.04
        else:
            est_p = 0.20
        bd['fdr'] = int(max(0.0, (0.15 - est_p) / 0.15) * 20)
    
    # Stability (30) — повышен с 25
    old_stab = bd_raw.get('stability', 0)
    if stability_score is not None:
        bd['stability'] = int(stability_score * 30)
    else:
        # Реконструкция: old_stab = stability_score * 25
        est_stab_ratio = old_stab / 25.0 if old_stab > 0 else 0
        bd['stability'] = int(est_stab_ratio * 30)
    
    # Hurst (20)
    if hurst is not None:
        if hurst_is_fallback:
            bd['hurst'] = 10
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
    else:
        old_h = bd_raw.get('hurst', 0)
        if old_h == 5:  # was fallback
            bd['hurst'] = 10
        else:
            bd['hurst'] = old_h  # keep same
    
    # ADF (10) — снижен с 15
    if adf_passed is not None:
        bd['adf'] = 10 if adf_passed else 0
    else:
        old_adf = bd_raw.get('adf', 0)
        bd['adf'] = 10 if old_adf > 0 else 0  # был 15→10
    
    # Hedge ratio (20) — повышен с 15, ужесточены зоны
    if hedge_ratio is not None:
        abs_hr = abs(hedge_ratio)
        if abs_hr == 0 or abs_hr > 30:
            bd['hedge_ratio'] = 0
        elif 0.2 <= abs_hr <= 3.5:
            bd['hedge_ratio'] = 20
        elif 0.1 <= abs_hr <= 5.0:
            bd['hedge_ratio'] = 12
        elif 0.05 <= abs_hr <= 8.0:
            bd['hedge_ratio'] = 5
        else:
            bd['hedge_ratio'] = 0
    else:
        old_hr = bd_raw.get('hedge_ratio', 0)
        # Консервативная оценка: если был 15 (оптимум), даём 20
        if old_hr >= 15:
            bd['hedge_ratio'] = 20
        elif old_hr >= 10:
            bd['hedge_ratio'] = 12
        elif old_hr >= 5:
            bd['hedge_ratio'] = 5
        else:
            bd['hedge_ratio'] = 0
    
    # Модификаторы — берём из старого breakdown
    bd['crossing_penalty'] = bd_raw.get('crossing_penalty', 0)
    bd['data_penalty'] = bd_raw.get('data_penalty', 0)
    
    # HR uncertainty — градуированный штраф
    if hr_std is not None and hedge_ratio and abs(hedge_ratio) > 0:
        _hr_unc = hr_std / abs(hedge_ratio)
        if _hr_unc > 0.7:
            bd['hr_unc_penalty'] = -25
        elif _hr_unc > 0.5:
            bd['hr_unc_penalty'] = -15
        elif _hr_unc > 0.3:
            bd['hr_unc_penalty'] = -8
        else:
            bd['hr_unc_penalty'] = 0
    else:
        bd['hr_unc_penalty'] = bd_raw.get('hr_unc_penalty', 0)
    
    bd['ubt_penalty'] = bd_raw.get('ubt_penalty', 0)
    
    total = max(0, min(100, sum(bd.values())))
    return int(total), bd


# ═══════════════════════════════════════════════════════
# ЗАГРУЗКА ДАННЫХ
# ═══════════════════════════════════════════════════════

def load_breakdown_log(path="scan_exports/quality_breakdown_log.jsonl"):
    """Загрузить лог quality breakdown."""
    entries = []
    if not os.path.exists(path):
        print(f"⚠️  Файл {path} не найден. Запустите 2-3 скана для накопления данных.")
        return entries
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except Exception:
                    pass
    return entries


def load_trade_history(path="trade_history.csv"):
    """Загрузить историю сделок."""
    trades = {}
    if not os.path.exists(path):
        # Попробовать positions.json
        pos_path = "positions.json"
        if os.path.exists(pos_path):
            try:
                with open(pos_path, 'r', encoding='utf-8') as f:
                    positions = json.load(f)
                for p in positions:
                    if p.get('status') != 'CLOSED':
                        continue
                    reason = p.get('exit_reason', '')
                    # Фильтруем только автоматические сделки
                    if reason in ('MANUAL',):
                        continue
                    pair = f"{p.get('coin1', '')}/{p.get('coin2', '')}"
                    pnl = float(p.get('pnl_pct', 0) or 0)
                    if pair not in trades:
                        trades[pair] = []
                    trades[pair].append(pnl)
            except Exception:
                pass
        return trades
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                pair = f"{row.get('coin1', '')}/{row.get('coin2', '')}"
                pnl = float(row.get('pnl_pct', 0) or 0)
                reason = row.get('exit_reason', '')
                if reason in ('MANUAL',):
                    continue
                if pair not in trades:
                    trades[pair] = []
                trades[pair].append(pnl)
    except Exception:
        pass
    return trades


# ═══════════════════════════════════════════════════════
# СИМУЛЯЦИЯ
# ═══════════════════════════════════════════════════════

def run_simulation(log_path="scan_exports/quality_breakdown_log.jsonl"):
    entries = load_breakdown_log(log_path)
    if not entries:
        print("Нет данных для симуляции. Запустите сканер для накопления quality_breakdown_log.jsonl")
        return
    
    trades = load_trade_history()
    
    # Дедупликация по паре (берём последний скан)
    latest = {}
    for e in entries:
        pair = e.get('pair', '')
        if pair:
            latest[pair] = e
    
    print(f"\n{'='*80}")
    print(f"Q-SCORE SIMULATION: {len(latest)} уникальных пар из {len(entries)} записей")
    print(f"{'='*80}\n")
    
    # Таблица сравнения
    results = []
    for pair, e in sorted(latest.items()):
        bd_raw = e.get('quality_bd', {})
        q_old = e.get('quality', q_score_old(bd_raw))
        
        q_new, bd_new = q_score_new_from_breakdown(
            bd_raw,
            pvalue_adj=e.get('pvalue_adj'),
            hedge_ratio=e.get('hedge_ratio'),
            hurst=e.get('hurst'),
        )
        
        delta = q_new - q_old
        pair_trades = trades.get(pair, [])
        avg_pnl = sum(pair_trades) / len(pair_trades) if pair_trades else None
        
        results.append({
            'pair': pair,
            'q_old': q_old,
            'q_new': q_new,
            'delta': delta,
            'signal': e.get('signal', ''),
            'entry': e.get('entry_label', ''),
            'n_trades': len(pair_trades),
            'avg_pnl': avg_pnl,
        })
    
    # Печать таблицы
    print(f"{'Пара':<16} {'Q_old':>5} {'Q_new':>5} {'Δ':>4} {'Signal':<8} {'Trades':>6} {'Avg PnL':>8}")
    print("-" * 65)
    
    status_changes = []
    for r in results:
        pnl_str = f"{r['avg_pnl']:+.2f}%" if r['avg_pnl'] is not None else "  —"
        delta_str = f"{r['delta']:+d}"
        print(f"{r['pair']:<16} {r['q_old']:>5} {r['q_new']:>5} {delta_str:>4} {r['signal']:<8} {r['n_trades']:>6} {pnl_str:>8}")
        
        # Проверка смены статуса
        threshold = 65
        was_above = r['q_old'] >= threshold
        now_above = r['q_new'] >= threshold
        if was_above != now_above:
            status_changes.append(r)
    
    # Статистика
    print(f"\n{'='*80}")
    print("СТАТИСТИКА")
    print(f"{'='*80}\n")
    
    deltas = [r['delta'] for r in results]
    print(f"Средний Δ(Q): {sum(deltas)/len(deltas):+.1f}")
    print(f"Макс рост:    {max(deltas):+d}")
    print(f"Макс падение: {min(deltas):+d}")
    
    # WR по квантилям нового Q
    traded = [r for r in results if r['n_trades'] > 0]
    if traded:
        print(f"\nWin Rate по квантилям Q_new ({len(traded)} торгованных пар):")
        for lo, hi in [(0, 50), (50, 65), (65, 80), (80, 101)]:
            bucket = [r for r in traded if lo <= r['q_new'] < hi]
            if bucket:
                wins = sum(1 for r in bucket if r['avg_pnl'] and r['avg_pnl'] > 0)
                avg = sum(r['avg_pnl'] for r in bucket if r['avg_pnl']) / len(bucket)
                print(f"  Q {lo:>3}-{hi-1:<3}: {len(bucket)} пар, WR={wins/len(bucket)*100:.0f}%, avg={avg:+.2f}%")
    
    # Смена статуса
    if status_changes:
        print(f"\n⚠️  СМЕНА СТАТУСА (порог {threshold}):")
        for r in status_changes:
            direction = "↗ выше порога" if r['q_new'] >= threshold else "↘ ниже порога"
            pnl_str = f"PnL={r['avg_pnl']:+.2f}%" if r['avg_pnl'] is not None else "не торговалась"
            print(f"  {r['pair']}: {r['q_old']}→{r['q_new']} ({direction}) [{pnl_str}]")
    
    # Проверка 3 критериев
    print(f"\n{'='*80}")
    print("ПРОВЕРКА КРИТЕРИЕВ ПРИНЯТИЯ")
    print(f"{'='*80}\n")
    
    # Критерий 1: корреляция Q с PnL
    if traded:
        q_new_vals = [r['q_new'] for r in traded]
        pnl_vals = [r['avg_pnl'] for r in traded]
        high_q = [r for r in traded if r['q_new'] >= 65]
        mid_q = [r for r in traded if 55 <= r['q_new'] < 65]
        if high_q and mid_q:
            wr_high = sum(1 for r in high_q if r['avg_pnl'] and r['avg_pnl'] > 0) / len(high_q)
            wr_mid = sum(1 for r in mid_q if r['avg_pnl'] and r['avg_pnl'] > 0) / len(mid_q)
            c1_pass = wr_high > wr_mid
            print(f"1. WR(Q≥65)={wr_high*100:.0f}% vs WR(Q 55-64)={wr_mid*100:.0f}% → {'✅ PASS' if c1_pass else '❌ FAIL'}")
        else:
            print(f"1. Недостаточно данных для сравнения квантилей → ⏳ SKIP")
    else:
        print("1. Нет торгованных пар → ⏳ SKIP")
    
    # Критерий 2: нет регрессии
    profitable_below_50 = [r for r in traded if r['avg_pnl'] and r['avg_pnl'] > 0 and r['q_new'] < 50]
    total_profitable = [r for r in traded if r['avg_pnl'] and r['avg_pnl'] > 0]
    if total_profitable:
        pct_below = len(profitable_below_50) / len(total_profitable) * 100
        c2_pass = pct_below <= 5
        print(f"2. Прибыльных пар с Q_new<50: {len(profitable_below_50)}/{len(total_profitable)} ({pct_below:.0f}%) → {'✅ PASS' if c2_pass else '❌ FAIL'}")
    else:
        print("2. Нет прибыльных пар → ⏳ SKIP")
    
    # Критерий 3: распределение (двугорбость)
    import statistics
    q_new_all = [r['q_new'] for r in results]
    if len(q_new_all) >= 10:
        stdev = statistics.stdev(q_new_all)
        median = statistics.median(q_new_all)
        # Простая проверка: если stdev > 15 и есть пары в диапазоне 40-90 — нормально
        in_range = sum(1 for q in q_new_all if 40 <= q <= 90)
        pct_in_range = in_range / len(q_new_all) * 100
        c3_pass = pct_in_range >= 60
        print(f"3. Q_new в диапазоне 40-90: {in_range}/{len(q_new_all)} ({pct_in_range:.0f}%), stdev={stdev:.1f} → {'✅ PASS' if c3_pass else '❌ FAIL'}")
    else:
        print(f"3. Мало данных ({len(q_new_all)} пар) → ⏳ SKIP")
    
    print(f"\n{'='*80}")
    print("Для полноценной симуляции накопите данные за 5-7 дней (10+ сканов).")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    path = "scan_exports/quality_breakdown_log.jsonl"
    if len(sys.argv) > 1 and sys.argv[1] == "--log" and len(sys.argv) > 2:
        path = sys.argv[2]
    run_simulation(path)
