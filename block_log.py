"""
block_log.py — Логирование причин блокировки пар
Roadmap Волна 2, пункт 14: "Логирование причин блокировки каждой пары"

Формат лог-файла: block_log.jsonl (один JSON-объект на строку)
Каждая запись:
{
  "ts":        "2026-03-10T14:23:11+03:00",   # время МСК
  "pair":      "SOL/AVAX",                      # пара
  "direction": "LONG",                           # направление (если известно)
  "source":    "auto_monitor" | "manual",        # откуда пришёл блок
  "reason":    "cooldown 12ч (3.2ч осталось)",  # причина
  "category":  "cooldown" | "hr" | "z" | ...    # категория для агрегации
}

API:
  log_block(pair, reason, source, direction="")
  get_block_stats(hours=24)  → dict с топ-причинами
  get_recent_blocks(n=50)    → list последних записей
  clear_old_blocks(days=7)   → удалить старые записи
"""

import json
import os
from datetime import datetime, timedelta, timezone

# ── константы ──────────────────────────────────────────────────────────────
MSK = timezone(timedelta(hours=3))
BLOCK_LOG_FILE = "block_log.jsonl"
MAX_FILE_SIZE_MB = 5  # ротация при превышении размера


def _now_msk() -> datetime:
    return datetime.now(MSK)


def _categorize(reason: str) -> str:
    """Определить категорию блокировки по тексту причины."""
    r = reason.lower()
    if "cooldown" in r or ("блок" in r and "ч" in r):  # FIX BLOCK_LOG-1: explicit grouping
        return "cooldown"
    if "hr=" in r or "hr " in r or "hr:" in r or "hedge" in r:  # N-07 FIX: specific match
        return "hr"
    if "|z|" in r or "zscore" in r or "z=" in r:
        return "z_filter"
    if "bt fail" in r or "bt:" in r:
        return "bt"
    if "daily" in r or "дневной" in r:
        return "daily_limit"
    if "memory" in r or "mem" in r:
        return "pair_memory"
    if "short-only" in r or "short_only" in r:
        return "direction"
    if "deep_rally" in r or "rally" in r:
        return "rally"
    if "entry_filter" in r or "ждать" in r:
        return "entry_label"
    if "coin" in r and "позици" in r:
        return "coin_limit"
    if "conflict" in r:
        return "coin_conflict"
    if "max_pos" in r or ("лимит" in r and "позиц" in r):  # FIX BLOCK_LOG-1
        return "max_positions"
    if "quality" in r or "q=" in r:
        return "quality"
    if "anti-repeat" in r:
        return "anti_repeat"
    return "other"


def log_block(pair: str, reason: str, source: str = "auto_monitor",
              direction: str = "") -> None:
    """
    Записать одну причину блокировки в лог.

    Args:
        pair:      название пары, напр. "SOL/AVAX"
        reason:    текст причины блокировки
        source:    "auto_monitor" или "manual"
        direction: "LONG" / "SHORT" / "" (если неизвестно)
    """
    try:
        # Ротация по размеру файла (PERF-04 FIX: убран подсчёт строк — O(N) на каждый log_block)
        if os.path.exists(BLOCK_LOG_FILE):
            size_mb = os.path.getsize(BLOCK_LOG_FILE) / 1024 / 1024
            if size_mb > MAX_FILE_SIZE_MB:
                _rotate_log()

        record = {
            "ts":        _now_msk().isoformat(),
            "pair":      pair,
            "direction": direction,
            "source":    source,
            "reason":    reason,
            "category":  _categorize(reason),
        }
        with open(BLOCK_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as _e:
        # E-003 FIX: логируем ошибку вместо молчаливого pass
        import logging as _bl_log
        _bl_log.getLogger("block_log").debug("log_block failed: %s", _e)


def log_blocks_batch(skip_reasons: list, source: str = "auto_monitor",
                     direction_map: dict = None) -> None:
    """
    Записать список причин из _skip_reasons одним вызовом.
    direction_map: {pair_name: direction} — опционально.

    Формат элемента skip_reasons: "PAIR: reason text"
    """
    direction_map = direction_map or {}
    for sr in skip_reasons:
        try:
            if ": " in sr:
                pair_part, reason_part = sr.split(": ", 1)
            else:
                pair_part, reason_part = "", sr
            pair_part = pair_part.strip()
            dir_ = direction_map.get(pair_part, "")
            log_block(pair_part, reason_part.strip(), source=source, direction=dir_)
        except Exception:
            pass


def get_recent_blocks(n: int = 50) -> list:
    """Вернуть последние n записей блокировок (новые первыми)."""
    if not os.path.exists(BLOCK_LOG_FILE):
        return []
    try:
        records = []
        with open(BLOCK_LOG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except Exception:
                        pass
        return list(reversed(records[-n:]))
    except Exception:
        return []


def get_block_stats(hours: int = 24) -> dict:
    """
    Агрегированная статистика за последние N часов.
    Возвращает:
      {
        "total": int,
        "by_category": {"cooldown": 12, "hr": 5, ...},
        "by_pair": {"SOL/AVAX": 6, ...},
        "by_reason": {"cooldown 12ч ...": 4, ...},
        "period_hours": hours
      }
    """
    if not os.path.exists(BLOCK_LOG_FILE):
        return {"total": 0, "by_category": {}, "by_pair": {}, "by_reason": {}, "period_hours": hours}

    cutoff = _now_msk() - timedelta(hours=hours)
    total = 0
    by_cat: dict = {}
    by_pair: dict = {}
    by_reason: dict = {}

    try:
        with open(BLOCK_LOG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    ts = datetime.fromisoformat(rec["ts"])
                    if ts < cutoff:
                        continue
                    total += 1
                    cat = rec.get("category", "other")
                    pair = rec.get("pair", "?")
                    reason = rec.get("reason", "")[:80]  # обрезаем длинные
                    by_cat[cat] = by_cat.get(cat, 0) + 1
                    by_pair[pair] = by_pair.get(pair, 0) + 1
                    by_reason[reason] = by_reason.get(reason, 0) + 1
                except Exception:
                    pass
    except Exception:
        pass

    # Сортировка по убыванию
    by_cat = dict(sorted(by_cat.items(), key=lambda x: -x[1]))
    by_pair = dict(sorted(by_pair.items(), key=lambda x: -x[1])[:20])
    by_reason = dict(sorted(by_reason.items(), key=lambda x: -x[1])[:20])

    return {
        "total": total,
        "by_category": by_cat,
        "by_pair": by_pair,
        "by_reason": by_reason,
        "period_hours": hours,
    }


def clear_old_blocks(days: int = 7) -> int:
    """
    Удалить записи старше N дней.
    Возвращает количество удалённых записей.
    """
    if not os.path.exists(BLOCK_LOG_FILE):
        return 0
    cutoff = _now_msk() - timedelta(days=days)
    kept = []
    removed = 0
    try:
        with open(BLOCK_LOG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    ts = datetime.fromisoformat(rec["ts"])
                    if ts >= cutoff:
                        kept.append(line)
                    else:
                        removed += 1
                except Exception:
                    kept.append(line)
        with open(BLOCK_LOG_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(kept) + ("\n" if kept else ""))
    except Exception:
        pass
    return removed


def _rotate_log() -> None:
    """
    Ротация: block_log.jsonl → block_log.1.jsonl → block_log.2.jsonl → block_log.3.jsonl.
    Старый .3 удаляется. Сохраняет историю за три поколения вместо одного .bak.
    """
    try:
        max_bak = 3
        # сдвигаем: .2 → .3, .1 → .2, текущий → .1
        for i in range(max_bak, 0, -1):
            src = f"{BLOCK_LOG_FILE}.{i - 1}" if i > 1 else BLOCK_LOG_FILE
            dst = f"{BLOCK_LOG_FILE}.{i}"
            if i == max_bak and os.path.exists(dst):
                os.remove(dst)
            if os.path.exists(src):
                os.rename(src, dst)
    except Exception:
        pass


def _auto_cleanup_if_needed() -> None:
    """
    Удаляет записи старше 7 дней, чтобы лог не рос бесконечно.
    Запускается автоматически при импорте модуля, но не чаще раза в 24 часа.
    """
    stamp_file = BLOCK_LOG_FILE + ".cleanup_ts"
    try:
        if os.path.exists(stamp_file):
            mtime = datetime.fromtimestamp(os.path.getmtime(stamp_file), tz=MSK)
            if (_now_msk() - mtime).total_seconds() < 86400:
                return  # уже чистили менее 24ч назад
        clear_old_blocks(days=7)
        with open(stamp_file, "w") as f:
            f.write(_now_msk().isoformat())
    except Exception:
        pass


# LOG-09 FIX: убран вызов _auto_cleanup_if_needed() при импорте модуля.
# В Streamlit каждый rerun переимпортирует модуль — side-effect при import
# создаёт лишнюю I/O нагрузку. Вызывайте block_log.auto_cleanup() явно.
def auto_cleanup():
    """Явный вызов автоочистки. Вызовите вручную из main loop."""
    _auto_cleanup_if_needed()
