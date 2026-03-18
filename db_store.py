"""
db_store.py — E-006 FIX: SQLite хранилище для positions, cooldowns, pair_memory.

Заменяет JSON-файлы, решая проблему конкурентного доступа:
  - SQLite WAL mode: несколько reader + один writer одновременно
  - Автоматическая блокировка на уровне БД (без fcntl/flock)
  - Быстрые запросы: SELECT WHERE status='OPEN' вместо загрузки 347KB JSON

Автомиграция: при первом импорте данные из JSON переносятся в SQLite.
Обратная совместимость: API идентичен JSON-функциям (drop-in replacement).

Использование:
    from db_store import (
        db_load_positions, db_save_positions,
        db_load_cooldowns, db_save_cooldowns,
        db_pair_memory_load, db_pair_memory_save,
    )
"""

import sqlite3
import json
import os
import logging
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager

MSK = timezone(timedelta(hours=3))
logger = logging.getLogger("db_store")

# ═══════════════════════════════════════════════════════
# DATABASE PATH
# ═══════════════════════════════════════════════════════

_DB_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_DB_DIR, "trading_data.db")

# ═══════════════════════════════════════════════════════
# CONNECTION POOL
# ═══════════════════════════════════════════════════════

@contextmanager
def _get_conn(readonly=False):
    """Thread-safe connection с autocommit для write.
    D-03 FIX: readonly=True использует SQLite URI mode=ro."""
    if readonly:
        conn = sqlite3.connect(
            f"file:{DB_PATH}?mode=ro",
            uri=True,
            timeout=15,
            isolation_level=None,
        )
    else:
        conn = sqlite3.connect(
            DB_PATH,
            timeout=15,
            isolation_level=None,
        )
    # N-06 FIX: journal_mode=WAL — персистентная настройка, перенесена в _init_db
    conn.execute("PRAGMA busy_timeout=15000")      # 15с retry при lock
    conn.execute("PRAGMA synchronous=NORMAL")      # баланс скорость/надёжность
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════
# SCHEMA
# ═══════════════════════════════════════════════════════

def _init_db():
    """Создать таблицы если не существуют."""
    with _get_conn() as conn:
        # N-06 FIX: WAL — персистентная настройка, достаточно один раз при инициализации
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS positions (
                id          INTEGER PRIMARY KEY,
                status      TEXT NOT NULL DEFAULT 'OPEN',
                coin1       TEXT NOT NULL,
                coin2       TEXT NOT NULL,
                direction   TEXT NOT NULL,
                data        TEXT NOT NULL,
                entry_time  TEXT,
                exit_time   TEXT,
                pnl_pct     REAL,
                auto_opened INTEGER DEFAULT 0,
                entry_label TEXT DEFAULT ''
            );

            CREATE INDEX IF NOT EXISTS idx_positions_status
                ON positions(status);
            CREATE INDEX IF NOT EXISTS idx_positions_coins
                ON positions(coin1, coin2);

            CREATE TABLE IF NOT EXISTS cooldowns (
                pair        TEXT PRIMARY KEY,
                session_pnl REAL DEFAULT 0,
                last_loss_time TEXT,
                last_dir    TEXT,
                date        TEXT,
                sl_exit     INTEGER DEFAULT 0,
                consecutive_sl INTEGER DEFAULT 0,
                data        TEXT
            );

            CREATE TABLE IF NOT EXISTS pair_memory (
                pair        TEXT PRIMARY KEY,
                trades      INTEGER DEFAULT 0,
                wins        INTEGER DEFAULT 0,
                total_pnl   REAL DEFAULT 0,
                avg_hold    REAL DEFAULT 0,
                best_pnl    REAL DEFAULT -999,
                worst_pnl   REAL DEFAULT 999,
                last_trade  TEXT,
                data        TEXT NOT NULL
            );
        """)


# ═══════════════════════════════════════════════════════
# POSITIONS
# ═══════════════════════════════════════════════════════

def db_load_positions(status_filter=None):
    """Загрузить позиции из SQLite.
    
    status_filter: 'OPEN', 'CLOSED', или None (все).
    Возвращает list[dict] — совместимо с JSON load_positions().
    """
    with _get_conn(readonly=True) as conn:
        if status_filter:
            rows = conn.execute(
                "SELECT data FROM positions WHERE status = ? ORDER BY id",
                (status_filter,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT data FROM positions ORDER BY id"
            ).fetchall()
    return [json.loads(row['data']) for row in rows]


def db_save_positions(positions):
    """Полное перезаписывание позиций (совместимо с JSON save_positions).
    
    DAT-01 FIX: Использует UPSERT (INSERT OR REPLACE) вместо DELETE ALL + INSERT ALL.
    Удаляет только позиции, отсутствующие в новом списке (вместо удаления всех).
    Это уменьшает нагрузку на WAL journal и снижает вероятность stale read.
    """
    with _get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            # Собираем ID из нового списка
            new_ids = set()
            for p in positions:
                pid = p.get('id', 0)
                new_ids.add(pid)
                conn.execute(
                    """INSERT OR REPLACE INTO positions (id, status, coin1, coin2, direction,
                       data, entry_time, exit_time, pnl_pct, auto_opened, entry_label)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        pid,
                        p.get('status', 'OPEN'),
                        p.get('coin1', ''),
                        p.get('coin2', ''),
                        p.get('direction', ''),
                        json.dumps(p, ensure_ascii=False, default=str),
                        p.get('entry_time', ''),
                        p.get('exit_time', ''),
                        p.get('pnl_pct'),
                        1 if p.get('auto_opened') else 0,
                        p.get('entry_label', ''),
                    )
                )
            # Удаляем только позиции, которых нет в новом списке
            if new_ids:
                placeholders = ','.join('?' * len(new_ids))
                conn.execute(
                    f"DELETE FROM positions WHERE id NOT IN ({placeholders})",
                    list(new_ids)
                )
            else:
                conn.execute("DELETE FROM positions")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def db_update_position(pos_id, updates):
    """Обновить одну позицию по ID (без полной перезаписи).
    
    B-09 FIX: обновляет ВСЕ dedicated columns, не только status/exit_time/pnl_pct.
    """
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT data FROM positions WHERE id = ?", (pos_id,)
        ).fetchone()
        if not row:
            return False
        data = json.loads(row['data'])
        data.update(updates)
        conn.execute(
            """UPDATE positions SET
                status = ?, data = ?, exit_time = ?, pnl_pct = ?,
                auto_opened = ?, entry_label = ?,
                coin1 = ?, coin2 = ?, direction = ?
               WHERE id = ?""",
            (
                data.get('status', 'OPEN'),
                json.dumps(data, ensure_ascii=False, default=str),
                data.get('exit_time', ''),
                data.get('pnl_pct'),
                1 if data.get('auto_opened') else 0,
                data.get('entry_label', ''),
                data.get('coin1', ''),
                data.get('coin2', ''),
                data.get('direction', ''),
                pos_id,
            )
        )
        return True


def db_get_open_positions():
    """Быстрый запрос только OPEN позиций (без загрузки CLOSED)."""
    return db_load_positions(status_filter='OPEN')


def db_get_next_id():
    """Следующий ID для новой позиции."""
    with _get_conn(readonly=True) as conn:
        row = conn.execute("SELECT MAX(id) as max_id FROM positions").fetchone()
        return (row['max_id'] or 0) + 1


# ═══════════════════════════════════════════════════════
# COOLDOWNS
# ═══════════════════════════════════════════════════════

def db_load_cooldowns():
    """Загрузить cooldowns. Возвращает dict {pair: {...}} — совместимо с JSON."""
    with _get_conn(readonly=True) as conn:
        rows = conn.execute("SELECT pair, data FROM cooldowns").fetchall()
    return {row['pair']: json.loads(row['data']) for row in rows}


def db_save_cooldowns(data):
    """B-05 FIX: UPSERT cooldowns (was DELETE ALL + INSERT ALL).
    Удаляет только записи, отсутствующие в новом наборе (как db_save_positions)."""
    with _get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            new_pairs = set()
            for pair, entry in data.items():
                new_pairs.add(pair)
                conn.execute(
                    """INSERT OR REPLACE INTO cooldowns (pair, session_pnl, last_loss_time,
                       last_dir, date, sl_exit, consecutive_sl, data)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        pair,
                        entry.get('session_pnl', 0),
                        entry.get('last_loss_time'),
                        entry.get('last_dir'),
                        entry.get('date'),
                        1 if entry.get('sl_exit') else 0,
                        entry.get('consecutive_sl', 0),
                        json.dumps(entry, ensure_ascii=False, default=str),
                    )
                )
            # Удаляем только отсутствующие в новом наборе
            if new_pairs:
                placeholders = ','.join('?' * len(new_pairs))
                conn.execute(
                    f"DELETE FROM cooldowns WHERE pair NOT IN ({placeholders})",
                    list(new_pairs)
                )
            else:
                conn.execute("DELETE FROM cooldowns")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def db_update_cooldown(pair, entry):
    """Обновить cooldown для одной пары (upsert)."""
    with _get_conn() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO cooldowns
               (pair, session_pnl, last_loss_time, last_dir, date, sl_exit, consecutive_sl, data)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                pair,
                entry.get('session_pnl', 0),
                entry.get('last_loss_time'),
                entry.get('last_dir'),
                entry.get('date'),
                1 if entry.get('sl_exit') else 0,
                entry.get('consecutive_sl', 0),
                json.dumps(entry, ensure_ascii=False, default=str),
            )
        )


def db_get_cooldown(pair):
    """Получить cooldown для одной пары."""
    with _get_conn(readonly=True) as conn:
        row = conn.execute(
            "SELECT data FROM cooldowns WHERE pair = ?", (pair,)
        ).fetchone()
    return json.loads(row['data']) if row else {}


def db_get_today_cooldowns(today_date):
    """Получить cooldowns только за сегодня (для daily_loss_limit)."""
    with _get_conn(readonly=True) as conn:
        rows = conn.execute(
            "SELECT pair, data FROM cooldowns WHERE date = ?", (today_date,)
        ).fetchall()
    return {row['pair']: json.loads(row['data']) for row in rows}


# ═══════════════════════════════════════════════════════
# PAIR MEMORY
# ═══════════════════════════════════════════════════════

def db_pair_memory_load():
    """Загрузить pair memory. Возвращает dict {pair: {...}} — совместимо с JSON."""
    with _get_conn(readonly=True) as conn:
        rows = conn.execute("SELECT pair, data FROM pair_memory").fetchall()
    return {row['pair']: json.loads(row['data']) for row in rows}


def db_pair_memory_save(data):
    """B-05 FIX: UPSERT pair_memory (was DELETE ALL + INSERT ALL)."""
    with _get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            new_pairs = set()
            for pair, entry in data.items():
                new_pairs.add(pair)
                conn.execute(
                    """INSERT OR REPLACE INTO pair_memory
                       (pair, trades, wins, total_pnl, avg_hold,
                        best_pnl, worst_pnl, last_trade, data)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        pair,
                        entry.get('trades', 0),
                        entry.get('wins', 0),
                        entry.get('total_pnl', 0),
                        entry.get('avg_hold', 0),
                        entry.get('best_pnl', -999),
                        entry.get('worst_pnl', 999),
                        entry.get('last_trade', ''),
                        json.dumps(entry, ensure_ascii=False, default=str),
                    )
                )
            if new_pairs:
                placeholders = ','.join('?' * len(new_pairs))
                conn.execute(
                    f"DELETE FROM pair_memory WHERE pair NOT IN ({placeholders})",
                    list(new_pairs)
                )
            else:
                conn.execute("DELETE FROM pair_memory")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def db_pair_memory_get(pair):
    """Получить pair memory для одной пары."""
    with _get_conn(readonly=True) as conn:
        row = conn.execute(
            "SELECT data FROM pair_memory WHERE pair = ?", (pair,)
        ).fetchone()
    return json.loads(row['data']) if row else None


def db_pair_memory_update(pair, entry):
    """Upsert pair memory для одной пары."""
    with _get_conn() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO pair_memory
               (pair, trades, wins, total_pnl, avg_hold,
                best_pnl, worst_pnl, last_trade, data)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                pair,
                entry.get('trades', 0),
                entry.get('wins', 0),
                entry.get('total_pnl', 0),
                entry.get('avg_hold', 0),
                entry.get('best_pnl', -999),
                entry.get('worst_pnl', 999),
                entry.get('last_trade', ''),
                json.dumps(entry, ensure_ascii=False, default=str),
            )
        )


# ═══════════════════════════════════════════════════════
# MIGRATION: JSON → SQLite
# ═══════════════════════════════════════════════════════

def _migrate_json_to_sqlite():
    """Автомиграция: перенос данных из JSON файлов в SQLite.
    
    Запускается один раз при первом создании БД.
    JSON-файлы НЕ удаляются (backup).
    """
    migrated = []
    
    # Positions
    pos_file = os.path.join(_DB_DIR, "positions.json")
    if os.path.exists(pos_file):
        try:
            with open(pos_file, 'r', encoding='utf-8') as f:
                positions = json.load(f)
            if positions:
                with _get_conn(readonly=True) as conn:
                    count = conn.execute("SELECT COUNT(*) as n FROM positions").fetchone()['n']
                if count == 0:
                    db_save_positions(positions)
                    migrated.append(f"positions: {len(positions)} записей")
        except Exception as e:
            logger.error("Migration positions.json failed: %s", e)
    
    # Cooldowns
    cd_file = os.path.join(_DB_DIR, "pair_cooldowns.json")
    if os.path.exists(cd_file):
        try:
            with open(cd_file, 'r', encoding='utf-8') as f:
                cooldowns = json.load(f)
            if cooldowns:
                with _get_conn(readonly=True) as conn:
                    count = conn.execute("SELECT COUNT(*) as n FROM cooldowns").fetchone()['n']
                if count == 0:
                    db_save_cooldowns(cooldowns)
                    migrated.append(f"cooldowns: {len(cooldowns)} пар")
        except Exception as e:
            logger.error("Migration pair_cooldowns.json failed: %s", e)
    
    # Pair memory
    mem_file = os.path.join(_DB_DIR, "pair_memory.json")
    if os.path.exists(mem_file):
        try:
            with open(mem_file, 'r', encoding='utf-8') as f:
                memory = json.load(f)
            if memory:
                with _get_conn(readonly=True) as conn:
                    count = conn.execute("SELECT COUNT(*) as n FROM pair_memory").fetchone()['n']
                if count == 0:
                    db_pair_memory_save(memory)
                    migrated.append(f"pair_memory: {len(memory)} пар")
        except Exception as e:
            logger.error("Migration pair_memory.json failed: %s", e)
    
    if migrated:
        logger.info("E-006 SQLite migration: %s", ", ".join(migrated))
    
    return migrated


# ═══════════════════════════════════════════════════════
# INIT
# ═══════════════════════════════════════════════════════

_initialized = False
_init_lock = __import__('threading').Lock()  # DAT-02 FIX

def ensure_db():
    """Инициализировать БД и мигрировать данные (один раз). DAT-02 FIX: thread-safe."""
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return
        _init_db()
        _migrate_json_to_sqlite()
        _initialized = True


# Auto-init при импорте
try:
    ensure_db()
except Exception as e:
    logger.error("db_store init failed: %s", e)
