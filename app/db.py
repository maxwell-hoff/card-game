from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Tuple
from datetime import datetime

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS puzzles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    players INTEGER NOT NULL,
    level INTEGER NOT NULL,
    num_actions INTEGER NOT NULL,
    opponent_row_index INTEGER NOT NULL,
    suite_row_index INTEGER,
    column_index INTEGER,
    start_layout_json TEXT NOT NULL,
    solved_layout_json TEXT NOT NULL,
    actions_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_puzzles_level_players ON puzzles(level, players);

CREATE TABLE IF NOT EXISTS game_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    puzzle_id INTEGER NOT NULL,
    user_id INTEGER,
    solved INTEGER NOT NULL,
    seconds INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY(puzzle_id) REFERENCES puzzles(id),
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_key TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_game_results_user_id ON game_results(user_id);
CREATE INDEX IF NOT EXISTS idx_game_results_created_at ON game_results(created_at);
"""


def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    return conn


def init_db(db_path: str) -> None:
    conn = get_conn(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def ensure_migrations(conn: sqlite3.Connection) -> None:
    """Apply lightweight migrations for existing databases.

    - Add user_id column to game_results if missing
    - Add helpful indexes
    """
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(game_results)")
    cols = [r[1] for r in cur.fetchall()]
    if "user_id" not in cols:
        cur.execute("ALTER TABLE game_results ADD COLUMN user_id INTEGER")
        conn.commit()
    # Create indexes (IF NOT EXISTS is safe even if they already exist)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_game_results_user_id ON game_results(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_game_results_created_at ON game_results(created_at)")
    conn.commit()


@dataclass
class StoredPuzzle:
    id: int
    players: int
    level: int
    num_actions: int
    opponent_row_index: int
    suite_row_index: Optional[int]
    column_index: Optional[int]
    start_layout_json: str
    solved_layout_json: str
    actions_json: str


def insert_puzzle(
    conn: sqlite3.Connection,
    players: int,
    level: int,
    num_actions: int,
    opponent_row_index: int,
    suite_row_index: Optional[int],
    column_index: Optional[int],
    start_layout_json: str,
    solved_layout_json: str,
    actions_json: str,
) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO puzzles (
            players, level, num_actions, opponent_row_index, suite_row_index,
            column_index, start_layout_json, solved_layout_json, actions_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            players,
            level,
            num_actions,
            opponent_row_index,
            suite_row_index,
            column_index,
            start_layout_json,
            solved_layout_json,
            actions_json,
            datetime.utcnow().isoformat(),
        ),
    )
    conn.commit()
    return cur.lastrowid


def get_random_puzzle(conn: sqlite3.Connection) -> Optional[StoredPuzzle]:
    cur = conn.cursor()
    cur.execute(
        "SELECT id, players, level, num_actions, opponent_row_index, suite_row_index, column_index, start_layout_json, solved_layout_json, actions_json FROM puzzles ORDER BY RANDOM() LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return None
    return StoredPuzzle(*row)


def get_random_puzzle_for_players(conn: sqlite3.Connection, players: int) -> Optional[StoredPuzzle]:
    cur = conn.cursor()
    cur.execute(
        "SELECT id, players, level, num_actions, opponent_row_index, suite_row_index, column_index, start_layout_json, solved_layout_json, actions_json FROM puzzles WHERE players=? ORDER BY RANDOM() LIMIT 1",
        (players,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return StoredPuzzle(*row)


def get_random_puzzle_for_filters(conn: sqlite3.Connection, players: Optional[int], turns: Optional[int]) -> Optional[StoredPuzzle]:
    cur = conn.cursor()
    if players is not None and turns is not None:
        cur.execute(
            "SELECT id, players, level, num_actions, opponent_row_index, suite_row_index, column_index, start_layout_json, solved_layout_json, actions_json FROM puzzles WHERE players=? AND num_actions=? ORDER BY RANDOM() LIMIT 1",
            (players, turns),
        )
    elif players is not None:
        cur.execute(
            "SELECT id, players, level, num_actions, opponent_row_index, suite_row_index, column_index, start_layout_json, solved_layout_json, actions_json FROM puzzles WHERE players=? ORDER BY RANDOM() LIMIT 1",
            (players,),
        )
    elif turns is not None:
        cur.execute(
            "SELECT id, players, level, num_actions, opponent_row_index, suite_row_index, column_index, start_layout_json, solved_layout_json, actions_json FROM puzzles WHERE num_actions=? ORDER BY RANDOM() LIMIT 1",
            (turns,),
        )
    else:
        cur.execute(
            "SELECT id, players, level, num_actions, opponent_row_index, suite_row_index, column_index, start_layout_json, solved_layout_json, actions_json FROM puzzles ORDER BY RANDOM() LIMIT 1"
        )
    row = cur.fetchone()
    if not row:
        return None
    return StoredPuzzle(*row)


def get_puzzle_by_id(conn: sqlite3.Connection, puzzle_id: int) -> Optional[StoredPuzzle]:
    cur = conn.cursor()
    cur.execute(
        "SELECT id, players, level, num_actions, opponent_row_index, suite_row_index, column_index, start_layout_json, solved_layout_json, actions_json FROM puzzles WHERE id=?",
        (puzzle_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return StoredPuzzle(*row)


def add_game_result(
    conn: sqlite3.Connection,
    puzzle_id: int,
    solved: bool,
    seconds: Optional[int],
    user_id: Optional[int] = None,
) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO game_results (puzzle_id, user_id, solved, seconds, created_at) VALUES (?, ?, ?, ?, ?)",
        (puzzle_id, user_id, 1 if solved else 0, seconds, datetime.utcnow().isoformat()),
    )
    conn.commit()


def get_puzzle_stats(conn: sqlite3.Connection, puzzle_id: int) -> Tuple[int, int]:
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*), SUM(solved) FROM game_results WHERE puzzle_id=?",
        (puzzle_id,),
    )
    count, solved = cur.fetchone()
    return count or 0, solved or 0


def ensure_user(conn: sqlite3.Connection, user_key: str, display_name: str) -> int:
    """Create or update a user by opaque user_key. Returns user id.

    user_key should be a non-PII stable identifier (e.g., HMAC of Firebase UID).
    """
    cur = conn.cursor()
    cur.execute("SELECT id, display_name FROM users WHERE user_key=?", (user_key,))
    row = cur.fetchone()
    now = datetime.utcnow().isoformat()
    if row:
        user_id, existing_display = row
        if existing_display != display_name and display_name:
            cur.execute(
                "UPDATE users SET display_name=?, updated_at=? WHERE id=?",
                (display_name, now, user_id),
            )
            conn.commit()
        return user_id
    cur.execute(
        "INSERT INTO users (user_key, display_name, created_at, updated_at) VALUES (?, ?, ?, ?)",
        (user_key, display_name or "Player", now, now),
    )
    conn.commit()
    return cur.lastrowid


def get_all_users(conn: sqlite3.Connection) -> List[Tuple[int, str]]:
    cur = conn.cursor()
    cur.execute("SELECT id, display_name FROM users ORDER BY id ASC")
    rows = cur.fetchall()
    return [(r[0], r[1]) for r in rows]


def get_user_results_with_puzzle_meta(
    conn: sqlite3.Connection, user_id: int
) -> List[Tuple[str, int, int, Optional[int]]]:
    """Return list of (created_at_iso, solved_int, level, seconds) for a user, ordered by time asc."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT gr.created_at, gr.solved, p.level, gr.seconds
        FROM game_results gr
        JOIN puzzles p ON p.id = gr.puzzle_id
        WHERE gr.user_id = ?
        ORDER BY gr.created_at ASC, gr.id ASC
        """,
        (user_id,),
    )
    return cur.fetchall()


def get_user_basic_stats(
    conn: sqlite3.Connection,
    user_id: int,
    recent_days: int,
) -> Tuple[int, int, Optional[float], Optional[float], Optional[float]]:
    """Return (attempts, wins, win_rate_pct, avg_level_all, avg_level_recent)."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*), SUM(solved)
        FROM game_results
        WHERE user_id = ?
        """,
        (user_id,),
    )
    row = cur.fetchone()
    attempts = int(row[0] or 0)
    wins = int(row[1] or 0)
    win_rate = (wins / attempts * 100.0) if attempts else None
    cur.execute(
        """
        SELECT AVG(p.level)
        FROM game_results gr
        JOIN puzzles p ON p.id = gr.puzzle_id
        WHERE gr.user_id = ?
        """,
        (user_id,),
    )
    row2 = cur.fetchone()
    avg_level_all = float(row2[0]) if row2 and row2[0] is not None else None
    cur.execute(
        """
        SELECT AVG(p.level)
        FROM game_results gr
        JOIN puzzles p ON p.id = gr.puzzle_id
        WHERE gr.user_id = ? AND gr.created_at >= datetime('now', ?)
        """,
        (user_id, f'-{int(recent_days)} days'),
    )
    row3 = cur.fetchone()
    avg_level_recent = float(row3[0]) if row3 and row3[0] is not None else None
    return attempts, wins, win_rate, avg_level_all, avg_level_recent
