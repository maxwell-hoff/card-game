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
    solved INTEGER NOT NULL,
    seconds INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY(puzzle_id) REFERENCES puzzles(id)
);

-- Users table for authentication (no emails/passwords stored)
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_key TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
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


def add_game_result(conn: sqlite3.Connection, puzzle_id: int, solved: bool, seconds: Optional[int]) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO game_results (puzzle_id, solved, seconds, created_at) VALUES (?, ?, ?, ?)",
        (puzzle_id, 1 if solved else 0, seconds, datetime.utcnow().isoformat()),
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
