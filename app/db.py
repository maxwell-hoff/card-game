from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
import secrets
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
    user_code TEXT UNIQUE, -- short public code for inviting
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_game_results_user_id ON game_results(user_id);
CREATE INDEX IF NOT EXISTS idx_game_results_created_at ON game_results(created_at);

CREATE TABLE IF NOT EXISTS invites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inviter_id INTEGER NOT NULL,
    invitee_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', -- pending | accepted | expired | cancelled
    token TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    FOREIGN KEY(inviter_id) REFERENCES users(id),
    FOREIGN KEY(invitee_id) REFERENCES users(id)
);
CREATE INDEX IF NOT EXISTS idx_invites_invitee_status ON invites(invitee_id, status);
CREATE INDEX IF NOT EXISTS idx_invites_expires_at ON invites(expires_at);

-- Sessions to allow resuming puzzles
CREATE TABLE IF NOT EXISTS game_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    puzzle_id INTEGER NOT NULL,
    expected_turns INTEGER NOT NULL,
    inviter_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'active', -- active | completed
    started_at TEXT NOT NULL,
    completed_at TEXT,
    FOREIGN KEY(puzzle_id) REFERENCES puzzles(id),
    FOREIGN KEY(inviter_id) REFERENCES users(id)
);
CREATE TABLE IF NOT EXISTS game_session_members (
    session_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    PRIMARY KEY(session_id, user_id),
    FOREIGN KEY(session_id) REFERENCES game_sessions(id),
    FOREIGN KEY(user_id) REFERENCES users(id)
);
CREATE INDEX IF NOT EXISTS idx_sessions_status ON game_sessions(status);
CREATE INDEX IF NOT EXISTS idx_sessions_inviter ON game_sessions(inviter_id);
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
    # Ensure invites table and indexes exist
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS invites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inviter_id INTEGER NOT NULL,
            invitee_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            token TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            FOREIGN KEY(inviter_id) REFERENCES users(id),
            FOREIGN KEY(invitee_id) REFERENCES users(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_invites_invitee_status ON invites(invitee_id, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_invites_expires_at ON invites(expires_at)")
    # Ensure sessions tables exist
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS game_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            puzzle_id INTEGER NOT NULL,
            expected_turns INTEGER NOT NULL,
            inviter_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            started_at TEXT NOT NULL,
            completed_at TEXT,
            FOREIGN KEY(puzzle_id) REFERENCES puzzles(id),
            FOREIGN KEY(inviter_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS game_session_members (
            session_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY(session_id, user_id),
            FOREIGN KEY(session_id) REFERENCES game_sessions(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_status ON game_sessions(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_inviter ON game_sessions(inviter_id)")
    # Add session_id column to game_results if missing
    cur.execute("PRAGMA table_info(game_results)")
    cols_gr = [r[1] for r in cur.fetchall()]
    if "session_id" not in cols_gr:
        cur.execute("ALTER TABLE game_results ADD COLUMN session_id INTEGER")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_game_results_session_id ON game_results(session_id)")
    # Add user_code column to users if missing and backfill with unique codes
    cur.execute("PRAGMA table_info(users)")
    cols_users = [r[1] for r in cur.fetchall()]
    if "user_code" not in cols_users:
        cur.execute("ALTER TABLE users ADD COLUMN user_code TEXT")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_code ON users(user_code)")
        conn.commit()
        # Backfill existing users with codes
        cur.execute("SELECT id FROM users WHERE user_code IS NULL")
        to_fill = [int(r[0]) for r in cur.fetchall()]
        for uid in to_fill:
            code = generate_unique_user_code(conn)
            cur.execute("UPDATE users SET user_code = ? WHERE id = ?", (code, uid))
        conn.commit()
    else:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_code ON users(user_code)")
        conn.commit()

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
    # Generate a short unique user_code on first create
    code = generate_unique_user_code(conn)
    cur.execute(
        "INSERT INTO users (user_key, display_name, user_code, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        (user_key, display_name or "Player", code, now, now),
    )
    conn.commit()
    return cur.lastrowid


def generate_unique_user_code(conn: sqlite3.Connection, length: int = 6) -> str:
    """Generate a short, human-friendly, unique code for a user.

    Uses an alphabet without ambiguous characters and ensures uniqueness in DB.
    """
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # no I, O, 1, 0
    cur = conn.cursor()
    while True:
        code = "".join(secrets.choice(alphabet) for _ in range(length))
        cur.execute("SELECT 1 FROM users WHERE user_code = ?", (code,))
        if not cur.fetchone():
            return code


def get_user_by_code(conn: sqlite3.Connection, user_code: str) -> Optional[Tuple[int, str, str]]:
    """Return (id, display_name, user_code) for a user by code, or None."""
    cur = conn.cursor()
    cur.execute("SELECT id, display_name, user_code FROM users WHERE user_code = ?", (user_code.strip().upper(),))
    row = cur.fetchone()
    if not row:
        return None
    return int(row[0]), str(row[1]), str(row[2])


def get_user_code(conn: sqlite3.Connection, user_id: int) -> Optional[str]:
    cur = conn.cursor()
    cur.execute("SELECT user_code FROM users WHERE id = ?", (user_id,))
    row = cur.fetchone()
    return str(row[0]) if row and row[0] else None


def get_all_users(conn: sqlite3.Connection) -> List[Tuple[int, str]]:
    cur = conn.cursor()
    cur.execute("SELECT id, display_name FROM users ORDER BY id ASC")
    rows = cur.fetchall()
    return [(r[0], r[1]) for r in rows]


def get_user_by_display_name(conn: sqlite3.Connection, display_name: str) -> Optional[Tuple[int, str]]:
    cur = conn.cursor()
    cur.execute("SELECT id, display_name FROM users WHERE display_name = ?", (display_name,))
    row = cur.fetchone()
    if not row:
        return None
    return int(row[0]), row[1]


def create_invite(
    conn: sqlite3.Connection,
    inviter_id: int,
    invitee_id: int,
    token: str,
    expires_at_iso: str,
) -> int:
    now = datetime.utcnow().isoformat()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO invites (inviter_id, invitee_id, status, token, created_at, expires_at)
        VALUES (?, ?, 'pending', ?, ?, ?)
        """,
        (inviter_id, invitee_id, token, now, expires_at_iso),
    )
    conn.commit()
    return cur.lastrowid


def expire_old_invites(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        "UPDATE invites SET status = 'expired' WHERE status = 'pending' AND expires_at <= ?",
        (datetime.utcnow().isoformat(),),
    )
    conn.commit()


def get_pending_invites_for_user(conn: sqlite3.Connection, invitee_id: int) -> List[Tuple[int, int, str, str, str]]:
    """Return list of (id, inviter_id, inviter_display, created_at, expires_at) for pending, non-expired invites."""
    expire_old_invites(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT i.id, u.id AS inviter_id, u.display_name, i.created_at, i.expires_at
        FROM invites i
        JOIN users u ON u.id = i.inviter_id
        WHERE i.invitee_id = ? AND i.status = 'pending' AND i.expires_at > ?
        ORDER BY i.created_at DESC, i.id DESC
        """,
        (invitee_id, datetime.utcnow().isoformat()),
    )
    return cur.fetchall()


def accept_invite(conn: sqlite3.Connection, invite_id: int, invitee_id: int) -> bool:
    expire_old_invites(conn)
    cur = conn.cursor()
    # Ensure it belongs to the invitee and is pending
    cur.execute(
        "SELECT status, expires_at FROM invites WHERE id = ? AND invitee_id = ?",
        (invite_id, invitee_id),
    )
    row = cur.fetchone()
    if not row:
        return False
    status, expires_at = row
    if status != 'pending' or expires_at <= datetime.utcnow().isoformat():
        return False
    cur.execute("UPDATE invites SET status = 'accepted' WHERE id = ?", (invite_id,))
    conn.commit()
    return True


def get_party_for_inviter(conn: sqlite3.Connection, inviter_id: int) -> List[Tuple[int, str]]:
    """Return accepted invitees (user_id, display_name) for inviter where invite not expired yet."""
    expire_old_invites(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT u.id, u.display_name
        FROM invites i
        JOIN users u ON u.id = i.invitee_id
        WHERE i.inviter_id = ? AND i.status = 'accepted' AND i.expires_at > ?
        ORDER BY i.created_at ASC, i.id ASC
        """,
        (inviter_id, datetime.utcnow().isoformat()),
    )
    rows = cur.fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]


def create_game_session(
    conn: sqlite3.Connection,
    inviter_id: int,
    puzzle_id: int,
    expected_turns: int,
    member_user_ids: List[int],
) -> int:
    """Create a session, snapshot members (including inviter), and record a started attempt as failed until solved."""
    now = datetime.utcnow().isoformat()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO game_sessions (puzzle_id, expected_turns, inviter_id, status, started_at)
        VALUES (?, ?, ?, 'active', ?)
        """,
        (puzzle_id, expected_turns, inviter_id, now),
    )
    session_id = cur.lastrowid
    # Insert members: inviter + unique invitees
    seen = set()
    all_members = [inviter_id] + [m for m in (member_user_ids or []) if m not in (inviter_id,)]
    for uid in all_members:
        if uid in seen:
            continue
        seen.add(uid)
        cur.execute(
            "INSERT OR IGNORE INTO game_session_members (session_id, user_id) VALUES (?, ?)",
            (session_id, uid),
        )
    # Record a started attempt as unsolved to affect stats until solved
    cur.execute(
        "INSERT INTO game_results (puzzle_id, user_id, solved, seconds, created_at, session_id) VALUES (?, NULL, 0, NULL, ?, ?)",
        (puzzle_id, now, session_id),
    )
    conn.commit()
    return session_id


def get_active_sessions_for_user(
    conn: sqlite3.Connection,
    user_id: int,
) -> List[Tuple[int, int, int, str]]:
    """Return list of (session_id, puzzle_id, expected_turns, started_at) for active sessions where user is inviter or member."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.id, s.puzzle_id, s.expected_turns, s.started_at
        FROM game_sessions s
        LEFT JOIN game_session_members m ON m.session_id = s.id
        WHERE s.status = 'active' AND (s.inviter_id = ? OR m.user_id = ?)
        GROUP BY s.id
        ORDER BY s.started_at DESC, s.id DESC
        """,
        (user_id, user_id),
    )
    return cur.fetchall()


def get_session_members_with_names(
    conn: sqlite3.Connection, session_id: int
) -> List[Tuple[int, str]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT u.id, u.display_name
        FROM game_session_members sm
        JOIN users u ON u.id = sm.user_id
        WHERE sm.session_id = ?
        ORDER BY u.id ASC
        """,
        (session_id,),
    )
    rows = cur.fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]


def get_session_by_id(conn: sqlite3.Connection, session_id: int) -> Optional[Tuple[int, int, int, int, str, Optional[str], str]]:
    """Return (id, puzzle_id, expected_turns, inviter_id, status, completed_at, started_at) or None."""
    cur = conn.cursor()
    cur.execute(
        "SELECT id, puzzle_id, expected_turns, inviter_id, status, completed_at, started_at FROM game_sessions WHERE id = ?",
        (session_id,),
    )
    row = cur.fetchone()
    return row


def complete_session(conn: sqlite3.Connection, session_id: int) -> None:
    cur = conn.cursor()
    cur.execute(
        "UPDATE game_sessions SET status = 'completed', completed_at = ? WHERE id = ?",
        (datetime.utcnow().isoformat(), session_id),
    )
    conn.commit()


def update_session_result(
    conn: sqlite3.Connection,
    session_id: int,
    solved: bool,
    seconds: Optional[int],
    user_id: Optional[int],
) -> None:
    """Update the single game_results row tied to this session. If none exists, insert it."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM game_results WHERE session_id = ?", (session_id,))
    row = cur.fetchone()
    now = datetime.utcnow().isoformat()
    if row:
        cur.execute(
            "UPDATE game_results SET solved = ?, seconds = ?, user_id = ?, created_at = ? WHERE id = ?",
            (1 if solved else 0, seconds, user_id, now, int(row[0])),
        )
    else:
        # fallback insert
        cur.execute(
            "INSERT INTO game_results (puzzle_id, user_id, solved, seconds, created_at, session_id) SELECT puzzle_id, ?, ?, ?, ?, ? FROM game_sessions WHERE id = ?",
            (user_id, 1 if solved else 0, seconds, now, session_id, session_id),
        )
    conn.commit()


def get_user_attempts_wins_including_sessions(
    conn: sqlite3.Connection, user_id: int
) -> Tuple[int, int]:
    """Compute attempts and wins for a user, counting unfinished sessions as failures.

    Attempts = sessions where user is inviter or member (active or completed)
               + legacy results without session (distinct puzzle_id)
    Wins     = sessions completed
               + legacy results without session where solved=1 (distinct puzzle_id)
    """
    cur = conn.cursor()
    # Sessions attempts and wins
    cur.execute(
        """
        SELECT
            SUM(CASE WHEN s.status IN ('active','completed') THEN 1 ELSE 0 END) AS attempts,
            SUM(CASE WHEN s.status = 'completed' THEN 1 ELSE 0 END) AS wins
        FROM game_sessions s
        LEFT JOIN game_session_members m ON m.session_id = s.id
        WHERE (s.inviter_id = ? OR m.user_id = ?)
        """,
        (user_id, user_id),
    )
    row = cur.fetchone() or (0, 0)
    sess_attempts = int(row[0] or 0)
    sess_wins = int(row[1] or 0)
    # Legacy attempts and wins (distinct puzzles) without session
    cur.execute(
        """
        SELECT COUNT(DISTINCT puzzle_id) FROM game_results WHERE user_id = ? AND session_id IS NULL
        """,
        (user_id,),
    )
    legacy_attempts = int((cur.fetchone() or (0,))[0] or 0)
    cur.execute(
        """
        SELECT COUNT(DISTINCT puzzle_id) FROM game_results WHERE user_id = ? AND session_id IS NULL AND solved = 1
        """,
        (user_id,),
    )
    legacy_wins = int((cur.fetchone() or (0,))[0] or 0)
    return sess_attempts + legacy_attempts, sess_wins + legacy_wins


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
