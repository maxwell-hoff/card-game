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
    ranked INTEGER NOT NULL DEFAULT 0,
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
    mode TEXT NOT NULL DEFAULT 'quick', -- quick | ranked (source lobby)
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
    mode TEXT NOT NULL DEFAULT 'quick', -- quick | ranked
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
CREATE INDEX IF NOT EXISTS idx_sessions_mode ON game_sessions(mode);
-- Lobby ready state keyed by (inviter_id, mode, user_id)
CREATE TABLE IF NOT EXISTS lobby_ready (
    inviter_id INTEGER NOT NULL,
    mode TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    ready INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(inviter_id, mode, user_id),
    FOREIGN KEY(inviter_id) REFERENCES users(id),
    FOREIGN KEY(user_id) REFERENCES users(id)
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
            mode TEXT NOT NULL DEFAULT 'quick',
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
    # Add mode column to invites if missing
    cur.execute("PRAGMA table_info(invites)")
    cols_invites = [r[1] for r in cur.fetchall()]
    if "mode" not in cols_invites:
        cur.execute("ALTER TABLE invites ADD COLUMN mode TEXT NOT NULL DEFAULT 'quick'")
    # Ensure sessions tables exist
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS game_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            puzzle_id INTEGER NOT NULL,
            expected_turns INTEGER NOT NULL,
            inviter_id INTEGER NOT NULL,
            mode TEXT NOT NULL DEFAULT 'quick',
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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_mode ON game_sessions(mode)")
    # Ensure lobby_ready table exists
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS lobby_ready (
            inviter_id INTEGER NOT NULL,
            mode TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            ready INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(inviter_id, mode, user_id),
            FOREIGN KEY(inviter_id) REFERENCES users(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    # Add session_id column to game_results if missing
    cur.execute("PRAGMA table_info(game_results)")
    cols_gr = [r[1] for r in cur.fetchall()]
    if "session_id" not in cols_gr:
        cur.execute("ALTER TABLE game_results ADD COLUMN session_id INTEGER")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_game_results_session_id ON game_results(session_id)")
    # Add ranked column to game_results if missing
    cur.execute("PRAGMA table_info(game_results)")
    cols_gr2 = [r[1] for r in cur.fetchall()]
    if "ranked" not in cols_gr2:
        cur.execute("ALTER TABLE game_results ADD COLUMN ranked INTEGER NOT NULL DEFAULT 0")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_game_results_ranked ON game_results(ranked)")
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
    mode: str,
    token: str,
    expires_at_iso: str,
) -> int:
    now = datetime.utcnow().isoformat()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO invites (inviter_id, invitee_id, status, mode, token, created_at, expires_at)
        VALUES (?, ?, 'pending', ?, ?, ?, ?)
        """,
        (inviter_id, invitee_id, mode, token, now, expires_at_iso),
    )
    conn.commit()
    return cur.lastrowid


def expire_old_invites(conn: sqlite3.Connection) -> None:
    try:
        cur = conn.cursor()
        # Let SQLite compute current time to avoid Python param issues and clock skew
        cur.execute(
            "UPDATE invites SET status = 'expired' WHERE status = 'pending' AND expires_at <= datetime('now')"
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def get_pending_invites_for_user(conn: sqlite3.Connection, invitee_id: int) -> List[Tuple[int, int, str, str, str, str]]:
    """Return list of (id, inviter_id, inviter_display, mode, created_at, expires_at) for pending, non-expired invites."""
    expire_old_invites(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT i.id, u.id AS inviter_id, u.display_name, i.mode, i.created_at, i.expires_at
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


def get_party_for_inviter(conn: sqlite3.Connection, inviter_id: int, mode: str = 'quick') -> List[Tuple[int, str]]:
    """Return accepted invitees (user_id, display_name) for inviter in a given mode where invite not expired yet."""
    expire_old_invites(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT u.id, u.display_name
        FROM invites i
        JOIN users u ON u.id = i.invitee_id
        WHERE i.inviter_id = ? AND i.mode = ? AND i.status = 'accepted' AND i.expires_at > ?
        ORDER BY i.created_at ASC, i.id ASC
        """,
        (inviter_id, mode, datetime.utcnow().isoformat()),
    )
    rows = cur.fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]


def get_latest_inviter_for_invitee_and_mode(
    conn: sqlite3.Connection, invitee_id: int, mode: str
) -> Optional[int]:
    """Return inviter_id for the most recent accepted, non-expired invite for this invitee in the given mode."""
    expire_old_invites(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT inviter_id
        FROM invites
        WHERE invitee_id = ? AND mode = ? AND status = 'accepted' AND expires_at > ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (invitee_id, mode, datetime.utcnow().isoformat()),
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def lobby_set_ready(
    conn: sqlite3.Connection, inviter_id: int, mode: str, user_id: int, ready: bool
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO lobby_ready (inviter_id, mode, user_id, ready)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(inviter_id, mode, user_id)
        DO UPDATE SET ready=excluded.ready
        """,
        (inviter_id, mode, user_id, 1 if ready else 0),
    )
    conn.commit()


def lobby_get_ready_map(
    conn: sqlite3.Connection, inviter_id: int, mode: str
) -> dict:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_id, ready FROM lobby_ready WHERE inviter_id = ? AND mode = ?
        """,
        (inviter_id, mode),
    )
    rows = cur.fetchall()
    return {int(r[0]): bool(r[1]) for r in rows}


def get_user_by_id(conn: sqlite3.Connection, user_id: int) -> Optional[Tuple[int, str]]:
    cur = conn.cursor()
    cur.execute("SELECT id, display_name FROM users WHERE id = ?", (user_id,))
    row = cur.fetchone()
    if not row:
        return None
    return int(row[0]), str(row[1])


def get_recent_accepted_invites_for_inviter(
    conn: sqlite3.Connection, inviter_id: int, limit: int = 5
) -> List[Tuple[int, int, str]]:
    """Return recent accepted invites for this inviter: (invite_id, invitee_id, mode).

    Uses accepted status and orders by id desc, bounded by non-expired invites.
    """
    expire_old_invites(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT i.id, i.invitee_id, i.mode
        FROM invites i
        WHERE i.inviter_id = ? AND i.status = 'accepted' AND i.expires_at > ?
        ORDER BY i.id DESC
        LIMIT ?
        """,
        (inviter_id, datetime.utcnow().isoformat(), limit),
    )
    rows = cur.fetchall()
    return [(int(r[0]), int(r[1]), str(r[2])) for r in rows]


def cancel_accepted_invite(
    conn: sqlite3.Connection, inviter_id: int, invitee_id: int, mode: str
) -> bool:
    """Cancel an accepted invite (kick/leave) and clear readiness for that invitee.

    Returns True if a row was updated, False otherwise.
    """
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE invites
        SET status = 'cancelled'
        WHERE inviter_id = ? AND invitee_id = ? AND mode = ? AND status = 'accepted'
        """,
        (inviter_id, invitee_id, mode),
    )
    changed = cur.rowcount > 0
    # Clear ready flag for the user in this lobby
    cur.execute(
        "DELETE FROM lobby_ready WHERE inviter_id = ? AND mode = ? AND user_id = ?",
        (inviter_id, mode, invitee_id),
    )
    conn.commit()
    return changed


def lobby_clear_all_ready(
    conn: sqlite3.Connection, inviter_id: int, mode: str
) -> None:
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM lobby_ready WHERE inviter_id = ? AND mode = ?",
        (inviter_id, mode),
    )
    conn.commit()


def create_game_session(
    conn: sqlite3.Connection,
    inviter_id: int,
    puzzle_id: int,
    expected_turns: int,
    member_user_ids: List[int],
    mode: str = 'quick',
) -> int:
    """Create a session, snapshot members (including inviter), and record a started attempt as failed until solved."""
    now = datetime.utcnow().isoformat()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO game_sessions (puzzle_id, expected_turns, inviter_id, mode, status, started_at)
        VALUES (?, ?, ?, ?, 'active', ?)
        """,
        (puzzle_id, expected_turns, inviter_id, mode, now),
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


def get_active_ranked_sessions_for_user(
    conn: sqlite3.Connection,
    user_id: int,
) -> List[Tuple[int, int, int, str]]:
    """Active ranked sessions where user is inviter or member."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.id, s.puzzle_id, s.expected_turns, s.started_at
        FROM game_sessions s
        LEFT JOIN game_session_members m ON m.session_id = s.id
        WHERE s.status = 'active' AND s.mode = 'ranked' AND (s.inviter_id = ? OR m.user_id = ?)
        GROUP BY s.id
        ORDER BY s.started_at DESC, s.id DESC
        """,
        (user_id, user_id),
    )
    return cur.fetchall()


def get_single_active_ranked_session_for_user(
    conn: sqlite3.Connection,
    user_id: int,
) -> Optional[Tuple[int, int, int, str]]:
    rows = get_active_ranked_sessions_for_user(conn, user_id)
    return rows[0] if rows else None


def get_user_ranked_attempts_wins(conn: sqlite3.Connection, user_id: int) -> Tuple[int, int]:
    """Attempts/wins for ranked mode only.

    Attempts = ranked sessions where user is inviter or member (active or completed)
    Wins     = ranked sessions completed with solved=1
    Plus legacy ranked results without session (distinct puzzle_id)
    """
    cur = conn.cursor()
    # Sessions attempts and wins (ranked only). Count each session once and consider solved=1 as win.
    cur.execute(
        """
        SELECT
            SUM(CASE WHEN s.status IN ('active','completed') THEN 1 ELSE 0 END) AS attempts,
            SUM(CASE WHEN s.status = 'completed' AND (
                SELECT MAX(gr.solved) FROM game_results gr WHERE gr.session_id = s.id
            ) = 1 THEN 1 ELSE 0 END) AS wins
        FROM game_sessions s
        WHERE s.mode = 'ranked' AND (s.inviter_id = ? OR EXISTS (
            SELECT 1 FROM game_session_members m WHERE m.session_id = s.id AND m.user_id = ?
        ))
        """,
        (user_id, user_id),
    )
    row = cur.fetchone() or (0, 0)
    sess_attempts = int(row[0] or 0)
    sess_wins = int(row[1] or 0)
    # Legacy ranked results without session (if any)
    cur.execute(
        """
        SELECT COUNT(DISTINCT puzzle_id) FROM game_results WHERE user_id = ? AND session_id IS NULL AND ranked = 1
        """,
        (user_id,),
    )
    legacy_attempts = int((cur.fetchone() or (0,))[0] or 0)
    cur.execute(
        """
        SELECT COUNT(DISTINCT puzzle_id) FROM game_results WHERE user_id = ? AND session_id IS NULL AND ranked = 1 AND solved = 1
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


def get_user_ranked_results_with_puzzle_meta(
    conn: sqlite3.Connection, user_id: int
) -> List[Tuple[str, int, int, Optional[int]]]:
    """Like get_user_results_with_puzzle_meta, but only ranked results.

    Includes both session-bound results (where the session mode is ranked) and legacy results marked ranked=1.
    Only rows attributed to the user_id are counted.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT gr.created_at, gr.solved, p.level, gr.seconds
        FROM game_results gr
        JOIN puzzles p ON p.id = gr.puzzle_id
        LEFT JOIN game_sessions s ON s.id = gr.session_id
        WHERE gr.user_id = ? AND (gr.ranked = 1 OR (s.mode = 'ranked'))
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


def get_user_ranked_history_puzzle_ids(conn: sqlite3.Connection, user_id: int) -> List[int]:
    """List of puzzle_ids the user has participated in via ranked sessions (inviter or member), plus legacy ranked results."""
    cur = conn.cursor()
    # From ranked sessions
    cur.execute(
        """
        SELECT DISTINCT s.puzzle_id
        FROM game_sessions s
        LEFT JOIN game_session_members m ON m.session_id = s.id
        WHERE s.mode = 'ranked' AND (s.inviter_id = ? OR m.user_id = ?)
        """,
        (user_id, user_id),
    )
    ids = {int(r[0]) for r in cur.fetchall()}
    # From legacy ranked results without session
    cur.execute(
        """
        SELECT DISTINCT puzzle_id FROM game_results WHERE user_id = ? AND session_id IS NULL AND ranked = 1
        """,
        (user_id,),
    )
    for r in cur.fetchall():
        ids.add(int(r[0]))
    return sorted(list(ids))


def get_random_puzzle_for_level_and_players_excluding(
    conn: sqlite3.Connection,
    level: int,
    players: int,
    exclude_puzzle_ids: List[int],
) -> Optional[StoredPuzzle]:
    """Return a random puzzle for level and players, excluding specified puzzle ids."""
    cur = conn.cursor()
    if exclude_puzzle_ids:
        qmarks = ",".join(["?"] * len(exclude_puzzle_ids))
        cur.execute(
            f"""
            SELECT id, players, level, num_actions, opponent_row_index, suite_row_index, column_index, start_layout_json, solved_layout_json, actions_json
            FROM puzzles
            WHERE level = ? AND players = ? AND id NOT IN ({qmarks})
            ORDER BY RANDOM() LIMIT 1
            """,
            (level, players, *exclude_puzzle_ids),
        )
    else:
        cur.execute(
            """
            SELECT id, players, level, num_actions, opponent_row_index, suite_row_index, column_index, start_layout_json, solved_layout_json, actions_json
            FROM puzzles
            WHERE level = ? AND players = ?
            ORDER BY RANDOM() LIMIT 1
            """,
            (level, players),
        )
    row = cur.fetchone()
    return StoredPuzzle(*row) if row else None


def update_session_result(
    conn: sqlite3.Connection,
    session_id: int,
    solved: bool,
    seconds: Optional[int],
    user_id: Optional[int],
) -> None:
    """Upsert results for all members of this session, including the host.

    Note: user_id parameter is ignored for attribution; results are attributed
    to the session members list so that both inviter (host) and invitees
    receive the outcome. This ensures solo sessions also record results.
    """
    cur = conn.cursor()
    # Determine mode, puzzle, and inviter for this session
    cur.execute("SELECT mode, puzzle_id, inviter_id FROM game_sessions WHERE id = ?", (session_id,))
    sess_row = cur.fetchone()
    sess_mode = str(sess_row[0]) if sess_row else 'quick'
    sess_puzzle_id = int(sess_row[1]) if sess_row else None
    sess_inviter_id = int(sess_row[2]) if sess_row else None

    if sess_puzzle_id is None:
        return

    # Fetch session members
    cur.execute("SELECT user_id FROM game_session_members WHERE session_id = ?", (session_id,))
    member_rows = [int(r[0]) for r in cur.fetchall()]
    # Attribute results to all members; ensure inviter is included even if not present in member rows
    target_user_ids = list(dict.fromkeys(member_rows + ([sess_inviter_id] if sess_inviter_id is not None else [])))

    now = datetime.utcnow().isoformat()
    ranked_flag = 1 if sess_mode == 'ranked' else 0

    for uid in target_user_ids:
        # If a row exists for this session+uid, update it; otherwise insert
        cur.execute("SELECT id FROM game_results WHERE session_id = ? AND user_id = ?", (session_id, uid))
        row = cur.fetchone()
        if row:
            cur.execute(
                "UPDATE game_results SET solved = ?, seconds = ?, ranked = ?, created_at = ? WHERE id = ?",
                (1 if solved else 0, seconds, ranked_flag, now, int(row[0])),
            )
        else:
            cur.execute(
                "INSERT INTO game_results (puzzle_id, user_id, solved, seconds, ranked, created_at, session_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (sess_puzzle_id, uid, 1 if solved else 0, seconds, ranked_flag, now, session_id),
            )
    conn.commit()


def get_active_quick_sessions_for_user(
    conn: sqlite3.Connection,
    user_id: int,
) -> List[Tuple[int, int, int, str]]:
    """Active quick sessions where user is inviter or member."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.id, s.puzzle_id, s.expected_turns, s.started_at
        FROM game_sessions s
        LEFT JOIN game_session_members m ON m.session_id = s.id
        WHERE s.status = 'active' AND s.mode = 'quick' AND (s.inviter_id = ? OR m.user_id = ?)
        GROUP BY s.id
        ORDER BY s.started_at DESC, s.id DESC
        """,
        (user_id, user_id),
    )
    return cur.fetchall()


def get_user_attempts_wins_including_sessions(
    conn: sqlite3.Connection, user_id: int
) -> Tuple[int, int]:
    """Compute attempts and wins for a user, counting unfinished sessions as failures.

    Attempts = sessions where user is inviter or member (active or completed)
               + legacy results without session (distinct puzzle_id)
    Wins     = sessions completed with a solved=1 result
               + legacy results without session where solved=1 (distinct puzzle_id)
    """
    cur = conn.cursor()
    # Sessions attempts and wins (all modes). Count each session once and consider solved=1 as win.
    cur.execute(
        """
        SELECT
            SUM(CASE WHEN s.status IN ('active','completed') THEN 1 ELSE 0 END) AS attempts,
            SUM(CASE WHEN s.status = 'completed' AND (
                SELECT MAX(gr.solved) FROM game_results gr WHERE gr.session_id = s.id
            ) = 1 THEN 1 ELSE 0 END) AS wins
        FROM game_sessions s
        WHERE (s.inviter_id = ? OR EXISTS (
            SELECT 1 FROM game_session_members m WHERE m.session_id = s.id AND m.user_id = ?
        ))
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
