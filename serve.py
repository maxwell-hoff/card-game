from __future__ import annotations

import argparse
import json
from typing import Optional
import secrets
from datetime import datetime, timedelta
import os

from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from app.actions import humanize_actions_dicts, Action

from app.db import (
    get_conn,
    get_random_puzzle,
    add_game_result,
    get_puzzle_stats,
    get_random_puzzle_for_filters,
    get_puzzle_by_id,
    ensure_user,
    SCHEMA,
    ensure_migrations,
    get_all_users,
    get_user_results_with_puzzle_meta,
    get_user_basic_stats,
    get_user_attempts_wins_including_sessions,
    get_user_by_display_name,
    create_invite,
    get_pending_invites_for_user,
    accept_invite,
    get_party_for_inviter,
    create_game_session,
    get_active_sessions_for_user,
    get_session_members_with_names,
    get_session_by_id,
    complete_session,
    update_session_result,
    get_user_by_code,
    get_user_code,
    get_user_ranked_results_with_puzzle_meta,
    get_user_ranked_history_puzzle_ids,
    get_single_active_ranked_session_for_user,
    get_random_puzzle_for_level_and_players_excluding,
    get_latest_inviter_for_invitee_and_mode,
    lobby_set_ready,
    lobby_get_ready_map,
    get_active_quick_sessions_for_user,
    cancel_accepted_invite,
    lobby_clear_all_ready,
)
 
def inviter_display_name(conn, inviter_id: int) -> str:
    cur = conn.cursor()
    cur.execute("SELECT display_name FROM users WHERE id = ?", (inviter_id,))
    row = cur.fetchone()
    return str(row[0]) if row and row[0] else "Host"
from app.elo import EloConfig, compute_user_elo, RankedDifficultyConfig, select_ranked_level

import firebase_admin
from firebase_admin import auth as fb_auth, credentials
from google.auth.transport import requests as google_auth_requests
from google.oauth2 import id_token as google_id_token
def _normalize_layout_and_actions(start_layout_json: str, actions_json: str, opponent_row_index: int):
    """Return (layout_dict, actions_list, new_opponent_row_index) with rows reordered so that
    the non-player (opponent) row is at index 0, followed by player rows in ascending original order.
    Action row indices are remapped to the new ordering. Column actions remain unchanged.
    """
    try:
        layout = json.loads(start_layout_json) if start_layout_json else None
    except Exception:
        layout = None
    try:
        actions = json.loads(actions_json) if actions_json else None
    except Exception:
        actions = None
    if not layout or not isinstance(layout, dict):
        return layout, actions if isinstance(actions, list) else [], 0
    rows = list(layout.get("rows") or [])
    row_res = list(layout.get("row_reserves") or [])
    # If invalid or single row, nothing to do
    if not rows or opponent_row_index is None or opponent_row_index < 0 or opponent_row_index >= len(rows):
        return layout, (actions if isinstance(actions, list) else []), int(layout.get("opponent_row_index", 0) or 0)

    # Build new index mapping: old -> new
    num_rows = len(rows)
    new_order = [opponent_row_index] + [i for i in range(num_rows) if i != opponent_row_index]
    old_to_new = {old: new for new, old in enumerate(new_order)}

    # Reorder rows and row reserves
    new_rows = [rows[old] for old in new_order]
    new_row_res = [row_res[old] for old in new_order] if row_res and len(row_res) == num_rows else row_res

    # Update layout
    layout_norm = dict(layout)
    layout_norm["rows"] = new_rows
    if new_row_res:
        layout_norm["row_reserves"] = new_row_res
    layout_norm["opponent_row_index"] = 0

    # Remap actions row_index if present
    actions_norm = []
    if isinstance(actions, list):
        for a in actions:
            if not isinstance(a, dict):
                actions_norm.append(a)
                continue
            t = a.get("type")
            p = dict(a.get("params") or {})
            if t in ("row_left", "row_right", "swap"):
                if "row_index" in p:
                    try:
                        old_idx = int(p.get("row_index"))
                        p["row_index"] = int(old_to_new.get(old_idx, old_idx))
                    except Exception:
                        pass
            # swap also has i, j unaffected; col_up/down unaffected
            actions_norm.append({"type": t, "params": p})
    else:
        actions_norm = []

    return layout_norm, actions_norm, 0


def create_app(db_path: str) -> Flask:
    app = Flask(__name__)
    # Load optional JSON config (fallback to env vars)
    app_config = {}
    cfg_path = os.environ.get("APP_CONFIG_JSON", "config.json")
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r") as f:
                app_config = json.load(f)
        except Exception as e:
            app_config = {}
            print("[AUTH] Failed to load app config JSON:", cfg_path, e)
    else:
        print("[AUTH] App config JSON not found:", cfg_path)

    # Secret key for session cookies; prefer env, then config, then default
    app.secret_key = (
        os.environ.get("FLASK_SECRET_KEY")
        or app_config.get("FLASK_SECRET_KEY")
        or "dev-secret-change-me"
    )

    conn = get_conn(db_path)
    # Ensure schema exists and run light migrations (including users)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    except Exception:
        pass
    try:
        ensure_migrations(conn)
    except Exception as e:
        print("[DB] Migrations failed:", e)

    # Initialize Firebase Admin if credentials are present or ADC is configured.
    # If neither is available, we will fallback to verifying ID tokens with google-oauth public certs.
    if not firebase_admin._apps:
        raw_cred_path = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON") or app_config.get("FIREBASE_SERVICE_ACCOUNT_JSON")
        cred_path = None
        if raw_cred_path:
            # Expand user and make relative paths relative to home, since config.json likely stores relative to $HOME
            expanded = os.path.expanduser(raw_cred_path)
            if not os.path.isabs(expanded):
                expanded = os.path.join(os.path.expanduser('~'), expanded)
            cred_path = expanded
        try:
            if cred_path and os.path.exists(cred_path):
                print("[AUTH] Initializing Firebase Admin with service account:", cred_path)
                firebase_admin.initialize_app(credentials.Certificate(cred_path))
            else:
                print("[AUTH] No service account JSON found. Attempting ADC (env/application default credentials)...")
                firebase_admin.initialize_app()
        except Exception as e:
            # Proceed without admin app
            print("[AUTH] Firebase Admin initialization failed; will fallback to public cert verification:", e)

    # Expose Firebase web config and current session user to templates
    @app.context_processor
    def inject_globals():
        firebase_config = {
            "apiKey": os.environ.get("FIREBASE_WEB_API_KEY") or app_config.get("FIREBASE_WEB_API_KEY", ""),
            "authDomain": os.environ.get("FIREBASE_AUTH_DOMAIN") or app_config.get("FIREBASE_AUTH_DOMAIN", ""),
            "projectId": os.environ.get("FIREBASE_PROJECT_ID") or app_config.get("FIREBASE_PROJECT_ID", ""),
            "appId": os.environ.get("FIREBASE_APP_ID") or app_config.get("FIREBASE_APP_ID", ""),
        }
        # Minimal server-side debug print (won't include secrets)
        print("[AUTH] Injecting Firebase web config:", {
            "hasApiKey": bool(firebase_config.get("apiKey")),
            "authDomain": firebase_config.get("authDomain"),
            "projectId": firebase_config.get("projectId"),
            "hasAppId": bool(firebase_config.get("appId")),
        })
        return {
            "firebase_config": firebase_config,
            "session_user": session.get("user"),
        }

    @app.route("/")
    def index():
        # Always show landing page, regardless of auth
        return render_template("landing.html")

    @app.route("/play")
    def play():
        # Require sign-in
        if not session.get("user"):
            return redirect(url_for("index"))
        current_user = session.get("user")
        inviter_id = current_user.get("id") if current_user else None
        # Use ranked lobby/invites to drive party selection
        host_id = inviter_id
        if inviter_id:
            latest_inviter = get_latest_inviter_for_invitee_and_mode(conn, inviter_id, 'ranked')
            if latest_inviter:
                host_id = latest_inviter
        party_rows = get_party_for_inviter(conn, host_id, mode='ranked') if host_id else []
        party = [{"user_id": host_id, "display_name": inviter_display_name(conn, host_id)}] + [{"user_id": r[0], "display_name": r[1]} for r in party_rows]
        requested = request.args.get("go") == "1"
        session_q = request.args.get("session_id")
        players_q = request.args.get("players")
        # Players = selected override but cannot be less than party size
        invited_count = len(party_rows)
        try:
            players_selected = int(players_q) if players_q else None
        except Exception:
            players_selected = None
        min_party = max(1, min(5, 1 + invited_count))
        players_filter = min_party if players_selected is None else max(min_party, min(5, max(1, players_selected)))

        # ELO across all results? Requirement: ELO applied to all games. We'll compute using ranked history for difficulty, but all games are ranked.
        ranked_rows = get_user_results_with_puzzle_meta(conn, inviter_id)
        elo_cfg = EloConfig()
        elo_value = compute_user_elo(ranked_rows, elo_cfg)

        # Resume specific session
        if session_q:
            try:
                resume_session_id = int(session_q)
            except Exception:
                return redirect(url_for("play"))
            sess = get_session_by_id(conn, resume_session_id)
            if not sess:
                return redirect(url_for("play"))
            sess_id, sess_puzzle_id, sess_expected_turns, sess_inviter_id, sess_status, sess_completed_at, sess_started_at = sess
            if sess_status != 'active':
                return redirect(url_for("play"))
            member_rows = get_session_members_with_names(conn, sess_id)
            member_ids = [m[0] for m in member_rows]
            if inviter_id not in ([sess_inviter_id] + member_ids):
                return redirect(url_for("play"))
            sp = get_puzzle_by_id(conn, sess_puzzle_id)
            if not sp:
                return redirect(url_for("play"))
            layout, _actions_unused, _opp_idx = _normalize_layout_and_actions(sp.start_layout_json, sp.actions_json, sp.opponent_row_index)
            solved_layout = json.loads(sp.solved_layout_json)
            count, solved = get_puzzle_stats(conn, sp.id)
            solve_pct = (solved / count * 100.0) if count else None
            players_count_from_session = max(1, min(5, len(member_rows)))
            return render_template(
                "play.html",
                puzzle=sp,
                layout=layout,
                expected_turns=sp.num_actions,
                solve_pct=solve_pct,
                requested=True,
                require_login=False,
                party=[{"user_id": uid, "display_name": name} for (uid, name) in member_rows if uid != inviter_id],
                players_count=players_count_from_session,
                session_id=sess_id,
                solved_layout=solved_layout,
                elo=elo_value,
                host_id=sess_inviter_id,
                is_host=bool(inviter_id == sess_inviter_id),
            )

        # If not requested, show lobby and single resumable session if any
        if not requested:
            rows = get_active_sessions_for_user(conn, inviter_id) if inviter_id else []
            active = []
            for sid, pid, exp_turns, started_at in rows:
                members = get_session_members_with_names(conn, sid)
                players_cnt = max(1, min(5, len(members)))
                other_names = [name for (uid, name) in members if uid != inviter_id]
                active.append({
                    "session_id": sid,
                    "puzzle_id": pid,
                    "players_count": players_cnt,
                    "others": other_names,
                    "expected_turns": exp_turns,
                    "started_at": started_at,
                })
            return render_template(
                "play.html",
                puzzle=None,
                requested=False,
                require_login=False,
                party=party,
                players_count=players_filter,
                active_sessions=active,
                elo=elo_value,
            )

        # Enforce single active session per user
        existing_rows = get_active_sessions_for_user(conn, host_id) if host_id else []
        if existing_rows:
            sid, pid, exp_turns, _started_at = existing_rows[0]
            return redirect(url_for("play", session_id=int(sid)))

        # Select difficulty from ELO and pick puzzle
        diff_cfg = RankedDifficultyConfig()
        level_choice = select_ranked_level(ranked_rows, elo_value, diff_cfg)
        exclude_ids = get_user_ranked_history_puzzle_ids(conn, inviter_id)
        puzzle = get_random_puzzle_for_level_and_players_excluding(conn, level_choice, players_filter, exclude_ids)
        if not puzzle:
            tried = set([level_choice])
            for delta in [1, -1, 2, -2, 3, -3]:
                cand = max(diff_cfg.min_level, min(diff_cfg.max_level, level_choice + delta))
                if cand in tried:
                    continue
                tried.add(cand)
                puzzle = get_random_puzzle_for_level_and_players_excluding(conn, cand, players_filter, exclude_ids)
                if puzzle:
                    break
        if not puzzle:
            return render_template(
                "play.html",
                puzzle=None,
                requested=True,
                require_login=False,
                party=party,
                players_count=players_filter,
                active_sessions=[],
                elo=elo_value,
                note="No suitable puzzle found. Please generate more puzzles.",
            )
        layout, _actions_unused, _opp_idx = _normalize_layout_and_actions(puzzle.start_layout_json, puzzle.actions_json, puzzle.opponent_row_index)
        solved_layout = json.loads(puzzle.solved_layout_json)
        count, solved = get_puzzle_stats(conn, puzzle.id)
        solve_pct = (solved / count * 100.0) if count else None
        member_ids = [m[0] for m in party_rows]
        session_id = create_game_session(
            conn,
            inviter_id=host_id or 0,
            puzzle_id=puzzle.id,
            expected_turns=puzzle.num_actions,
            member_user_ids=member_ids,
            mode='ranked',
        )
        return render_template(
            "play.html",
            puzzle=puzzle,
            layout=layout,
            expected_turns=puzzle.num_actions,
            solve_pct=solve_pct,
            requested=True,
            require_login=False,
            party=party,
            players_count=players_filter,
            session_id=session_id,
            solved_layout=solved_layout,
            elo=elo_value,
            host_id=host_id,
            is_host=True,
        )

    @app.route("/puzzle/<int:puzzle_id>")
    def puzzle_detail(puzzle_id: int):
        if not session.get("user"):
            return redirect(url_for("index"))
        sp = get_puzzle_by_id(conn, puzzle_id)
        if not sp:
            return render_template("puzzle.html", puzzle=None)
        start_layout = json.loads(sp.start_layout_json)
        solved_layout = json.loads(sp.solved_layout_json)
        actions = json.loads(sp.actions_json)
        return render_template("puzzle.html", puzzle=sp, start_layout=start_layout, solved_layout=solved_layout, actions=actions)

    @app.route("/report", methods=["POST"])
    def report():
        # Block unauthenticated submissions
        if not session.get("user"):
            return redirect(url_for("index"))
        puzzle_id = int(request.form.get("puzzle_id"))
        solved_flag = False
        seconds = request.form.get("seconds")
        players_filter = request.form.get("players_filter")
        turns_filter = request.form.get("turns_filter")
        session_id_q = request.form.get("session_id")
        seconds_val: Optional[int] = int(seconds) if seconds else None
        actions_json = request.form.get("actions") or "[]"
        # Recompute solved from actions applied to start layout
        try:
            sp = get_puzzle_by_id(conn, puzzle_id)
            if sp:
                start_layout = json.loads(sp.start_layout_json)
                from app.models import Layout as _Layout
                working = _Layout.from_dict(start_layout)
                posted_actions = json.loads(actions_json)
                if isinstance(posted_actions, list):
                    for a in posted_actions:
                        if isinstance(a, dict) and "type" in a and "params" in a:
                            try:
                                apply_action(working, Action.from_dict(a))
                            except Exception:
                                pass
                from app.generator import is_layout_success
                solved_flag = is_layout_success(working)
        except Exception:
            solved_flag = False
        user_id = (session.get("user") or {}).get("id")
        if session_id_q:
            try:
                sid = int(session_id_q)
            except Exception:
                sid = None
        else:
            sid = None
        if sid:
            # Only host may submit results for a session
            cur = conn.cursor()
            cur.execute("SELECT inviter_id, mode FROM game_sessions WHERE id = ?", (sid,))
            r = cur.fetchone()
            inviter_id = int(r[0]) if r else None
            mode = str(r[1]) if r and r[1] else 'quick'
            if not user_id or inviter_id is None or int(user_id) != int(inviter_id):
                return redirect(url_for("play"))
            update_session_result(conn, sid, solved=solved_flag, seconds=seconds_val, user_id=user_id)
            if solved_flag:
                complete_session(conn, sid)
            # After completion or submit, always return to lobby and clear readiness
            if inviter_id:
                try:
                    lobby_clear_all_ready(conn, inviter_id, mode)
                except Exception:
                    pass
            if mode == 'ranked':
                return redirect(url_for("play"))
        else:
            # Legacy fallback without session
            add_game_result(conn, puzzle_id, solved_flag, seconds_val, user_id=user_id)
        return redirect(url_for("play"))

    @app.route("/play/give_up", methods=["POST"])
    def play_give_up():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        data = request.get_json(silent=True) or {}
        session_id = data.get("session_id")
        try:
            session_id = int(session_id)
        except Exception:
            return jsonify({"ok": False, "error": "invalid_session_id"}), 400
        sess = get_session_by_id(conn, session_id)
        if not sess:
            return jsonify({"ok": False, "error": "session_not_found"}), 404
        sid, puzzle_id, _exp_turns, inviter_id, status, _completed_at, _started_at = sess
        if status != 'active':
            return jsonify({"ok": False, "error": "not_active"}), 400
        # Verify membership
        members = get_session_members_with_names(conn, sid)
        member_ids = [m[0] for m in members]
        if current.get("id") not in ([inviter_id] + member_ids):
            return jsonify({"ok": False, "error": "forbidden"}), 403
        # Only host can give up to finalize the session
        if int(current.get("id")) != int(inviter_id):
            return jsonify({"ok": False, "error": "only_host_can_give_up"}), 403
        # Mark loss and complete
        update_session_result(conn, sid, solved=False, seconds=None, user_id=current.get("id"))
        complete_session(conn, sid)
        # Clear lobby readiness for ranked lobby (since unified play uses ranked mode)
        try:
            lobby_clear_all_ready(conn, inviter_id, 'ranked')
        except Exception:
            pass
        # Fetch solution steps and initial layout
        steps: list = []
        layout: Optional[dict] = None
        try:
            if puzzle_id:
                sp = get_puzzle_by_id(conn, int(puzzle_id))
                if sp:
                    # Normalize layout and actions so non-player row is first, then players 1..N
                    layout_norm, actions_norm, _opp_idx = _normalize_layout_and_actions(
                        sp.start_layout_json, sp.actions_json, sp.opponent_row_index
                    )
                    layout = layout_norm
                    steps = humanize_actions_dicts(actions_norm) if isinstance(actions_norm, list) else []
        except Exception:
            steps = []
            layout = None
        return jsonify({"ok": True, "steps": steps, "layout": layout})

    # Alias for quick mode front-end which posts to /quick/give_up
    @app.route("/quick/give_up", methods=["POST"])
    def quick_give_up():
        # Reuse the same logic as /play/give_up
        return play_give_up()

    @app.route("/play/try_again", methods=["POST"])
    def play_try_again():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        data = request.get_json(silent=True) or {}
        session_id = data.get("session_id")
        try:
            session_id = int(session_id)
        except Exception:
            return jsonify({"ok": False, "error": "invalid_session_id"}), 400
        sess = get_session_by_id(conn, session_id)
        if not sess:
            return jsonify({"ok": False, "error": "session_not_found"}), 404
        sid, puzzle_id, exp_turns, inviter_id, status, _completed_at, _started_at = sess
        # Verify membership
        members = get_session_members_with_names(conn, sid)
        member_ids = [m[0] for m in members]
        if current.get("id") not in ([inviter_id] + member_ids):
            return jsonify({"ok": False, "error": "forbidden"}), 403
        # Only host can try again (as it creates a new session for the party)
        if int(current.get("id")) != int(inviter_id):
            return jsonify({"ok": False, "error": "only_host_can_try_again"}), 403
        # Record a loss if still active, then complete the session
        if status == 'active':
            update_session_result(conn, sid, solved=False, seconds=None, user_id=current.get("id"))
            complete_session(conn, sid)
        # Clear readiness for ranked lobby
        try:
            lobby_clear_all_ready(conn, inviter_id, 'ranked')
        except Exception:
            pass
        # Start a new session with the same puzzle and same party members
        try:
            new_session_id = create_game_session(
                conn,
                inviter_id=inviter_id,
                puzzle_id=int(puzzle_id) if puzzle_id is not None else None,
                expected_turns=int(exp_turns) if exp_turns is not None else None,
                member_user_ids=member_ids,
                mode='ranked',
            )
        except Exception as e:
            return jsonify({"ok": False, "error": "failed_to_create_session"}), 500
        return jsonify({"ok": True, "redirect_url": "/play?session_id=" + str(new_session_id)})

    # --- Invites API ---
    @app.route("/api/invites/create", methods=["POST"])
    def api_invites_create():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        data = request.get_json(silent=True) or {}
        user_code = (data.get("user_code") or "").strip().upper()
        mode = (data.get("mode") or "ranked").strip().lower()
        if mode not in ("quick", "ranked"):
            mode = "ranked"
        if not user_code:
            return jsonify({"ok": False, "error": "missing_user_code"}), 400
        # Look up invitee by code
        target = get_user_by_code(conn, user_code)
        if not target:
            return jsonify({"ok": False, "error": "user_not_found"}), 404
        invitee_id, _display, _code = target
        inviter_id = current.get("id")
        if invitee_id == inviter_id:
            return jsonify({"ok": False, "error": "cannot_invite_self"}), 400
        token = secrets.token_urlsafe(16)
        expires_at = (datetime.utcnow() + timedelta(minutes=5)).isoformat()
        # Force unified play mode invites to 'ranked'
        invite_id = create_invite(conn, inviter_id=inviter_id, invitee_id=invitee_id, mode='ranked', token=token, expires_at_iso=expires_at)
        return jsonify({"ok": True, "invite_id": invite_id, "expires_at": expires_at, "mode": 'ranked'})

    @app.route("/api/invites/pending", methods=["GET"])
    def api_invites_pending():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        user_id = current.get("id")
        rows = get_pending_invites_for_user(conn, user_id)
        # rows: (id, inviter_id, inviter_display, mode, created_at, expires_at)
        invites = [
            {
                "id": r[0],
                "inviter_id": r[1],
                "inviter_display": r[2],
                "mode": r[3],
                "created_at": r[4],
                "expires_at": r[5],
            }
            for r in rows
        ]
        return jsonify({"ok": True, "invites": invites})

    @app.route("/api/invites/accept", methods=["POST"])
    def api_invites_accept():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        data = request.get_json(silent=True) or {}
        invite_id = data.get("invite_id")
        try:
            invite_id = int(invite_id)
        except Exception:
            return jsonify({"ok": False, "error": "invalid_invite_id"}), 400
        # Look up inviter and mode for redirect hints
        cur = conn.cursor()
        cur.execute("SELECT inviter_id, mode FROM invites WHERE id = ?", (invite_id,))
        inv_row = cur.fetchone()
        inviter_id = int(inv_row[0]) if inv_row else None
        mode = str(inv_row[1]) if inv_row and inv_row[1] else 'ranked'
        ok = accept_invite(conn, invite_id, current.get("id"))
        return jsonify({"ok": bool(ok), "mode": 'ranked', "inviter_id": inviter_id})

    @app.route("/api/party", methods=["GET"])
    def api_party():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        inviter_id = current.get("id")
        mode = (request.args.get("mode") or "ranked").strip().lower()
        if mode not in ("quick", "ranked"):
            mode = "ranked"
        rows = get_party_for_inviter(conn, inviter_id, mode='ranked')
        party = [{"user_id": r[0], "display_name": r[1]} for r in rows]
        # players_count is total participants (host + invitees)
        return jsonify({"ok": True, "party": party, "players_count": 1 + len(party)})

    @app.route("/api/login", methods=["POST"])
    def api_login():
        data = request.get_json(silent=True) or {}
        id_token = data.get("id_token")
        requested_display_name = data.get("display_name")
        if not id_token:
            print("[AUTH] /api/login missing id_token in request body")
            return jsonify({"ok": False, "error": "missing_id_token"}), 400
        decoded = None
        # First try Firebase Admin
        if firebase_admin._apps:
            try:
                decoded = fb_auth.verify_id_token(id_token)
                print("[AUTH] /api/login verified token via Firebase Admin", {"uid": decoded.get("uid") or decoded.get("user_id")})
            except Exception as e:
                print("[AUTH] Firebase Admin verification failed, will fallback:", e)
                decoded = None
        # Fallback to Google public cert verification
        if decoded is None:
            try:
                request_adapter = google_auth_requests.Request()
                decoded = google_id_token.verify_firebase_token(id_token, request_adapter)
                print("[AUTH] /api/login verified token via public certs", {"uid": decoded.get("uid") or decoded.get("user_id")})
            except Exception as e:
                print("[AUTH] Public cert verification failed:", e)
                return jsonify({"ok": False, "error": "invalid_token"}), 401
        uid = decoded.get("uid") or decoded.get("user_id")
        name_from_token = decoded.get("name")
        display_name = requested_display_name or name_from_token or "Player"
        user_id = ensure_user(conn, user_key=uid, display_name=display_name)
        # Fetch user_code for session display
        user_code = get_user_code(conn, user_id)
        session["user"] = {"id": user_id, "display_name": display_name, "user_code": user_code}
        print("[AUTH] /api/login success; session user set", session["user"]) 
        return jsonify({"ok": True, "user": session["user"]})

    @app.route("/api/logout", methods=["POST"])
    def api_logout():
        session.pop("user", None)
        return jsonify({"ok": True})

    @app.route("/api/session", methods=["GET"])
    def api_session():
        return jsonify({"user": session.get("user")})

    @app.route("/ranked")
    def ranked():
        # Preserve existing links by redirecting to unified Play
        return redirect(url_for("play"))

    @app.route("/ranked/give_up", methods=["POST"])
    def ranked_give_up():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        data = request.get_json(silent=True) or {}
        session_id = data.get("session_id")
        try:
            session_id = int(session_id)
        except Exception:
            return jsonify({"ok": False, "error": "invalid_session_id"}), 400
        sess = get_session_by_id(conn, session_id)
        if not sess:
            return jsonify({"ok": False, "error": "session_not_found"}), 404
        sid, puzzle_id, _exp_turns, inviter_id, status, _completed_at, _started_at = sess
        if status != 'active':
            return jsonify({"ok": False, "error": "not_active"}), 400
        # Verify membership
        members = get_session_members_with_names(conn, sid)
        member_ids = [m[0] for m in members]
        if current.get("id") not in ([inviter_id] + member_ids):
            return jsonify({"ok": False, "error": "forbidden"}), 403
        # Mark loss and complete
        update_session_result(conn, sid, solved=False, seconds=None, user_id=current.get("id"))
        complete_session(conn, sid)
        # Clear lobby readiness for ranked lobby
        try:
            lobby_clear_all_ready(conn, inviter_id, 'ranked')
        except Exception:
            pass
        # Fetch solution steps and initial layout
        steps: list = []
        layout: Optional[dict] = None
        try:
            if puzzle_id:
                sp = get_puzzle_by_id(conn, int(puzzle_id))
                if sp:
                    layout_norm, actions_norm, _opp_idx = _normalize_layout_and_actions(
                        sp.start_layout_json, sp.actions_json, sp.opponent_row_index
                    )
                    layout = layout_norm
                    steps = humanize_actions_dicts(actions_norm) if isinstance(actions_norm, list) else []
        except Exception:
            steps = []
            layout = None
        return jsonify({"ok": True, "steps": steps, "layout": layout})

    @app.route("/stats")
    def stats():
        if not session.get("user"):
            return redirect(url_for("index"))
        # Show a simple log summary
        cur = conn.cursor()
        cur.execute(
            """
            SELECT gr.id, gr.created_at, gr.solved, gr.seconds, p.players, p.level, p.num_actions
            FROM game_results gr
            JOIN puzzles p ON p.id = gr.puzzle_id
            ORDER BY gr.id DESC LIMIT 200
            """
        )
        rows = cur.fetchall()
        # Compute win percentage: a puzzle counts as started if it has any result row,
        # and as won if it has any solved=1 row. Once solved, it remains a win.
        cur.execute("SELECT COUNT(DISTINCT puzzle_id) FROM game_results")
        started_count_row = cur.fetchone()
        started_count = started_count_row[0] if started_count_row and started_count_row[0] else 0
        cur.execute("SELECT COUNT(DISTINCT puzzle_id) FROM game_results WHERE solved=1")
        solved_count_row = cur.fetchone()
        solved_count = solved_count_row[0] if solved_count_row and solved_count_row[0] else 0
        win_pct = (solved_count / started_count * 100.0) if started_count else None
        # Compute current user's ranked-only ELO if logged in
        elo_placeholder = "ELO: -"
        current = session.get("user")
        if current:
            ranked_rows = get_user_ranked_results_with_puzzle_meta(conn, current.get("id"))
            elo_placeholder = f"Ranked ELO: {compute_user_elo(ranked_rows, EloConfig()):.0f}"
        return render_template(
            "stats.html",
            results=rows,
            elo=elo_placeholder,
            win_pct=win_pct,
            started_count=started_count,
            solved_count=solved_count,
        )

    @app.route("/leaderboard")
    def leaderboard():
        if not session.get("user"):
            return redirect(url_for("index"))
        # Pagination
        try:
            page = int(request.args.get("page", "1"))
        except ValueError:
            page = 1
        page = max(1, page)
        page_size = 20

        # Configurable ELO parameters (could be surfaced via query params later)
        cfg = EloConfig()
        # Gather per-user results
        users = get_all_users(conn)
        per_user = []
        for uid, display_name in users:
            rows = get_user_ranked_results_with_puzzle_meta(conn, uid)
            elo_value = compute_user_elo(rows, cfg)
            # Compute attempts and wins including sessions; compute win_rate to match stats definition
            attempts_all, wins_all = get_user_attempts_wins_including_sessions(conn, uid)
            win_rate = (wins_all / attempts_all * 100.0) if attempts_all else None
            # Keep basic stats averages as before
            _, _, _, avg_level_all, avg_level_recent = get_user_basic_stats(conn, uid, recent_days=int(cfg.recency_halflife_days))
            per_user.append({
                "user_id": uid,
                "display_name": display_name,
                "elo": elo_value,
                "attempts": attempts_all,
                "wins": wins_all,
                "win_rate": win_rate,
                "avg_level_all": avg_level_all,
                "avg_level_recent": avg_level_recent,
            })
        # Include anonymous/legacy results without user_id as a single synthetic user
        cur = conn.cursor()
        cur.execute(
            """
            SELECT gr.created_at, gr.solved, p.level, gr.seconds
            FROM game_results gr
            JOIN puzzles p ON p.id = gr.puzzle_id
            WHERE gr.user_id IS NULL AND gr.ranked = 1
            ORDER BY gr.created_at ASC, gr.id ASC
            """
        )
        anon_rows = cur.fetchall()
        if anon_rows:
            anon_elo = compute_user_elo(anon_rows, cfg)
            cur.execute(
                """
                SELECT COUNT(*), SUM(solved) FROM game_results WHERE user_id IS NULL AND ranked = 1
                """
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
                WHERE gr.user_id IS NULL AND gr.ranked = 1
                """
            )
            r2 = cur.fetchone()
            avg_level_all = float(r2[0]) if r2 and r2[0] is not None else None
            cur.execute(
                """
                SELECT AVG(p.level)
                FROM game_results gr
                JOIN puzzles p ON p.id = gr.puzzle_id
                WHERE gr.user_id IS NULL AND gr.ranked = 1 AND gr.created_at >= datetime('now', ?)
                """,
                (f'-{int(cfg.recency_halflife_days)} days',)
            )
            r3 = cur.fetchone()
            avg_level_recent = float(r3[0]) if r3 and r3[0] is not None else None
            per_user.append({
                "user_id": 0,
                "display_name": "Anonymous",
                "elo": anon_elo,
                "attempts": attempts,
                "wins": wins,
                "win_rate": win_rate,
                "avg_level_all": avg_level_all,
                "avg_level_recent": avg_level_recent,
            })

        # Sort by ELO desc and assign ranks
        per_user.sort(key=lambda x: x["elo"], reverse=True)
        for idx, row in enumerate(per_user, start=1):
            row["rank"] = idx

        # Slice page
        total = len(per_user)
        start = (page - 1) * page_size
        end = start + page_size
        page_rows = per_user[start:end]
        num_pages = (total + page_size - 1) // page_size if page_size else 1

        # Current user summary (if signed in)
        user_summary = None
        current = session.get("user")
        if current:
            cur_uid = current.get("id")
            for r in per_user:
                if r["user_id"] == cur_uid:
                    user_summary = r
                    break

        return render_template(
            "leaderboard.html",
            rows=page_rows,
            page=page,
            num_pages=num_pages,
            total=total,
            page_size=page_size,
            user_summary=user_summary,
        )

    # --- Lobby API: status/ready/start ---
    @app.route("/api/lobby/status", methods=["GET"])
    def api_lobby_status():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        user_id = int(current.get("id"))
        mode = (request.args.get("mode") or "ranked").strip().lower()
        if mode not in ("quick", "ranked"):
            mode = "ranked"
        inviter_id = user_id
        latest_inviter = get_latest_inviter_for_invitee_and_mode(conn, user_id, mode)
        if latest_inviter:
            inviter_id = latest_inviter
        # Build party list (inviter + accepted invitees)
        inviter_display = inviter_display_name(conn, inviter_id)
        party_rows = get_party_for_inviter(conn, inviter_id, mode='ranked')
        members = [(inviter_id, inviter_display)] + party_rows
        ready_map = lobby_get_ready_map(conn, inviter_id, mode)
        party = [{"user_id": uid, "display_name": name, "ready": bool(ready_map.get(uid, False))} for (uid, name) in members]
        # If solo, allow immediate start (no ready gate)
        can_start = True if len(party) <= 1 else all(m.get("ready") for m in party)
        # Existing active session id
        active_session_id = None
        # Unified play treats any active session as blocking
        rows = get_active_sessions_for_user(conn, inviter_id)
        active_session_id = rows[0][0] if rows else None
        return jsonify({"ok": True, "inviter_id": inviter_id, "mode": mode, "party": party, "players_count": len(party), "can_start": can_start, "active_session_id": active_session_id})

    @app.route("/api/lobby/ready", methods=["POST"])
    def api_lobby_ready():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        data = request.get_json(silent=True) or {}
        mode = (data.get("mode") or "ranked").strip().lower()
        if mode not in ("quick", "ranked"):
            mode = "ranked"
        # Optional client-selected players count
        players_selected = data.get("players")
        try:
            players_selected = int(players_selected) if players_selected is not None else None
        except Exception:
            players_selected = None
        ready_flag = True if data.get("ready") is True else False
        user_id = int(current.get("id"))
        inviter_id = user_id
        latest_inviter = get_latest_inviter_for_invitee_and_mode(conn, user_id, mode)
        if latest_inviter:
            inviter_id = latest_inviter
        lobby_set_ready(conn, inviter_id, mode, user_id, ready_flag)
        return jsonify({"ok": True})

    @app.route("/api/lobby/kick", methods=["POST"])
    def api_lobby_kick():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        data = request.get_json(silent=True) or {}
        mode = (data.get("mode") or "ranked").strip().lower()
        if mode not in ("quick", "ranked"):
            mode = "ranked"
        target_user_id = data.get("user_id")
        try:
            target_user_id = int(target_user_id)
        except Exception:
            return jsonify({"ok": False, "error": "invalid_user_id"}), 400
        inviter_id = int(current.get("id"))
        # Only host can kick; verify target is in party
        party_rows = get_party_for_inviter(conn, inviter_id, mode=mode)
        party_ids = {uid for (uid, _name) in party_rows}
        if target_user_id not in party_ids:
            return jsonify({"ok": False, "error": "not_in_party"}), 400
        ok = cancel_accepted_invite(conn, inviter_id, target_user_id, mode)
        return jsonify({"ok": bool(ok)})

    @app.route("/api/lobby/leave", methods=["POST"])
    def api_lobby_leave():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        data = request.get_json(silent=True) or {}
        mode = (data.get("mode") or "ranked").strip().lower()
        if mode not in ("quick", "ranked"):
            mode = "ranked"
        user_id = int(current.get("id"))
        inviter_id = get_latest_inviter_for_invitee_and_mode(conn, user_id, mode)
        if not inviter_id:
            return jsonify({"ok": False, "error": "no_lobby"}), 400
        ok = cancel_accepted_invite(conn, inviter_id, user_id, mode)
        return jsonify({"ok": bool(ok)})

    @app.route("/api/lobby/start", methods=["POST"])
    def api_lobby_start():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        data = request.get_json(silent=True) or {}
        mode = (data.get("mode") or "quick").strip().lower()
        if mode not in ("quick", "ranked"):
            mode = "quick"
        user_id = int(current.get("id"))
        inviter_id = user_id
        latest_inviter = get_latest_inviter_for_invitee_and_mode(conn, user_id, mode)
        if latest_inviter:
            inviter_id = latest_inviter
        # Only host can start the game
        if int(user_id) != int(inviter_id):
            return jsonify({"ok": False, "error": "only_host_can_start"}), 403
        # Validate readiness
        inviter_display = inviter_display_name(conn, inviter_id)
        party_rows = get_party_for_inviter(conn, inviter_id, mode=mode)
        members = [(inviter_id, inviter_display)] + party_rows
        ready_map = lobby_get_ready_map(conn, inviter_id, mode)
        # Allow solo start without readiness; for multi, require all ready
        if len(members) > 1 and not all(bool(ready_map.get(uid, False)) for (uid, _name) in members):
            return jsonify({"ok": False, "error": "not_all_ready"}), 400
        # Reuse existing active session for this host/mode if any
        existing_session_id = None
        r = get_single_active_ranked_session_for_user(conn, inviter_id)
        existing_session_id = r[0] if r else None
        if existing_session_id:
            redirect_url = "/play?session_id=" + str(existing_session_id)
            return jsonify({"ok": True, "redirect_url": redirect_url})
        # Create a new session and choose one puzzle for the entire lobby (always ranked)
        min_party = max(1, min(5, len(members)))
        players_count = min_party if players_selected is None else max(min_party, min(5, max(1, players_selected)))
        ranked_rows = get_user_results_with_puzzle_meta(conn, inviter_id)
        elo_cfg = EloConfig()
        elo_value = compute_user_elo(ranked_rows, elo_cfg)
        diff_cfg = RankedDifficultyConfig()
        level_choice = select_ranked_level(ranked_rows, elo_value, diff_cfg)
        exclude_ids = get_user_ranked_history_puzzle_ids(conn, inviter_id)
        puzzle = get_random_puzzle_for_level_and_players_excluding(conn, level_choice, players_count, exclude_ids)
        if not puzzle:
            tried = set([level_choice])
            for delta in [1, -1, 2, -2, 3, -3]:
                cand = max(diff_cfg.min_level, min(diff_cfg.max_level, level_choice + delta))
                if cand in tried:
                    continue
                tried.add(cand)
                puzzle = get_random_puzzle_for_level_and_players_excluding(conn, cand, players_count, exclude_ids)
                if puzzle:
                    break
        if not puzzle:
            return jsonify({"ok": False, "error": "no_puzzle"}), 400
        member_ids = [uid for (uid, _name) in party_rows]
        session_id = create_game_session(
            conn,
            inviter_id=inviter_id,
            puzzle_id=puzzle.id,
            expected_turns=puzzle.num_actions,
            member_user_ids=member_ids,
            mode='ranked',
        )
        return jsonify({"ok": True, "redirect_url": "/play?session_id=" + str(session_id)})

    # --- Game session status API for client polling ---
    @app.route("/api/game_session/status", methods=["GET"])
    def api_game_session_status():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        try:
            sid = int(request.args.get("session_id"))
        except Exception:
            return jsonify({"ok": False, "error": "invalid_session_id"}), 400
        sess = get_session_by_id(conn, sid)
        if not sess:
            return jsonify({"ok": False, "error": "not_found"}), 404
        # Fetch status, mode, and host (inviter)
        cur = conn.cursor()
        cur.execute("SELECT status, mode, inviter_id FROM game_sessions WHERE id = ?", (sid,))
        row = cur.fetchone()
        status = str(row[0]) if row else 'active'
        mode = str(row[1]) if row and row[1] else 'quick'
        host_id = int(row[2]) if row and row[2] is not None else None
        # Determine if solved (any solved=1 in results for this session)
        cur.execute("SELECT MAX(solved) FROM game_results WHERE session_id = ?", (sid,))
        solved_row = cur.fetchone()
        solved_flag = bool(solved_row[0]) if solved_row and solved_row[0] is not None else False
        return jsonify({"ok": True, "status": status, "mode": mode, "solved": solved_flag, "host_id": host_id})

    return app


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run local web server")
    p.add_argument("--db-path", default="puzzles.sqlite", help="SQLite DB path")
    p.add_argument("--host", default="127.0.0.1", help="Host to bind")
    p.add_argument("--port", type=int, default=8003, help="Port to bind (avoid 5000/5001)")
    p.add_argument("--debug", action="store_true", help="Enable Flask debug")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    app = create_app(args.db_path)
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
