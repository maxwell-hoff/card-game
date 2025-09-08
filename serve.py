from __future__ import annotations

import argparse
import json
from typing import Optional
import secrets
from datetime import datetime, timedelta
import os

from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from app.actions import humanize_actions_dicts

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
)
from app.elo import EloConfig, compute_user_elo, RankedDifficultyConfig, select_ranked_level

import firebase_admin
from firebase_admin import auth as fb_auth, credentials
from google.auth.transport import requests as google_auth_requests
from google.oauth2 import id_token as google_id_token


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

    @app.route("/quick")
    def quick():
        # Require sign-in to play/fetch puzzles
        if not session.get("user"):
            return redirect(url_for("index"))
        current_user = session.get("user")
        inviter_id = current_user.get("id") if current_user else None
        # Current party for inviter: list of (user_id, display_name)
        party_rows = get_party_for_inviter(conn, inviter_id) if inviter_id else []
        party = [{"user_id": r[0], "display_name": r[1]} for r in party_rows]
        turns_q = request.args.get("turns")
        requested = request.args.get("go") == "1"
        session_q = request.args.get("session_id")
        try:
            turns_filter = int(turns_q) if turns_q else None
        except ValueError:
            turns_filter = None
        if turns_filter is not None and turns_filter < 1:
            turns_filter = None

        # Determine players count from party (inviter + accepted invitees), min 1, max 5
        players_filter = 1 + len(party)
        if players_filter < 1:
            players_filter = 1
        if players_filter > 5:
            players_filter = 5

        # If resuming a session by id, load that specific puzzle
        if session_q:
            try:
                resume_session_id = int(session_q)
            except Exception:
                return redirect(url_for("quick"))
            sess = get_session_by_id(conn, resume_session_id)
            if not sess:
                return redirect(url_for("quick"))
            sess_id, sess_puzzle_id, sess_expected_turns, sess_inviter_id, sess_status, sess_completed_at, sess_started_at = sess
            # Must be active to resume
            if sess_status != 'active':
                return redirect(url_for("quick"))
            # Verify membership
            member_rows = get_session_members_with_names(conn, sess_id)
            member_ids = [m[0] for m in member_rows]
            if inviter_id not in ([sess_inviter_id] + member_ids):
                return redirect(url_for("quick"))
            sp = get_puzzle_by_id(conn, sess_puzzle_id)
            if not sp:
                return redirect(url_for("quick"))
            layout = json.loads(sp.start_layout_json)
            count, solved = get_puzzle_stats(conn, sp.id)
            solve_pct = (solved / count * 100.0) if count else None
            players_count_from_session = max(1, min(5, len(member_rows)))
            return render_template(
                "quick.html",
                puzzle=sp,
                layout=layout,
                expected_turns=sp.num_actions,
                solve_pct=solve_pct,
                turns_filter=turns_filter,
                requested=True,
                require_login=False,
                party=[{"user_id": uid, "display_name": name} for (uid, name) in member_rows if uid != inviter_id],
                players_count=players_count_from_session,
                session_id=sess_id,
            )

        # If user hasn't clicked Go yet, show setup and any resumable sessions
        if not requested:
            # Build active sessions table
            active = []
            if inviter_id:
                rows = get_active_sessions_for_user(conn, inviter_id)
                for sid, pid, exp_turns, started_at in rows:
                    members = get_session_members_with_names(conn, sid)
                    players_cnt = max(1, min(5, len(members)))
                    # Other players (exclude self by name/id)
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
                "quick.html",
                puzzle=None,
                turns_filter=turns_filter,
                requested=False,
                require_login=False,
                party=party,
                players_count=players_filter,
                active_sessions=active,
            )

        puzzle = get_random_puzzle_for_filters(conn, players_filter, turns_filter)
        if not puzzle:
            return render_template(
                "quick.html",
                puzzle=None,
                turns_filter=turns_filter,
                requested=True,
                require_login=False,
                party=party,
                players_count=players_filter,
            )
        layout = json.loads(puzzle.start_layout_json)
        count, solved = get_puzzle_stats(conn, puzzle.id)
        solve_pct = (solved / count * 100.0) if count else None
        # Create a session for this new puzzle start
        member_ids = [m[0] for m in party_rows]
        session_id = create_game_session(
            conn,
            inviter_id=inviter_id or 0,
            puzzle_id=puzzle.id,
            expected_turns=puzzle.num_actions,
            member_user_ids=member_ids,
        )
        return render_template(
            "quick.html",
            puzzle=puzzle,
            layout=layout,
            expected_turns=puzzle.num_actions,
            solve_pct=solve_pct,
            turns_filter=turns_filter,
            requested=True,
            require_login=False,
            party=party,
            players_count=players_filter,
            session_id=session_id,
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
        solved_flag = request.form.get("solved") == "1"
        seconds = request.form.get("seconds")
        players_filter = request.form.get("players_filter")
        turns_filter = request.form.get("turns_filter")
        session_id_q = request.form.get("session_id")
        seconds_val: Optional[int] = int(seconds) if seconds else None
        user_id = (session.get("user") or {}).get("id")
        if session_id_q:
            try:
                sid = int(session_id_q)
            except Exception:
                sid = None
        else:
            sid = None
        if sid:
            update_session_result(conn, sid, solved=solved_flag, seconds=seconds_val, user_id=user_id)
            if solved_flag:
                complete_session(conn, sid)
            # Decide where to redirect based on session mode
            cur = conn.cursor()
            cur.execute("SELECT mode FROM game_sessions WHERE id = ?", (sid,))
            r = cur.fetchone()
            mode = r[0] if r else 'quick'
            if mode == 'ranked':
                return redirect(url_for("ranked"))
        else:
            # Legacy fallback without session
            add_game_result(conn, puzzle_id, solved_flag, seconds_val, user_id=user_id)
        kwargs = {}
        if turns_filter:
            kwargs["turns"] = turns_filter
        return redirect(url_for("quick", **kwargs))

    @app.route("/quick/give_up", methods=["POST"])
    def quick_give_up():
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
        # Fetch solution steps and initial layout
        steps: list = []
        layout: Optional[dict] = None
        try:
            if puzzle_id:
                sp = get_puzzle_by_id(conn, int(puzzle_id))
                if sp:
                    if sp.actions_json:
                        actions = json.loads(sp.actions_json)
                        steps = humanize_actions_dicts(actions) if isinstance(actions, list) else []
                    if sp.start_layout_json:
                        layout = json.loads(sp.start_layout_json)
        except Exception:
            steps = []
            layout = None
        return jsonify({"ok": True, "steps": steps, "layout": layout})

    # --- Invites API ---
    @app.route("/api/invites/create", methods=["POST"])
    def api_invites_create():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        data = request.get_json(silent=True) or {}
        user_code = (data.get("user_code") or "").strip().upper()
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
        invite_id = create_invite(conn, inviter_id=inviter_id, invitee_id=invitee_id, token=token, expires_at_iso=expires_at)
        return jsonify({"ok": True, "invite_id": invite_id, "expires_at": expires_at})

    @app.route("/api/invites/pending", methods=["GET"])
    def api_invites_pending():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        user_id = current.get("id")
        rows = get_pending_invites_for_user(conn, user_id)
        # rows: (id, inviter_id, inviter_display, created_at, expires_at)
        invites = [
            {
                "id": r[0],
                "inviter_id": r[1],
                "inviter_display": r[2],
                "created_at": r[3],
                "expires_at": r[4],
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
        ok = accept_invite(conn, invite_id, current.get("id"))
        return jsonify({"ok": bool(ok)})

    @app.route("/api/party", methods=["GET"])
    def api_party():
        current = session.get("user")
        if not current:
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
        inviter_id = current.get("id")
        rows = get_party_for_inviter(conn, inviter_id)
        party = [{"user_id": r[0], "display_name": r[1]} for r in rows]
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
        # Require sign-in
        if not session.get("user"):
            return redirect(url_for("index"))
        current_user = session.get("user")
        inviter_id = current_user.get("id") if current_user else None
        # Current party
        party_rows = get_party_for_inviter(conn, inviter_id) if inviter_id else []
        party = [{"user_id": r[0], "display_name": r[1]} for r in party_rows]
        requested = request.args.get("go") == "1"
        session_q = request.args.get("session_id")
        # Players count (inviter + invitees)
        players_filter = max(1, min(5, 1 + len(party)))

        # Ranked-only ELO
        ranked_rows = get_user_ranked_results_with_puzzle_meta(conn, inviter_id)
        elo_cfg = EloConfig()
        elo_value = compute_user_elo(ranked_rows, elo_cfg)

        # Resuming a specific session
        if session_q:
            try:
                resume_session_id = int(session_q)
            except Exception:
                return redirect(url_for("ranked"))
            sess = get_session_by_id(conn, resume_session_id)
            if not sess:
                return redirect(url_for("ranked"))
            sess_id, sess_puzzle_id, sess_expected_turns, sess_inviter_id, sess_status, sess_completed_at, sess_started_at = sess
            if sess_status != 'active':
                return redirect(url_for("ranked"))
            # Verify membership
            member_rows = get_session_members_with_names(conn, sess_id)
            member_ids = [m[0] for m in member_rows]
            if inviter_id not in ([sess_inviter_id] + member_ids):
                return redirect(url_for("ranked"))
            sp = get_puzzle_by_id(conn, sess_puzzle_id)
            if not sp:
                return redirect(url_for("ranked"))
            layout = json.loads(sp.start_layout_json)
            count, solved = get_puzzle_stats(conn, sp.id)
            solve_pct = (solved / count * 100.0) if count else None
            players_count_from_session = max(1, min(5, len(member_rows)))
            return render_template(
                "ranked.html",
                puzzle=sp,
                layout=layout,
                expected_turns=sp.num_actions,
                solve_pct=solve_pct,
                requested=True,
                require_login=False,
                party=[{"user_id": uid, "display_name": name} for (uid, name) in member_rows if uid != inviter_id],
                players_count=players_count_from_session,
                session_id=sess_id,
                elo=elo_value,
            )

        # If not requested, show setup and single resumable ranked session if present
        if not requested:
            active_row = get_single_active_ranked_session_for_user(conn, inviter_id) if inviter_id else None
            active = []
            if active_row:
                sid, pid, exp_turns, started_at = active_row
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
                "ranked.html",
                puzzle=None,
                requested=False,
                require_login=False,
                party=party,
                players_count=players_filter,
                active_sessions=active,
                elo=elo_value,
            )

        # Enforce single active ranked session per user
        existing = get_single_active_ranked_session_for_user(conn, inviter_id) if inviter_id else None
        if existing:
            sid, pid, exp_turns, _started_at = existing
            return redirect(url_for("ranked", session_id=sid))

        # Select difficulty for ranked
        diff_cfg = RankedDifficultyConfig()
        level_choice = select_ranked_level(ranked_rows, elo_value, diff_cfg)
        exclude_ids = get_user_ranked_history_puzzle_ids(conn, inviter_id)
        puzzle = get_random_puzzle_for_level_and_players_excluding(conn, level_choice, players_filter, exclude_ids)
        if not puzzle:
            # Try nearby levels
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
                "ranked.html",
                puzzle=None,
                requested=True,
                require_login=False,
                party=party,
                players_count=players_filter,
                active_sessions=[],
                elo=elo_value,
                note="No suitable ranked puzzle found. Please generate more puzzles.",
            )
        layout = json.loads(puzzle.start_layout_json)
        count, solved = get_puzzle_stats(conn, puzzle.id)
        solve_pct = (solved / count * 100.0) if count else None
        member_ids = [m[0] for m in party_rows]
        session_id = create_game_session(
            conn,
            inviter_id=inviter_id or 0,
            puzzle_id=puzzle.id,
            expected_turns=puzzle.num_actions,
            member_user_ids=member_ids,
            mode='ranked',
        )
        return render_template(
            "ranked.html",
            puzzle=puzzle,
            layout=layout,
            expected_turns=puzzle.num_actions,
            solve_pct=solve_pct,
            requested=True,
            require_login=False,
            party=party,
            players_count=players_filter,
            session_id=session_id,
            elo=elo_value,
        )

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
        # Fetch solution steps and initial layout
        steps: list = []
        layout: Optional[dict] = None
        try:
            if puzzle_id:
                sp = get_puzzle_by_id(conn, int(puzzle_id))
                if sp:
                    if sp.actions_json:
                        actions = json.loads(sp.actions_json)
                        steps = humanize_actions_dicts(actions) if isinstance(actions, list) else []
                    if sp.start_layout_json:
                        layout = json.loads(sp.start_layout_json)
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
