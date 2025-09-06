from __future__ import annotations

import argparse
import json
from typing import Optional
import os

from flask import Flask, render_template, request, redirect, url_for, session, jsonify

from app.db import get_conn, get_random_puzzle, add_game_result, get_puzzle_stats, get_random_puzzle_for_filters, get_puzzle_by_id, ensure_user, SCHEMA

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
        except Exception:
            app_config = {}

    # Secret key for session cookies; prefer env, then config, then default
    app.secret_key = (
        os.environ.get("FLASK_SECRET_KEY")
        or app_config.get("FLASK_SECRET_KEY")
        or "dev-secret-change-me"
    )

    conn = get_conn(db_path)
    # Ensure schema exists (including users)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    except Exception:
        pass

    # Initialize Firebase Admin if credentials are present or ADC is configured.
    # If neither is available, we will fallback to verifying ID tokens with google-oauth public certs.
    if not firebase_admin._apps:
        cred_path = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON") or app_config.get("FIREBASE_SERVICE_ACCOUNT_JSON")
        try:
            if cred_path and os.path.exists(cred_path):
                firebase_admin.initialize_app(credentials.Certificate(cred_path))
            else:
                # Try ADC; if it fails at runtime we'll still have the manual verifier
                firebase_admin.initialize_app()
        except Exception:
            # Proceed without admin app
            pass

    # Expose Firebase web config and current session user to templates
    @app.context_processor
    def inject_globals():
        firebase_config = {
            "apiKey": os.environ.get("FIREBASE_WEB_API_KEY") or app_config.get("FIREBASE_WEB_API_KEY", ""),
            "authDomain": os.environ.get("FIREBASE_AUTH_DOMAIN") or app_config.get("FIREBASE_AUTH_DOMAIN", ""),
            "projectId": os.environ.get("FIREBASE_PROJECT_ID") or app_config.get("FIREBASE_PROJECT_ID", ""),
            "appId": os.environ.get("FIREBASE_APP_ID") or app_config.get("FIREBASE_APP_ID", ""),
        }
        return {
            "firebase_config": firebase_config,
            "session_user": session.get("user"),
        }

    @app.route("/")
    def index():
        return redirect(url_for("quick"))

    @app.route("/quick")
    def quick():
        players_q = request.args.get("players")
        turns_q = request.args.get("turns")
        requested = request.args.get("go") == "1"
        try:
            players_filter = int(players_q) if players_q else None
        except ValueError:
            players_filter = None
        try:
            turns_filter = int(turns_q) if turns_q else None
        except ValueError:
            turns_filter = None
        if players_filter and (players_filter < 1 or players_filter > 5):
            players_filter = None
        if turns_filter is not None and turns_filter < 1:
            turns_filter = None

        # If user hasn't clicked Go yet, do not fetch or render a puzzle
        if not requested:
            return render_template(
                "quick.html",
                puzzle=None,
                players_filter=players_filter,
                turns_filter=turns_filter,
                requested=False,
            )

        puzzle = get_random_puzzle_for_filters(conn, players_filter, turns_filter)
        if not puzzle:
            return render_template(
                "quick.html",
                puzzle=None,
                players_filter=players_filter,
                turns_filter=turns_filter,
                requested=True,
            )
        layout = json.loads(puzzle.start_layout_json)
        count, solved = get_puzzle_stats(conn, puzzle.id)
        solve_pct = (solved / count * 100.0) if count else None
        return render_template(
            "quick.html",
            puzzle=puzzle,
            layout=layout,
            expected_turns=puzzle.num_actions,
            solve_pct=solve_pct,
            players_filter=players_filter,
            turns_filter=turns_filter,
            requested=True,
        )

    @app.route("/puzzle/<int:puzzle_id>")
    def puzzle_detail(puzzle_id: int):
        sp = get_puzzle_by_id(conn, puzzle_id)
        if not sp:
            return render_template("puzzle.html", puzzle=None)
        start_layout = json.loads(sp.start_layout_json)
        solved_layout = json.loads(sp.solved_layout_json)
        actions = json.loads(sp.actions_json)
        return render_template("puzzle.html", puzzle=sp, start_layout=start_layout, solved_layout=solved_layout, actions=actions)

    @app.route("/report", methods=["POST"])
    def report():
        puzzle_id = int(request.form.get("puzzle_id"))
        solved_flag = request.form.get("solved") == "1"
        seconds = request.form.get("seconds")
        players_filter = request.form.get("players_filter")
        turns_filter = request.form.get("turns_filter")
        seconds_val: Optional[int] = int(seconds) if seconds else None
        add_game_result(conn, puzzle_id, solved_flag, seconds_val)
        kwargs = {}
        if players_filter:
            kwargs["players"] = players_filter
        if turns_filter:
            kwargs["turns"] = turns_filter
        return redirect(url_for("quick", **kwargs))

    @app.route("/api/login", methods=["POST"])
    def api_login():
        data = request.get_json(silent=True) or {}
        id_token = data.get("id_token")
        requested_display_name = data.get("display_name")
        if not id_token:
            return jsonify({"ok": False, "error": "missing_id_token"}), 400
        decoded = None
        # First try Firebase Admin
        if firebase_admin._apps:
            try:
                decoded = fb_auth.verify_id_token(id_token)
            except Exception:
                decoded = None
        # Fallback to Google public cert verification
        if decoded is None:
            try:
                request_adapter = google_auth_requests.Request()
                decoded = google_id_token.verify_firebase_token(id_token, request_adapter)
            except Exception:
                return jsonify({"ok": False, "error": "invalid_token"}), 401
        uid = decoded.get("uid") or decoded.get("user_id")
        name_from_token = decoded.get("name")
        display_name = requested_display_name or name_from_token or "Player"
        user_id = ensure_user(conn, user_key=uid, display_name=display_name)
        session["user"] = {"id": user_id, "display_name": display_name}
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
        # WIP placeholder - ranked puzzle selection to be implemented once enough data exists
        note = "Ranked mode is a WIP. Placeholder selection shown below."
        return render_template("ranked.html", note=note, puzzles=[])

    @app.route("/stats")
    def stats():
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
        # Placeholder ELO computation entry point
        elo_placeholder = "ELO: WIP"
        return render_template(
            "stats.html",
            results=rows,
            elo=elo_placeholder,
            win_pct=win_pct,
            started_count=started_count,
            solved_count=solved_count,
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
