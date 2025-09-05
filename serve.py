from __future__ import annotations

import argparse
import json
from typing import Optional

from flask import Flask, render_template, request, redirect, url_for

from app.db import get_conn, get_random_puzzle, add_game_result, get_puzzle_stats, get_random_puzzle_for_filters, get_puzzle_by_id


def create_app(db_path: str) -> Flask:
    app = Flask(__name__)
    conn = get_conn(db_path)

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
        # Placeholder ELO computation entry point
        elo_placeholder = "ELO: WIP"
        return render_template("stats.html", results=rows, elo=elo_placeholder)

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
