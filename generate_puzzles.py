from __future__ import annotations

import argparse
import json
import random
from typing import Optional

from app.db import init_db, get_conn, insert_puzzle
from app.generator import build_solved_layout, scramble_from_solved


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate card game puzzles into SQLite DB")
    p.add_argument("--db-path", default="puzzles.sqlite", help="Path to SQLite DB file")
    p.add_argument("--levels", type=int, default=25, help="Number of difficulty levels")
    p.add_argument("--per-level", type=int, default=100, help="Puzzles per level per players")
    p.add_argument("--players", type=str, default="all", help="Players count (1-5 or 'all')")
    p.add_argument("--seed", type=int, default=42, help="Random seed")
    p.add_argument("--max-attempts", type=int, default=2000, help="Max attempts per solved layout")
    p.add_argument("--per-puzzle-retries", type=int, default=50, help="Retries per puzzle before giving up")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    init_db(args.db_path)
    conn = get_conn(args.db_path)
    rng = random.Random(args.seed)

    if args.players == "all":
        players_list = [1, 2, 3, 4, 5]
    else:
        p = int(args.players)
        if p < 1 or p > 5:
            raise ValueError("players must be 1..5 or 'all'")
        players_list = [p]

    for players in players_list:
        for level in range(1, args.levels + 1):
            steps = level
            for n in range(args.per_level):
                success = False
                for _ in range(args.per_puzzle_retries):
                    try:
                        solved_layout, meta = build_solved_layout(players=players, rng=rng, max_attempts=args.max_attempts)
                    except RuntimeError:
                        continue
                    scrambled_layout, solution_actions = scramble_from_solved(solved_layout, steps=steps, rng=rng, players=players)
                    # Ensure row indices in actions start from non-player row at 0 followed by players 1..N
                    # Since generator normalized opponent_row_index to 0 above, actions produced from scrambling already reference new indices.
                    insert_puzzle(
                        conn=conn,
                        players=players,
                        level=level,
                        num_actions=len(solution_actions),
                        opponent_row_index=solved_layout.opponent_row_index,
                        suite_row_index=meta.get("suite_row_index"),
                        column_index=meta.get("column_index"),
                        start_layout_json=json.dumps(scrambled_layout.to_dict()),
                        solved_layout_json=json.dumps(solved_layout.to_dict()),
                        actions_json=json.dumps([a.to_dict() for a in solution_actions]),
                    )
                    success = True
                    break
                if not success:
                    raise RuntimeError(f"Failed to create puzzle for players={players} level={level} after retries")
            print(f"Generated players={players} level={level} x {args.per_level}")
    conn.close()


if __name__ == "__main__":
    main()
