from __future__ import annotations

import argparse
import json
import os
import sqlite3
from typing import List, Dict, Any, Optional

# Reuse the humanizer from app.actions
from app.actions import humanize_actions_dicts


def read_json_actions(path: str) -> List[Dict[str, Any]]:
    with open(path, 'r') as f:
        data = json.load(f)
    if isinstance(data, dict) and 'actions' in data:
        data = data['actions']
    if not isinstance(data, list):
        raise ValueError('JSON must be a list of actions or an object with key "actions" as a list')
    return data


def read_db_actions(db_path: str, puzzle_id: int) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute('SELECT actions_json FROM puzzles WHERE id = ?', (puzzle_id,))
        row = cur.fetchone()
        if not row or not row[0]:
            raise ValueError(f'No actions_json found for puzzle id {puzzle_id}')
        actions = json.loads(row[0])
        if not isinstance(actions, list):
            raise ValueError('actions_json is not a list')
        return actions
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Convert puzzle actions JSON to human-readable steps')
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument('--json', dest='json_path', help='Path to a JSON file containing actions or {"actions": [...]}')
    src.add_argument('--puzzle-id', dest='puzzle_id', type=int, help='Puzzle id to read from SQLite DB')
    p.add_argument('--db', dest='db_path', default='puzzles.sqlite', help='Path to SQLite DB (default: puzzles.sqlite)')
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.json_path:
        actions = read_json_actions(args.json_path)
    else:
        db_path = args.db_path or 'puzzles.sqlite'
        if not os.path.exists(db_path):
            raise SystemExit(f'Database not found: {db_path}')
        if args.puzzle_id is None:
            raise SystemExit('--puzzle-id is required when using --db')
        actions = read_db_actions(db_path, args.puzzle_id)

    steps = humanize_actions_dicts(actions)
    for idx, step in enumerate(steps, start=1):
        print(f'{idx}. {step}')


if __name__ == '__main__':
    main()


