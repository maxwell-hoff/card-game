# Card Game Assistant

A Python application to generate and serve cooperative card-game puzzles using a standard 52-card deck. It includes:

- A generator that creates solvable puzzles and stores them in SQLite
- A minimal Flask web app to play quick puzzles and track simple stats
- Placeholders for ranked mode and ELO scoring

## Inspiration

Inspired by reading "Bowling Alone" and the idea that previous generations had stronger real-world communities and that online communities are not a full replacement. One example discussed is how the Greatest Generation regularly met to play bridge. This project aims to bring some of the social, cooperative problem-solving spirit of games back to in-person gatherings, while using the internet to track progress, manage difficulty, and enable friendly competition via leaderboards.

## Gameplay Summary

- Lay out 5 columns. Create 1 row per player plus an opponent row (so players 1..5 → rows 2..6).
- Each row also has two face-up reserve cards placed to its left. Each column has two face-up reserve cards placed above it.
- Legal actions (on a player's assigned row or on columns) include:
  1. Swap two cards within the player row; swap the same-index cards in opponent row.
  2. Row shift right with row reserve interaction.
  3. Row shift left with row reserve interaction.
  4. Column shift down with column reserve interaction.
  5. Column shift up with column reserve interaction.
- Win when:
  1. One row (can be opponent) is all same suit,
  2. One column is ascending or descending by rank (Ace high),
  3. Highest card in opponent row is lower than all cards in other rows.

## Project Structure

- `app/models.py`: Card and Layout models; legal moves
- `app/actions.py`: Action representation and inverse calculation
- `app/generator.py`: Build solved layouts and scramble to create starting positions
- `app/db.py`: SQLite schema and persistence helpers
- `generate_puzzles.py`: CLI to populate the database with puzzles
- `serve.py`: Flask web server with Quick, Ranked (WIP), and Stats tabs
- `templates/`: HTML templates (very minimal)

## Requirements

- Python 3.10+
- macOS/Linux recommended

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Generate Puzzles

By default, creates 5 (players) × 25 (levels) × 100 (per level) = 12,500 puzzles. You can adjust counts via flags.

```bash
python generate_puzzles.py --db-path puzzles.sqlite --levels 25 --per-level 100 --players all --seed 42
```

Notes:
- Generation may take time. Start smaller (e.g., `--levels 2 --per-level 3`) to validate.
- The difficulty proxy currently equals the scramble steps used to mix the solved layout.

## Run Server

Avoiding ports 5000/5001 as requested. Default port is 8002:

```bash
python serve.py --db-path puzzles.sqlite --port 8002 --debug
```

Open `http://127.0.0.1:8002` in your browser.

### Configuration

The server reads configuration from environment variables or an optional `config.json` (not committed). Env vars take precedence.

Supported keys:

- `FLASK_SECRET_KEY`
- `FIREBASE_SERVICE_ACCOUNT_JSON`
- `FIREBASE_WEB_API_KEY`
- `FIREBASE_AUTH_DOMAIN`
- `FIREBASE_PROJECT_ID`
- `FIREBASE_APP_ID`

Example `config.json`:

```json
{
  "FLASK_SECRET_KEY": "replace-me",
  "FIREBASE_SERVICE_ACCOUNT_JSON": "/absolute/path/to/service-account.json",
  "FIREBASE_WEB_API_KEY": "xxxx",
  "FIREBASE_AUTH_DOMAIN": "your-project.firebaseapp.com",
  "FIREBASE_PROJECT_ID": "your-project",
  "FIREBASE_APP_ID": "1:1234567890:web:abcdef"
}
```

Generate a secret key using: <python3 -c 'import secrets; print(secrets.token_hex(32))'>
You can also point to a different file via `APP_CONFIG_JSON=/path/to/config.json`.

## Ranked Mode & ELO

- Ranked mode is a WIP with placeholder code to select puzzles once enough data exists.
- ELO calculation is stubbed in the Stats page and will be implemented later.

## Development Notes

- Code aims for clarity and testability (short methods, object orientation, readable loops).
- Only standard library plus Flask used.

## License

MIT
