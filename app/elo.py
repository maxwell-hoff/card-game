from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple


@dataclass
class EloConfig:
    """Adjustable parameters for ELO calculations.

    Assumptions:
    - Initial rating starts at 1200, like chess.
    - Each puzzle level is mapped to an "opponent rating" via a linear transform:
      opponent_rating = level_base + level_scale * level.
    - Recent results are weighted higher using exponential decay by days since played.
      weight = 0.5 ** (age_days / recency_halflife_days).
    - K-factor controls update magnitude and is scaled by the recency weight per game.
    """

    initial_rating: float = 1200.0
    k_factor: float = 32.0
    level_base: float = 1000.0
    level_scale: float = 80.0
    recency_halflife_days: float = 30.0  # half the weight every 30 days


def _expected_score(player_rating: float, opponent_rating: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((opponent_rating - player_rating) / 400.0))


def _game_weight(created_at_iso: str, now: Optional[datetime], cfg: EloConfig) -> float:
    try:
        played = datetime.fromisoformat(created_at_iso)
    except Exception:
        return 1.0
    ref = now or datetime.utcnow()
    age_days = max(0.0, (ref - played).total_seconds() / 86400.0)
    if cfg.recency_halflife_days <= 0:
        return 1.0
    return 0.5 ** (age_days / cfg.recency_halflife_days)


def _opponent_rating_from_level(level: int, cfg: EloConfig) -> float:
    return cfg.level_base + cfg.level_scale * float(level)


def compute_user_elo(
    results: List[Tuple[str, int, int, Optional[int]]],
    cfg: Optional[EloConfig] = None,
) -> float:
    """Compute user's ELO given their chronological results.

    results: sequence of tuples (created_at_iso, solved_int, level, seconds)
    """
    if cfg is None:
        cfg = EloConfig()
    rating = cfg.initial_rating
    now = datetime.utcnow()
    for created_at, solved_int, level, _seconds in results:
        opp_rating = _opponent_rating_from_level(int(level), cfg)
        expected = _expected_score(rating, opp_rating)
        actual = 1.0 if int(solved_int) == 1 else 0.0
        w = _game_weight(created_at, now, cfg)
        k = cfg.k_factor * w
        rating = rating + k * (actual - expected)
    return rating


def compute_all_elos(
    per_user_results: Dict[int, List[Tuple[str, int, int, Optional[int]]]],
    cfg: Optional[EloConfig] = None,
) -> Dict[int, float]:
    if cfg is None:
        cfg = EloConfig()
    return {uid: compute_user_elo(rows, cfg) for uid, rows in per_user_results.items()}


