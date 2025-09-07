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


@dataclass
class RankedDifficultyConfig:
    """Controls how ranked difficulty is chosen.

    Uses recent ranked win rate and/or ELO to pick a target difficulty level.
    """
    min_level: int = 1
    max_level: int = 20
    # Win-rate driven mapping
    window_games: int = 20  # use last N ranked games
    floor_level_at_low_winrate: int = 1
    ceil_level_at_high_winrate: int = 10
    low_winrate_threshold: float = 0.35  # 35%
    high_winrate_threshold: float = 0.65  # 65%
    # ELO blending
    blend_with_elo: bool = True
    elo_min: float = 1000.0
    elo_max: float = 1800.0
    elo_to_level_min: int = 2
    elo_to_level_max: int = 12
    # Smoothing
    clamp_delta_per_game: int = 2


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


def select_ranked_level(
    recent_ranked_results: List[Tuple[str, int, int, Optional[int]]],
    user_elo: float,
    cfg: RankedDifficultyConfig,
) -> int:
    """Pick a level for the next ranked puzzle based on recent ranked win rate and ELO.

    recent_ranked_results: chronological list of (created_at_iso, solved_int, level, seconds)
    user_elo: current user's ranked-only ELO
    cfg: tweakable parameters
    """
    # Compute win rate over last N games
    tail = recent_ranked_results[-cfg.window_games:] if cfg.window_games and len(recent_ranked_results) > cfg.window_games else recent_ranked_results
    attempts = len(tail)
    wins = sum(1 for _ts, solved, _lvl, _sec in tail if int(solved) == 1)
    win_rate = (wins / attempts) if attempts > 0 else 0.5

    # Map win rate to a baseline level range
    if win_rate <= cfg.low_winrate_threshold:
        wr_level = cfg.floor_level_at_low_winrate
    elif win_rate >= cfg.high_winrate_threshold:
        wr_level = cfg.ceil_level_at_high_winrate
    else:
        # Linear interpolation between floor and ceil
        span = cfg.high_winrate_threshold - cfg.low_winrate_threshold
        frac = (win_rate - cfg.low_winrate_threshold) / span if span > 0 else 0.5
        wr_level = int(round(cfg.floor_level_at_low_winrate + frac * (cfg.ceil_level_at_high_winrate - cfg.floor_level_at_low_winrate)))

    # Optionally blend with ELO → level mapping
    if cfg.blend_with_elo:
        if user_elo <= cfg.elo_min:
            elo_level = cfg.elo_to_level_min
        elif user_elo >= cfg.elo_max:
            elo_level = cfg.elo_to_level_max
        else:
            frac = (user_elo - cfg.elo_min) / (cfg.elo_max - cfg.elo_min)
            elo_level = int(round(cfg.elo_to_level_min + frac * (cfg.elo_to_level_max - cfg.elo_to_level_min)))
        target_level = int(round((wr_level + elo_level) / 2))
    else:
        target_level = wr_level

    # Smooth difficulty change per game if we have a last level
    if recent_ranked_results:
        last_level = int(recent_ranked_results[-1][2])
        delta = target_level - last_level
        if delta > cfg.clamp_delta_per_game:
            target_level = last_level + cfg.clamp_delta_per_game
        elif delta < -cfg.clamp_delta_per_game:
            target_level = last_level - cfg.clamp_delta_per_game

    # Clamp to allowed range
    target_level = max(cfg.min_level, min(cfg.max_level, target_level))
    return target_level


