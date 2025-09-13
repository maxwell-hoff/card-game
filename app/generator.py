from __future__ import annotations

import json
import random
from typing import List, Dict, Tuple, Optional

from .models import Card, standard_deck, card_sort_desc, cards_to_codes, Layout
from .actions import Action, inverse_action


def draw_deck(rng: random.Random) -> List[Card]:
    deck = standard_deck()
    rng.shuffle(deck)
    return deck


def required_reserve_count(num_rows: int, num_cols: int = 5) -> int:
    return num_rows * 2 + num_cols * 2


def pick_opponent_row_from_grid(grid_cards: List[Card]) -> List[Card]:
    sorted_cards = sorted(grid_cards, key=lambda c: (c.rank, c.suit))
    return sorted_cards[:5]


def fifth_vs_sixth_highest_ok(cards: List[Card]) -> bool:
    desc = card_sort_desc(cards)
    if len(desc) < 6:
        return False
    return desc[4].rank != desc[5].rank


def find_suit_row(opponent_row: List[Card], remaining_grid: List[Card], rng: random.Random) -> Tuple[Optional[str], List[Card]]:
    suits_in_opp = {c.suit for c in opponent_row}
    if len(suits_in_opp) == 1:
        return None, opponent_row
    suit_to_cards: Dict[str, List[Card]] = {}
    for c in remaining_grid:
        suit_to_cards.setdefault(c.suit, []).append(c)
    candidates = [s for s, lst in suit_to_cards.items() if len(lst) >= 5]
    if not candidates:
        return None, []
    suit = rng.choice(candidates)
    chosen = list(suit_to_cards[suit])
    rng.shuffle(chosen)
    return suit, chosen[:5]


def find_column_sequence(
    num_rows: int,
    opponent_row_index: int,
    suite_row_index: Optional[int],
    opponent_row: List[Card],
    suit_row_cards: List[Card],
    remaining: List[Card],
) -> Optional[List[Card]]:
    base_rank = max(c.rank for c in opponent_row)
    start_rank = base_rank - opponent_row_index
    needed: List[int] = [start_rank + r for r in range(num_rows)]
    if min(needed) < 2 or max(needed) > 14:
        return None
    pool_remaining = list(remaining)
    pool_suit = list(suit_row_cards)

    sequence: List[Card] = []
    for r in range(num_rows):
        target_rank = needed[r]
        if suite_row_index is not None and r == suite_row_index:
            candidates = [c for c in pool_suit if c.rank == target_rank]
            if not candidates:
                return None
            chosen = candidates[0]
            pool_suit.remove(chosen)
            sequence.append(chosen)
            continue
        if r == opponent_row_index and target_rank in [c.rank for c in opponent_row]:
            candidates = [c for c in opponent_row if c.rank == target_rank]
            if not candidates:
                return None
            chosen = candidates[0]
            sequence.append(chosen)
            continue
        candidates = [c for c in pool_remaining if c.rank == target_rank]
        if not candidates:
            return None
        chosen = candidates[0]
        pool_remaining.remove(chosen)
        sequence.append(chosen)
    return sequence


def build_solved_layout(
    players: int,
    rng: random.Random,
    max_attempts: int = 2000,
) -> Tuple[Layout, Dict]:
    num_rows = players + 1
    num_cols = 5

    for _ in range(max_attempts):
        deck = draw_deck(rng)
        grid_count = num_rows * num_cols
        reserve_need = required_reserve_count(num_rows, num_cols)
        if grid_count + reserve_need > len(deck):
            raise RuntimeError("Not enough cards to build layout")
        grid_cards = deck[:grid_count]
        reserve_pool = deck[grid_count:]

        if not fifth_vs_sixth_highest_ok(grid_cards):
            continue

        opp_row_cards = pick_opponent_row_from_grid(grid_cards)
        remaining_grid = [c for c in grid_cards if c not in opp_row_cards]
        suit, suit_row_cards = find_suit_row(opp_row_cards, remaining_grid, rng)

        opponent_row_index = rng.randrange(num_rows)

        # Try all feasible suit row indices to find a working column sequence
        candidate_indices: List[Optional[int]]
        if suit is None and set(suit_row_cards) == set(opp_row_cards):
            candidate_indices = [opponent_row_index]
        else:
            candidate_indices = list(range(num_rows))
            rng.shuffle(candidate_indices)

        chosen_seq: Optional[List[Card]] = None
        chosen_suit_row_index: Optional[int] = None
        for s_idx in candidate_indices:
            col_seq = find_column_sequence(
                num_rows=num_rows,
                opponent_row_index=opponent_row_index,
                suite_row_index=s_idx,
                opponent_row=opp_row_cards,
                suit_row_cards=suit_row_cards if suit_row_cards else [],
                remaining=[c for c in remaining_grid if c not in suit_row_cards],
            )
            if col_seq is not None:
                chosen_seq = col_seq
                chosen_suit_row_index = s_idx
                break
        if chosen_seq is None:
            continue

        rows: List[List[str]] = [["" for _ in range(num_cols)] for _ in range(num_rows)]
        row_reserves: List[List[str]] = [["", ""] for _ in range(num_rows)]
        col_reserves: List[List[str]] = [["", ""] for _ in range(num_cols)]

        col_index = rng.randrange(num_cols)
        for r in range(num_rows):
            rows[r][col_index] = chosen_seq[r].code

        # Fill opponent row with its remaining cards
        already_in_opp = set(code for code in rows[opponent_row_index] if code)
        remaining_opp = [c for c in opp_row_cards if c.code not in already_in_opp]
        empties_opp = [i for i in range(num_cols) if rows[opponent_row_index][i] == ""]
        if len(empties_opp) < len(remaining_opp):
            continue
        for c_obj, idx in zip(remaining_opp, empties_opp):
            rows[opponent_row_index][idx] = c_obj.code

        # Fill suit row
        if chosen_suit_row_index is not None and chosen_suit_row_index != opponent_row_index and suit_row_cards:
            already_in_suit = set(code for code in rows[chosen_suit_row_index] if code)
            remaining_suit = [c for c in suit_row_cards if c.code not in already_in_suit]
            empties_suit = [i for i in range(num_cols) if rows[chosen_suit_row_index][i] == ""]
            if len(empties_suit) < len(remaining_suit):
                continue
            for c_obj, idx in zip(remaining_suit, empties_suit):
                rows[chosen_suit_row_index][idx] = c_obj.code

        # Place remaining grid cards
        used_codes = {code for row in rows for code in row if code}
        remaining_pool = [c for c in remaining_grid if c.code not in used_codes]
        rng.shuffle(remaining_pool)
        for r in range(num_rows):
            for c_idx in range(num_cols):
                if rows[r][c_idx] == "":
                    rows[r][c_idx] = remaining_pool.pop().code

        # Assign reserves from reserve pool
        pool = list(reserve_pool)
        rng.shuffle(pool)
        need = required_reserve_count(num_rows, num_cols)
        if len(pool) < need:
            continue
        # Row reserves
        for r in range(num_rows):
            row_reserves[r][0] = pool.pop().code
            row_reserves[r][1] = pool.pop().code
        # Column reserves
        for c_idx in range(num_cols):
            col_reserves[c_idx][0] = pool.pop().code
            col_reserves[c_idx][1] = pool.pop().code

        # Normalize row ordering so that non-player (opponent) row is at index 0,
        # followed by player rows in ascending original order. Adjust suite row index accordingly.
        new_order = [opponent_row_index] + [i for i in range(num_rows) if i != opponent_row_index]
        old_to_new = {old: new for new, old in enumerate(new_order)}
        rows = [rows[old] for old in new_order]
        row_reserves = [row_reserves[old] for old in new_order]
        # chosen_suit_row_index may be None
        if chosen_suit_row_index is not None:
            chosen_suit_row_index = old_to_new.get(chosen_suit_row_index, chosen_suit_row_index)
        opponent_row_index = 0

        layout = Layout(rows=rows, row_reserves=row_reserves, col_reserves=col_reserves, opponent_row_index=opponent_row_index)
        meta = {
            "opponent_row_index": opponent_row_index,
            "suite_row_index": chosen_suit_row_index,
            "column_index": col_index,
        }
        return layout, meta
    raise RuntimeError("Failed to build solved layout within attempts")


def apply_action(layout: Layout, action: Action) -> None:
    t = action.type
    p = action.params
    if t == "swap":
        layout.swap_with_opponent(p["row_index"], p["i"], p["j"])
    elif t == "row_right":
        layout.shift_row_right(p["row_index"])
    elif t == "row_left":
        layout.shift_row_left(p["row_index"])
    elif t == "col_down":
        layout.shift_col_down(p["col_index"])
    elif t == "col_up":
        layout.shift_col_up(p["col_index"])
    else:
        raise ValueError(f"Unknown action: {t}")


def random_legal_action(layout: Layout, rng: random.Random, players: int) -> Action:
    choices = []
    for r in range(layout.num_rows):
        if r == layout.opponent_row_index:
            continue
        choices.append(Action("row_right", {"row_index": r}))
        choices.append(Action("row_left", {"row_index": r}))
        i = rng.randrange(layout.num_cols)
        j = rng.randrange(layout.num_cols)
        if i != j:
            choices.append(Action("swap", {"row_index": r, "i": i, "j": j}))
    for c_idx in range(layout.num_cols):
        choices.append(Action("col_down", {"col_index": c_idx}))
        choices.append(Action("col_up", {"col_index": c_idx}))
    return rng.choice(choices)


def scramble_from_solved(
    solved: Layout,
    steps: int,
    rng: random.Random,
    players: int,
) -> Tuple[Layout, List[Action]]:
    layout = solved.clone()
    actions_taken: List[Action] = []
    for _ in range(steps):
        a = random_legal_action(layout, rng, players)
        apply_action(layout, a)
        actions_taken.append(a)
    solution_actions = [inverse_action(a) for a in reversed(actions_taken)]
    return layout, solution_actions
