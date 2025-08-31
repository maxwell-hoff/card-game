from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Tuple

SUITS = ["S", "H", "D", "C"]
RANKS = list(range(2, 15))  # 2..10, 11=J,12=Q,13=K,14=A (Ace high)


@dataclass(frozen=True)
class Card:
    rank: int
    suit: str

    @property
    def code(self) -> str:
        return f"{self.rank}{self.suit}"

    @staticmethod
    def from_code(code: str) -> "Card":
        rank = int(code[:-1])
        suit = code[-1]
        return Card(rank=rank, suit=suit)


def standard_deck() -> List[Card]:
    return [Card(rank=r, suit=s) for s in SUITS for r in RANKS]


def card_sort_desc(cards: List[Card]) -> List[Card]:
    return sorted(cards, key=lambda c: (c.rank, c.suit), reverse=True)


@dataclass
class Layout:
    rows: List[List[str]]  # grid rows x 5 of card codes
    row_reserves: List[List[str]]  # per row: [top, bottom]
    col_reserves: List[List[str]]  # per col: [top, bottom]
    opponent_row_index: int

    @property
    def num_rows(self) -> int:
        return len(self.rows)

    @property
    def num_cols(self) -> int:
        return 5

    def clone(self) -> "Layout":
        return Layout(
            rows=[list(r) for r in self.rows],
            row_reserves=[list(rr) for rr in self.row_reserves],
            col_reserves=[list(cr) for cr in self.col_reserves],
            opponent_row_index=self.opponent_row_index,
        )

    def to_dict(self) -> Dict:
        return {
            "rows": self.rows,
            "row_reserves": self.row_reserves,
            "col_reserves": self.col_reserves,
            "opponent_row_index": self.opponent_row_index,
        }

    @staticmethod
    def from_dict(data: Dict) -> "Layout":
        return Layout(
            rows=[list(r) for r in data["rows"]],
            row_reserves=[list(rr) for rr in data["row_reserves"]],
            col_reserves=[list(cr) for cr in data["col_reserves"]],
            opponent_row_index=int(data["opponent_row_index"]),
        )

    # --- Legal actions ---
    def swap_with_opponent(self, row_index: int, i: int, j: int) -> None:
        self.rows[row_index][i], self.rows[row_index][j] = (
            self.rows[row_index][j],
            self.rows[row_index][i],
        )
        opp = self.opponent_row_index
        self.rows[opp][i], self.rows[opp][j] = self.rows[opp][j], self.rows[opp][i]

    def shift_row_right(self, row_index: int) -> None:
        # Reserves are [top, bottom]
        top, bottom = self.row_reserves[row_index]
        rightmost = self.rows[row_index][-1]
        for k in range(self.num_cols - 1, 0, -1):
            self.rows[row_index][k] = self.rows[row_index][k - 1]
        self.rows[row_index][0] = top
        # After consuming top and adding rightmost to bottom, reserve becomes [old_bottom, rightmost]
        self.row_reserves[row_index] = [bottom, rightmost]

    def shift_row_left(self, row_index: int) -> None:
        top, bottom = self.row_reserves[row_index]
        leftmost = self.rows[row_index][0]
        for k in range(0, self.num_cols - 1):
            self.rows[row_index][k] = self.rows[row_index][k + 1]
        self.rows[row_index][-1] = bottom
        # After moving leftmost to top and consuming bottom, reserve becomes [leftmost, old_top]
        self.row_reserves[row_index] = [leftmost, top]

    def shift_col_down(self, col_index: int) -> None:
        top, bottom = self.col_reserves[col_index]
        bottommost = self.rows[-1][col_index]
        for r in range(self.num_rows - 1, 0, -1):
            self.rows[r][col_index] = self.rows[r - 1][col_index]
        self.rows[0][col_index] = top
        self.col_reserves[col_index] = [bottom, bottommost]

    def shift_col_up(self, col_index: int) -> None:
        top, bottom = self.col_reserves[col_index]
        topmost = self.rows[0][col_index]
        for r in range(0, self.num_rows - 1):
            self.rows[r][col_index] = self.rows[r + 1][col_index]
        self.rows[-1][col_index] = bottom
        self.col_reserves[col_index] = [topmost, top]

    # --- Utility ---
    def find_highest_in_row(self, row_index: int) -> Tuple[int, str]:
        best_rank = -1
        best_code = ""
        for code in self.rows[row_index]:
            rank = Card.from_code(code).rank
            if rank > best_rank:
                best_rank = rank
                best_code = code
        return best_rank, best_code


def cards_to_codes(cards: List[Card]) -> List[str]:
    return [c.code for c in cards]
