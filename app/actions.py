from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, List


@dataclass
class Action:
    type: str  # 'swap' | 'row_right' | 'row_left' | 'col_down' | 'col_up'
    params: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {"type": self.type, "params": self.params}

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Action":
        return Action(type=d["type"], params=d["params"])


def inverse_action(action: Action) -> Action:
    t = action.type
    p = dict(action.params)
    if t == "row_right":
        return Action(type="row_left", params=p)
    if t == "row_left":
        return Action(type="row_right", params=p)
    if t == "col_down":
        return Action(type="col_up", params=p)
    if t == "col_up":
        return Action(type="col_down", params=p)
    if t == "swap":
        return Action(type="swap", params=p)
    raise ValueError(f"Unknown action type: {t}")


# --- Humanization helpers ---

def _ordinal(n: int) -> str:
    """Return 1 -> '1st', 2 -> '2nd', etc."""
    if 10 <= (n % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def humanize_action_dict(action: Dict[str, Any]) -> str:
    """Convert a single action dict into a human-readable instruction.

    Expected action schema examples:
    - {"type": "col_up", "params": {"col_index": 0}}
    - {"type": "row_left", "params": {"row_index": 2}}
    - {"type": "swap", "params": {"row_index": 1, "i": 0, "j": 4}}
    """
    a_type = str(action.get("type"))
    params = dict(action.get("params") or {})
    if a_type == "col_up":
        c = int(params.get("col_index", 0)) + 1
        return f"move {_ordinal(c)} column up one row"
    if a_type == "col_down":
        c = int(params.get("col_index", 0)) + 1
        return f"move {_ordinal(c)} column down one row"
    if a_type == "row_left":
        r = int(params.get("row_index", 0)) + 1
        return f"move {_ordinal(r)} row left by one"
    if a_type == "row_right":
        r = int(params.get("row_index", 0)) + 1
        return f"move {_ordinal(r)} row right by one"
    if a_type == "swap":
        r = int(params.get("row_index", 0)) + 1
        i = int(params.get("i", 0)) + 1
        j = int(params.get("j", 0)) + 1
        return f"swap {_ordinal(i)} and {_ordinal(j)} cards in the {_ordinal(r)} row"
    # Fallback
    return f"perform action: {a_type} {params}"


def humanize_actions_dicts(actions: List[Dict[str, Any]]) -> List[str]:
    """Convert a list of action dicts to a list of human-readable steps."""
    return [humanize_action_dict(a) for a in actions]
