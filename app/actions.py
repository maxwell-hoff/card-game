from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any


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
