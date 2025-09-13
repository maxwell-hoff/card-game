import unittest
import random

from app.generator import build_solved_layout, scramble_from_solved


class GeneratorTests(unittest.TestCase):
    def test_build_solved_layout_players_1(self):
        rng = random.Random(1)
        layout, meta = build_solved_layout(players=1, rng=rng, max_attempts=100)
        self.assertEqual(layout.num_cols, 5)
        self.assertEqual(layout.num_rows, 2)
        self.assertIn("column_index", meta)

    def test_scramble_inverse_solution(self):
        rng = random.Random(2)
        solved, meta = build_solved_layout(players=2, rng=rng, max_attempts=150)
        scrambled, solution_actions = scramble_from_solved(solved, steps=5, rng=rng, players=2)
        # Re-apply the solution to scrambled should return to solved
        from app.generator import apply_action
        cur = scrambled.clone()
        for a in solution_actions:
            apply_action(cur, a)
        self.assertEqual(cur.to_dict(), solved.to_dict())

    def test_solution_player_turn_order(self):
        rng = random.Random(3)
        players = 3
        steps = 9
        solved, _ = build_solved_layout(players=players, rng=rng, max_attempts=300)
        scrambled, solution_actions = scramble_from_solved(solved, steps=steps, rng=rng, players=players)
        # Non-player row must be index 0 by generator normalization
        self.assertEqual(solved.opponent_row_index, 0)
        # For row-based actions, enforce player turn order 1,2,3 repeating
        for idx, act in enumerate(solution_actions):
            t = act.type
            p = dict(act.params)
            if t in ("row_left", "row_right", "swap"):
                expected_row = 1 + (idx % players)
                self.assertEqual(int(p.get("row_index")), expected_row, f"step {idx} expected row {expected_row} got {p.get('row_index')}")


if __name__ == '__main__':
    unittest.main()
