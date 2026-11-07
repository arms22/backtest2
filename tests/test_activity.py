import gzip
import json
import tempfile
import unittest
from pathlib import Path

from backtest2.analysis import (
    load_audited_results,
    match_fills_fifo,
    read_jsonl_gz,
    summarize_activity,
    summarize_position_activity,
)


class ActivitySummaryTests(unittest.TestCase):
    def test_position_activity_from_fifo_slices_handles_reversal_and_terminal(self):
        fills = [
            {'side': 'BUY', 'kind': 'fill', 'size': 2, 'price': 100, 't': 0, 'order': 'a'},
            {'side': 'SELL', 'kind': 'fill', 'size': 3, 'price': 110, 't': 30, 'order': 'b'},
            {'side': 'BUY', 'kind': 'fill', 'size': 2, 'price': 100, 't': 90, 'order': 'c'},
            {'side': 'SELL', 'kind': 'terminal', 'size': 1, 'price': 90, 't': 120, 'order': 'd'},
        ]
        matches, _ = match_fills_fifo(fills)
        long = summarize_position_activity(matches, entry_side='BUY')
        short = summarize_position_activity(matches, entry_side='SELL')
        whole = summarize_activity(fills, matches)

        # BUY fill volume is 4 and SELL fill volume is 4, but the closed
        # position entries are long 3 and short 1 after FIFO reversal slicing.
        self.assertEqual(whole['buy_quantity'], 4)
        self.assertEqual(whole['sell_quantity'], 3)  # terminal SELL excluded
        self.assertEqual(long['entry_quantity'], 3)
        self.assertEqual(short['entry_quantity'], 1)
        self.assertEqual(long['normal_profit'], 20)
        self.assertEqual(long['terminal_pnl'], -10)
        self.assertEqual(short['normal_profit'], 10)
        self.assertEqual(long['normal_profit'] + long['normal_loss'] + long['terminal_pnl']
                         + short['normal_profit'] + short['normal_loss'] + short['terminal_pnl'],
                         sum(m['gross_realized_pnl'] for m in matches))

    def test_terminal_buyback_and_terminal_entry_filtering(self):
        matches = [
            {'entry_side': 'SELL', 'entry_kind': 'fill', 'exit_kind': 'terminal',
             'size': 2, 'gross_realized_pnl': 6, 'holding_seconds': 20},
            {'entry_side': 'SELL', 'entry_kind': 'terminal', 'exit_kind': 'normal',
             'size': 9, 'gross_realized_pnl': 90, 'holding_seconds': 10},
        ]
        short = summarize_position_activity(matches, entry_side='SELL')
        self.assertEqual(short['entry_quantity'], 2)
        self.assertEqual(short['terminal_pnl'], 6)
        self.assertEqual(short['normal_profit'], 0)
        self.assertEqual(short['fast_fraction'], 0)

    def test_position_activity_weighting_boundary_empty_and_side_validation(self):
        matches = [
            {'entry_side': 'BUY', 'entry_kind': 'fill', 'exit_kind': 'normal',
             'size': 1, 'gross_realized_pnl': 1, 'holding_seconds': 59},
            {'entry_side': 'BUY', 'entry_kind': 'fill', 'exit_kind': 'normal',
             'size': 3, 'gross_realized_pnl': -1, 'holding_seconds': 60},
            {'entry_side': 'SELL', 'entry_kind': 'fill', 'exit_kind': 'normal',
             'size': 2, 'gross_realized_pnl': 1, 'holding_seconds': 30},
        ]
        long = summarize_position_activity(matches, entry_side='BUY')
        self.assertEqual(long['holding_seconds'], (59 + 180) / 4)
        self.assertEqual(long['fast_fraction'], 1 / 4)
        self.assertEqual(summarize_position_activity(matches, entry_side='BUY', fast_seconds=60)
                         ['fast_fraction'], 1 / 4)
        empty = summarize_position_activity(matches, entry_side='BUY', fast_seconds=59)
        self.assertEqual(empty['fast_fraction'], 0)
        no_matches = summarize_position_activity([], entry_side='SELL')
        self.assertEqual(no_matches['entry_quantity'], 0)
        self.assertIsNone(no_matches['holding_seconds'])
        self.assertIsNone(no_matches['fast_fraction'])
        with self.assertRaisesRegex(ValueError, 'entry_side'):
            summarize_position_activity(matches, entry_side='LONG')

    def test_pnl_signs_and_terminal_contribution(self):
        matches = [
            {'exit_kind': 'normal', 'gross_realized_pnl': 8, 'size': 1, 'holding_seconds': 10},
            {'exit_kind': 'normal', 'gross_realized_pnl': -3, 'size': 2, 'holding_seconds': 30},
            {'exit_kind': 'normal', 'gross_realized_pnl': 0, 'size': 1, 'holding_seconds': 90},
            {'exit_kind': 'terminal', 'gross_realized_pnl': -4, 'size': 5, 'holding_seconds': 1},
        ]
        result = summarize_activity([{'side': 'BUY', 'kind': 'fill', 'size': 4}], matches)
        self.assertEqual(result['normal_profit'], 8)
        self.assertEqual(result['normal_loss'], -3)
        self.assertEqual(result['terminal_pnl'], -4)
        self.assertEqual(result['holding_seconds'], 40)
        self.assertEqual(result['fast_fraction'], 3 / 4)

    def test_terminal_buy_fill_is_excluded_from_denominator(self):
        fills = [
            {'side': 'BUY', 'kind': 'fill', 'size': 2},
            {'side': 'BUY', 'kind': 'terminal', 'size': 100},
            {'side': 'SELL', 'kind': 'fill', 'size': 100},
        ]
        matches = [{'exit_kind': 'normal', 'gross_realized_pnl': 1,
                    'size': 1, 'holding_seconds': 20}]
        result = summarize_activity(fills, matches)
        self.assertEqual(result['buy_quantity'], 2)
        self.assertEqual(result['fast_fraction'], 0.5)

    def test_fast_boundary_is_strict_and_quantity_weighted(self):
        fills = [{'side': 'BUY', 'kind': 'fill', 'size': 8}]
        matches = [
            {'exit_kind': 'normal', 'gross_realized_pnl': 1, 'size': 1, 'holding_seconds': 59},
            {'exit_kind': 'normal', 'gross_realized_pnl': 1, 'size': 3, 'holding_seconds': 60},
            {'exit_kind': 'normal', 'gross_realized_pnl': 1, 'size': 2, 'holding_seconds': 120},
        ]
        result = summarize_activity(fills, matches)
        self.assertEqual(result['fast_fraction'], 1 / 8)
        self.assertEqual(result['holding_seconds'], (59 + 180 + 240) / 6)
        self.assertEqual(summarize_activity(fills, matches, fast_seconds=120)['fast_fraction'], 4 / 8)

    def test_empty_activity_and_zero_denominators(self):
        result = summarize_activity([], [])
        self.assertEqual(result['buy_quantity'], 0)
        self.assertEqual(result['normal_profit'], 0)
        self.assertEqual(result['normal_loss'], 0)
        self.assertEqual(result['terminal_pnl'], 0)
        self.assertIsNone(result['holding_seconds'])
        self.assertIsNone(result['fast_fraction'])
        no_buy = summarize_activity([], [
            {'exit_kind': 'normal', 'gross_realized_pnl': -2, 'size': 1, 'holding_seconds': 30}
        ])
        self.assertIsNone(no_buy['fast_fraction'])
        terminal_only = summarize_activity(
            [{'side': 'BUY', 'kind': 'fill', 'size': 3}],
            [{'exit_kind': 'terminal', 'gross_realized_pnl': 2,
              'size': 3, 'holding_seconds': 45}],
        )
        self.assertEqual(terminal_only['terminal_pnl'], 2)
        self.assertIsNone(terminal_only['holding_seconds'])
        self.assertEqual(terminal_only['fast_fraction'], 0)

    def test_audited_results_hash_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / 'results.json').write_text('{"results": []}', encoding='utf-8')
            (directory / 'audit_checks.json').write_text('{"results_sha256": "wrong"}', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'SHA256'):
                load_audited_results(directory)

    def test_reads_jsonl_gzip(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'ledger.jsonl.gz'
            with gzip.open(path, 'wt', encoding='utf-8') as stream:
                stream.write('{"value": 1}\n{"value": 2}\n')
            self.assertEqual(read_jsonl_gz(path), [{'value': 1}, {'value': 2}])


if __name__ == '__main__':
    unittest.main()
