import unittest
from unittest.mock import patch

from backtest2.analysis import aggregate_results, load_result_rows, merge_activity


def activity(buy=0, sell=0, normal_quantity=0, holding_quantity_seconds=0,
             fast_quantity=0, fast_seconds=60, normal_profit=0, normal_loss=0,
             terminal_pnl=0):
    return dict(
        buy_quantity=buy, sell_quantity=sell, normal_profit=normal_profit,
        normal_loss=normal_loss, terminal_pnl=terminal_pnl,
        normal_quantity=normal_quantity,
        holding_quantity_seconds=holding_quantity_seconds,
        fast_quantity=fast_quantity, fast_seconds=fast_seconds,
        holding_seconds=(holding_quantity_seconds / normal_quantity
                         if normal_quantity else None),
        fast_fraction=(fast_quantity / buy if buy else None),
    )


def markout(volume, yen, count, eligible_count=None, observed_count=None,
            censored_count=0, eligible_volume=None, observed_volume=None,
            censored_volume=0):
    return dict(count=count, volume=volume, yen_per_btc=yen / volume if volume else None,
                markout_yen=yen,
                eligible_count=eligible_count if eligible_count is not None else count,
                observed_count=observed_count if observed_count is not None else count,
                censored_count=censored_count,
                eligible_volume=eligible_volume if eligible_volume is not None else volume,
                observed_volume=observed_volume if observed_volume is not None else volume,
                censored_volume=censored_volume)


class ResultSummaryTests(unittest.TestCase):
    def test_merge_activity_recalculates_weighted_values_and_checks_threshold(self):
        merged = merge_activity([
            activity(buy=1, normal_quantity=1, holding_quantity_seconds=10,
                     fast_quantity=1, normal_profit=3),
            activity(buy=3, normal_quantity=3, holding_quantity_seconds=180,
                     fast_quantity=0, normal_loss=-2),
        ])
        self.assertEqual(merged['holding_seconds'], 47.5)
        self.assertEqual(merged['fast_fraction'], 0.25)
        self.assertEqual(merged['normal_profit'], 3)
        self.assertEqual(merged['normal_loss'], -2)
        self.assertEqual(merged['buy_quantity'], 4)
        with self.assertRaisesRegex(ValueError, 'fast'):
            merge_activity([activity(fast_seconds=60), activity(fast_seconds=30)])

    def test_merge_activity_zero_denominators_and_boundary(self):
        zero = merge_activity([activity()])
        self.assertIsNone(zero['holding_seconds'])
        self.assertIsNone(zero['fast_fraction'])
        at_boundary = merge_activity([activity(buy=2, normal_quantity=2,
                                               holding_quantity_seconds=120,
                                               fast_quantity=1)])
        self.assertEqual(at_boundary['holding_seconds'], 60)
        self.assertEqual(at_boundary['fast_fraction'], 0.5)

    def test_aggregate_results_groups_config_and_recomputes_ratios_and_markouts(self):
        rows = []
        for cfg, pnl, turnover, buys, filled, dd, mo in (
            ('a', 10, 100, 2, 1, 4, markout(2, 4, 2, censored_count=1,
                                             eligible_count=3, observed_count=2,
                                             eligible_volume=3, observed_volume=2,
                                             censored_volume=1)),
            ('a', 30, 300, 1, 1, 7, markout(1, 8, 1)),
            ('b', 5, 100, 1, 0, 2, markout(1, 1, 1)),
        ):
            rows.append(dict(
                case='c', config=cfg, bot_metadata={'name': 'bot'}, segment=cfg,
                gross_pnl=pnl, turnover=turnover, buy_orders=buys,
                sell_orders=0, buy_filled_orders=filled, sell_filled_orders=0,
                max_drawdown_liquidation_mtm=dd,
                activity=activity(buy=buys),
                position_activity={side: dict(
                    entry_quantity=buys if side == 'BUY' else 0,
                    **{k: v for k, v in activity().items()
                       if k not in ('buy_quantity', 'sell_quantity')})
                    for side in ('BUY', 'SELL')},
                fill_markouts={'60': mo}, fill_markouts_by_side_kind={'BUY/maker': {'60': mo}},
            ))
        results = aggregate_results(rows)
        self.assertEqual(len(results), 2)
        a = next(row for row in results if row['config'] == 'a')
        self.assertEqual(a['gross_pnl'], 40)
        self.assertEqual(a['pnl_per_million'], 100000)
        self.assertEqual(a['buy_fill_rate'], 2 / 3)
        self.assertEqual(a['max_segment_drawdown_liquidation_mtm'], 7)
        self.assertEqual(a['fill_markouts']['60']['markout_yen'], 12)
        self.assertEqual(a['fill_markouts']['60']['yen_per_btc'], 4)
        self.assertEqual(a['fill_markouts']['60']['eligible_count'], 4)
        self.assertEqual(a['fill_markouts']['60']['censored_count'], 1)
        self.assertEqual(a['fill_markouts_by_side_kind']['BUY/maker'], a['fill_markouts'])
        self.assertEqual(a['position_activity']['BUY']['entry_quantity'], 3)
        self.assertIsNone(a['sell_fill_rate'])

    def test_load_rows_retains_comparison_metadata_and_old_results(self):
        bundle = dict(results=[{'case': 'old', 'segment': 's'}],
                      input_raw_sha256='input', code_sha256={'bot.py': 'code'})
        with patch('backtest2.analysis.load_audited_results', return_value=bundle) as load:
            rows = load_result_rows(['first', 'second'])
        self.assertEqual(load.call_count, 2)
        self.assertEqual([r['source_directory'] for r in rows], ['first', 'second'])
        self.assertEqual(rows[0]['input_raw_sha256'], 'input')
        self.assertEqual(rows[0]['code_sha256'], {'bot.py': 'code'})
        self.assertNotIn('activity', rows[0])

    def test_aggregate_old_rows_fail_explicitly_without_activity(self):
        row = dict(case='c', config={}, bot_metadata={}, segment='s')
        with self.assertRaisesRegex(ValueError, 'activity'):
            aggregate_results([row])


if __name__ == '__main__':
    unittest.main()
