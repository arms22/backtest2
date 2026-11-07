import unittest

from backtest2.analysis import summarize_orders


def order(side='BUY', **overrides):
    value = dict(order='o', side=side, decision=0, arrival=1,
                 cancel_requested=None, cancelled_at=None, first_fill=None,
                 last_fill=None, price=99, size=1, executed_size=0,
                 remaining=1, state='active', bid=99, ask=101, cutoff=100)
    value.update(overrides)
    return value


class OrderSummaryTests(unittest.TestCase):
    def test_empty_result_has_both_sides_and_empty_statistics(self):
        result = summarize_orders([])
        for side in ('BUY', 'SELL'):
            self.assertEqual(result[side]['orders'], 0)
            self.assertEqual(result[side]['state_counts']['active'], 0)
            for metric in ('cancel_request_seconds', 'cancel_complete_seconds',
                           'first_fill_seconds', 'order_interval_seconds',
                           'distance_mid_bp'):
                self.assertEqual(result[side][metric],
                                 dict(count=0, p50=None, p90=None, p99=None))

    def test_percentiles_use_linear_interpolation_and_missing_times_are_skipped(self):
        orders = [
            order(decision=0, cancel_requested=1, first_fill=2, price=98,
                  executed_size=.5, state='filled'),
            order(decision=10, cancel_requested=13, price=97, state='cancelled'),
            order(decision=30, cancel_requested=None, price=96),
        ]
        buy = summarize_orders(orders)['BUY']
        self.assertEqual(buy['orders'], 3)
        self.assertEqual(buy['filled_orders'], 1)
        self.assertEqual(buy['unfilled_cancelled'], 1)
        self.assertEqual(buy['cancel_request_seconds']['count'], 2)
        self.assertEqual(buy['cancel_request_seconds']['p50'], 2)
        self.assertEqual(buy['cancel_request_seconds']['p90'], 2.8)
        self.assertEqual(buy['first_fill_seconds']['p50'], 2)
        self.assertEqual(buy['order_interval_seconds']['count'], 2)
        self.assertEqual(buy['order_interval_seconds']['p50'], 15)

    def test_distance_sign_is_side_relative_and_cancel_completion_is_observed(self):
        result = summarize_orders([
            order('BUY', price=99, cancelled_at=4, state='cancelled'),
            order('SELL', price=101, executed_size=1, state='filled'),
        ])
        self.assertEqual(result['BUY']['distance_mid_bp']['p50'], 100)
        self.assertEqual(result['SELL']['distance_mid_bp']['p50'], 100)
        self.assertEqual(result['BUY']['cancel_complete_seconds']['p50'], 4)
        self.assertEqual(result['SELL']['cancel_complete_seconds']['count'], 0)

if __name__ == '__main__':
    unittest.main()
