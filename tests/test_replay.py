import asyncio
import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from backtest2.data import iter_market_rows
from backtest2.accounting import Position
from backtest2.replay import (ACTIVE, Executions, MarketBoard, ReplayConfig, ReplayEngine, sleep,
                             wakeup_every_seconds)
from helpers import Adapter, PassiveBot, market


class ReplayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = ReplayEngine(PassiveBot(), ReplayConfig(), 0, 100, 'test', 'test', Adapter)
        await self.engine.launch()

    async def asyncTearDown(self):
        await self.engine.stop()

    def place(self, price=100., size=.004, **kwargs):
        self.engine.now, self.engine.row = 1., market(1)
        return self.engine.submit_order('BUY', size, price, **kwargs)

    async def test_market_data_preserves_saved_columns_and_isolates_changes(self):
        sources = []
        for second in (1, 2):
            row = market(second)
            for key in ('second', 'timestamp'):
                del row[key]
            row['time'] = f'1970-01-01T00:00:0{second}Z'
            row['bids_raw'] = json.dumps(row.pop('bids'))
            row['asks_raw'] = json.dumps(row.pop('asks'))
            row['exchange_extra'] = {'values': [second], 'optional': None}
            sources.append(row)
        with self.assertRaisesRegex(RuntimeError, 'No market data yet'):
            self.engine.get_market_data()
        with tempfile.TemporaryDirectory() as directory:
            with gzip.open(Path(directory)/'market_rows.jsonl.gz', 'wt', encoding='utf-8') as target:
                for row in sources:
                    target.write(json.dumps(row) + '\n')
            rows = iter_market_rows(directory, 0, 100)
            await self.engine.tick(next(rows))
            data = self.engine.get_market_data()
            self.assertEqual(data, sources[0])
            data['exchange_extra']['values'].append(999)
            data['buy_volume'] = 999
            self.assertEqual(self.engine.get_market_data(), sources[0])
            self.assertEqual(self.engine.row['buy_volume'], sources[0]['buy_volume'])
            await self.engine.tick(next(rows))
            self.assertEqual(self.engine.get_market_data(), sources[1])
            rows.close()

    async def test_partial_fills_share_directional_volume(self):
        low, high = self.place(), self.place(price=101.)
        await self.engine.tick(market(5))
        self.assertEqual(high['executed_size'], .004)
        self.assertEqual(low['executed_size'], .002)
        self.assertAlmostEqual(self.engine.position, .006)

    async def test_order_fill_rates_count_orders_not_fill_events(self):
        order = self.place(size=.01)
        self.engine.submit_order('BUY', .001, 80.)
        self.engine.submit_order('SELL', .001, 80., post_only=True)
        cancelled = self.engine.submit_order('SELL', .001, 130.)
        self.engine.cancel_order(cancelled)
        await self.engine.tick(market(5))
        await self.engine.tick(market(6))
        self.assertEqual(order['executed_size'], .01)
        result = await self.engine.finish()
        self.assertEqual(result['buy_orders'], 2)
        self.assertEqual(result['buy_filled_orders'], 1)
        self.assertEqual(result['buy_fill_rate'], .5)
        self.assertEqual(result['sell_orders'], 2)
        self.assertEqual(result['sell_filled_orders'], 0)
        self.assertEqual(result['sell_fill_rate'], 0.)
        self.assertEqual(result['counts']['maker_fills'], 2)

    async def test_order_fill_rates_without_orders_are_undefined(self):
        await self.engine.tick(market(1))
        result = await self.engine.finish()
        for side in ('buy', 'sell'):
            self.assertEqual(result[side + '_orders'], 0)
            self.assertEqual(result[side + '_filled_orders'], 0)
            self.assertIsNone(result[side + '_fill_rate'])

    async def test_post_only_rejects_crossing_at_arrival(self):
        order = self.place(price=120., post_only=True)
        await self.engine.tick(market(5))
        self.assertEqual(order['state'], 'rejected')
        self.assertEqual(self.engine.position, 0)
        self.assertEqual(self.engine.counts['post_only_rejections'], 1)
        self.assertEqual(self.engine.orders, [])

    async def test_arrival_and_cancel_pending_fill(self):
        order = self.place()
        self.engine.now = 4.9
        self.engine.cancel_order(order)
        await self.engine.tick(market(5))
        self.assertEqual(order['executed_size'], .004)

    async def test_no_fill_from_pre_arrival_aggregate(self):
        self.engine.now, self.engine.row = 4.9, market(4)
        order = self.engine.submit_order('BUY', .004, 100.)
        await self.engine.tick(market(5))
        self.assertEqual(order['executed_size'], 0)

    async def test_trade_through_uses_only_eligible_trades_in_arrival_row(self):
        self.engine.config = ReplayConfig(model='trade_through')
        await self.engine.tick(market(1, timestamp=1., trades_raw='[]'))
        buy = self.engine.submit_order('BUY', .005, 100.)
        better_buy = self.engine.submit_order('BUY', .002, 101.)
        sell = self.engine.submit_order('SELL', .005, 110., expire=1.5)
        trades = [
            [-950, -1, 99, .010],  # Before arrival.
            [-895, -1, 99, .010],  # At the arrival millisecond.
            [-800, -1, 100, .001],  # Strictly through only the better buy.
            [-700, -1, 99, .003],  # Shared across buys in price order.
            [-600, 1, 110, .010],  # At the sell limit.
            [-550, 1, 111, .002],  # Eligible before expiry.
            [-500, 1, 111, .010],  # At expiry.
            [1, -1, 99, .010],  # After the row timestamp.
        ]
        await self.engine.tick(market(2, timestamp=2., trades_raw=json.dumps(trades)))
        self.assertEqual(buy['executed_size'], .002)
        self.assertEqual(better_buy['executed_size'], .002)
        self.assertEqual(sell['executed_size'], .002)
        self.assertEqual([(f['kind'], f['price'], f['t']) for f in self.engine.fills],
                         [('maker', 101., 2.), ('maker', 100., 2.), ('maker', 110., 2.)])

    async def test_trade_through_requires_raw_data(self):
        self.engine.config = ReplayConfig(model='trade_through')
        with self.assertRaisesRegex(ValueError, 'requires a trades_raw'):
            await self.engine.tick(market(1))

    async def test_takers_share_visible_depth(self):
        one, two = self.place(price=125), self.place(price=125)
        await self.engine.tick(market(5, asks=[[120., .006], [130., .1]],
                                      sell_volume=0., sell_count=0, trades=0))
        self.assertAlmostEqual(one['executed_size'] + two['executed_size'], .006)

    async def test_takers_share_partial_level_across_prices(self):
        self.place(price=125, size=.004)
        self.place(price=125, size=.004)
        asks = [[120., .006], [124., .01], [130., .1]]
        await self.engine.tick(market(5, asks=asks, trades=0))
        self.assertEqual([f['price'] for f in self.engine.fills], [120., 122.])
        self.assertEqual(asks, [[120., .006], [124., .01], [130., .1]])

    async def test_no_vwap_extrapolation(self):
        self.place()
        await self.engine.tick(market(5))
        self.engine.row['bids'] = [[90., .001]]
        with self.assertRaisesRegex(ValueError, 'Insufficient stored depth'):
            self.engine.equity()

    async def test_decision_offset_is_relative_to_snapshot(self):
        await self.engine.tick(market(5, timestamp=5.99))
        self.assertAlmostEqual(self.engine.now, 5.995)
        order = self.engine.submit_order('BUY', .003, 100.)
        self.assertAlmostEqual(order['arrival'], 6.095)

    async def test_warmup_suppresses_new_orders(self):
        self.engine.config = ReplayConfig(warmup_seconds=10)
        self.assertIsNone(self.place())
        self.assertEqual(self.engine.counts['new_orders'], 0)

    async def test_executions_report_sides_with_positive_trade_counts(self):
        executions = Executions(self.engine)
        for buy_count, sell_count, expected in (
                (0, 0, []), (2, 0, ['BUY']), (0, 3, ['SELL']), (2, 3, ['BUY', 'SELL'])):
            with self.subTest(buy_count=buy_count, sell_count=sell_count):
                self.engine.row = market(1, buy_count=buy_count, sell_count=sell_count)
                self.assertEqual(await executions.get_data(), [dict(side=side) for side in expected])

    async def test_market_board_returns_stored_depth_without_adding_orders(self):
        order = self.place()
        board = MarketBoard(self.engine)
        self.assertEqual(board.sort(), ([[90., .1]], [[120., .1]]))
        self.engine.now = 3.
        self.engine.submit_order('BUY', .002, 90.)
        self.engine.submit_order('SELL', .003, 120.)
        self.engine.now = 5.
        bids, asks = board.sort()
        self.assertIs(bids, self.engine.row['bids'])
        self.assertIs(asks, self.engine.row['asks'])
        self.assertEqual((board.best_bid, board.best_ask), (90., 120.))
        self.engine.cancel_order(order)
        self.assertEqual(board.sort()[0][0], [90., .1])
        self.engine.now = 6.
        self.assertEqual(board.sort()[0][0][0], 90.)

    async def test_finish_liquidates_position_and_notifies_adapter(self):
        notifications = []
        self.engine.strategy.on_fill = lambda order, quantity, price, kind: notifications.append(
            (order['id'], order['side'], quantity, price, kind, self.engine.position))
        self.place()
        await self.engine.tick(market(5))
        self.engine.submit_order('BUY', .002, 80.)
        result = await self.engine.finish()
        self.assertEqual(notifications[-1], ('terminal', 'SELL', .004, 90., 'terminal', 0.))
        self.assertEqual(self.engine.orders, [])
        self.assertEqual(result['counts']['terminal_fills'], 1)
        self.assertEqual(result['counts']['terminal_cancelled_orders'], 1)
        self.assertAlmostEqual(result['terminal_position'], .004)
        self.assertAlmostEqual(result['gross_pnl'], -.04)


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_bot_exception_is_propagated(self):
        class BrokenBot(PassiveBot):
            async def run(self, strategy):
                raise ValueError('broken startup')

        engine = ReplayEngine(BrokenBot(), ReplayConfig(), 0, 10,
                              'test', 'broken', Adapter)
        with self.assertRaisesRegex(ValueError, 'broken startup'):
            await engine.launch()
        await engine.stop()

    async def test_missing_virtual_clock_binding_fails_and_cleans_up(self):
        stopped = []

        class SleepingBot(PassiveBot):
            async def run(self, strategy):
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    stopped.append(ACTIVE.get())
                    raise

        engine = ReplayEngine(SleepingBot(), ReplayConfig(), 0, 10,
                              'test', 'sleeping', Adapter)
        try:
            with self.assertRaisesRegex(RuntimeError, 'suspended outside the replay clock'):
                await engine.launch()
        finally:
            await engine.stop()
        self.assertEqual(stopped, [engine])

    async def test_tick_timeout_is_checked_when_bot_returns_to_clock(self):
        engine = ReplayEngine(PassiveBot(), ReplayConfig(tick_timeout=.01),
                              0, 10, 'test', 'slow', Adapter)
        try:
            await engine.launch()
            with patch('backtest2.replay.perf_counter', side_effect=[0., .02]):
                with self.assertRaisesRegex(TimeoutError, 'took too long'):
                    await engine.tick(market(0))
        finally:
            await engine.stop()

    async def test_periodic_clock_and_fractional_sleep_use_virtual_time(self):
        class TimedBot:
            def __init__(self):
                self.times = []

            async def run(self, strategy):
                while True:
                    await wakeup_every_seconds(15)
                    self.times.append(ACTIVE.get().now)
                    await sleep(.5)
                    self.times.append(ACTIVE.get().now)

        bot = TimedBot()
        engine = ReplayEngine(bot, ReplayConfig(), 0, 40, 'test', 'clock', Adapter)
        try:
            await engine.launch()
            for second in range(35):
                await engine.tick(market(second))
            self.assertEqual(bot.times, [15.015, 16.015, 30.015, 31.015])
        finally:
            await engine.stop()


class AccountingTests(unittest.TestCase):
    def test_fifo_partial_close_and_reverse(self):
        position = Position()
        for side, size, price, remaining, realized in (
                ('BUY', .01, 100, .01, 0),
                ('BUY', .02, 110, .03, 0),
                ('SELL', .005, 120, .025, .1),
                ('SELL', .035, 120, -.01, .4),
                ('BUY', .01, 115, 0, .45)):
            with self.subTest(side=side, size=size, price=price):
                position.add(dict(side=side, size=size, price=price, commission=0))
                self.assertAlmostEqual(position.position_size, remaining)
                self.assertAlmostEqual(position.netprofit, realized)


class ConfigurationTests(unittest.TestCase):
    def test_default_model_accounts_for_visible_queue(self):
        engine = ReplayEngine(PassiveBot(), ReplayConfig(), 0, 100, 'test', 'test', Adapter)
        engine.now, engine.row = 1., market(1)
        order = engine.submit_order('BUY', .004, 100.)
        engine.market_update(market(5, bids=[[100., .005]], sell_volume=.006))
        self.assertAlmostEqual(order['executed_size'], .001)

    def test_negative_latency_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Latency must be nonnegative'):
            ReplayConfig(latency=-1)


if __name__ == '__main__':
    unittest.main()
