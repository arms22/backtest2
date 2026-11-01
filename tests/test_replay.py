import asyncio
import unittest

from backtest2.accounting import Position
from backtest2.replay import (ACTIVE, Executions, MarketBoard, ReplayConfig, ReplayEngine, sleep,
                             wakeup_every_seconds)
from helpers import Adapter, PassiveBot, market


class ReplayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = ReplayEngine(PassiveBot(), ReplayConfig(), 0, 100, 'test', 'coin_jpy', 'test', Adapter)
        await self.engine.launch()

    async def asyncTearDown(self):
        await self.engine.stop()

    def place(self, price=100., size=.004, **kwargs):
        self.engine.now, self.engine.row = 1., market(1)
        return self.engine.submit_order('BUY', size, price, **kwargs)

    async def test_partial_fills_share_directional_volume(self):
        low, high = self.place(), self.place(price=101.)
        await self.engine.tick(market(5))
        self.assertEqual(high['executed_size'], .004)
        self.assertEqual(low['executed_size'], .002)
        self.assertAlmostEqual(self.engine.position, .006)

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

    async def test_takers_share_visible_depth(self):
        one, two = self.place(price=125), self.place(price=125)
        await self.engine.tick(market(5, asks=[[120., .006], [130., .1]],
                                      sell_volume=0., sell_count=0, trades=0))
        self.assertAlmostEqual(one['executed_size'] + two['executed_size'], .006)

    async def test_no_vwap_extrapolation(self):
        self.place()
        await self.engine.tick(market(5))
        self.engine.row['bids'] = [[90., .001]]
        with self.assertRaisesRegex(ValueError, 'Insufficient stored depth'):
            self.engine.equity()

    async def test_future_snapshot_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'newer than'):
            await self.engine.tick(market(5, timestamp=5.99))

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

    async def test_market_board_displays_arrived_orders_until_cancel_is_effective(self):
        order = self.place()
        board = MarketBoard(self.engine)
        self.assertEqual(board.sort(), [[(90., .1)], [(120., .1)]])
        self.engine.now = 3.
        self.engine.submit_order('BUY', .002, 90.)
        self.engine.submit_order('SELL', .003, 120.)
        self.engine.now = 5.
        bids, asks = board.sort()
        self.assertEqual(bids[0], (100., .004))
        self.assertAlmostEqual(bids[1][1], .102)
        self.assertAlmostEqual(asks[0][1], .103)
        self.assertEqual((board.best_bid, board.best_ask), (100., 120.))
        self.engine.cancel_order(order)
        self.assertEqual(board.sort()[0][0], (100., .004))
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
    async def test_bot_exception_is_propagated_and_task_finishes(self):
        class BrokenBot(PassiveBot):
            async def run(self, strategy):
                raise ValueError('broken startup')

        engine = ReplayEngine(BrokenBot(), ReplayConfig(), 0, 10,
                              'test', 'TEST_JPY', 'broken', Adapter)
        with self.assertRaisesRegex(ValueError, 'broken startup'):
            await engine.launch()
        self.assertTrue(engine.task.done())

    async def test_missing_virtual_clock_binding_times_out(self):
        class SleepingBot(PassiveBot):
            async def run(self, strategy):
                await asyncio.sleep(10)

        engine = ReplayEngine(SleepingBot(), ReplayConfig(tick_timeout=.01),
                              0, 10, 'test', 'TEST_JPY', 'sleeping', Adapter)
        try:
            with self.assertRaises(TimeoutError):
                await engine.launch()
        finally:
            await engine.stop()
        self.assertTrue(engine.task.cancelled())

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
        engine = ReplayEngine(bot, ReplayConfig(), 0, 40, 'test', 'coin_jpy', 'clock', Adapter)
        try:
            await engine.launch()
            for second in range(35):
                await engine.tick(market(second))
            self.assertEqual(bot.times, [15.9, 16.9, 30.9, 31.9])
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
    def test_negative_latency_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Latency must be nonnegative'):
            ReplayConfig(latency=-1)


if __name__ == '__main__':
    unittest.main()
