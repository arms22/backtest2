import asyncio
import sys
import unittest
from pathlib import Path

from backtest2.accounting import Position
from backtest2.replay import (ACTIVE, ReplayConfig, ReplayEngine, sleep,
                             wakeup, wakeup_every_seconds)
from backtest2.runner import BotCase, source_hashes


def market(second, **changes):
    row = dict(second=second, timestamp=second+.01, bids=[[90., .1]], asks=[[120., .1]],
               bid=90., ask=120., high=99., low=98., sell_volume=.006, buy_volume=0.,
               sell_count=1, buy_count=0, trades=1)
    row.update(changes)
    return row


class Adapter:
    def __init__(self, engine):
        self.engine = engine

    def on_fill(self, order, quantity, price, kind):
        pass

    def on_market_update(self):
        pass


class PassiveBot:
    async def run(self, strategy):
        while True:
            await wakeup()


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
    def test_fifo_partial_close_reverse_and_terminal(self):
        position = Position()
        for side, size, price in [('BUY', .01, 100), ('BUY', .02, 110),
                                  ('SELL', .04, 120), ('BUY', .01, 115)]:
            position.add(dict(side=side, size=size, price=price, commission=0))
        self.assertEqual(position.position_size, 0)
        self.assertAlmostEqual(position.netprofit, .45)

    def test_shared_package_imports_no_exchange_repository(self):
        self.assertFalse(any(name.split('.')[0] in ('firebots', 'bankbots', 'coinbots', 'gmobots')
                             for name in sys.modules))

    def test_source_provenance_accepts_external_repository(self):
        source = Path(__file__).resolve()
        hashes = source_hashes([BotCase('test', PassiveBot, Adapter, code_files=(source,))], source)
        self.assertTrue(any(path.endswith('tests/test_replay.py') for path in hashes))
        self.assertTrue(any(path.endswith('backtest2/replay.py') for path in hashes))


if __name__ == '__main__':
    unittest.main()
