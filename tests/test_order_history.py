import json
from pathlib import Path
import tempfile
import unittest

from backtest2.analysis import read_jsonl_gz, summarize_orders
from backtest2.audit import audit
from backtest2.data import iso
from backtest2.replay import ReplayConfig, ReplayEngine, wakeup
from backtest2.runner import BotCase, run_backtest
from helpers import Adapter, PassiveBot, market, save_market


class OrderLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def make_engine(self, *, latency=0., cancel_latency=1.):
        engine = ReplayEngine(PassiveBot(),
                              ReplayConfig(latency=latency, cancel_latency=cancel_latency),
                              0, 10, 'test', 'test', Adapter)
        engine.row = market(1)
        engine.now = 1.
        await engine.launch()
        return engine

    async def test_cancel_fill_expiry_and_rejection_lifecycle(self):
        engine = await self.make_engine()
        unfilled = engine.submit_order('BUY', .004, 80.)
        engine.cancel_order(unfilled)
        await engine.tick(market(5, low=99., sell_volume=0., trades=0))
        self.assertEqual(engine.order_history[unfilled['id']]['state'], 'cancelled')
        self.assertEqual(engine.order_history[unfilled['id']]['executed_size'], 0.)
        await engine.finish()

        engine = await self.make_engine()
        partial = engine.submit_order('BUY', .01, 100.)
        engine.now = 4.9
        engine.cancel_order(partial)
        await engine.tick(market(5, sell_volume=.006))
        self.assertAlmostEqual(engine.order_history[partial['id']]['executed_size'], .006)
        self.assertEqual(engine.order_history[partial['id']]['state'], 'canceling')
        history = engine.order_history[partial['id']]
        self.assertEqual(history['cancel_requested'], 4.9)
        self.assertEqual(history['first_fill'], 5.01)
        self.assertEqual(history['last_fill'], 5.01)
        self.assertAlmostEqual(history['remaining'], .004)
        await engine.tick(market(6, low=99., sell_volume=0., trades=0))
        self.assertEqual(engine.order_history[partial['id']]['state'], 'cancelled')
        self.assertEqual(history['cancelled_at'], 5.9)
        await engine.finish()

        engine = await self.make_engine()
        filled = engine.submit_order('BUY', .004, 100.)
        await engine.tick(market(5, sell_volume=.006))
        self.assertEqual(engine.order_history[filled['id']]['state'], 'filled')
        self.assertAlmostEqual(engine.order_history[filled['id']]['executed_size'], .004)
        await engine.finish()

        engine = await self.make_engine()
        pending_cancel = engine.submit_order('BUY', .004, 80.)
        engine.now = 4.9
        engine.cancel_order(pending_cancel)
        # The cancel completes at 5.9, so a market touch at 5 can still fill.
        await engine.tick(market(5, low=79., sell_volume=.004))
        self.assertEqual(engine.order_history[pending_cancel['id']]['state'], 'filled')
        await engine.finish()

        engine = await self.make_engine()
        post_only = engine.submit_order('BUY', .004, 120., post_only=True)
        await engine.tick(market(5))
        self.assertEqual(engine.order_history[post_only['id']]['state'], 'rejected')
        self.assertEqual(engine.order_history[post_only['id']]['executed_size'], 0.)
        await engine.finish()

    async def test_expiry_and_cutoff_states_are_preserved(self):
        engine = await self.make_engine(latency=1., cancel_latency=4.)
        expiring = engine.submit_order('BUY', .004, 80., expire=5.)
        active = engine.submit_order('SELL', .004, 140.)
        canceling = engine.submit_order('BUY', .004, 70.)
        await engine.tick(market(5, low=99., high=121., trades=0))
        self.assertEqual(engine.order_history[expiring['id']]['state'], 'expired')
        self.assertEqual(engine.order_history[active['id']]['state'], 'active')
        engine.now = 5.9
        engine.cancel_order(canceling)
        await engine.tick(market(6, low=99., high=121., trades=0))
        self.assertEqual(engine.order_history[canceling['id']]['state'], 'canceling')
        pending = engine.submit_order('BUY', .004, 80.)
        await engine.finish()
        self.assertEqual(engine.order_history[active['id']]['state'], 'active')
        self.assertTrue(engine.order_history[active['id']]['open_at_cutoff'])
        self.assertEqual(engine.order_history[canceling['id']]['state'], 'canceling')
        self.assertTrue(engine.order_history[canceling['id']]['open_at_cutoff'])
        self.assertEqual(engine.order_history[pending['id']]['state'], 'pending')
        self.assertTrue(engine.order_history[pending['id']]['open_at_cutoff'])


class OrderHistoryRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_recording_does_not_change_replay_and_saves_linked_orders(self):
        class OrderStrategy(Adapter):
            def submit_order(self, side, size, price, **kwargs):
                return self.engine.submit_order(side, size, price, **kwargs)

        class OrderBot:
            async def run(self, strategy):
                await wakeup()
                strategy.submit_order('BUY', .004, 100.)
                while True:
                    await wakeup()

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / 'data'
            data.mkdir()
            save_market(data, range(2000, 2008))
            manifest_path = data / 'fetch_manifest.json'
            manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
            manifest['end_utc_exclusive'] = iso(2008)
            manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
            cases = [BotCase(name, OrderBot, OrderStrategy)
                     for name in ('first', 'second')]
            configs = [ReplayConfig(model='touch'), ReplayConfig(model='through'),
                       ReplayConfig(model='touch', latency=.2)]
            outputs = {}
            for record_orders in (True, False):
                output = root / f'output_{record_orders}'
                active_configs = [ReplayConfig(model=config.model, latency=config.latency,
                                               record_orders=record_orders)
                                  for config in configs]
                bundle = await run_backtest(
                    data, output, cases, active_configs, __file__, split_at=[2004],
                    progress=lambda text: None)
                outputs[record_orders] = (output, bundle)

            recorded, baseline = outputs[True][1]['results'], outputs[False][1]['results']
            self.assertEqual(len(recorded), 12)
            self.assertEqual(len(baseline), 12)
            self.assertGreater(sum(result['buy_filled_orders'] for result in recorded), 0)
            for with_history, without_history in zip(recorded, baseline):
                for key in ('gross_pnl', 'turnover', 'terminal_position',
                            'buy_orders', 'sell_orders', 'buy_filled_orders',
                            'sell_filled_orders', 'counts'):
                    self.assertEqual(with_history[key], without_history[key])
                self.assertIsNotNone(with_history['orders_file'])
                self.assertIsNone(without_history['orders_file'])

                output = outputs[True][0]
                order_path = output / with_history['orders_file']
                orders = read_jsonl_gz(order_path)
                self.assertEqual(with_history['order_summary'], summarize_orders(orders))
                self.assertEqual(
                    with_history['buy_orders'], with_history['order_summary']['BUY']['orders'])
                self.assertEqual(
                    with_history['buy_filled_orders'],
                    with_history['order_summary']['BUY']['filled_orders'])
                self.assertEqual(
                    with_history['sell_orders'], with_history['order_summary']['SELL']['orders'])
                self.assertEqual(
                    with_history['sell_filled_orders'],
                    with_history['order_summary']['SELL']['filled_orders'])
                prefix = f"{with_history['segment']}_{with_history['name']}"
                fills = read_jsonl_gz(output / f'{prefix}_fills.jsonl.gz')
                self.assertTrue(all(fill['order'] in {order['order'] for order in orders}
                                    for fill in fills if fill['kind'] != 'terminal'))
                baseline_output = outputs[False][0]
                baseline_fills = read_jsonl_gz(
                    baseline_output / f'{prefix}_fills.jsonl.gz')
                self.assertEqual(fills, baseline_fills)

            # The filename includes segment and case/model/latency, so all twelve
            # runs keep a distinct order ledger.
            order_files = [result['orders_file'] for result in recorded]
            self.assertEqual(len(order_files), len(set(order_files)))
            self.assertEqual(len(order_files), 12)
            for output, _ in outputs.values():
                self.assertTrue(audit(data, output)['checks'])


if __name__ == '__main__':
    unittest.main()
