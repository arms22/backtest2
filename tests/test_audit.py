import json
from pathlib import Path
import tempfile
import unittest

from backtest2.audit import audit
from backtest2.replay import ReplayConfig, wakeup
from backtest2.runner import BotCase, run_backtest
from helpers import Adapter, save_market


class AuditTests(unittest.IsolatedAsyncioTestCase):
    async def test_saved_fills_reconcile_and_changed_pnl_is_rejected(self):
        class TradingBot:
            async def run(self, strategy):
                while True:
                    await wakeup()
                    strategy.engine.submit_order('BUY', .003, 100.)

        with tempfile.TemporaryDirectory() as directory:
            data, output = Path(directory)/'data', Path(directory)/'output'
            data.mkdir()
            save_market(data, range(2000, 2004))
            bundle = await run_backtest(data, output, [BotCase('trading', TradingBot, Adapter)],
                                        [ReplayConfig()], __file__, progress=lambda text: None)
            checked = audit(data, output)
            self.assertAlmostEqual(checked['checks'][0]['pnl'], -.03)
            self.assertTrue(checked['checks'][0]['flat'])
            bundle['results'][0]['gross_pnl'] += 1
            (output/'results.json').write_text(json.dumps(bundle), encoding='utf-8')
            with self.assertRaises(AssertionError):
                audit(data, output)
