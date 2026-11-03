import asyncio
import inspect
import unittest

from types import ModuleType

from backtest2.asyncio_support import ReplayAsyncio, sequential_asyncio


class ReplayAsyncioTests(unittest.IsolatedAsyncioTestCase):
    async def test_sequential_execution_and_patch_scope(self):
        events = []

        async def operation(number):
            events.append(('start', number))
            await asyncio.sleep(0)
            events.append(('end', number))
            return number

        bot_module = ModuleType("bot")
        bot_module.asyncio = asyncio
        strategy2 = ModuleType("strategy")
        strategy2.asyncio = asyncio
        original_gather = asyncio.gather
        with sequential_asyncio((bot_module, strategy2, bot_module)):
            self.assertIs(bot_module.asyncio, strategy2.asyncio)
            self.assertIs(asyncio.gather, original_gather)
            self.assertIs(bot_module.asyncio.sleep, asyncio.sleep)
            result = await bot_module.asyncio.gather(operation(1), operation(2))
        self.assertIs(bot_module.asyncio, asyncio)
        self.assertIs(strategy2.asyncio, asyncio)
        self.assertEqual(result, [1, 2])
        self.assertEqual(events, [('start', 1), ('end', 1), ('start', 2), ('end', 2)])

    async def test_failure_and_cancellation_close_unstarted_coroutines(self):
        async def fail(error):
            raise error

        async def unused():
            self.fail('Remaining operation must not run')

        for error in (ValueError('failed'), asyncio.CancelledError()):
            remaining = unused()
            with self.assertRaises(type(error)):
                await ReplayAsyncio().gather(fail(error), remaining)
            self.assertEqual(inspect.getcoroutinestate(remaining), inspect.CORO_CLOSED)

    async def test_empty_and_return_exceptions(self):
        async def fail():
            raise ValueError('failed')

        proxy = ReplayAsyncio()
        self.assertEqual(await proxy.gather(), [])
        results = await proxy.gather(fail(), asyncio.sleep(0, result=2), return_exceptions=True)
        self.assertIsInstance(results[0], ValueError)
        self.assertEqual(results[1], 2)
