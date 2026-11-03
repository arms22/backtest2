"""Sequential offline operations, scoped to explicitly selected bot modules."""
import asyncio
import inspect
from contextlib import ExitStack, contextmanager
from unittest.mock import patch


class ReplayAsyncio:
    """Delegate to asyncio except for sequential gather calls."""
    def __getattr__(self, name):
        return getattr(asyncio, name)

    async def gather(self, *awaitables, return_exceptions=False):
        results = []
        index = -1
        try:
            for index, awaitable in enumerate(awaitables):
                try:
                    results.append(await awaitable)
                except Exception as error:
                    if not return_exceptions:
                        raise
                    results.append(error)
            return results
        finally:
            for awaitable in awaitables[index + 1:]:
                if inspect.iscoroutine(awaitable):
                    awaitable.close()


@contextmanager
def sequential_asyncio(modules):
    """Replace module-local asyncio references and restore them on exit."""
    proxy = ReplayAsyncio()
    with ExitStack() as patches:
        for module in dict.fromkeys(modules):
            patches.enter_context(patch.object(module, 'asyncio', proxy))
        yield
