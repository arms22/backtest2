"""Offline single-product GTC limit-order replay; exchange behavior is supplied by a factory."""
import asyncio
import contextvars
import math
from collections import Counter, deque
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from time import perf_counter
from types import coroutine

from .accounting import Position
from .data import decode_market_json

ACTIVE = contextvars.ContextVar('backtest_engine')
_NEXT_TICK = object()
FILL_MODELS = ('touch', 'through', 'queue_touch', 'trade_through')


def vwap(book, quantity):
    remaining, cost = quantity, 0.0
    for price, size in book:
        taken = min(remaining, size)
        cost += price * taken
        remaining -= taken
        if remaining < 1e-10:
            return cost / quantity
    raise ValueError('Insufficient stored depth; do not extrapolate the last price')


@dataclass(frozen=True)
class ReplayConfig:
    model: str = 'queue_touch'
    latency: float = 0.1
    cancel_latency: float = 1.0
    decision_offset: float = 0.005
    warmup_seconds: int = 0
    tick_timeout: float = 10.0
    record_orders: bool = True

    def __post_init__(self):
        if self.model not in FILL_MODELS:
            raise ValueError('Unknown fill model')
        for name in ('latency', 'cancel_latency', 'decision_offset', 'tick_timeout'):
            if not math.isfinite(getattr(self, name)):
                raise ValueError(name + ' must be finite')
        if self.latency < 0 or self.cancel_latency < 0:
            raise ValueError('Latency must be nonnegative')
        if not 0 <= self.decision_offset < 1 or self.tick_timeout <= 0:
            raise ValueError('Invalid decision offset or tick timeout')
        if not isinstance(self.warmup_seconds, int) or self.warmup_seconds < 0:
            raise ValueError('Warmup must be a nonnegative integer')


class ReplayLogger:
    """Discard informational bot output; surface caught exceptions to the runner."""
    def info(self, *args, **kwargs):
        pass

    debug = info
    warning = info

    def exception(self, message, *args, **kwargs):
        ACTIVE.get().errors.append(str(message))

    error = exception


class VirtualDatetime(datetime):
    @classmethod
    def utcnow(cls):
        return datetime.fromtimestamp(ACTIVE.get().now, timezone.utc).replace(tzinfo=None)


@coroutine
def _wait_for_tick():
    yield _NEXT_TICK


async def wakeup(*args):
    await _wait_for_tick()


async def sleep(seconds):
    """Resume on the first stored second at or after the deadline."""
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError('Sleep duration must be finite and nonnegative')
    engine = ACTIVE.get()
    deadline = engine.now + seconds
    while True:
        await wakeup()
        if engine.now >= deadline:
            return


async def wakeup_every_seconds(interval):
    """Wait for the next market row at or after a whole-second deadline."""
    if not isinstance(interval, int) or interval <= 0:
        raise ValueError('Interval must be a positive integer number of seconds')
    engine = ACTIVE.get()
    deadline = (math.floor(engine.now / interval) + 1) * interval
    while True:
        await wakeup()
        if engine.row['second'] >= deadline:
            return


def clock_range(start, end):
    seconds = ACTIVE.get().now % 86400
    return start <= seconds <= end


class MarketBoard:
    def __init__(self, engine):
        self.engine = engine

    def sort(self):
        row = self.engine.row
        bids, asks = row['bids'], row['asks']
        self.best_bid, self.best_ask = bids[0][0], asks[0][0]
        return bids, asks


class Executions:
    def __init__(self, engine):
        self.engine = engine

    async def get_data(self):
        return [{'side': side.upper()} for side in ('buy', 'sell')
                if self.engine.row[side + '_count'] > 0]


class ReplayEngine:
    def __init__(self, bot, config, start, end, segment, name, strategy_factory, interval_seconds=1):
        if end <= start:
            raise ValueError('Invalid replay period')
        self.config, self.start, self.end, self.segment = config, start, end, segment
        self.interval_seconds = interval_seconds
        self.bot, self.name = bot, name
        if hasattr(bot, 'logger'):
            bot.logger = ReplayLogger()
        self.now, self.row, self.previous_time = float(start), None, None
        self._bot_run = None
        self.account = Position()
        self.orders, self.fills, self.requests, self.hourly = [], [], [], []
        self.order_history = {}
        self.sequence = 0
        self.counts, self.errors = Counter(), []
        self.cash, self.turnover, self.peak, self.max_dd = 0., 0., 0., 0.
        self.max_position, self.max_pending, self.held_seconds = 0., 0., 0
        self.daily = {}
        self.day_number, self.day_label = None, None
        self.last_equity, self.first_position_time = 0., None
        self.longest_holding, self.previous_position = 0., 0.
        self.close_profit, self.close_loss = 0., 0.
        self.strategy = strategy_factory(self)

    def get_market_data(self):
        """Return an independent copy of the current closed interval's saved row."""
        if self.row is None:
            raise RuntimeError('No market data yet; wait for the first replay tick')
        return deepcopy(self.row.source)

    async def launch(self):
        self._bot_context = contextvars.copy_context()
        self._bot_context.run(ACTIVE.set, self)
        self._bot_run = self._bot_context.run(self.bot.run, self.strategy)
        self._resume_bot()

    def _resume_bot(self):
        started = perf_counter()
        try:
            request = self._bot_context.run(self._bot_run.send, None)
        except StopIteration:
            raise RuntimeError('Bot stopped before the next tick') from None
        if request is not _NEXT_TICK:
            raise RuntimeError('Bot suspended outside the replay clock')
        if perf_counter() - started > self.config.tick_timeout:
            raise TimeoutError('Bot took too long to reach the next replay tick')

    @property
    def position(self):
        return self.account.position_size

    def submit_order(self, side, size, price, expire=math.inf, post_only=False):
        if side not in ('BUY', 'SELL') or not all(math.isfinite(x) and x > 0 for x in (size, price)):
            raise ValueError('Invalid limit order')
        if self.row['second'] < self.start + self.config.warmup_seconds:
            return None
        self.sequence += 1
        order = dict(id=str(self.sequence), side=side, price=price, size=size,
                     remaining=size, executed_size=0., state='pending',
                     arrival=self.now + self.config.latency, expire=expire,
                     cancel_done=math.inf, queue=None, activated=False, post_only=post_only)
        if self.config.record_orders:
            self.order_history[order['id']] = dict(
                order=order['id'], side=side, price=price, size=size,
                decision=self.now, arrival=order['arrival'],
                cancel_requested=None, cancelled_at=None, expired_at=None,
                first_fill=None, last_fill=None, executed_size=0., remaining=size,
                state='pending', bid=self.row['bid'], ask=self.row['ask'],
                cutoff=None, open_at_cutoff=False)
        self.orders.append(order)
        self.counts['new_orders'] += 1
        self.counts[side.lower() + '_orders'] += 1
        self.requests.append((self.now, 'new'))
        return order

    def cancel_order(self, order):
        record = self.order_history.get(order['id'])
        if record is not None and record['cancel_requested'] is None:
            record['cancel_requested'] = self.now
        order['state'] = 'canceling'
        order['cancel_done'] = min(order['cancel_done'], self.now + self.config.cancel_latency)
        self.counts['cancel_requests'] += 1
        self.requests.append((self.now, 'cancel'))

    def fill(self, order, quantity, price, timestamp, kind):
        quantity = round(quantity, 8)
        if quantity <= 0:
            return
        if kind != 'terminal' and order['executed_size'] == 0:
            self.counts[order['side'].lower() + '_filled_orders'] += 1
        sign = 1 if order['side'] == 'BUY' else -1
        before_position = self.position
        fifo_position = self.account
        before_realized = fifo_position.netprofit
        self.cash -= sign * quantity * price
        self.turnover += quantity * price
        self.account.add(
            dict(side=order['side'], size=quantity, price=price, commission=0))
        realized = fifo_position.netprofit - before_realized
        self.max_position = max(self.max_position, abs(self.position))
        if before_position * sign < 0:
            self.counts['closing_fill_events'] += 1
            self.counts['winning_closing_fill_events'] += int(realized > 0)
            self.close_profit += max(realized, 0)
            self.close_loss += max(-realized, 0)
        order['remaining'] = round(order['remaining'] - quantity, 8)
        order['executed_size'] = round(order['executed_size'] + quantity, 8)
        if order['remaining'] <= 0:
            order['state'] = 'filled'
        self.fills.append(dict(fill_id=len(self.fills) + 1,
                               t=timestamp, side=order['side'], size=quantity,
                               price=price, kind=kind, order=order['id'],
                               realized_pnl=realized,
                               decision=order.get('decision', order['arrival'] - self.config.latency),
                               arrival=order['arrival'], cancel_done=(order['cancel_done']
                               if math.isfinite(order['cancel_done']) else None)))
        record = self.order_history.get(order['id']) if kind != 'terminal' else None
        if record is not None:
            if record['first_fill'] is None:
                record['first_fill'] = timestamp
            record['last_fill'] = timestamp
            record.update(state=order['state'], executed_size=order['executed_size'],
                          remaining=order['remaining'])
        self.strategy.on_fill(order, quantity, price, kind)
        self.counts['fill_events'] += 1
        self.counts[kind + '_fills'] += 1
        if quantity < order['size']:
            self.counts['partial_fill_events'] += 1
        if order['cancel_done'] < math.inf and timestamp <= order['cancel_done']:
            self.counts['fills_during_cancel'] += 1

    def market_update(self, row):
        timestamp = row['timestamp']
        trade_through = self.config.model == 'trade_through'
        trades_by_side = {'BUY': [], 'SELL': []}
        if trade_through:
            raw = row.get('trades_raw')
            if not isinstance(raw, str):
                raise ValueError('trade_through requires a trades_raw JSON array in every market row')
            if raw != '[]':
                trades = decode_market_json(raw)
                if not isinstance(trades, list):
                    raise ValueError('trades_raw must be a JSON array')
                for offset, side, price, size in trades:
                    if (type(offset) is not int or type(side) is not int or side not in (-1, 1)
                            or not all(math.isfinite(value) and value > 0 for value in (price, size))):
                        raise ValueError('Invalid trades_raw execution')
                    # Offsets are floored. An offset of zero or later cannot
                    # establish that the execution occurred before this row.
                    if offset < 0:
                        earliest = timestamp + offset / 1000
                        latest = timestamp + (offset + 1) / 1000
                        trades_by_side['BUY' if side == -1 else 'SELL'].append(
                            [earliest, latest, price, size])
                for trades in trades_by_side.values():
                    trades.sort(key=lambda trade: trade[0])
        self.row = row
        interval_start = self.previous_time if self.previous_time is not None else timestamp - self.interval_seconds
        # Each side has one shared aggregate budget, including overlapping orders.
        budget = {'BUY': row['sell_volume'], 'SELL': row['buy_volume']}
        taker_books = {}
        # Different orders share depth; maker allocation follows price/time priority.
        ordered = sorted(self.orders, key=lambda o: (o['side'],
                         -o['price'] if o['side'] == 'BUY' else o['price'], o['arrival']))
        for order in ordered:
            if order['remaining'] <= 0:
                continue
            stop = min(order['cancel_done'], order['expire'])
            if stop <= interval_start:
                order['state'] = 'cancelled'
                continue
            if order['arrival'] > timestamp:
                continue
            buy = order['side'] == 'BUY'
            if not order['activated']:
                book = taker_books.get(order['side'])
                if book is None:
                    book = row['asks'] if buy else row['bids']
                order['activated'] = True
                if order['state'] == 'pending':
                    order['state'] = 'active'
                same_book = row['bids'] if buy else row['asks']
                order['queue'] = sum(q for p, q in same_book if p == order['price'])
                # A newly arriving limit may cross. Execute only visible liquidity
                # satisfying the limit, preserving quantity and price impact.
                if order['post_only'] and (book[0][0] <= order['price'] if buy else book[0][0] >= order['price']):
                    order['state'] = 'rejected'
                    order['remaining'] = 0.
                    self.counts['post_only_rejections'] += 1
                    continue
                executable = [(p, q) for p, q in book
                              if (p <= order['price'] if buy else p >= order['price'])]
                available = sum(q for p, q in executable)
                quantity = min(order['remaining'], available) if stop > timestamp else 0
                if quantity > 0:
                    if order['side'] not in taker_books:
                        book = [list(level) for level in book]
                        taker_books[order['side']] = book
                    self.fill(order, quantity, vwap(executable, quantity), timestamp, 'taker')
                    remaining = quantity
                    for level in book:
                        if (level[0] > order['price'] if buy else level[0] < order['price']):
                            break
                        taken = min(level[1], remaining)
                        level[1] -= taken
                        remaining -= taken
            if order['remaining'] <= 0:
                continue
            if trade_through:
                quantity = 0.
                for trade in trades_by_side[order['side']]:
                    earliest, latest, price, available = trade
                    if earliest <= order['arrival'] or latest > stop:
                        continue
                    crossed = price < order['price'] if buy else price > order['price']
                    if not crossed or available <= 0:
                        continue
                    taken = round(min(order['remaining'] - quantity, available), 8)
                    trade[3] = round(available - taken, 8)
                    quantity = round(quantity + taken, 8)
                    if quantity >= order['remaining']:
                        break
                self.fill(order, quantity, order['price'], timestamp, 'maker')
            elif order['arrival'] <= interval_start:
                if self.config.model == 'through':
                    crossed = row['high'] < order['price'] if buy else row['low'] > order['price']
                else:
                    crossed = row['low'] <= order['price'] if buy else row['high'] >= order['price']
                if crossed and row['trades'] > 0:
                    quantity = budget[order['side']]
                    if self.config.model == 'queue_touch':
                        ahead = min(order['queue'], quantity)
                        order['queue'] -= ahead
                        quantity -= ahead
                        budget[order['side']] -= ahead
                    quantity = min(order['remaining'], quantity)
                    self.fill(order, quantity, order['price'], timestamp, 'maker')
                    budget[order['side']] = max(0, budget[order['side']] - quantity)
            # If cancellation completes inside this aggregated interval, fills
            # earlier in the interval are possible; this convention is explicit.
            if stop <= timestamp and order['remaining'] > 0:
                order['state'] = 'cancelled'
        for order in ordered:
            record = self.order_history.get(order['id'])
            if record is None:
                continue
            record.update(state=order['state'], executed_size=order['executed_size'],
                          remaining=round(order['size'] - order['executed_size'], 8))
            if order['state'] == 'cancelled':
                if order['expire'] < order['cancel_done']:
                    record.update(state='expired', expired_at=order['expire'])
                else:
                    record['cancelled_at'] = order['cancel_done']
        self.orders = [o for o in self.orders if o['remaining'] > 0 and
                       min(o['cancel_done'], o['expire']) > timestamp]
        self.now = timestamp + self.config.decision_offset
        self.previous_time = timestamp
        self.strategy.on_market_update()

    def equity(self):
        position = self.position
        if abs(position) < 1e-9:
            return self.cash
        book = self.row['bids'] if position > 0 else self.row['asks']
        return self.cash + position * vwap(book, abs(position))

    def observe(self):
        position, equity = self.position, self.equity()
        self.max_position = max(self.max_position, abs(position))
        # Potential one-sided inventory if every executable/pending order fills.
        for side, sign in (('BUY', 1), ('SELL', -1)):
            pending = sum(o['remaining'] for o in self.orders if o['side'] == side)
            self.max_pending = max(self.max_pending, abs(position + sign * pending))
        self.peak = max(self.peak, equity)
        self.max_dd = max(self.max_dd, self.peak - equity)
        self.held_seconds += self.interval_seconds * int(abs(position) > 1e-9)
        second = self.row['second']
        if abs(position) > 1e-9:
            if not self.previous_position or self.previous_position * position < 0:
                self.first_position_time = second
            self.longest_holding = max(
                self.longest_holding, second + self.interval_seconds - self.first_position_time)
        else:
            self.first_position_time = None
        self.previous_position = position
        day_number = (second + 9 * 3600) // 86400
        if day_number != self.day_number:
            self.day_number = day_number
            self.day_label = datetime.fromtimestamp(second + 9 * 3600, timezone.utc).strftime('%Y-%m-%d')
        day = self.day_label
        self.daily[day] = self.daily.get(day, 0.) + equity - self.last_equity
        self.last_equity = equity
        if (second + self.interval_seconds - self.start) % 3600 == 0:
            self.hourly.append([second + self.interval_seconds, equity])

    async def tick(self, row):
        self.market_update(row)
        self._resume_bot()
        if self.errors:
            raise RuntimeError(self.errors[:5])
        self.observe()

    async def finish(self):
        await self.stop()
        if self.row is None:
            raise ValueError('Cannot finish an empty replay')
        terminal_position = self.position
        self.counts['terminal_cancelled_orders'] = len(self.orders)
        for order in self.orders:
            record = self.order_history.get(order['id'])
            if record is not None:
                record.update(state=order['state'], open_at_cutoff=True)
        for record in self.order_history.values():
            record['cutoff'] = self.row['timestamp']
        self.orders.clear()
        if abs(terminal_position) > 1e-9:
            side = 'SELL' if terminal_position > 0 else 'BUY'
            book = self.row['bids'] if terminal_position > 0 else self.row['asks']
            order = dict(side=side, size=abs(terminal_position), remaining=abs(terminal_position),
                         executed_size=0, id='terminal',
                         arrival=self.row['timestamp'], cancel_done=math.inf)
            self.fill(order, abs(terminal_position), vwap(book, abs(terminal_position)),
                      self.row['timestamp'], 'terminal')
        return self.summary(terminal_position)

    async def stop(self):
        bot_run, self._bot_run = self._bot_run, None
        if bot_run is None or bot_run.cr_frame is None:
            return
        try:
            self._bot_context.run(bot_run.throw, asyncio.CancelledError())
        except (asyncio.CancelledError, StopIteration):
            pass
        finally:
            self._bot_context.run(bot_run.close)

    def summary(self, terminal_position):
        assert abs(self.position) < 1e-8
        # Independently sum cash flows and reconcile with FIFO realization.
        independent = math.fsum((1 if f['side'] == 'SELL' else -1) * f['size'] * f['price']
                                for f in self.fills)
        fifo = self.account.netprofit
        assert math.isclose(independent, self.cash, abs_tol=1e-5)
        assert math.isclose(fifo, self.cash, abs_tol=1e-5)
        assert all(f['t'] >= f['arrival'] for f in self.fills)
        assert all(a['t'] <= b['t'] for a, b in zip(self.fills, self.fills[1:]))
        assert math.isclose(self.cash, sum(self.daily.values()), abs_tol=1e-5)
        request_times = deque()
        peak_requests = 0
        new_times, peak_new = deque(), 0
        for timestamp, kind in self.requests:
            request_times.append(timestamp)
            while request_times and request_times[0] <= timestamp - 300:
                request_times.popleft()
            peak_requests = max(peak_requests, len(request_times))
            if kind == 'new':
                new_times.append(timestamp)
                while new_times and new_times[0] <= timestamp - 300:
                    new_times.popleft()
                peak_new = max(peak_new, len(new_times))
        order_stats = {}
        for side in ('buy', 'sell'):
            submitted = self.counts[side + '_orders']
            filled = self.counts[side + '_filled_orders']
            order_stats.update({side + '_orders': submitted,
                                side + '_filled_orders': filled,
                                side + '_fill_rate': filled / submitted if submitted else None})
        return dict(name=self.name, segment=self.segment, config=asdict(self.config),
                    **order_stats,
                    gross_pnl=self.cash, turnover=self.turnover, fees=0.,
                    pnl_per_million=(self.cash / self.turnover * 1e6 if self.turnover else None),
                    fee_sensitivity={str(bp): self.cash - self.turnover * bp / 10000
                                     for bp in (.1, .5, 1)},
                    break_even_one_way_bp=(self.cash / self.turnover * 10000
                                           if self.turnover else None),
                    max_drawdown_liquidation_mtm=self.max_dd, max_abs_position=self.max_position,
                    max_abs_position_including_pending=self.max_pending,
                    holding_fraction=self.held_seconds / (self.end - self.start),
                    closing_fill_profit_factor=(self.close_profit / self.close_loss if self.close_loss else None),
                    longest_nonflat_seconds=self.longest_holding,
                    terminal_position=terminal_position, counts=dict(self.counts),
                    daily_pnl=self.daily, hourly_equity=self.hourly,
                    peak_requests_per_300s=peak_requests, peak_new_orders_per_300s=peak_new,
                    independent_cash_pnl=independent, fifo_realized_pnl=fifo)
