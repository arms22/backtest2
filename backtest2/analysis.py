"""Post-process replay fills into FIFO close slices and summaries."""
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from math import fsum
import gzip
import json
from pathlib import Path

from .data import file_hash


def load_audited_results(directory):
    """Read results whose SHA256 agrees with saved audit_checks.json.

    This checks the audit's result reference, not the ledgers or source data.
    """
    directory = Path(directory)
    path = directory / 'results.json'
    saved = json.loads(path.read_text(encoding='utf-8'))
    audit = json.loads((directory / 'audit_checks.json').read_text(encoding='utf-8'))
    if audit['results_sha256'] != file_hash(path):
        raise ValueError('Results SHA256 does not match the saved audit')
    return saved


def read_jsonl_gz(path):
    """Read a saved gzip JSONL ledger into a list."""
    with gzip.open(path, 'rt', encoding='utf-8') as source:
        return [json.loads(line) for line in source]


def summarize_activity(fills, matches, *, fast_seconds=60):
    """Return normal/terminal P&L and quantity-weighted activity.

    Normal means exit_kind != terminal. Holding time uses all normal slices;
    fast_fraction uses normal slices below fast_seconds divided by nonterminal
    BUY fill quantity (the original long-inventory experiment definition).
    Empty sums are zero and ratios with a zero denominator are None.
    """
    fills = list(fills)
    buy_quantity = fsum(f['size'] for f in fills
                        if f['side'] == 'BUY' and f['kind'] != 'terminal')
    sell_quantity = fsum(f['size'] for f in fills
                         if f['side'] == 'SELL' and f['kind'] != 'terminal')
    return dict(buy_quantity=buy_quantity, sell_quantity=sell_quantity,
                **_summarize_activity(matches, buy_quantity, fast_seconds))


def summarize_position_activity(matches, *, entry_side, fast_seconds=60):
    """Summarize closed FIFO entry quantity by BUY (long) or SELL (short).

    entry_quantity includes normal and terminal exits, excluding terminal
    entries. It counts entry slices rather than fill volume, so reversals
    do not mix new positions with closing trades. Unmatched open lots are
    not included; use completed, terminal-liquidated results for full totals.
    """
    if entry_side not in ('BUY', 'SELL'):
        raise ValueError('entry_side must be BUY or SELL')
    selected = [m for m in matches
                if m['entry_side'] == entry_side and m['entry_kind'] != 'terminal']
    quantity = fsum(m['size'] for m in selected)
    return dict(entry_quantity=quantity,
                **_summarize_activity(selected, quantity, fast_seconds))


def _summarize_activity(matches, quantity, fast_seconds):
    matches = list(matches)
    normal = [m for m in matches if m['exit_kind'] != 'terminal']
    terminal = [m for m in matches if m['exit_kind'] == 'terminal']
    normal_quantity = fsum(m['size'] for m in normal)
    holding_quantity_seconds = fsum(m['size'] * m['holding_seconds'] for m in normal)
    fast_quantity = fsum(m['size'] for m in normal if m['holding_seconds'] < fast_seconds)
    return dict(
        fast_seconds=fast_seconds, normal_quantity=normal_quantity,
        holding_quantity_seconds=holding_quantity_seconds, fast_quantity=fast_quantity,
        normal_profit=fsum(m['gross_realized_pnl'] for m in normal
                           if m['gross_realized_pnl'] > 0),
        normal_loss=fsum(m['gross_realized_pnl'] for m in normal
                         if m['gross_realized_pnl'] < 0),
        terminal_pnl=fsum(m['gross_realized_pnl'] for m in terminal),
        holding_seconds=holding_quantity_seconds / normal_quantity if normal_quantity else None,
        fast_fraction=fast_quantity / quantity if quantity else None,
    )


def merge_activity(summaries):
    """Merge saved activity totals without loading fills or FIFO slices."""
    summaries = list(summaries)
    if not summaries:
        raise ValueError('At least one activity summary is required')
    thresholds = {s['fast_seconds'] for s in summaries}
    if len(thresholds) != 1:
        raise ValueError('Cannot merge different fast_seconds thresholds')
    quantity_fields = (('entry_quantity',) if 'entry_quantity' in summaries[0]
                       else ('buy_quantity', 'sell_quantity'))
    fields = (*quantity_fields, 'normal_profit', 'normal_loss', 'terminal_pnl',
              'normal_quantity', 'holding_quantity_seconds', 'fast_quantity')
    result = {key: fsum(s[key] for s in summaries) for key in fields}
    quantity = result[quantity_fields[0]]
    normal = result['normal_quantity']
    result.update(fast_seconds=summaries[0]['fast_seconds'],
                  holding_seconds=result['holding_quantity_seconds'] / normal if normal else None,
                  fast_fraction=result['fast_quantity'] / quantity if quantity else None)
    return result


def load_result_rows(directories):
    """Load audited result rows for comparison; retain source and condition metadata.

    This uses the existing audit reference check, without re-auditing ledgers.
    Old results remain readable; aggregation requires the saved activity totals.
    """
    rows = []
    for directory in directories:
        bundle = load_audited_results(directory)
        for result in bundle['results']:
            rows.append(dict(result, source_directory=str(Path(directory)),
                             input_raw_sha256=bundle['input_raw_sha256'],
                             code_sha256=bundle['code_sha256']))
    return rows


def _merge_markouts(summaries):
    groups = defaultdict(list)
    for summary in summaries:
        for horizon, values in summary.items():
            groups[str(horizon)].append(values)
    result = {}
    for horizon, values in groups.items():
        totals = {key: fsum(v[key] for v in values) for key in (
            'volume', 'markout_yen', 'eligible_volume', 'observed_volume', 'censored_volume')}
        totals.update({key: sum(v[key] for v in values) for key in (
            'count', 'eligible_count', 'observed_count', 'censored_count')})
        totals['yen_per_btc'] = (totals['markout_yen'] / totals['volume']
                                 if totals['volume'] else None)
        result[horizon] = totals
    return result


def aggregate_results(rows):
    """Aggregate independent periods by case, replay config and bot metadata.

    Select compatible products, units and nonoverlapping periods before calling.
    Code hashes are retained as provenance, not used as grouping keys. No ledger
    reads or source-file checks are performed. Quantiles and continuous drawdown
    cannot be recovered from per-period summaries and are not returned.
    """
    groups = defaultdict(list)
    for row in rows:
        key = json.dumps([row['case'], row['config'], row['bot_metadata']], sort_keys=True)
        groups[key].append(row)
    totals = []
    for group in groups.values():
        if any('activity' not in row or 'position_activity' not in row for row in group):
            raise ValueError('Saved activity totals are missing; aggregate_results requires new results')
        first = group[0]
        total = {key: first[key] for key in ('case', 'config', 'bot_metadata')}
        total['segments'] = [
            {key: row[key] for key in ('segment', 'source_directory', 'input_raw_sha256',
                                       'code_sha256') if key in row}
            for row in group]
        total['segment_count'] = len(group)
        for key in ('gross_pnl', 'turnover'):
            total[key] = fsum(row[key] for row in group)
        total['pnl_per_million'] = (total['gross_pnl'] / total['turnover'] * 1e6
                                     if total['turnover'] else None)
        for side in ('buy', 'sell'):
            for suffix in ('orders', 'filled_orders'):
                key = f'{side}_{suffix}'
                total[key] = sum(row[key] for row in group)
            total[f'{side}_fill_rate'] = (total[f'{side}_filled_orders'] / total[f'{side}_orders']
                                           if total[f'{side}_orders'] else None)
        total['max_segment_drawdown_liquidation_mtm'] = max(
            row['max_drawdown_liquidation_mtm'] for row in group)
        total['activity'] = merge_activity(row['activity'] for row in group)
        total['position_activity'] = {
            side: merge_activity(row['position_activity'][side] for row in group)
            for side in ('BUY', 'SELL')}
        total['fill_markouts'] = _merge_markouts(row['fill_markouts'] for row in group)
        markout_groups = defaultdict(list)
        for row in group:
            for key, summary in row['fill_markouts_by_side_kind'].items():
                markout_groups[key].append(summary)
        total['fill_markouts_by_side_kind'] = {
            key: _merge_markouts(values) for key, values in markout_groups.items()}
        totals.append(total)
    return totals


_EPSILON = 1e-10
_JST = timezone(timedelta(hours=9))


def _open_lots(lots, as_of):
    return [
        dict(entry_fill_id=lot['fill_id'], entry_order_id=lot['order'],
             entry_time=lot['t'], entry_price=lot['price'],
             entry_side=lot['side'],
             position_direction='long' if lot['side'] == 'BUY' else 'short',
             entry_kind=lot['kind'], size=lot['remaining'], as_of=as_of)
        for lot in lots if lot['remaining'] > _EPSILON
    ]


def match_fills_fifo(fills):
    """Return FIFO fill-to-fill close slices and inventory before terminal close."""
    lots = deque()
    matches = []
    open_before_terminal = None

    for index, fill in enumerate(fills, start=1):
        fill_id = fill.get('fill_id', index)
        if fill['kind'] == 'terminal' and open_before_terminal is None:
            open_before_terminal = _open_lots(lots, 'before_terminal')

        remaining = round(float(fill['size']), 8)
        while remaining > _EPSILON and lots and lots[0]['side'] != fill['side']:
            lot = lots[0]
            quantity = round(min(remaining, lot['remaining']), 8)
            direction_sign = 1 if lot['side'] == 'BUY' else -1
            pnl = direction_sign * (fill['price'] - lot['price']) * quantity
            matches.append(dict(
                entry_fill_id=lot['fill_id'], exit_fill_id=fill_id,
                entry_order_id=lot['order'], exit_order_id=fill['order'],
                entry_time=lot['t'], exit_time=fill['t'],
                entry_price=lot['price'], exit_price=fill['price'],
                entry_side=lot['side'], exit_side=fill['side'],
                position_direction='long' if lot['side'] == 'BUY' else 'short',
                entry_kind=lot['kind'], exit_kind=fill['kind'],
                size=quantity, holding_seconds=fill['t'] - lot['t'],
                gross_realized_pnl=pnl,
                turnover=quantity * (lot['price'] + fill['price']),
            ))
            remaining = round(remaining - quantity, 8)
            lot['remaining'] = round(lot['remaining'] - quantity, 8)
            if lot['remaining'] <= _EPSILON:
                lots.popleft()

        if remaining > _EPSILON:
            lots.append(dict(
                fill_id=fill_id, order=fill['order'], t=fill['t'],
                price=fill['price'], side=fill['side'], kind=fill['kind'],
                remaining=remaining,
            ))

    if open_before_terminal is None:
        open_before_terminal = _open_lots(lots, 'end_of_ledger')
    return matches, open_before_terminal


def _holding_bucket(seconds):
    if seconds < 60:
        return '<1m'
    if seconds < 5 * 60:
        return '1-5m'
    if seconds < 15 * 60:
        return '5-15m'
    if seconds < 60 * 60:
        return '15-60m'
    return '>=60m'


def _hour_jst(timestamp):
    return datetime.fromtimestamp(timestamp, _JST).hour


def _summary_row(group_by, key, matches):
    pnl = fsum(match['gross_realized_pnl'] for match in matches)
    quantity = fsum(match['size'] for match in matches)
    turnover = fsum(match['turnover'] for match in matches)
    count = len(matches)
    return dict(
        group_by=group_by, group_key=key, match_count=count,
        quantity=quantity, turnover=turnover, gross_realized_pnl=pnl,
        win_rate=sum(match['gross_realized_pnl'] > 0 for match in matches) / count,
        average_pnl_per_match=pnl / count,
        pnl_per_million_turnover=pnl / turnover * 1e6 if turnover else None,
    )


def summarize_matches(matches):
    """Summarize match slices by direction, liquidity, time and holding time."""
    groupers = [
        ('position_direction', lambda m: m['position_direction']),
        ('entry_kind', lambda m: m['entry_kind']),
        ('exit_kind', lambda m: m['exit_kind']),
        ('entry_hour_jst', lambda m: _hour_jst(m['entry_time'])),
        ('exit_hour_jst', lambda m: _hour_jst(m['exit_time'])),
        ('holding_duration', lambda m: _holding_bucket(m['holding_seconds'])),
        ('direction_entry_exit_kind', lambda m: '|'.join((
            m['position_direction'], m['entry_kind'], m['exit_kind']))),
    ]
    rows = []
    for group_by, key_for in groupers:
        groups = defaultdict(list)
        for match in matches:
            groups[key_for(match)].append(match)
        for key in sorted(groups, key=str):
            rows.append(_summary_row(group_by, key, groups[key]))
    return rows


_ORDER_STATES = ('pending', 'active', 'canceling', 'filled', 'cancelled',
                 'expired', 'rejected')


def _order_quantiles(values):
    values = sorted(values)
    if not values:
        return dict(count=0, p50=None, p90=None, p99=None)

    def percentile(p):
        position = (len(values) - 1) * p
        lower = int(position)
        upper = min(lower + 1, len(values) - 1)
        fraction = position - lower
        return values[lower] + (values[upper] - values[lower]) * fraction

    return dict(count=len(values), p50=percentile(.5), p90=percentile(.9),
                p99=percentile(.99))


def summarize_orders(orders):
    """Summarize saved orders by side, excluding unobserved durations."""
    orders = list(orders)
    result = {}
    for side in ('BUY', 'SELL'):
        selected = [order for order in orders if order['side'] == side]
        states = {state: 0 for state in _ORDER_STATES}
        for order in selected:
            states[order['state']] = states.get(order['state'], 0) + 1

        durations = {key: [] for key in (
            'cancel_request_seconds', 'cancel_complete_seconds', 'first_fill_seconds')}
        decisions = []
        distances = []
        for order in selected:
            decision = order.get('decision')
            if decision is not None:
                decisions.append(decision)
                for key, field in (('cancel_request_seconds', 'cancel_requested'),
                                   ('cancel_complete_seconds', 'cancelled_at'),
                                   ('first_fill_seconds', 'first_fill')):
                    timestamp = order.get(field)
                    if timestamp is not None:
                        durations[key].append(timestamp - decision)
            bid, ask, price = (order.get(k) for k in ('bid', 'ask', 'price'))
            if bid is not None and ask is not None and price is not None:
                mid = (bid + ask) / 2
                if mid:
                    distance = ((mid - price) if side == 'BUY' else (price - mid)) / mid * 1e4
                    distances.append(distance)

        decisions.sort()
        intervals = [later - earlier for earlier, later in zip(decisions, decisions[1:])]
        result[side] = dict(
            orders=len(selected),
            filled_orders=sum(order.get('executed_size', 0) > 0 for order in selected),
            unfilled_cancelled=sum(order['state'] == 'cancelled'
                                   and order.get('executed_size', 0) == 0
                                   for order in selected),
            state_counts=states,
            cancel_request_seconds=_order_quantiles(durations['cancel_request_seconds']),
            cancel_complete_seconds=_order_quantiles(durations['cancel_complete_seconds']),
            first_fill_seconds=_order_quantiles(durations['first_fill_seconds']),
            order_interval_seconds=_order_quantiles(intervals),
            distance_mid_bp=_order_quantiles(distances),
        )
    return result
