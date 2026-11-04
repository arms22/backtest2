"""Post-process replay fills into FIFO close slices and summaries."""
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from math import fsum


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
