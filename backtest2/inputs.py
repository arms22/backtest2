"""Resolve saved market files into one ordered replay input."""
import hashlib
import json
from pathlib import Path

from .data import iso, iter_market_rows, parse_time, validate_market_manifest


def load_inputs(data, *, saved_result=None):
    paths = [Path(data)] if isinstance(data, (str, Path)) else list(map(Path, data))
    if not paths:
        raise ValueError('Specify one or more market-data directories')
    # Older results can name files that were overridden at replay time.
    saved_result = saved_result or {}
    data_file = saved_result.get('input_data_file')
    if saved_result and 'input_files' not in saved_result and 'input_data_file' not in saved_result:
        data_file = 'market_rows.jsonl.gz'
    manifest_file = saved_result.get('input_manifest_file', 'fetch_manifest.json')
    sources = []
    for path in paths:
        if not path.is_dir():
            raise ValueError(f'Expected a market-data directory: {path}')
        manifest_path = (path/manifest_file).resolve()
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        filename = data_file or (manifest.get('output') or {}).get('file') or 'market_rows.jsonl.gz'
        raw = (path/filename).resolve()
        try:
            manifest = validate_market_manifest(manifest, raw, allow_gaps=True)
        except ValueError as error:
            raise ValueError(f'{raw}: {error}') from error
        sources.append(dict(path=raw, directory=path.resolve(),
                            manifest_path=manifest_path, manifest=manifest,
                            start=parse_time(manifest['start_utc']),
                            end=parse_time(manifest['end_utc_exclusive'])))
    sources.sort(key=lambda source: source['start'])
    first = sources[0]
    interval = first['manifest']['interval_seconds']
    for source in sources:
        if (source['manifest']['interval_seconds'] != interval
                or (source['start'] - first['start']) % interval
                or (source['end'] - first['start']) % interval):
            raise ValueError(f"Market-data interval or alignment differs: {source['path']}")
    for key in ('server', 'database', 'source', 'tags', 'product_code'):
        known = [source['manifest'][key] for source in sources if source['manifest'].get(key) is not None]
        if known and any(value != known[0] for value in known):
            raise ValueError(f'Market-data manifests disagree on {key}')
    manifest = dict(first['manifest']) if len(sources) == 1 else dict(
        start_utc=iso(first['start']), end_utc_exclusive=iso(max(s['end'] for s in sources)),
        interval_seconds=interval)
    return sources, manifest


def select_inputs(sources, start, end):
    selected = []
    covered = start
    for source in sources:
        left, right = max(covered, source['start']), min(end, source['end'])
        if left < right:
            selected.append(dict(path=str(source['path']), manifest_path=str(source['manifest_path']),
                                 sha256=source['manifest']['raw_sha256'],
                                 start_utc=iso(left), end_utc_exclusive=iso(right)))
            covered = right
    return selected


def input_digest(selected):
    identity = [(item['sha256'], item['start_utc'], item['end_utc_exclusive']) for item in selected]
    return hashlib.sha256(json.dumps(identity).encode('utf-8')).hexdigest()


def iter_selected_rows(selected, **kwargs):
    for item in selected:
        path = Path(item['path'])
        yield from iter_market_rows(path.parent, parse_time(item['start_utc']),
                                    parse_time(item['end_utc_exclusive']), path.name, **kwargs)


def result_inputs(data, bundle):
    sources, manifest = load_inputs(data, saved_result=bundle)
    selected = select_inputs(sources, parse_time(bundle['start_utc']),
                             parse_time(bundle['end_utc_exclusive']))
    actual_hashes = {manifest.get('raw_sha256')}
    if 'input_files' in bundle:
        selected_hash = input_digest(selected)
        if selected_hash != input_digest(bundle['input_files']):
            raise ValueError('Market data differs from the backtest input')
        actual_hashes.add(selected_hash)
    if (bundle['input_raw_sha256'] not in actual_hashes
            or manifest['interval_seconds'] != bundle.get('input_manifest', {}).get(
                'interval_seconds', manifest['interval_seconds'])):
        raise ValueError('Market data differs from the backtest input')
    return selected, manifest['interval_seconds']


def segment_ranges(bundle):
    if 'segments' in bundle:
        return [(parse_time(segment['start_utc']), parse_time(segment['end_utc_exclusive']))
                for segment in bundle['segments']]
    boundaries = [parse_time(bundle['start_utc']), *map(parse_time, bundle['split_at']),
                  parse_time(bundle['end_utc_exclusive'])]
    return list(zip(boundaries, boundaries[1:]))
