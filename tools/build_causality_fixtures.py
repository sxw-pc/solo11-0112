"""Build synthetic causality fixtures (stdlib only, run once; outputs are data).

Generated under fixtures/causality-cases/:

* multi-event/      one source file carrying two correction events; the whole
                    file is the atomic intervention unit
* nonmonotonic/     an evidence-completing improvement unit and a duplicate
                    revision worsening unit acting on the same target flip
* same-key-variant/ same event_key with different payload (identity_conflict)
* cycle/            a circular dependency configuration
* scale/            13 subjects to prove the search-limit gate is indeterminate

The builder never imports backfillcomp; pytest consumes only the generated
files. Re-running it is byte-idempotent.
"""
import json
import sqlite3
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / 'fixtures' / 'causality-cases'


def write_jsonl(path, lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        ('\n'.join(json.dumps(x, ensure_ascii=False, separators=(',', ':'))
                   for x in lines) + '\n').encode('utf-8'))


def write_config(path, cfg):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding='utf-8')


def write_ledger(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    con = sqlite3.connect(path)
    try:
        con.execute('CREATE TABLE ledger(id TEXT PRIMARY KEY, partition TEXT NOT NULL, '
                    'amount TEXT NOT NULL, computed TEXT NOT NULL)')
        con.executemany('INSERT INTO ledger VALUES (?,?,?,?)', rows)
        con.commit()
    finally:
        con.close()


def base_config(batch, subjects, dependencies=None, ranges=('2026-01',)):
    return {'batch_id': batch, 'rule_version': 'v2', 'old_multiplier': '1.00',
            'new_multiplier': '1.10', 'ranges': list(ranges),
            'subjects': {s: {'partition': '2026-01'} for s in subjects},
            'dependencies': dependencies or {}}


def correction(key, subject, at='2026-03-01'):
    return {'event_key': key, 'subject': subject, 'kind': 'correction',
            'at': at, 'reason': 'rule_revision'}


def build_multi_event():
    d = OUT / 'multi-event'
    write_ledger(d / 'ledger.db', [
        ('m1', '2026-01', '10.00', '10.00'),
        ('m2', '2026-01', '20.00', '20.00')])
    write_config(d / 'config.json', base_config('causal-multi', ['m1', 'm2']))
    write_jsonl(d / 'baseline.jsonl',
                [correction('b1', 'm1'), correction('b2', 'm2')])
    # One file, two events: the unit must be included/excluded atomically.
    write_jsonl(d / 'unit-both.jsonl',
                [correction('u-m1', 'm1', '2026-03-02'),
                 correction('u-m2', 'm2', '2026-03-02')])


def build_nonmonotonic():
    d = OUT / 'nonmonotonic'
    write_ledger(d / 'ledger.db', [
        ('s1', '2026-01', '10.00', '10.00'),
        ('s2', '2026-01', '20.00', '20.00')])
    cfg = base_config('causal-nonmono', ['s1', 's2'], {'s1': ['s2']})
    write_config(d / 'config.json', cfg)
    # Baseline: s2 lacks its correction (待核) and therefore blocks s1 (待核);
    # s1 itself already has a valid correction.
    write_jsonl(d / 'baseline.jsonl', [correction('b-s1', 's1')])
    # Improvement unit: completes s2's missing evidence, unblocking s1.
    write_jsonl(d / 'improve-s2.jsonl', [correction('add-s2', 's2', '2026-03-02')])
    # Worsening unit: a second rule_revision for s1 makes its evidence ambiguous.
    write_jsonl(d / 'worsen-s1.jsonl', [correction('dup-s1', 's1', '2026-03-03')])


def build_same_key_variant():
    d = OUT / 'same-key-variant'
    write_ledger(d / 'ledger.db', [('k1', '2026-01', '10.00', '10.00')])
    write_config(d / 'config.json', base_config('causal-variant', ['k1']))
    write_jsonl(d / 'baseline.jsonl', [correction('shared-key', 'k1')])
    # Same event_key, different payload -> ingest records identity_conflict,
    # no second raw_event is inserted.
    write_jsonl(d / 'variant.jsonl',
                [{'event_key': 'shared-key', 'subject': 'k1', 'kind': 'correction',
                  'at': '2026-09-09', 'reason': 'rule_revision'}])


def build_cycle():
    d = OUT / 'cycle'
    write_ledger(d / 'ledger.db', [
        ('y1', '2026-01', '10.00', '10.00'),
        ('y2', '2026-01', '20.00', '20.00')])
    write_config(d / 'config.json',
                 base_config('causal-cycle', ['y1', 'y2'],
                             {'y1': ['y2'], 'y2': ['y1']}))
    write_jsonl(d / 'baseline.jsonl',
                [correction('cy1', 'y1'), correction('cy2', 'y2')])
    write_jsonl(d / 'unit-y1.jsonl', [correction('dup-y1', 'y1', '2026-03-02')])


def build_missing_bytes():
    # A unit source whose stored raw bytes are unavailable at assessment time.
    # SQLite NOT NULL forbids a true NULL at build time; the test empties the
    # BLOB after freezing, so this fixture only supplies the normal inputs.
    d = OUT / 'missing-bytes'
    write_ledger(d / 'ledger.db', [('q1', '2026-01', '10.00', '10.00')])
    write_config(d / 'config.json', base_config('causal-missing', ['q1']))
    write_jsonl(d / 'baseline.jsonl', [correction('bq1', 'q1')])
    write_jsonl(d / 'unit-q1.jsonl', [correction('dup-q1', 'q1', '2026-03-02')])


def build_scale():
    d = OUT / 'scale'
    subjects = [f'z{i:02d}' for i in range(13)]
    write_ledger(d / 'ledger.db',
                 [(s, '2026-01', '10.00', '10.00') for s in subjects])
    write_config(d / 'config.json', base_config('causal-scale', subjects))
    write_jsonl(d / 'baseline.jsonl', [correction(f'b-{s}', s) for s in subjects])
    write_jsonl(d / 'unit-z00.jsonl', [correction('dup-z00', 'z00', '2026-03-02')])


if __name__ == '__main__':
    build_multi_event()
    build_nonmonotonic()
    build_same_key_variant()
    build_cycle()
    build_missing_bytes()
    build_scale()
    print('built', OUT)
