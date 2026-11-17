"""Synthetic worker-meter intake and total energy reconstruction.

The current summary handles a certified counter reset and total emissions;
it does not attribute shared intervals to individual backfill jobs.
"""

import hashlib
import json
from fractions import Fraction
from pathlib import Path

from .store import connect


SCHEMA = """
CREATE TABLE IF NOT EXISTS energy_source (
 source_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batch(id),
 digest TEXT NOT NULL, raw BLOB NOT NULL);
"""


def _nonnegative(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError('energy_bad_integer')
    return value


def record(db_path, input_path):
    raw = Path(input_path).read_bytes()
    doc = json.loads(raw)
    if not doc['source_id'] or not doc['slots'] or not doc['jobs']:
        raise ValueError('energy_empty_source')
    jobs = {job['id'] for job in doc['jobs']}
    if len(jobs) != len(doc['jobs']) or not all(jobs):
        raise ValueError('energy_duplicate_job')
    if len(doc['boundaries']) != len(doc['slots']) + 1:
        raise ValueError('energy_bad_boundaries')
    names = [boundary['id'] for boundary in doc['boundaries']]
    if len(names) != len(set(names)):
        raise ValueError('energy_duplicate_boundary')
    for boundary in doc['boundaries']:
        _nonnegative(boundary['reading_wh'])
    slot_ids = set()
    for slot in doc['slots']:
        if slot['id'] in slot_ids or not set(slot['active_jobs']) <= jobs:
            raise ValueError('energy_bad_slot')
        slot_ids.add(slot['id'])
        _nonnegative(slot['intensity_g_per_kwh'])
    reset_ids = set()
    for reset in doc['resets']:
        if reset['after_boundary'] not in names or reset['after_boundary'] in reset_ids:
            raise ValueError('energy_bad_reset')
        reset_ids.add(reset['after_boundary'])
        _nonnegative(reset['old_wh'])
        _nonnegative(reset['new_wh'])
        index = names.index(reset['after_boundary'])
        if index == len(names) - 1 or doc['boundaries'][index]['reading_wh'] != reset['old_wh']:
            raise ValueError('energy_reset_not_attested')
    for value in doc['coalition_calibration_wh'].values():
        _nonnegative(value)
    digest = hashlib.sha256(raw).hexdigest()
    db = connect(db_path)
    try:
        db.executescript(SCHEMA)
        batch = db.execute('SELECT 1 FROM batch WHERE id=?',(doc['batch_id'],)).fetchone()
        if not batch:
            raise ValueError('energy_unknown_batch')
        prior = db.execute('SELECT digest FROM energy_source WHERE source_id=?',
                           (doc['source_id'],)).fetchone()
        if prior:
            if prior['digest'] != digest:
                raise ValueError('energy_source_conflict')
            return doc['source_id']
        with db:
            db.execute('INSERT INTO energy_source VALUES (?,?,?,?)',
                       (doc['source_id'], doc['batch_id'], digest, raw))
        return doc['source_id']
    finally:
        db.close()


def meter_summary(db_path, source_id):
    db = connect(db_path)
    try:
        db.executescript(SCHEMA)
        row = db.execute('SELECT raw FROM energy_source WHERE source_id=?',
                         (source_id,)).fetchone()
        if not row:
            raise ValueError('energy_unknown_source')
        doc = json.loads(row['raw'])
        resets = {item['after_boundary']: item for item in doc['resets']}
        intervals = []
        total_wh = 0
        total_g = Fraction(0)
        for i, slot in enumerate(doc['slots']):
            previous = doc['boundaries'][i]
            current = doc['boundaries'][i + 1]
            start = resets[previous['id']]['new_wh'] if previous['id'] in resets else previous['reading_wh']
            wh = current['reading_wh'] - start
            if wh < 0:
                raise ValueError('energy_unexplained_counter_drop')
            grams = Fraction(wh * slot['intensity_g_per_kwh'], 1000)
            intervals.append({'slot': slot['id'], 'active_jobs': slot['active_jobs'],
                              'energy_wh': wh, 'grams_num': grams.numerator,
                              'grams_den': grams.denominator})
            total_wh += wh
            total_g += grams
        return {'source_id': source_id, 'intervals': intervals,
                'total_wh': total_wh, 'total_grams_num': total_g.numerator,
                'total_grams_den': total_g.denominator}
    finally:
        db.close()
