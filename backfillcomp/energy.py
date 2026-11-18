"""Synthetic worker-meter intake, total reconstruction and job attribution.

The meter summary handles certified counter resets and total emissions.
The attribution ledger assigns each half-open slot's measured Wh and gCO2e
to individual backfill jobs and the platform under a pre-approved contract,
using only the frozen source bytes; raw metering and written ledgers are
never rewritten.
"""

import hashlib
import json
from fractions import Fraction
from itertools import combinations
from math import factorial
from pathlib import Path

from .store import connect


SCHEMA = """
CREATE TABLE IF NOT EXISTS energy_source (
 source_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batch(id),
 digest TEXT NOT NULL, raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS energy_attribution (
 source_id TEXT NOT NULL REFERENCES energy_source(source_id),
 contract TEXT NOT NULL, version INTEGER NOT NULL,
 status TEXT NOT NULL, digest TEXT NOT NULL, body TEXT NOT NULL,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (source_id, contract, version));
"""

CONTRACTS = ('direct_platform_overhead', 'shapley_shared_overhead')
MAX_SHAPLEY_ACTIVE = 4
STATUS_ATTRIBUTED = '已归属'
STATUS_PENDING = '待核'


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


def _frac(value):
    value = Fraction(value)
    return {'num': value.numerator, 'den': value.denominator}


def _coalition_key(members):
    return ','.join(sorted(members))


def _label(members):
    return _coalition_key(members) or '(idle)'


def _subsets(members):
    ordered = sorted(members)
    for size in range(len(ordered) + 1):
        for combo in combinations(ordered, size):
            yield frozenset(combo)


def _parse_calibration(doc):
    """Canonicalize coalition keys; duplicate or bad entries are 待核 issues."""
    table = {}
    issues = []
    raw = doc.get('coalition_calibration_wh', {})
    for key in sorted(raw):
        value = raw[key]
        members = frozenset(part for part in (p.strip() for p in key.split(',')) if part)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            issues.append({'slot': None, 'code': 'calibration_bad_value', 'detail': key})
            continue
        if members in table:
            issues.append({'slot': None, 'code': 'calibration_identity_conflict',
                           'detail': _label(members)})
            continue
        table[members] = value
    return table, issues


def _reconstruct_slots(doc):
    """Rebuild each half-open slot's non-negative Wh from boundary readings.

    A counter reset is bridged only by its own attestation (old_wh at the
    boundary, new_wh as the next slot's base); a reading drop without such
    proof is a metering gap, never a negative consumption or a guessed reset.
    """
    boundaries = doc['boundaries']
    names = [boundary['id'] for boundary in boundaries]
    jobs = {job['id'] for job in doc['jobs']}
    resets = {}
    issues = []
    for reset in doc.get('resets', []):
        boundary = reset.get('after_boundary')
        index = names.index(boundary) if boundary in names else None
        if (index is None or index == len(names) - 1
                or boundaries[index]['reading_wh'] != reset.get('old_wh')):
            issues.append({'slot': None, 'code': 'reset_identity_conflict',
                           'detail': str(boundary)})
            continue
        resets[boundary] = reset
    slots = []
    for i, slot in enumerate(doc['slots']):
        previous = boundaries[i]
        current = boundaries[i + 1]
        start = resets[previous['id']]['new_wh'] if previous['id'] in resets else previous['reading_wh']
        wh = current['reading_wh'] - start
        active = frozenset(slot.get('active_jobs') or [])
        if not active <= jobs:
            issues.append({'slot': slot['id'], 'code': 'active_set_mismatch',
                           'detail': ','.join(sorted(active - jobs))})
        intensity = slot.get('intensity_g_per_kwh')
        if isinstance(intensity, bool) or not isinstance(intensity, int) or intensity < 0:
            issues.append({'slot': slot['id'], 'code': 'missing_intensity',
                           'detail': slot['id']})
            intensity = None
        if wh < 0:
            issues.append({'slot': slot['id'], 'code': 'unexplained_counter_drop',
                           'detail': f"{previous['id']}->{current['id']}"})
        slots.append({'slot': slot['id'], 'active': active, 'wh': wh,
                      'intensity': intensity})
    return slots, issues


def _shapley_terms(active, value):
    """Exact Shapley marginal contributions of v(S) over all sub-coalitions."""
    n = len(active)
    result = {}
    for job in sorted(active):
        others = sorted(active - {job})
        share = Fraction(0)
        terms = []
        for size in range(n):
            weight = Fraction(factorial(size) * factorial(n - 1 - size), factorial(n))
            for combo in combinations(others, size):
                subset = frozenset(combo)
                marginal = value[subset | {job}] - value[subset]
                share += weight * marginal
                terms.append({'over': _coalition_key(subset), 'weight': _frac(weight),
                              'marginal_wh': marginal})
        result[job] = (share, terms)
    return result


def _attribute_slot(entry, calibration, contract):
    """Split one slot's measured Wh between jobs and platform.

    Every needed coalition calibration (idle set plus all subsets of the
    active set) must be present and monotone, and the measured slot Wh must
    equal the calibrated E(active); otherwise the slot is 待核 and no
    guessed share is produced.
    """
    slot_id = entry['slot']
    active = entry['active']
    wh = entry['wh']
    intensity = entry['intensity']
    if contract == 'shapley_shared_overhead' and len(active) > MAX_SHAPLEY_ACTIVE:
        return {'issues': [{'slot': slot_id, 'code': 'too_many_active_jobs',
                            'detail': str(len(active))}]}
    needed = list(_subsets(active))
    missing = [subset for subset in needed if subset not in calibration]
    if missing:
        return {'issues': [{'slot': slot_id, 'code': 'missing_calibration',
                            'detail': ';'.join(_label(s) for s in missing)}]}
    issues = []
    for subset in needed:
        for job in sorted(active - subset):
            if calibration[subset] > calibration[subset | {job}]:
                issues.append({'slot': slot_id, 'code': 'calibration_not_monotonic',
                               'detail': f"{_label(subset)}>{_label(subset | {job})}"})
    if calibration[frozenset(active)] != wh:
        issues.append({'slot': slot_id, 'code': 'observed_calibration_mismatch',
                       'detail': f"observed={wh},calibrated={calibration[frozenset(active)]}"})
    if issues:
        return {'issues': issues}
    idle = calibration[frozenset()]
    if contract == 'direct_platform_overhead':
        increments = {job: calibration[frozenset((job,))] - idle for job in sorted(active)}
        residual = wh - idle - sum(increments.values())
        if residual < 0:
            return {'issues': [{'slot': slot_id, 'code': 'negative_platform_residual',
                                'detail': str(residual)}]}
        shares = {job: Fraction(increments[job]) for job in increments}
        platform = Fraction(idle + residual)
        terms = {job: [{'over': '', 'weight': _frac(1),
                        'marginal_wh': increments[job]}]
                 for job in increments}
        platform_terms = [{'kind': 'idle', 'marginal_wh': idle},
                          {'kind': 'shared_residual', 'marginal_wh': residual}]
    else:
        value = {subset: calibration[subset] - idle for subset in needed}
        shapley = _shapley_terms(active, value)
        shares = {job: shapley[job][0] for job in sorted(active)}
        terms = {job: shapley[job][1] for job in sorted(active)}
        platform = Fraction(idle)
        platform_terms = [{'kind': 'idle', 'marginal_wh': idle}]
    if sum(shares.values(), platform) != wh:
        raise AssertionError('energy_conservation_bug')
    slot_grams = Fraction(wh * intensity, 1000)
    job_grams = {job: Fraction(shares[job] * intensity, 1000) for job in shares}
    platform_grams = Fraction(platform * intensity, 1000)
    entry_out = {
        'slot': slot_id,
        'active_jobs': sorted(active),
        'coalition': _coalition_key(active),
        'energy_wh': wh,
        'intensity_g_per_kwh': intensity,
        'calibration_wh': {_coalition_key(s): calibration[s] for s in needed},
        'marginal_terms': terms,
        'platform_terms': platform_terms,
        'job_shares_wh': {job: _frac(shares[job]) for job in shares},
        'platform_wh': _frac(platform),
        'job_grams': {job: _frac(job_grams[job]) for job in shares},
        'platform_grams': _frac(platform_grams),
        'slot_grams': _frac(slot_grams),
    }
    return {'issues': [], 'entry': entry_out, 'shares': shares, 'platform': platform,
            'job_grams': job_grams, 'platform_grams': platform_grams,
            'slot_grams': slot_grams}


def _compute_ledger(doc, source_id, contract):
    """Deterministically derive the attribution ledger from a frozen source."""
    slots, issues = _reconstruct_slots(doc)
    calibration, cal_issues = _parse_calibration(doc)
    issues.extend(cal_issues)
    job_ids = sorted(job['id'] for job in doc['jobs'])
    entries = []
    totals_wh = {job: Fraction(0) for job in job_ids}
    totals_g = {job: Fraction(0) for job in job_ids}
    platform_wh = Fraction(0)
    platform_g = Fraction(0)
    total_wh = 0
    total_g = Fraction(0)
    if not issues:
        for entry in slots:
            result = _attribute_slot(entry, calibration, contract)
            if result['issues']:
                issues.extend(result['issues'])
                continue
            entries.append(result['entry'])
            for job, share in result['shares'].items():
                totals_wh[job] += share
                totals_g[job] += result['job_grams'][job]
            platform_wh += result['platform']
            platform_g += result['platform_grams']
            total_wh += result['entry']['energy_wh']
            total_g += result['slot_grams']
    if issues:
        return {'source_id': source_id, 'contract': contract, 'status': STATUS_PENDING,
                'issues': issues, 'slots': [], 'totals': None}
    if sum(totals_wh.values(), platform_wh) != total_wh or sum(totals_g.values(), platform_g) != total_g:
        raise AssertionError('energy_conservation_bug')
    return {'source_id': source_id, 'contract': contract, 'status': STATUS_ATTRIBUTED,
            'issues': [], 'slots': entries,
            'totals': {
                'jobs': {job: {'wh': _frac(totals_wh[job]), 'grams': _frac(totals_g[job])}
                         for job in job_ids},
                'platform': {'wh': _frac(platform_wh), 'grams': _frac(platform_g)},
                'energy_wh': total_wh,
                'grams': _frac(total_g)}}


def _canonical(ledger):
    return json.dumps(ledger, ensure_ascii=False, sort_keys=True)


def attribute(db_path, source_id, contract):
    """Attribute one frozen meter source under a pre-approved contract.

    The ledger is written once and never rewritten: replaying the same
    source and contract returns the stored ledger, and new readings or
    calibrations only ever arrive as new source versions.
    """
    if contract not in CONTRACTS:
        raise ValueError('energy_unknown_contract')
    db = connect(db_path)
    try:
        db.executescript(SCHEMA)
        row = db.execute('SELECT raw FROM energy_source WHERE source_id=?',
                         (source_id,)).fetchone()
        if not row:
            raise ValueError('energy_unknown_source')
        ledger = _compute_ledger(json.loads(row['raw']), source_id, contract)
        body = _canonical(ledger)
        digest = hashlib.sha256(body.encode('utf-8')).hexdigest()
        prior = db.execute(
            'SELECT digest, body FROM energy_attribution WHERE source_id=? AND contract=?'
            ' ORDER BY version DESC LIMIT 1', (source_id, contract)).fetchone()
        if prior:
            if prior['digest'] != digest:
                raise ValueError('energy_attribution_conflict')
            return json.loads(prior['body'])
        with db:
            db.execute('INSERT INTO energy_attribution VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP)',
                       (source_id, contract, 1, ledger['status'], digest, body))
        return ledger
    finally:
        db.close()


def verify_energy(db_path, source_id=None, contract=None):
    """Independently recompute every stored ledger from the frozen raw bytes.

    Re-derives slot Wh from boundary readings and reset events, re-applies
    the coalition calibration, contract and per-slot intensity, and compares
    against the stored ledger; also re-checks source digests and strict
    Wh/gCO2e conservation. Returns a list of problems (empty when sound).
    """
    db = connect(db_path)
    try:
        db.executescript(SCHEMA)
        problems = []
        sources = {}
        for row in db.execute('SELECT source_id, digest, raw FROM energy_source'):
            if hashlib.sha256(row['raw']).hexdigest() != row['digest']:
                problems.append(f"energy_source:{row['source_id']}")
            sources[row['source_id']] = row['raw']
        query = ('SELECT source_id, contract, version, status, digest, body'
                 ' FROM energy_attribution')
        clauses, params = [], []
        if source_id is not None:
            clauses.append('source_id=?')
            params.append(source_id)
        if contract is not None:
            clauses.append('contract=?')
            params.append(contract)
        if clauses:
            query += ' WHERE ' + ' AND '.join(clauses)
        query += ' ORDER BY source_id, contract, version'
        for row in db.execute(query, params):
            key = f"{row['source_id']}:{row['contract']}"
            raw = sources.get(row['source_id'])
            if raw is None:
                problems.append(f'energy_attribution_orphan:{key}')
                continue
            doc = json.loads(raw)
            recomputed = _compute_ledger(doc, row['source_id'], row['contract'])
            body = _canonical(recomputed)
            if hashlib.sha256(body.encode('utf-8')).hexdigest() != row['digest'] or body != row['body']:
                problems.append(f'energy_attribution:{key}')
                continue
            if row['status'] != recomputed['status']:
                problems.append(f'energy_attribution_status:{key}')
            if recomputed['status'] != STATUS_ATTRIBUTED:
                continue
            slots, slot_issues = _reconstruct_slots(doc)
            if slot_issues:
                problems.append(f'energy_metering:{key}')
                continue
            total_wh = sum(slot['wh'] for slot in slots)
            total_g = sum(Fraction(slot['wh'] * slot['intensity'], 1000) for slot in slots)
            totals = recomputed['totals']
            jobs_wh = sum((Fraction(t['wh']['num'], t['wh']['den'])
                           for t in totals['jobs'].values()), Fraction(0))
            jobs_g = sum((Fraction(t['grams']['num'], t['grams']['den'])
                          for t in totals['jobs'].values()), Fraction(0))
            platform_wh = Fraction(totals['platform']['wh']['num'],
                                   totals['platform']['wh']['den'])
            platform_g = Fraction(totals['platform']['grams']['num'],
                                  totals['platform']['grams']['den'])
            grand_g = Fraction(totals['grams']['num'], totals['grams']['den'])
            if (jobs_wh + platform_wh != total_wh or totals['energy_wh'] != total_wh
                    or jobs_g + platform_g != total_g or grand_g != total_g):
                problems.append(f'energy_conservation:{key}')
        return problems
    finally:
        db.close()
