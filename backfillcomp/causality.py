"""Causal diagnosis certificates for historical compensation decision flips.

Two rigorous causal contracts over the same frozen facts:

* ``necessary_change``           — which single new source unit is, alone,
  necessary for the target subject's flip; ``none`` when no unit is.
* ``minimal_sufficient_change``  — every minimal set of new source units whose
  addition to the baseline inputs reproduces the comparison-run target state.

Interventions are whole source files (all events of a file move together).
Every counterfactual is a real sandbox replay (``ingest.load`` then
``rules.evaluate``) on a temporary control database rebuilt from the original
baseline inputs. Production control databases, historical runs, target ledgers,
compensation amounts, capacity leases and consumer publications are never
modified. Incomparable runs yield ``unsupported``; an unfinished search yields
``indeterminate`` — a fake-complete attribution is never emitted.
"""
import hashlib
import json
import shutil
import tempfile
from pathlib import Path

from .store import connect
from .ingest import initialize, load, hash_bytes
from .rules import evaluate
from .report import verify as report_verify

SCHEMA = """
CREATE TABLE IF NOT EXISTS causality_pair(
 pair_id TEXT PRIMARY KEY, digest TEXT NOT NULL, snapshot TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS causality_certificate(
 pair_id TEXT NOT NULL, contract TEXT NOT NULL, digest TEXT NOT NULL,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, body TEXT NOT NULL,
 PRIMARY KEY(pair_id,contract));
"""

NECESSARY = 'necessary_change'
SUFFICIENT = 'minimal_sufficient_change'
CONTRACTS = (NECESSARY, SUFFICIENT)

# Hard search limits. Above these, only full enumeration would still count as
# complete; anything smaller returns indeterminate rather than pretending.
MAX_UNITS = 12
MAX_SUBJECTS = 12
MAX_EDGES = 30

INGEST_ISSUE_CODES = ('parse_error', 'identity_conflict')

# Problems that make a run genuinely incomparable: unsupported, no causation
# claim may be issued. Everything else detected at the gate (unattributable
# conflicts / parse failures) yields indeterminate instead of a fake "pending".
INCOMPARABLE = frozenset({
    'batch_mismatch', 'batch_missing', 'config_tampered', 'config_identity_drift',
    'config_snapshot_drift', 'ledger_snapshot_drift', 'ledger_snapshot_missing',
    'baseline_event_missing', 'event_frozen_drift', 'source_missing',
    'source_summary_mismatch', 'source_raw_mismatch', 'run_evidence_drift',
    'decision_evidence_missing', 'baseline_event_disappeared', 'old_source_mutated',
    'comparison_event_unresolved', 'report_verify_failed',
})
INDETERMINATE_GATE = frozenset({
    'source_raw_bytes_missing', 'unattributable_identity_conflict',
    'report_verify_unavailable_bytes',
})


def _sha_json(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False).encode('utf-8')
    ).hexdigest()


def _sha_bytes(raw):
    if isinstance(raw, str):
        raw = raw.encode('utf-8')
    return hashlib.sha256(raw or b'').hexdigest()


def _run(db, run_id):
    row = db.execute('SELECT id,batch_id,status FROM run WHERE id=?', (run_id,)).fetchone()
    if not row:
        raise ValueError('causality_unknown_run')
    decisions = {x['subject']: x['status'] for x in db.execute(
        'SELECT subject,status FROM decision WHERE run_id=?', (run_id,))}
    events = sorted({x['event_id'] for x in db.execute(
        'SELECT event_id FROM evidence WHERE run_id=? AND event_id IS NOT NULL', (run_id,))})
    return {'run': dict(row), 'decisions': decisions, 'event_ids': events}


def record_pair(db_path, pair_id, baseline_run, comparison_run, target_subject):
    if not pair_id or baseline_run >= comparison_run:
        raise ValueError('causality_bad_pair')
    db = connect(db_path)
    try:
        db.executescript(SCHEMA)
        first = _run(db, baseline_run)
        second = _run(db, comparison_run)
        if first['run']['batch_id'] != second['run']['batch_id']:
            raise ValueError('causality_batch_mismatch')
        if target_subject not in first['decisions'] or target_subject not in second['decisions']:
            raise ValueError('causality_unknown_subject')
        event_ids = sorted(set(first['event_ids']) | set(second['event_ids']))
        events = []
        for event_id in event_ids:
            row = db.execute('SELECT id,source_id,subject,payload,digest FROM raw_event WHERE id=?',
                             (event_id,)).fetchone()
            if not row:
                raise ValueError('causality_event_missing')
            source = db.execute('SELECT digest,raw FROM source_file WHERE id=?',
                                (row['source_id'],)).fetchone()
            if not source:
                raise ValueError('causality_source_missing')
            events.append({**dict(row), 'source_digest': source['digest'],
                           'source_raw_hash': hashlib.sha256(source['raw']).hexdigest()})
        # Freeze the configuration identity and the ledger snapshot identity so
        # later drift between the two runs cannot be mislabelled causation.
        cfg_digest = db.execute('SELECT config_hash FROM batch WHERE id=?',
                                (first['run']['batch_id'],)).fetchone()['config_hash']
        cfg_obj = json.loads(db.execute('SELECT config_raw FROM batch WHERE id=?',
                                        (first['run']['batch_id'],)).fetchone()['config_raw'])
        from .rules import _snapshot
        ledger_path = _snapshot(cfg_obj)
        ledger_digest = _snapshot_digest(ledger_path) if ledger_path.exists() else None
        snapshot = {'pair_id': pair_id, 'baseline': first, 'comparison': second,
                    'target_subject': target_subject, 'events': events,
                    'config_digest': cfg_digest,
                    'ledger_snapshot_digest': ledger_digest}
        raw = json.dumps(snapshot, sort_keys=True, ensure_ascii=False)
        digest = hashlib.sha256(raw.encode()).hexdigest()
        old = db.execute('SELECT digest FROM causality_pair WHERE pair_id=?', (pair_id,)).fetchone()
        if old:
            if old['digest'] != digest:
                raise ValueError('causality_pair_conflict')
            return pair_id
        with db:
            db.execute('INSERT INTO causality_pair VALUES (?,?,?)', (pair_id, digest, raw))
        return pair_id
    finally:
        db.close()


def manual_diff(db_path, pair_id):
    db = connect(db_path)
    try:
        db.executescript(SCHEMA)
        row = db.execute('SELECT snapshot FROM causality_pair WHERE pair_id=?', (pair_id,)).fetchone()
        if not row:
            raise ValueError('causality_unknown_pair')
        snap = json.loads(row['snapshot'])
        before = snap['baseline']['decisions']
        after = snap['comparison']['decisions']
        new_ids = sorted(set(snap['comparison']['event_ids']) -
                         set(snap['baseline']['event_ids']))
        return {'pair_id': pair_id, 'changed_subjects': sorted(
            key for key in before if before[key] != after[key]),
            'target_before': before[snap['target_subject']],
            'target_after': after[snap['target_subject']],
            'new_event_ids': new_ids}
    finally:
        db.close()


# --------------------------------------------------------------------------- #
# Frozen-fact loading and comparability gate
# --------------------------------------------------------------------------- #

def _snapshot_digest(path):
    """Frozen identity of a ledger snapshot file (schema + all rows)."""
    con = __import__('sqlite3').connect(path)
    try:
        objects = [r[0] for r in con.execute(
            "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY sql")]
        for table in [r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]:
            cols = [r[1] for r in con.execute(f'PRAGMA table_info("{table}")')]
            data = [list(r) for r in con.execute(
                f'SELECT * FROM "{table}" ORDER BY ' + ','.join(f'"{c}"' for c in cols))]
            objects.append({'table': table, 'columns': cols, 'rows': data})
        return _sha_json(objects)
    finally:
        con.close()


def _gate(db, snap, control_db_path):
    """Return (hard_problems, soft_problems, facts).

    Hard problems make the two runs incomparable (``unsupported``); soft
    problems are un-attributable parse/identity failures that keep the result
    ``indeterminate`` rather than being mistaken for a normal "pending".
    """
    hard = []
    soft = []
    base = snap['baseline']
    comp = snap['comparison']
    target = snap['target_subject']

    if base['run']['batch_id'] != comp['run']['batch_id']:
        hard.append('batch_mismatch')
    batch = db.execute('SELECT * FROM batch WHERE id=?',
                       (comp['run']['batch_id'],)).fetchone()
    if not batch:
        hard.append('batch_missing')
        return hard, soft, None
    if hash_bytes(batch['config_raw']) != batch['config_hash']:
        hard.append('config_tampered')
    cfg = json.loads(batch['config_raw'])
    config_digest = batch['config_hash']
    if snap.get('config_digest') and snap['config_digest'] != config_digest:
        hard.append('config_snapshot_drift')

    rule = db.execute('SELECT digest FROM rule_version WHERE id=?',
                      (cfg.get('rule_version', 'v1'),)).fetchone()
    if not rule or rule['digest'] != config_digest:
        hard.append('config_identity_drift')

    from .rules import _snapshot
    snap_path = _snapshot(cfg)
    if not snap_path.exists():
        hard.append('ledger_snapshot_missing')
        ledger_digest = None
    else:
        ledger_digest = _snapshot_digest(snap_path)
        if snap.get('ledger_snapshot_digest') and \
                snap['ledger_snapshot_digest'] != ledger_digest:
            hard.append('ledger_snapshot_drift')

    # Frozen events must still match the live source bytes line-for-line.
    frozen = {}
    for ev in snap['events']:
        frozen[ev['id']] = ev
        live = db.execute('SELECT id,source_id,subject,payload,digest FROM raw_event WHERE id=?',
                          (ev['id'],)).fetchone()
        if not live:
            hard.append('baseline_event_missing')
            continue
        if live['digest'] != ev['digest'] or live['source_id'] != ev['source_id'] \
                or live['subject'] != ev['subject'] or live['payload'] != ev['payload']:
            hard.append('event_frozen_drift')
        src = db.execute('SELECT digest,raw FROM source_file WHERE id=?',
                         (live['source_id'],)).fetchone()
        if not src:
            hard.append('source_missing')
        else:
            if src['raw'] is None or len(src['raw']) == 0:
                # Original bytes unavailable: no honest replay can include this
                # unit, so the diagnosis stays indeterminate. Skip the byte and
                # event-line re-derivation below; both need the original bytes.
                soft.append('source_raw_bytes_missing')
                continue
            if src['digest'] != ev['source_digest']:
                hard.append('source_summary_mismatch')
            if _sha_bytes(src['raw']) != ev['source_raw_hash']:
                hard.append('source_raw_mismatch')

    # Live evidence of both runs must equal the frozen run facts.
    for frozen_run in (base, comp):
        rid = frozen_run['run']['id']
        live_run = _run(db, rid)
        if live_run['decisions'] != frozen_run['decisions'] \
                or live_run['event_ids'] != frozen_run['event_ids']:
            hard.append('run_evidence_drift')
    if any(not db.execute('SELECT 1 FROM evidence WHERE run_id=? AND subject=?',
                          (r, target)).fetchone()
           for r in (base['run']['id'], comp['run']['id'])):
        hard.append('decision_evidence_missing')

    # Comparison event set must equal baseline events plus events carried by
    # genuinely new source files. No baseline event may disappear; no new
    # event may come from an old (mutated) source.
    base_ids = set(base['event_ids'])
    comp_ids = set(comp['event_ids'])
    if base_ids - comp_ids:
        hard.append('baseline_event_disappeared')
    new_ids = comp_ids - base_ids
    base_source_ids = {frozen[e]['source_id'] for e in base_ids if e in frozen}
    new_source_ids = sorted({frozen[e]['source_id'] for e in new_ids if e in frozen})
    if any(sid in base_source_ids for sid in new_source_ids):
        hard.append('old_source_mutated')

    # Intervention units = the new source files, whole file at a time.
    units = []
    for sid in new_source_ids:
        src = db.execute('SELECT id,name,digest,raw FROM source_file WHERE id=?',
                         (sid,)).fetchone()
        if not src:
            hard.append('source_missing')
            continue
        if src['raw'] is None or len(src['raw']) == 0:
            soft.append('source_raw_bytes_missing')
        unit_events = [dict(r) for r in db.execute(
            'SELECT id,event_key,subject,kind,digest FROM raw_event WHERE source_id=? ORDER BY line_no',
            (sid,))]
        units.append({'source_id': sid, 'name': src['name'], 'digest': src['digest'],
                      'raw_sha256': _sha_bytes(src['raw'] or b''),
                      'event_keys': [e['event_key'] for e in unit_events],
                      'event_ids': [e['id'] for e in unit_events]})
        # A unit that also carries an unattributable failure can never be
        # cleanly included/excluded: keep the diagnosis indeterminate.
        bad = [r['code'] for r in db.execute(
            'SELECT DISTINCT code FROM issue WHERE source_id=? AND code IN (?,?)',
            (sid,) + INGEST_ISSUE_CODES)]
        if bad:
            soft.append('unattributable_identity_conflict')

    # Unattributed between-run sources: new files that produced no evidence
    # event but did leave parse/identity failures. Their failure can never be
    # honestly labelled "pending".
    max_base_source = max(base_source_ids, default=0)
    unattributed = []
    for src in db.execute('SELECT id,name,digest FROM source_file WHERE id>? ORDER BY id',
                          (max_base_source,)):
        if src['id'] in new_source_ids:
            continue
        bad = [r['code'] for r in db.execute(
            'SELECT DISTINCT code FROM issue WHERE source_id=? AND code IN (?,?)',
            (src['id'],) + INGEST_ISSUE_CODES)]
        if bad:
            unattributed.append({'source_id': src['id'], 'name': src['name'],
                                 'digest': src['digest'], 'issue_codes': bad})
    if unattributed:
        soft.append('unattributable_identity_conflict')

    # Every comparison evidence event must resolve to a baseline event or a
    # frozen new unit event — no unknown event identities.
    known = set(frozen)
    if any(e not in known for e in comp_ids):
        hard.append('comparison_event_unresolved')

    # Independent end-to-end source-chain check for each run's database.
    # Failures caused by already-acknowledged unavailable bytes are recorded as
    # indeterminate (cannot replay), not as a broken/incomparable run.
    verify_problems = report_verify(control_db_path)
    missing_bytes = 'source_raw_bytes_missing' in soft
    if verify_problems and not missing_bytes:
        hard.append('report_verify_failed')
    elif verify_problems:
        soft.append('report_verify_unavailable_bytes')

    facts = {'config_digest': config_digest,
             'ledger_snapshot_digest': ledger_digest,
             'baseline_source_ids': sorted(base_source_ids),
             'units': units,
             'unattributed_sources': unattributed}
    unclassified = (set(hard) | set(soft)) - (INCOMPARABLE | INDETERMINATE_GATE)
    if unclassified:  # defensive: every gate problem must have a known class
        raise ValueError(f"causality_unclassified_gate_problem:{sorted(unclassified)}")
    return sorted(set(hard)), sorted(set(soft)), facts


# --------------------------------------------------------------------------- #
# Sandbox counterfactual replay
# --------------------------------------------------------------------------- #

def _replay(cfg_dict, baseline_source_raw, unit_raw_by_id, included, workdir):
    """Rebuild a temporary control DB from original inputs and really evaluate.

    Only deterministic facts are returned; the temporary database path is an
    implementation detail and never enters the certificate.
    """
    sandbox_db = workdir / 'counterfactual.db'
    cfg_path = workdir / 'config.json'
    snapshot_path = workdir / 'ledger-snapshot.db'
    from .rules import _snapshot
    shutil.copy2(_snapshot(cfg_dict), snapshot_path)
    replay_cfg = dict(cfg_dict)
    replay_cfg['snapshot_path'] = str(snapshot_path)
    cfg_path.write_text(json.dumps(replay_cfg, ensure_ascii=False), encoding='utf-8')
    initialize(sandbox_db, cfg_path)

    # Deterministic order: baseline sources in their original source_id order,
    # then included new units in source_id order — never cherry-picked lines.
    for order, (sid, raw) in enumerate(baseline_source_raw):
        path = workdir / f'baseline-{order:03d}.jsonl'
        path.write_bytes(raw)
        load(sandbox_db, path)
    for order, sid in enumerate(included):
        path = workdir / f'unit-{order:03d}.jsonl'
        path.write_bytes(unit_raw_by_id[sid])
        load(sandbox_db, path)

    con = connect(sandbox_db)
    try:
        unit_digests = {hash_bytes(unit_raw_by_id[sid]): sid for sid in included}
        issue_rows = [dict(r) for r in con.execute(
            "SELECT source_id,line_no,subject,code,detail FROM issue "
            "WHERE source_id IS NOT NULL AND code IN (?,?) ORDER BY id",
            INGEST_ISSUE_CODES)]
        replay_issues = []
        for row in issue_rows:
            src = con.execute('SELECT digest FROM source_file WHERE id=?',
                              (row['source_id'],)).fetchone()
            replay_issues.append({**row,
                                  'unit_source_id': unit_digests.get(src['digest']) if src else None})
    finally:
        con.close()

    result = evaluate(sandbox_db)
    return result, replay_issues


def _ancestor_chain(graph, subject):
    chain = []
    seen = set()

    def walk(node, stack):
        for parent in graph.get(node, []):
            if parent in stack:  # cycle: record and stop expanding it
                chain.append({'subject': parent, 'via': list(stack), 'cycle': True})
                continue
            if parent in seen:
                continue
            seen.add(parent)
            chain.append({'subject': parent, 'via': list(stack), 'cycle': False})
            walk(parent, stack + [parent])

    walk(subject, [subject])
    return chain


# --------------------------------------------------------------------------- #
# Public assessment API
# --------------------------------------------------------------------------- #

def _minimal_certificate(pair_id, contract, target, problems, before, after,
                         reason, status='unsupported'):
    comparability = {'passed': not problems, 'unsupported_problems': list(problems),
                     'indeterminate_problems': [], 'reason': reason}
    body = {'pair_id': pair_id, 'contract': contract, 'version': 1, 'status': status,
            'target_subject': target, 'target_baseline_status': before,
            'target_comparison_status': after, 'config_digest': None,
            'ledger_snapshot_digest': None, 'comparability': comparability,
            'search': {'strategy': None, 'subsets_total': 0,
                       'subsets_evaluated': 0, 'complete': False},
            'counterfactuals': [], 'findings': {contract: None},
            'investigation_checklist': [], 'frozen_source_units': []}
    body['digest'] = _sha_json({k: v for k, v in body.items() if k != 'digest'})
    return body


def assess(db_path, pair_id, contract):
    if contract not in CONTRACTS:
        raise ValueError('causality_unknown_contract')
    db = connect(db_path)
    try:
        db.executescript(SCHEMA)
        body = _build_certificate(db, db_path, pair_id, contract)
        return _persist(db, pair_id, contract, body)
    finally:
        db.close()


def _build_certificate(db, db_path, pair_id, contract):
    row = db.execute('SELECT snapshot FROM causality_pair WHERE pair_id=?',
                     (pair_id,)).fetchone()
    if not row:
        raise ValueError('causality_unknown_pair')
    snap = json.loads(row['snapshot'])
    target = snap['target_subject']
    base = snap['baseline']
    comp = snap['comparison']
    before = base['decisions'][target]
    after = comp['decisions'][target]

    hard, soft, facts = _gate(db, snap, db_path)
    if facts is None:  # batch itself is gone: no configuration can be rebuilt
        return _minimal_certificate(pair_id, contract, target, hard,
                                    before, after, reason='batch_missing')

    cfg = json.loads(db.execute(
        'SELECT config_raw FROM batch WHERE id=?',
        (comp['run']['batch_id'],)).fetchone()['config_raw'])
    graph = cfg.get('dependencies', {})
    edge_count = sum(len(parents) for parents in graph.values())
    subjects = sorted(cfg['subjects'])

    def certificate(status, reason=None, **extra):
        comparability = {'passed': not hard and not soft,
                         'unsupported_problems': list(hard),
                         'indeterminate_problems': list(soft),
                         'reason': reason or (hard[0] if hard else (soft[0] if soft else None))}
        body = {
            'pair_id': pair_id,
            'contract': contract,
            'version': 1,
            'status': status,
            'target_subject': target,
            'target_baseline_status': before,
            'target_comparison_status': after,
            'config_digest': facts['config_digest'] if facts else None,
            'ledger_snapshot_digest': facts['ledger_snapshot_digest'] if facts else None,
            'comparability': comparability,
            'search': {'strategy': None, 'subsets_total': 0,
                       'subsets_evaluated': 0, 'complete': False},
            'counterfactuals': [],
            'findings': {contract: None},
            'investigation_checklist': [],
            'frozen_source_units': facts['units'] if facts else [],
        }
        body.update(extra)
        body['digest'] = _sha_json({k: v for k, v in body.items() if k != 'digest'})
        return body

    if hard:
        return certificate('unsupported')
    if soft:
        return certificate('indeterminate')

    if before == after:
        return certificate('unsupported', reason='no_target_status_flip')

    units = facts['units']
    unit_ids = [u['source_id'] for u in units]
    limits_ok = (len(units) <= MAX_UNITS and len(subjects) <= MAX_SUBJECTS
                 and edge_count <= MAX_EDGES)
    if not limits_ok:
        return certificate('indeterminate', reason='search_scale_limit_exceeded',
                           search={'strategy': 'enumeration_blocked_by_limits',
                                   'subsets_total': 2 ** len(units),
                                   'subsets_evaluated': 0, 'complete': False,
                                   'limits': {'new_source_units': len(units),
                                              'subjects': len(subjects),
                                              'dependency_edges': edge_count,
                                              'max_units': MAX_UNITS,
                                              'max_subjects': MAX_SUBJECTS,
                                              'max_edges': MAX_EDGES}})

    # Original baseline inputs and unit bytes, read-only.
    baseline_source_raw = [(sid, db.execute(
        'SELECT raw FROM source_file WHERE id=?', (sid,)).fetchone()['raw'])
        for sid in facts['baseline_source_ids']]
    unit_raw_by_id = {sid: db.execute(
        'SELECT raw FROM source_file WHERE id=?', (sid,)).fetchone()['raw']
        for sid in unit_ids}
    unit_summary = {u['source_id']: {'name': u['name'], 'digest': u['digest'],
                                     'raw_sha256': u['raw_sha256'],
                                     'event_keys': u['event_keys']}
                    for u in units}
    dependency_chain = _ancestor_chain(graph, target)
    n = len(unit_ids)
    counterfactuals = []
    uncertain = []
    statuses = {}
    workdir = Path(tempfile.mkdtemp(prefix=f'causal-{pair_id}-{contract}-'))
    try:
        for mask in range(2 ** n):
            included = tuple(unit_ids[i] for i in range(n) if mask & (1 << i))
            result, replay_issues = _replay(
                cfg, baseline_source_raw, unit_raw_by_id, included, workdir)
            target_status = result.get(target)
            entry = {
                'subset_mask': mask,
                'subset_source_ids': list(included),
                'subset_sources': [unit_summary[sid] for sid in included],
                'target_status': target_status,
                'all_subject_statuses': {k: result[k] for k in sorted(result)},
                'parent_dependency_chain': dependency_chain,
                'replay_issues': replay_issues,
                'uncertain': bool(replay_issues) or target_status is None,
            }
            counterfactuals.append(entry)
            statuses[mask] = target_status
            if entry['uncertain']:
                uncertain.append(mask)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    empty_mask, full_mask = 0, (1 << n) - 1
    if statuses.get(empty_mask) != before or statuses.get(full_mask) != after:
        return certificate('unsupported', reason='replay_does_not_match_frozen_runs',
                           search={'strategy': 'full_enumeration',
                                   'subsets_total': 2 ** n,
                                   'subsets_evaluated': 2 ** n, 'complete': True},
                           counterfactuals=counterfactuals)
    if uncertain:
        return certificate('indeterminate', reason='counterfactual_replay_failure',
                           search={'strategy': 'full_enumeration_with_failures',
                                   'subsets_total': 2 ** n,
                                   'subsets_evaluated': 2 ** n, 'complete': False,
                                   'uncertain_subset_masks': sorted(uncertain)},
                           counterfactuals=counterfactuals)

    causes_flip = {mask for mask, st in statuses.items() if st == after}

    if contract == NECESSARY:
        # A unit alone is necessary iff removing only it from the full
        # comparison set returns the target to its baseline state.
        necessary = []
        for i, sid in enumerate(unit_ids):
            mask = full_mask & ~(1 << i)
            if statuses[mask] == before:
                necessary.append(unit_summary[sid])
        findings = {NECESSARY: necessary}  # [] is an explicit "none".
    else:
        # Every inclusion-minimal subset that already produces the comparison
        # target state. Do not report just the lexicographic first one.
        minimal = []
        for mask in sorted(causes_flip):
            members = tuple(unit_ids[i] for i in range(n) if mask & (1 << i))
            if any((mask & sub) == sub and sub != mask for sub in causes_flip):
                continue
            minimal.append({'subset_source_ids': list(members),
                            'subset_sources': [unit_summary[sid] for sid in members]})
        findings = {SUFFICIENT: minimal}

    checklist = _investigation_checklist(contract, findings, target, after,
                                         dependency_chain, unit_summary)
    return certificate(
        'complete',
        search={'strategy': 'full_enumeration', 'subsets_total': 2 ** n,
                'subsets_evaluated': 2 ** n, 'complete': True},
        counterfactuals=counterfactuals,
        findings=findings,
        investigation_checklist=checklist)


def _persist(db, pair_id, contract, body):
    old = db.execute('SELECT digest FROM causality_certificate WHERE pair_id=? AND contract=?',
                     (pair_id, contract)).fetchone()
    if old:
        if old['digest'] != body['digest']:
            raise ValueError('causality_certificate_conflict')
        return dict(body)
    with db:
        db.execute('INSERT INTO causality_certificate(pair_id,contract,digest,body) VALUES (?,?,?,?)',
                   (pair_id, contract, body['digest'],
                    json.dumps(body, sort_keys=True, ensure_ascii=False)))
    return dict(body)


def _investigation_checklist(contract, findings, target, after, chain, unit_summary):
    checklist = []
    if contract == NECESSARY:
        necessary = findings[NECESSARY]
        if not necessary:
            checklist.append({
                'action': 'escalate_review',
                'reason': 'no_single_necessary_source',
                'detail': f'目标主体 {target} 的翻转不存在单独必要来源；'
                          f'按必要原因合同只能升级复核，不得任选一个来源归责。',
                'sources': []})
        for unit in necessary:
            checklist.append({
                'action': 'investigate_source',
                'reason': 'single_necessary_change',
                'detail': f'撤去该来源后 {target} 即回到基线状态，须单独调查其原始字节与修订依据。',
                'sources': [unit]})
    else:
        minimal = findings[SUFFICIENT]
        if not minimal:
            checklist.append({
                'action': 'escalate_review',
                'reason': 'no_sufficient_subset',
                'detail': f'没有任何新增来源子集足以使 {target} 进入 {after}，升级复核。',
                'sources': []})
        for group in minimal:
            checklist.append({
                'action': 'investigate_source_group',
                'reason': 'independent_minimal_sufficient_change',
                'detail': f'该来源组合可独立使 {target} 翻转至 {after}，'
                          f'须作为一条独立责任链分别调查；父依赖传播链一并核验。',
                'sources': group['subset_sources'],
                'parent_dependency_chain': chain})
    return checklist


# --------------------------------------------------------------------------- #
# Independent verification
# --------------------------------------------------------------------------- #

def verify_certificate(db_path, pair_id, contract):
    """Re-derive everything independently from frozen bytes and sandbox replays."""
    if contract not in CONTRACTS:
        raise ValueError('causality_unknown_contract')
    db = connect(db_path)
    try:
        db.executescript(SCHEMA)
        row = db.execute('SELECT body FROM causality_certificate WHERE pair_id=? AND contract=?',
                         (pair_id, contract)).fetchone()
        if not row:
            raise ValueError('causality_unknown_certificate')
        stored = json.loads(row['body'])
        # Soft gate problems are part of the certificate's own verdict (kept
        # indeterminate on purpose); they do not make verification fail.
        problems = list(stored['comparability'].get('unsupported_problems', []))

        snap_row = db.execute('SELECT snapshot FROM causality_pair WHERE pair_id=?',
                              (pair_id,)).fetchone()
        if not snap_row:
            problems.append('pair_missing')
            return {'pair_id': pair_id, 'contract': contract, 'ok': False,
                    'status': stored['status'], 'problems': sorted(set(problems))}
        snap = json.loads(snap_row['snapshot'])
        hard, soft, facts = _gate(db, snap, db_path)
        problems.extend(hard)
        # The live gate must reproduce the stored verdict classification.
        if sorted(soft) != stored['comparability'].get('indeterminate_problems', []):
            problems.append('indeterminate_verdict_mismatch')

        if stored['digest'] != _sha_json({k: v for k, v in stored.items() if k != 'digest'}):
            problems.append('certificate_digest_mismatch')

        expected = _build_certificate(db, db_path, pair_id, contract)
        if expected['digest'] != stored['digest']:
            problems.append('recomputed_certificate_differs')
        if expected['status'] != stored['status']:
            problems.append('status_mismatch')
        if expected['findings'] != stored['findings']:
            problems.append('finding_mismatch')
        if expected['investigation_checklist'] != stored['investigation_checklist']:
            problems.append('investigation_checklist_mismatch')

        # Granular cross-checks so the failure mode is named, not just "differs".
        stored_cf = {(c['subset_mask']): c for c in stored.get('counterfactuals', [])}
        expected_cf = {(c['subset_mask']): c for c in expected.get('counterfactuals', [])}
        if set(stored_cf) != set(expected_cf):
            problems.append('counterfactual_subset_mismatch')
        for mask, entry in expected_cf.items():
            old = stored_cf.get(mask)
            if not old:
                continue
            if old['target_status'] != entry['target_status']:
                problems.append(f'counterfactual_status_mismatch:mask{mask}')
            if old['all_subject_statuses'] != entry['all_subject_statuses']:
                problems.append(f'upstream_propagation_mismatch:mask{mask}')
            if old['subset_sources'] != entry['subset_sources']:
                problems.append(f'forged_source:mask{mask}')
            if old['parent_dependency_chain'] != entry['parent_dependency_chain']:
                problems.append(f'dependency_chain_mismatch:mask{mask}')

        # Historical runs must be untouched: re-frozen pair identity must hold.
        if db.execute('SELECT digest FROM causality_pair WHERE pair_id=?',
                      (pair_id,)).fetchone()['digest'] != _sha_json(snap):
            problems.append('historical_run_modified')

        return {'pair_id': pair_id, 'contract': contract, 'ok': not problems,
                'status': stored['status'],
                'stored_digest': stored['digest'],
                'recomputed_digest': expected['digest'],
                'problems': sorted(set(problems))}
    finally:
        db.close()
