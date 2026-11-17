"""Freeze a pair of real decisions and list changed event identities.

No counterfactual attribution is implemented here.
"""
import hashlib
import json

from .store import connect

SCHEMA = """
CREATE TABLE IF NOT EXISTS causality_pair(
 pair_id TEXT PRIMARY KEY, digest TEXT NOT NULL, snapshot TEXT NOT NULL);
"""


def _run(db, run_id):
    row = db.execute('SELECT id,batch_id,status FROM run WHERE id=?',(run_id,)).fetchone()
    if not row:
        raise ValueError('causality_unknown_run')
    decisions = {x['subject']:x['status'] for x in db.execute(
        'SELECT subject,status FROM decision WHERE run_id=?',(run_id,))}
    events = sorted({x['event_id'] for x in db.execute(
        'SELECT event_id FROM evidence WHERE run_id=? AND event_id IS NOT NULL',(run_id,))})
    return {'run':dict(row),'decisions':decisions,'event_ids':events}


def record_pair(db_path,pair_id,baseline_run,comparison_run,target_subject):
    if not pair_id or baseline_run >= comparison_run:
        raise ValueError('causality_bad_pair')
    db = connect(db_path)
    try:
        db.executescript(SCHEMA)
        first = _run(db,baseline_run)
        second = _run(db,comparison_run)
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
            events.append({**dict(row),'source_digest':source['digest'],
                           'source_raw_hash':hashlib.sha256(source['raw']).hexdigest()})
        snapshot = {'pair_id':pair_id,'baseline':first,'comparison':second,
                    'target_subject':target_subject,'events':events}
        raw = json.dumps(snapshot,sort_keys=True,ensure_ascii=False)
        digest = hashlib.sha256(raw.encode()).hexdigest()
        old = db.execute('SELECT digest FROM causality_pair WHERE pair_id=?',(pair_id,)).fetchone()
        if old:
            if old['digest'] != digest:
                raise ValueError('causality_pair_conflict')
            return pair_id
        with db:
            db.execute('INSERT INTO causality_pair VALUES (?,?,?)',(pair_id,digest,raw))
        return pair_id
    finally:
        db.close()


def manual_diff(db_path,pair_id):
    db = connect(db_path)
    try:
        db.executescript(SCHEMA)
        row = db.execute('SELECT snapshot FROM causality_pair WHERE pair_id=?',(pair_id,)).fetchone()
        if not row:
            raise ValueError('causality_unknown_pair')
        snap = json.loads(row['snapshot'])
        before = snap['baseline']['decisions']
        after = snap['comparison']['decisions']
        new_ids = sorted(set(snap['comparison']['event_ids']) -
                         set(snap['baseline']['event_ids']))
        return {'pair_id':pair_id,'changed_subjects':sorted(
            key for key in before if before[key] != after[key]),
            'target_before':before[snap['target_subject']],
            'target_after':after[snap['target_subject']],
            'new_event_ids':new_ids}
    finally:
        db.close()
