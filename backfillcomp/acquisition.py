"""Frozen candidate correction files and manual acquisition replay.

This module scores a supplied set of files; it does not choose files.
"""
import hashlib
import json
import shutil
import tempfile
from pathlib import Path

from .ingest import load
from .rules import evaluate
from .store import connect


SCHEMA = """
CREATE TABLE IF NOT EXISTS acquisition_pool(
 pool_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batch(id),
 digest TEXT NOT NULL, raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS acquisition_file(
 pool_id TEXT NOT NULL REFERENCES acquisition_pool(pool_id),
 file_id TEXT NOT NULL, cost INTEGER NOT NULL, digest TEXT NOT NULL, raw BLOB NOT NULL,
 PRIMARY KEY(pool_id,file_id));
CREATE TABLE IF NOT EXISTS acquisition_manual(
 plan_id TEXT PRIMARY KEY, pool_id TEXT NOT NULL REFERENCES acquisition_pool(pool_id),
 digest TEXT NOT NULL, raw BLOB NOT NULL, result TEXT NOT NULL);
"""


def record(db_path, manifest_path):
    path = Path(manifest_path)
    raw = path.read_bytes()
    doc = json.loads(raw)
    if not doc['pool_id'] or not doc['files'] or not doc['weights']:
        raise ValueError('acquisition_empty_pool')
    ids = [item['id'] for item in doc['files']]
    if len(ids) != len(set(ids)):
        raise ValueError('acquisition_duplicate_file')
    frozen = []
    for item in doc['files']:
        if not item['id'] or type(item['cost']) is not int or item['cost'] < 1:
            raise ValueError('acquisition_bad_file')
        payload = (path.parent / item['path']).read_bytes()
        for line in payload.decode('utf-8-sig').splitlines():
            event = json.loads(line)
            if event.get('kind') != 'correction' or event.get('reason') != 'rule_revision':
                raise ValueError('acquisition_bad_event')
        frozen.append((item['id'], item['cost'], hashlib.sha256(payload).hexdigest(), payload))
    digest = hashlib.sha256(raw + b''.join(x[3] for x in frozen)).hexdigest()
    db = connect(db_path)
    try:
        db.executescript(SCHEMA)
        if not db.execute('SELECT 1 FROM batch WHERE id=?',(doc['batch_id'],)).fetchone():
            raise ValueError('acquisition_unknown_batch')
        old = db.execute('SELECT digest FROM acquisition_pool WHERE pool_id=?',
                         (doc['pool_id'],)).fetchone()
        if old:
            if old['digest'] != digest:
                raise ValueError('acquisition_pool_conflict')
            return doc['pool_id']
        with db:
            db.execute('INSERT INTO acquisition_pool VALUES (?,?,?,?)',
                       (doc['pool_id'],doc['batch_id'],digest,raw))
            for file_id,cost,file_digest,payload in frozen:
                db.execute('INSERT INTO acquisition_file VALUES (?,?,?,?,?)',
                           (doc['pool_id'],file_id,cost,file_digest,payload))
        return doc['pool_id']
    finally:
        db.close()


def evaluate_manual(db_path, plan_path):
    raw = Path(plan_path).read_bytes()
    plan = json.loads(raw)
    digest = hashlib.sha256(raw).hexdigest()
    db = connect(db_path)
    try:
        db.executescript(SCHEMA)
        old = db.execute('SELECT digest,result FROM acquisition_manual WHERE plan_id=?',
                         (plan['plan_id'],)).fetchone()
        if old:
            if old['digest'] != digest:
                raise ValueError('acquisition_plan_conflict')
            return json.loads(old['result'])
        pool = db.execute('SELECT raw FROM acquisition_pool WHERE pool_id=?',
                          (plan['pool_id'],)).fetchone()
        if not pool:
            raise ValueError('acquisition_unknown_pool')
        doc = json.loads(pool['raw'])
        rows = {r['file_id']:r for r in db.execute(
            'SELECT file_id,cost,raw FROM acquisition_file WHERE pool_id=?',
            (plan['pool_id'],))}
        selected = plan['selected']
        if len(selected) != len(set(selected)) or not set(selected) <= set(rows):
            raise ValueError('acquisition_bad_selection')
        cost = sum(rows[file_id]['cost'] for file_id in selected)
        with tempfile.TemporaryDirectory(prefix='acquisition-replay-') as tmp:
            sim_db = Path(tmp) / 'control.db'
            shutil.copy2(db_path,sim_db)
            for file_id in sorted(selected):
                candidate = Path(tmp) / (file_id + '.jsonl')
                candidate.write_bytes(rows[file_id]['raw'])
                load(sim_db,candidate)
            statuses = evaluate(sim_db)
        ready = sorted(k for k,v in statuses.items() if v in ('可补偿','不变'))
        score = sum(doc['weights'][k] for k in ready)
        result = {'pool_id':plan['pool_id'],'plan_id':plan['plan_id'],
                  'selected':sorted(selected),'cost':cost,'statuses':statuses,
                  'ready':ready,'weighted_ready':score}
        with db:
            db.execute('INSERT INTO acquisition_manual VALUES (?,?,?,?,?)',
                       (plan['plan_id'],plan['pool_id'],digest,raw,
                        json.dumps(result,sort_keys=True,ensure_ascii=False)))
        return result
    finally:
        db.close()
