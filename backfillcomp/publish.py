"""Downstream publication gate after compensation, distinct from target repair."""
import hashlib
import json
import sqlite3
from pathlib import Path

from .store import connect

SCHEMA="""
CREATE TABLE IF NOT EXISTS publication_gate(
 id TEXT PRIMARY KEY, consumer TEXT NOT NULL, run_id INTEGER NOT NULL REFERENCES run(id),
 plan_digest TEXT NOT NULL, target_hash TEXT NOT NULL, expected_hash TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('ready','blocked','uncertain','published','stale')),
 detail TEXT NOT NULL);
"""

def _hash(value):return hashlib.sha256(json.dumps(value,sort_keys=True).encode()).hexdigest()

def _target_snapshot(target,plan):
    if not Path(target).exists():return None
    db=sqlite3.connect(target)
    try:
        has_audit=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='backfill_audit'").fetchone()
        rows=[]
        for item in plan:
            row=db.execute('SELECT amount,computed FROM ledger WHERE id=?',(item['subject'],)).fetchone()
            audit=db.execute('SELECT state FROM backfill_audit WHERE subject=? AND plan_digest=?',
                             (item['subject'],item['_plan_digest'])).fetchone() if has_audit else None
            rows.append({'subject':item['subject'],'row':row,'audit':audit[0] if audit else None})
        return rows
    finally:db.close()

def check(db_path,target,gate_id,consumer,expected):
    """Independent consumer view must match each compensated value and audit."""
    db=connect(db_path);db.executescript(SCHEMA)
    run=db.execute('SELECT max(id) FROM run').fetchone()[0]
    plan_row=db.execute('SELECT * FROM plan WHERE run_id=? ORDER BY id DESC LIMIT 1',(run,)).fetchone()
    if not plan_row:raise ValueError('plan_required')
    plan=[{**x,'_plan_digest':plan_row['digest']} for x in json.loads(plan_row['body'])]
    if set(expected)!={x['subject'] for x in plan}:raise ValueError('consumer_scope_mismatch')
    snapshot=_target_snapshot(target,plan)
    problems=[]
    if snapshot is None:problems.append('target_missing')
    else:
        for item,seen in zip(plan,snapshot):
            if seen['row'] is None:problems.append(f"missing:{item['subject']}")
            elif seen['audit']!='applied':problems.append(f"unproven:{item['subject']}")
            elif seen['row'][1]!=item['new']:problems.append(f"target_drift:{item['subject']}")
            elif expected[item['subject']]!=seen['row'][1]:problems.append(f"consumer_diff:{item['subject']}")
    status='uncertain' if any(x.startswith(('target_missing','unproven','missing')) for x in problems) else ('blocked' if problems else 'ready')
    target_hash=_hash(snapshot)
    expected_hash=_hash(expected)
    old=db.execute('SELECT * FROM publication_gate WHERE id=?',(gate_id,)).fetchone()
    if old:
        if (old['consumer'],old['run_id'],old['plan_digest'],old['expected_hash'])!=(consumer,run,plan_row['digest'],expected_hash):
            raise ValueError('gate_identity_conflict')
        # 已发布是不可无声推翻的事实，保持原样；其余状态必须随目标值/backfill_audit
        # 的变化（如恢复回退把 applied 改为 rolled_back）重新裁定。
        if old['status']=='published':return dict(old)
        with db:
            db.execute('UPDATE publication_gate SET target_hash=?,status=?,detail=? WHERE id=?',
                       (target_hash,status,json.dumps(problems),gate_id))
        return {'gate':gate_id,'status':status,'problems':problems}
    with db:
        db.execute('INSERT INTO publication_gate VALUES (?,?,?,?,?,?,?,?)',
                   (gate_id,consumer,run,plan_row['digest'],target_hash,expected_hash,status,json.dumps(problems)))
    db.close();return {'gate':gate_id,'status':status,'problems':problems}

def publish(db_path,target,gate_id):
    db=connect(db_path);db.executescript(SCHEMA)
    with db:
        gate=db.execute('SELECT * FROM publication_gate WHERE id=?',(gate_id,)).fetchone()
        if not gate:raise ValueError('unknown_gate')
        if gate['status']=='published':return {'gate':gate_id,'status':'published'}
        if gate['status']!='ready':raise ValueError('gate_not_ready')
        plan_row=db.execute('SELECT body FROM plan WHERE digest=?',(gate['plan_digest'],)).fetchone()
        plan=[{**x,'_plan_digest':gate['plan_digest']} for x in json.loads(plan_row['body'])]
        latest=db.execute('SELECT max(id) FROM run').fetchone()[0]
        live=_hash(_target_snapshot(target,plan))
        if latest!=gate['run_id'] or live!=gate['target_hash']:
            db.execute("UPDATE publication_gate SET status='stale' WHERE id=?",(gate_id,))
            return {'gate':gate_id,'status':'stale'}
        db.execute("UPDATE publication_gate SET status='published' WHERE id=?",(gate_id,))
    db.close();return {'gate':gate_id,'status':'published'}
