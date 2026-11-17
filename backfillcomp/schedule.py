"""Capacity leases for planned compensation with durable contention decisions."""
import hashlib
import json
from datetime import datetime, timezone

from .store import connect

SCHEMA="""
CREATE TABLE IF NOT EXISTS capacity_window(
 id TEXT PRIMARY KEY, starts TEXT NOT NULL, ends TEXT NOT NULL, units INTEGER NOT NULL CHECK(units>0));
CREATE TABLE IF NOT EXISTS execution_lease(
 id TEXT PRIMARY KEY, plan_digest TEXT NOT NULL, run_id INTEGER NOT NULL REFERENCES run(id),
 window_id TEXT NOT NULL REFERENCES capacity_window(id), worker TEXT NOT NULL,
 units INTEGER NOT NULL, target_hash TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('waiting','reserved','active','released','blocked')));
"""

def _utc(value):
    dt=datetime.fromisoformat(value.replace('Z','+00:00'))
    if dt.tzinfo is None:raise ValueError('window_timezone_required')
    return dt.astimezone(timezone.utc)

def _target_hash(plan):
    return hashlib.sha256(json.dumps(plan,sort_keys=True).encode()).hexdigest()

def define_window(db_path, window_id, starts, ends, units):
    start,end=_utc(starts),_utc(ends)
    if start>=end or units<=0:raise ValueError('invalid_capacity_window')
    db=connect(db_path);db.executescript(SCHEMA)
    old=db.execute('SELECT * FROM capacity_window WHERE id=?',(window_id,)).fetchone()
    if old and (old['starts'],old['ends'],old['units'])!=(start.isoformat(),end.isoformat(),units):
        raise ValueError('window_identity_conflict')
    with db:db.execute('INSERT OR IGNORE INTO capacity_window VALUES (?,?,?,?)',(window_id,start.isoformat(),end.isoformat(),units))
    db.close();return window_id

def reserve(db_path, lease_id, plan_digest, window_id, worker, units):
    if not worker or units<=0:raise ValueError('invalid_lease')
    db=connect(db_path);db.executescript(SCHEMA)
    db.execute('BEGIN IMMEDIATE')
    try:
        window=db.execute('SELECT * FROM capacity_window WHERE id=?',(window_id,)).fetchone()
        plan=db.execute('SELECT * FROM plan WHERE digest=?',(plan_digest,)).fetchone()
        latest=db.execute('SELECT max(id) FROM run').fetchone()[0]
        if not window or not plan or plan['run_id']!=latest:raise ValueError('unknown_or_stale_plan')
        now=datetime.now(timezone.utc)
        if now<_utc(window['starts']) or now>=_utc(window['ends']):raise ValueError('window_not_active')
        if units>window['units']:raise ValueError('lease_exceeds_window')
        target_hash=_target_hash(json.loads(plan['body']))
        old=db.execute('SELECT * FROM execution_lease WHERE id=?',(lease_id,)).fetchone()
        if old:
            if (old['plan_digest'],old['window_id'],old['worker'],old['units'])!=(plan_digest,window_id,worker,units):
                raise ValueError('lease_identity_conflict')
            state=old['state']
        else:
            used=db.execute("SELECT COALESCE(sum(units),0) FROM execution_lease WHERE window_id=? AND state IN ('reserved','active')",(window_id,)).fetchone()[0]
            state='reserved' if used+units<=window['units'] else 'waiting'
            db.execute('INSERT INTO execution_lease VALUES (?,?,?,?,?,?,?,?)',
                       (lease_id,plan_digest,latest,window_id,worker,units,target_hash,state))
        db.commit()
    except Exception:
        db.rollback();raise
    finally:db.close()
    return {'lease':lease_id,'state':state}

def activate(db_path, lease_id):
    db=connect(db_path);db.executescript(SCHEMA)
    with db:
        row=db.execute('SELECT * FROM execution_lease WHERE id=?',(lease_id,)).fetchone()
        if not row:raise ValueError('unknown_lease')
        if row['state']!='reserved':raise ValueError('lease_not_reserved')
        plan=db.execute('SELECT * FROM plan WHERE digest=?',(row['plan_digest'],)).fetchone()
        latest=db.execute('SELECT max(id) FROM run').fetchone()[0]
        if latest!=row['run_id'] or not plan or _target_hash(json.loads(plan['body']))!=row['target_hash']:
            db.execute("UPDATE execution_lease SET state='blocked' WHERE id=?",(lease_id,))
            return {'lease':lease_id,'state':'blocked'}
        db.execute("UPDATE execution_lease SET state='active' WHERE id=?",(lease_id,))
    db.close();return {'lease':lease_id,'state':'active'}

def _release_conn(db,row):
    """在调用方事务内释放一条 active/reserved 租约并提升 waiting 候选；返回提升的租约 id。"""
    db.execute("UPDATE execution_lease SET state='released' WHERE id=?",(row['id'],))
    waiting=db.execute("SELECT * FROM execution_lease WHERE window_id=? AND state='waiting' ORDER BY id",(row['window_id'],)).fetchall()
    cap=db.execute('SELECT units FROM capacity_window WHERE id=?',(row['window_id'],)).fetchone()[0]
    used=db.execute("SELECT COALESCE(sum(units),0) FROM execution_lease WHERE window_id=? AND state IN ('reserved','active')",(row['window_id'],)).fetchone()[0]
    promoted=[]
    for candidate in waiting:
        if used+candidate['units']<=cap:
            db.execute("UPDATE execution_lease SET state='reserved' WHERE id=?",(candidate['id'],))
            used+=candidate['units'];promoted.append(candidate['id'])
    return promoted

def release(db_path,lease_id):
    db=connect(db_path);db.executescript(SCHEMA)
    with db:
        row=db.execute('SELECT * FROM execution_lease WHERE id=?',(lease_id,)).fetchone()
        if not row or row['state'] not in ('active','reserved'):raise ValueError('lease_not_releasable')
        promoted=_release_conn(db,row)
    db.close();return {'lease':lease_id,'state':'released','promoted':promoted}
