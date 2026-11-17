"""跨控制库与目标库的中断恢复裁定。

正常 ``apply`` 在两个独立 SQLite 文件中先后提交：先目标库（ledger 与
backfill_audit），再控制库（execution、租约释放）。进程在两个提交点之间中断时，
目标可能已经按计划 applied，而控制库没有 execution、租约仍 active。本模块把两侧
证据冻结为 repair_case，区分硬阻断与路线级阻断，并由维护者在冻结证据上明确选择
前进（补记 execution、完成租约）或回退（逆序恢复旧值、记录 rolled_back、释放租约）。
"""
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .store import connect, SCHEMA as STORE_SCHEMA
from .schedule import SCHEMA as SCHEDULE_SCHEMA, _release_conn
from .publish import SCHEMA as PUBLISH_SCHEMA

SCHEMA = """
CREATE TABLE IF NOT EXISTS repair_case(
 id TEXT PRIMARY KEY, run_id INTEGER NOT NULL REFERENCES run(id),
 plan_digest TEXT NOT NULL, target_path TEXT NOT NULL,
 lease_id TEXT, worker TEXT,
 evidence_hash TEXT NOT NULL, evidence TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('open','blocked','forwarded','rolled_back')),
 route TEXT CHECK(route IS NULL OR route IN ('forward','rollback')),
 blockers TEXT NOT NULL,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, decided_at TEXT);
CREATE TABLE IF NOT EXISTS repair_row(
 case_id TEXT NOT NULL REFERENCES repair_case(id), subject TEXT NOT NULL,
 expected_old TEXT NOT NULL, expected_new TEXT NOT NULL,
 amount TEXT, actual_value TEXT, audit_state TEXT,
 row_state TEXT NOT NULL, PRIMARY KEY(case_id,subject));
"""

# 测试故障注入点：在回退路线的目标事务提交后、控制事务提交前抛异常。
_fault_after_target = None

# 行状态码
APPLIED_PROVEN = 'applied_proven'
NOT_APPLIED = 'not_applied'
ROLLED_BACK = 'rolled_back'
UNTRACKED_VALUE = 'untracked_value'
THIRD_PARTY_REWRITE = 'third_party_rewrite'
AUDIT_CONTRADICTION = 'audit_contradiction'
MISSING_ROW = 'missing_row'

HARD_BLOCKERS = {
    'target_missing', 'target_audit_missing', 'plan_changed',
    'partial_execution', 'control_execution_conflict',
    'active_lease_required', 'lease_identity_conflict', 'lease_not_active',
}


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def item_hash(item):
    """与 rules.action 写入 execution.before_hash 的算法完全一致。"""
    return hashlib.sha256(json.dumps(item, sort_keys=True).encode()).hexdigest()


def migrate(db_path):
    """对既有控制库执行幂等 SQLite 迁移，补齐全部模块的表。"""
    db = connect(db_path)
    try:
        db.executescript(STORE_SCHEMA)
        db.executescript(SCHEDULE_SCHEMA)
        db.executescript(PUBLISH_SCHEMA)
        db.executescript(SCHEMA)
    finally:
        db.close()
    return str(db_path)


def _table_exists(db, name):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def _load_plan(db, plan_path):
    body = Path(plan_path).read_text(encoding='utf-8')
    digest = hashlib.sha256(body.encode('utf-8')).hexdigest()
    latest = db.execute('SELECT max(id) FROM run').fetchone()[0]
    plan_row = db.execute('SELECT * FROM plan WHERE digest=? AND body=? AND run_id=?',
                          (digest, body, latest)).fetchone()
    if plan_row:
        return json.loads(body), digest, latest, None
    stale = db.execute('SELECT run_id FROM plan WHERE digest=?', (digest,)).fetchone()
    if stale:
        return json.loads(body), digest, latest, 'plan_changed'
    raise ValueError('plan_tampered_or_stale')


def _row_state(item, row, audit):
    """逐行裁定目标值、业务原值与 backfill_audit 是否互相印证。"""
    if row is None:
        return MISSING_ROW, None, None
    amount, actual = row[0], row[1]
    audit_state = audit[0] if audit else None
    if amount != item['amount']:
        return THIRD_PARTY_REWRITE, actual, audit_state
    if actual == item['new']:
        if audit_state == 'applied' and audit[1] == item['old'] and audit[2] == item['new']:
            return APPLIED_PROVEN, actual, audit_state
        if audit_state == 'rolled_back':
            return AUDIT_CONTRADICTION, actual, audit_state
        return UNTRACKED_VALUE, actual, audit_state
    if actual == item['old']:
        if audit_state == 'applied':
            return AUDIT_CONTRADICTION, actual, audit_state
        if audit_state is None or (audit_state == 'rolled_back'
                                   and audit[1] == item['old'] and audit[2] == item['new']):
            return (ROLLED_BACK if audit_state == 'rolled_back' else NOT_APPLIED), actual, audit_state
        return AUDIT_CONTRADICTION, actual, audit_state
    return THIRD_PARTY_REWRITE, actual, audit_state


def _gather(db, target, plan, digest, run_id, latest_run, worker, lease_id):
    blockers = []
    target = Path(target).resolve()
    if target.is_symlink():
        raise ValueError('unsafe_target_symlink')
    evidence = {
        'plan_digest': digest, 'run_id': run_id, 'latest_run_id': latest_run,
        'target_path': str(target), 'frozen_at': datetime.now(timezone.utc).isoformat(),
        'plan_summary': [{'subject': x['subject'], 'old': x['old'], 'new': x['new'],
                          'amount': x['amount'], 'before_hash': item_hash(x)} for x in plan],
        'rows': [], 'executions': {}, 'leases': [], 'gates': [],
        'has_capacity_windows': False, 'blockers': blockers,
    }

    # ---- 目标库证据 ----
    if not target.exists():
        blockers.append('target_missing')
        for item in plan:
            evidence['rows'].append({'subject': item['subject'], 'expected_old': item['old'],
                                     'expected_new': item['new'], 'amount': item['amount'],
                                     'actual_value': None, 'audit_state': None, 'row_state': MISSING_ROW})
    else:
        con = sqlite3.connect(target)
        try:
            has_audit = bool(con.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='backfill_audit'").fetchone())
            if not has_audit:
                blockers.append('target_audit_missing')
            for item in plan:
                row = con.execute('SELECT amount,computed FROM ledger WHERE id=?',
                                  (item['subject'],)).fetchone()
                audit = None
                if has_audit:
                    audit = con.execute(
                        'SELECT state,before_value,after_value FROM backfill_audit '
                        'WHERE subject=? AND plan_digest=?', (item['subject'], digest)).fetchone()
                state, actual, audit_state = _row_state(item, row, audit)
                evidence['rows'].append({
                    'subject': item['subject'], 'expected_old': item['old'],
                    'expected_new': item['new'], 'amount': item['amount'],
                    'actual_value': actual, 'audit_state': audit_state,
                    'audit_before': audit[1] if audit else None,
                    'audit_after': audit[2] if audit else None, 'row_state': state})
                if state == MISSING_ROW:
                    blockers.append(f'missing_row:{item["subject"]}')
                elif state == THIRD_PARTY_REWRITE:
                    blockers.append(f'third_party_rewrite:{item["subject"]}')
                elif state == AUDIT_CONTRADICTION:
                    blockers.append(f'audit_contradiction:{item["subject"]}')
                elif state == UNTRACKED_VALUE:
                    blockers.append(f'untracked_value:{item["subject"]}')
        finally:
            con.close()

    # ---- 控制库 execution 证据 ----
    apply_present = 0
    for item in plan:
        rows = db.execute('SELECT action,before_hash,after_state FROM execution WHERE subject=?',
                          (item['subject'],)).fetchall()
        evidence['executions'][item['subject']] = [dict(r) for r in rows]
        expected_hash = item_hash(item)
        for r in rows:
            if r['action'] == 'apply' and r['before_hash'] == expected_hash and r['after_state'] == item['new']:
                apply_present += 1
            elif r['action'] == 'rollback' and r['before_hash'] == expected_hash and r['after_state'] == item['old']:
                continue
            else:
                blockers.append('control_execution_conflict')
    if 0 < apply_present < len(plan):
        blockers.append('partial_execution')
    evidence['apply_execution_count'] = apply_present

    # ---- 租约证据 ----
    if _table_exists(db, 'capacity_window') and db.execute('SELECT 1 FROM capacity_window LIMIT 1').fetchone():
        evidence['has_capacity_windows'] = True
        leases = [dict(r) for r in db.execute(
            'SELECT * FROM execution_lease WHERE plan_digest=? ORDER BY id', (digest,))]
        evidence['leases'] = leases
        active = [r for r in leases if r['state'] in ('active', 'reserved')]
        if lease_id:
            mine = next((r for r in leases if r['id'] == lease_id), None)
            if mine is None:
                blockers.append('lease_identity_conflict')
            elif mine['run_id'] != run_id or mine['plan_digest'] != digest:
                blockers.append('lease_identity_conflict')
            elif mine['state'] not in ('active', 'reserved', 'released'):
                blockers.append(f'lease_not_active:{lease_id}')
            elif mine['state'] in ('active', 'reserved') and worker and mine['worker'] != worker:
                blockers.append(f'lease_foreign_worker:{lease_id}')
        else:
            if active and not worker:
                blockers.append('active_lease_required')
        for r in active:
            if worker and r['worker'] != worker:
                blockers.append(f'lease_foreign_worker:{r["id"]}')
            if not lease_id and not any(b == 'active_lease_required' for b in blockers):
                blockers.append('active_lease_required')
        if not active and not lease_id and not any(b.startswith('lease_') for b in blockers):
            blockers.append('active_lease_required')

    # ---- 发布门禁证据（已发布消费者是不可无声推翻的事实）----
    if _table_exists(db, 'publication_gate'):
        gates = [dict(r) for r in db.execute(
            'SELECT * FROM publication_gate WHERE plan_digest=? ORDER BY id', (digest,))]
        evidence['gates'] = gates
        for g in gates:
            if g['status'] == 'published':
                blockers.append(f'published_consumer:{g["id"]}')

    evidence['evidence_hash'] = _hash({k: v for k, v in evidence.items()
                                       if k not in ('evidence_hash', 'frozen_at')})
    hard = [b for b in blockers if b in HARD_BLOCKERS or b.startswith(
        ('missing_row:', 'third_party_rewrite:', 'audit_contradiction:', 'untracked_value:',
         'lease_foreign_worker:', 'lease_not_active:', 'published_consumer:'))]
    all_applied = bool(plan) and all(r['row_state'] == APPLIED_PROVEN for r in evidence['rows'])
    interrupted = all_applied and apply_present == 0
    evidence['recoverable'] = not hard and interrupted
    evidence['routes'] = {
        'forward': evidence['recoverable'],
        'rollback': evidence['recoverable'] and not any(b.startswith('published_consumer:') for b in blockers),
    }
    return evidence


def _persist(db, case_id, evidence, worker, lease_id):
    old = db.execute('SELECT status,route FROM repair_case WHERE id=?', (case_id,)).fetchone()
    if old and old['status'] in ('forwarded', 'rolled_back'):
        # 已完成的裁定不可被再次取证静默改写；重复检查仍刷新证据快照但保留结论。
        db.execute('UPDATE repair_case SET evidence_hash=?,evidence=?,lease_id=?,worker=? WHERE id=?',
                   (evidence['evidence_hash'], json.dumps(evidence, ensure_ascii=False),
                    lease_id, worker, case_id))
        return old['status']
    status = 'open' if evidence['recoverable'] else 'blocked'
    route = None
    db.execute('DELETE FROM repair_row WHERE case_id=?', (case_id,))
    db.execute('INSERT INTO repair_case(id,run_id,plan_digest,target_path,lease_id,worker,'
               'evidence_hash,evidence,status,route,blockers) VALUES (?,?,?,?,?,?,?,?,?,?,?) '
               'ON CONFLICT(id) DO UPDATE SET evidence_hash=excluded.evidence_hash,'
               'evidence=excluded.evidence,status=excluded.status,blockers=excluded.blockers,'
               'lease_id=excluded.lease_id,worker=excluded.worker',
               (case_id, evidence['run_id'], evidence['plan_digest'], evidence['target_path'],
                lease_id, worker, evidence['evidence_hash'],
                json.dumps(evidence, ensure_ascii=False), status, route,
                json.dumps(evidence['blockers'], ensure_ascii=False)))
    for r in evidence['rows']:
        db.execute('INSERT INTO repair_row VALUES (?,?,?,?,?,?,?,?)',
                   (case_id, r['subject'], r['expected_old'], r['expected_new'],
                    r['amount'], r['actual_value'], r['audit_state'], r['row_state']))
    return status


def inspect(db_path, sandbox, plan_path, case_id, worker=None, lease_id=None):
    """冻结中断证据并给出阻断原因与可选恢复路线；不修改目标库与租约。"""
    migrate(db_path)
    db = connect(db_path)
    try:
        plan, digest, latest, plan_blocker = _load_plan(db, plan_path)
        target = Path(sandbox).resolve() / 'ledger.db'
        evidence = _gather(db, target, plan, digest,
                           db.execute('SELECT run_id FROM plan WHERE digest=?', (digest,)).fetchone()[0],
                           latest, worker, lease_id)
        if plan_blocker:
            evidence['blockers'].insert(0, plan_blocker)
            evidence['recoverable'] = False
            evidence['routes'] = {'forward': False, 'rollback': False}
        with db:
            status = _persist(db, case_id, evidence, worker, lease_id)
        return {'case': case_id, 'status': status, 'recoverable': evidence['recoverable'],
                'routes': evidence['routes'], 'blockers': evidence['blockers'],
                'evidence_hash': evidence['evidence_hash']}
    finally:
        db.close()


def _lease_row(db, lease_id, digest, run_id, worker):
    if not lease_id:
        return None
    row = db.execute('SELECT * FROM execution_lease WHERE id=?', (lease_id,)).fetchone()
    if not row or row['plan_digest'] != digest or row['run_id'] != run_id:
        raise ValueError('lease_identity_conflict')
    if worker and row['worker'] != worker:
        raise ValueError('lease_foreign_worker')
    if row['state'] not in ('active', 'reserved', 'released'):
        raise ValueError(f'lease_not_active:{lease_id}')
    return row


def _assert_live_safe(live, route):
    """决定时刻重新取证：安全相关证据相对冻结点只允许本路线自身的推进。"""
    published = [b for b in live['blockers'] if b.startswith('published_consumer:')]
    hard = [b for b in live['blockers'] if not b.startswith('published_consumer:')]
    if hard:
        raise ValueError('repair_blocked:' + ','.join(hard))
    if published:
        if route == 'rollback':
            raise ValueError('rollback_forbidden_published:' + ','.join(published))
        raise ValueError('repair_blocked:' + ','.join(published))
    if route == 'forward':
        bad = [r['subject'] for r in live['rows'] if r['row_state'] != APPLIED_PROVEN]
        if bad:
            raise ValueError('repair_target_drift:' + ','.join(bad))
    else:
        allowed = (APPLIED_PROVEN, ROLLED_BACK)
        bad = [r['subject'] for r in live['rows'] if r['row_state'] not in allowed]
        if bad:
            raise ValueError('repair_target_drift:' + ','.join(bad))
        if any(g['status'] == 'published' for g in live['gates']):
            raise ValueError('rollback_forbidden_published')


def decide(db_path, sandbox, plan_path, case_id, route, worker=None, lease_id=None):
    """在冻结证据上执行前进或回退路线；同案同路线重复调用幂等。"""
    if route not in ('forward', 'rollback'):
        raise ValueError('unknown_repair_route')
    migrate(db_path)
    db = connect(db_path)
    try:
        case = db.execute('SELECT * FROM repair_case WHERE id=?', (case_id,)).fetchone()
        if not case:
            raise ValueError('unknown_repair_case')
        if case['status'] in ('forwarded', 'rolled_back'):
            if case['route'] == route:
                return {'case': case_id, 'status': case['status'], 'route': route,
                        'replayed': True, 'evidence_hash': case['evidence_hash']}
            raise ValueError('repair_case_already_decided')
        plan, digest, latest, _ = _load_plan(db, plan_path)
        if digest != case['plan_digest'] or latest != case['run_id']:
            raise ValueError('plan_changed')
        run_id = case['run_id']
        target = Path(sandbox).resolve() / 'ledger.db'
        live = _gather(db, target, plan, digest, run_id, latest, worker, lease_id)
        _assert_live_safe(live, route)
        lease = _lease_row(db, lease_id, digest, run_id, worker) if lease_id else None
        if live['has_capacity_windows'] and lease is None:
            raise ValueError('active_lease_required')

        if route == 'rollback':
            con = sqlite3.connect(target)
            try:
                with con:
                    con.execute('CREATE TABLE IF NOT EXISTS backfill_audit('
                                'subject TEXT NOT NULL, plan_digest TEXT NOT NULL, state TEXT NOT NULL, '
                                'before_value TEXT NOT NULL, after_value TEXT NOT NULL, '
                                'PRIMARY KEY(subject,plan_digest))')
                    for item in reversed(plan):
                        row = con.execute('SELECT amount,computed FROM ledger WHERE id=?',
                                          (item['subject'],)).fetchone()
                        if not row or row[0] != item['amount']:
                            raise ValueError('optimistic_conflict')
                        audit = con.execute(
                            'SELECT state,before_value,after_value FROM backfill_audit '
                            'WHERE subject=? AND plan_digest=?', (item['subject'], digest)).fetchone()
                        # 重入：该行已由上次中断的回退恢复。
                        if row[1] == item['old'] and audit and audit[0] == 'rolled_back' \
                                and audit[1] == item['old'] and audit[2] == item['new']:
                            continue
                        if row[1] != item['new'] or not audit or audit[0] != 'applied' \
                                or audit[1] != item['old'] or audit[2] != item['new']:
                            raise ValueError('rollback_evidence_conflict')
                        con.execute('UPDATE ledger SET computed=? WHERE id=?',
                                    (item['old'], item['subject']))
                        con.execute('INSERT INTO backfill_audit(subject,plan_digest,state,before_value,'
                                    'after_value) VALUES (?,?,?,?,?) ON CONFLICT(subject,plan_digest) '
                                    'DO UPDATE SET state=excluded.state',
                                    (item['subject'], digest, 'rolled_back', item['old'], item['new']))
            finally:
                con.close()
            if _fault_after_target is not None:
                _fault_after_target()

        with db:
            if route == 'forward':
                for item in plan:
                    db.execute('INSERT OR IGNORE INTO execution(run_id,subject,action,before_hash,after_state)'
                               ' VALUES (?,?,?,?,?)',
                               (run_id, item['subject'], 'apply', item_hash(item), item['new']))
            else:
                for item in plan:
                    db.execute('INSERT OR IGNORE INTO execution(run_id,subject,action,before_hash,after_state)'
                               ' VALUES (?,?,?,?,?)',
                               (run_id, item['subject'], 'rollback', item_hash(item), item['old']))
            promoted = []
            if lease is not None and lease['state'] in ('active', 'reserved'):
                promoted = _release_conn(db, lease)
            db.execute("UPDATE repair_case SET status=?,route=?,decided_at=CURRENT_TIMESTAMP WHERE id=?",
                       ('forwarded' if route == 'forward' else 'rolled_back', route, case_id))
        result = {'case': case_id, 'status': 'forwarded' if route == 'forward' else 'rolled_back',
                  'route': route, 'replayed': False, 'lease_released': lease['id'] if lease is not None else None,
                  'promoted': promoted, 'evidence_hash': case['evidence_hash']}
        return result
    finally:
        db.close()
