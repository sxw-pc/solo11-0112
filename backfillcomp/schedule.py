"""Capacity leases for planned compensation with shared execution pools.

A window keeps its own local capacity (``capacity_window.units``).  Windows
may optionally be attached to a shared physical execution pool
(``capacity_pool``).  The pool capacity is the upper bound on the sum of
``reserved``/``active`` lease units over *overlapping* windows at any instant;
windows that only touch at a boundary (``end == start``) do not overlap.

All state transitions happen inside a single SQLite ``BEGIN IMMEDIATE``
transaction, so concurrent reservations across overlapping windows are
serialized by SQLite itself and only one writer can commit capacity.
"""
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .store import SCHEMA as CORE_SCHEMA

POLICIES = ('strict_fifo', 'fit_backfill')

SCHEDULE_SCHEMA = """
CREATE TABLE IF NOT EXISTS capacity_pool(
 id TEXT PRIMARY KEY,
 units INTEGER NOT NULL CHECK(units>0),
 policy TEXT NOT NULL CHECK(policy IN ('strict_fifo','fit_backfill')),
 created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS capacity_window(
 id TEXT PRIMARY KEY, starts TEXT NOT NULL, ends TEXT NOT NULL,
 units INTEGER NOT NULL CHECK(units>0), pool_id TEXT);
CREATE TABLE IF NOT EXISTS execution_lease(
 id TEXT PRIMARY KEY, plan_digest TEXT NOT NULL, run_id INTEGER NOT NULL REFERENCES run(id),
 window_id TEXT NOT NULL REFERENCES capacity_window(id), worker TEXT NOT NULL,
 units INTEGER NOT NULL, target_hash TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('waiting','reserved','active','released','blocked')),
 wait_seq INTEGER, skipped_count INTEGER NOT NULL DEFAULT 0,
 created_at TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS capacity_audit(
 id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, event TEXT NOT NULL,
 lease_id TEXT, window_id TEXT, pool_id TEXT, lease_state TEXT, units INTEGER,
 wait_position INTEGER, skipped_count INTEGER,
 window_used INTEGER, window_capacity INTEGER,
 pool_used INTEGER, pool_capacity INTEGER,
 detail TEXT);
"""

_LEASE_COLUMNS = (
    ('wait_seq', 'INTEGER'),
    ('skipped_count', 'INTEGER NOT NULL DEFAULT 0'),
    ('created_at', 'TEXT'),
    ('updated_at', 'TEXT'),
)


def _now():
    return datetime.now(timezone.utc)


def _utc(value):
    dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if dt.tzinfo is None:
        raise ValueError('window_timezone_required')
    return dt.astimezone(timezone.utc)


def _target_hash(plan):
    return hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()


def _columns(db, table):
    return {row[1] for row in db.execute('PRAGMA table_info(%s)' % table)}


def _ensure_schema(db):
    """Create the scheduling schema and upgrade databases from the first release.

    The original ``capacity_window``/``execution_lease`` tables had no pool or
    queue columns.  ``CREATE TABLE IF NOT EXISTS`` leaves them untouched, so
    the missing columns are added explicitly and existing waiting leases are
    given a durable queue position in insertion order.
    """
    db.executescript(SCHEDULE_SCHEMA)
    window_columns = _columns(db, 'capacity_window')
    if 'pool_id' not in window_columns:
        db.execute('ALTER TABLE capacity_window ADD COLUMN pool_id TEXT')
    lease_columns = _columns(db, 'execution_lease')
    for name, ddl in _LEASE_COLUMNS:
        if name not in lease_columns:
            try:
                db.execute('ALTER TABLE execution_lease ADD COLUMN %s %s' % (name, ddl))
            except sqlite3.OperationalError as exc:
                # A concurrent opener may have added the column first.
                if 'duplicate column' not in str(exc).lower():
                    raise
    db.execute('CREATE INDEX IF NOT EXISTS execution_lease_wait '
               'ON execution_lease(window_id,state,wait_seq)')
    db.execute('CREATE INDEX IF NOT EXISTS capacity_audit_scope '
               'ON capacity_audit(pool_id,lease_id)')
    seq = db.execute('SELECT COALESCE(max(wait_seq),0) FROM execution_lease').fetchone()[0]
    stamp = _now().isoformat()
    for row in db.execute('SELECT rowid FROM execution_lease WHERE wait_seq IS NULL ORDER BY rowid').fetchall():
        seq += 1
        db.execute('UPDATE execution_lease SET wait_seq=?, created_at=COALESCE(created_at,?) '
                   'WHERE rowid=?', (seq, stamp, row[0]))
    db.commit()


def _connect(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA busy_timeout=30000')
    db.execute('PRAGMA foreign_keys=ON')
    db.executescript(CORE_SCHEMA)
    _ensure_schema(db)
    return db


# --------------------------------------------------------------------------
# capacity arithmetic
# --------------------------------------------------------------------------

def _window_used(db, window_id, exclude_lease=None):
    sql = ("SELECT COALESCE(sum(units),0) FROM execution_lease "
           "WHERE window_id=? AND state IN ('reserved','active')")
    params = [window_id]
    if exclude_lease is not None:
        sql += ' AND id<>?'
        params.append(exclude_lease)
    return db.execute(sql, params).fetchone()[0]


def _max_used(entries):
    """Maximum simultaneous usage of half-open interval entries.

    Each entry is ``(start, end, units)``.  Usage only changes at an
    interval boundary, so evaluating every boundary is exact.
    """
    points = set()
    for start, end, _ in entries:
        points.add(start)
        points.add(end)
    if not points:
        return 0
    return max(sum(units for start, end, units in entries if start <= point < end)
               for point in points)


def _pool_used(db, pool_id, exclude_lease=None, extra_window_id=None, extra_units=0):
    windows = db.execute('SELECT id,starts,ends FROM capacity_window WHERE pool_id=?',
                         (pool_id,)).fetchall()
    entries = []
    for window in windows:
        used = _window_used(db, window['id'], exclude_lease)
        if window['id'] == extra_window_id:
            used += extra_units
        entries.append((_utc(window['starts']), _utc(window['ends']), used))
    return _max_used(entries)


def _pool(db, pool_id):
    return db.execute('SELECT * FROM capacity_pool WHERE id=?', (pool_id,)).fetchone()


def _window(db, window_id):
    return db.execute('SELECT * FROM capacity_window WHERE id=?', (window_id,)).fetchone()


def _fits(db, lease, window=None):
    """Whether a waiting lease may be promoted without breaking either bound."""
    window = window or _window(db, lease['window_id'])
    if window is None:
        return False
    if _window_used(db, window['id']) + lease['units'] > window['units']:
        return False
    if window['pool_id']:
        pool = _pool(db, window['pool_id'])
        if pool is None:
            return False
        if _pool_used(db, pool['id'], extra_window_id=window['id'],
                      extra_units=lease['units']) > pool['units']:
            return False
    return True


def _held_valid(db, lease):
    """Check the capacity decision an already held lease is bound to."""
    window = _window(db, lease['window_id'])
    if window is None:
        return False
    now = _now()
    if now < _utc(window['starts']) or now >= _utc(window['ends']):
        return False
    if _window_used(db, window['id']) > window['units']:
        return False
    if window['pool_id']:
        pool = _pool(db, window['pool_id'])
        if pool is None or _pool_used(db, pool['id']) > pool['units']:
            return False
    return True


# --------------------------------------------------------------------------
# audit
# --------------------------------------------------------------------------

def _wait_position(db, lease):
    window = _window(db, lease['window_id'])
    if window is None:
        return None
    if window['pool_id']:
        rows = db.execute(
            "SELECT id FROM execution_lease WHERE state='waiting' AND window_id IN "
            "(SELECT id FROM capacity_window WHERE pool_id=?) ORDER BY wait_seq,id",
            (window['pool_id'],)).fetchall()
    else:
        rows = db.execute(
            "SELECT id FROM execution_lease WHERE state='waiting' AND window_id=? "
            "ORDER BY wait_seq,id", (window['id'],)).fetchall()
    for position, row in enumerate(rows, 1):
        if row['id'] == lease['id']:
            return position
    return None


def _record(db, event, lease_id=None, window_id=None, pool_id=None, detail=None):
    """Append a queryable execution-audit row with the occupancy it observed."""
    lease = db.execute('SELECT * FROM execution_lease WHERE id=?', (lease_id,)).fetchone() \
        if lease_id is not None else None
    state = units = skipped = None
    if lease is not None:
        window_id = lease['window_id'] or window_id
        state, units, skipped = lease['state'], lease['units'], lease['skipped_count']
    window = _window(db, window_id) if window_id else None
    if pool_id is None and window is not None:
        pool_id = window['pool_id']
    window_used = _window_used(db, window['id']) if window is not None else None
    window_capacity = window['units'] if window is not None else None
    pool_used = pool_capacity = None
    if pool_id:
        pool = _pool(db, pool_id)
        pool_used = _pool_used(db, pool_id)
        pool_capacity = pool['units'] if pool is not None else None
    wait_position = _wait_position(db, lease) if lease is not None and state == 'waiting' else None
    db.execute(
        'INSERT INTO capacity_audit(at,event,lease_id,window_id,pool_id,lease_state,units,'
        'wait_position,skipped_count,window_used,window_capacity,pool_used,pool_capacity,detail) '
        'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (_now().isoformat(), event, lease_id, window_id, pool_id, state, units, wait_position,
         skipped, window_used, window_capacity, pool_used, pool_capacity,
         json.dumps(detail or {}, ensure_ascii=False, default=str)))


# --------------------------------------------------------------------------
# plan binding
# --------------------------------------------------------------------------

def _plan_valid(db, lease_or_digest, run_id=None, target_hash=None):
    if hasattr(lease_or_digest, 'keys'):
        digest = lease_or_digest['plan_digest']
        run_id = lease_or_digest['run_id']
        target_hash = lease_or_digest['target_hash']
    else:
        digest = lease_or_digest
    plan = db.execute('SELECT * FROM plan WHERE digest=?', (digest,)).fetchone()
    latest = db.execute('SELECT max(id) FROM run').fetchone()[0]
    if not plan or latest is None or plan['run_id'] != latest or run_id != latest:
        return False
    try:
        body = json.loads(plan['body'])
    except (TypeError, ValueError):
        return False
    return _target_hash(body) == target_hash


# --------------------------------------------------------------------------
# queue promotion
# --------------------------------------------------------------------------

def _mark_skipped(db, pending, policy):
    """Record that ``pending`` leases were overtaken by a later promotion."""
    for candidate in pending:
        db.execute('UPDATE execution_lease SET skipped_count=skipped_count+1,updated_at=? '
                   'WHERE id=?', (_now().isoformat(), candidate['id']))
        _record(db, 'skip', lease_id=candidate['id'],
                detail={'reason': 'head_does_not_fit', 'policy': policy})


def _promote_scope(db, policy, waiting):
    """Promote ``waiting`` in durable queue order under ``policy``."""
    promoted = []
    pending = []
    now = _now()
    for candidate in waiting:
        if not _plan_valid(db, candidate):
            db.execute("UPDATE execution_lease SET state='blocked',updated_at=? WHERE id=?",
                       (_now().isoformat(), candidate['id']))
            _record(db, 'blocked_stale_plan', lease_id=candidate['id'])
            # A stale lease is no longer a capacity contender; FIFO order of
            # the remaining live leases is preserved.
            continue
        candidate_window = _window(db, candidate['window_id'])
        if candidate_window is None or not (_utc(candidate_window['starts']) <= now <
                                            _utc(candidate_window['ends'])):
            # Never promote work into a window that has already closed.
            db.execute("UPDATE execution_lease SET state='blocked',updated_at=? WHERE id=?",
                       (_now().isoformat(), candidate['id']))
            _record(db, 'blocked_window_expired', lease_id=candidate['id'])
            continue
        if candidate['units'] > candidate_window['units']:
            # The window shrank while this lease was queued; it can never fit.
            db.execute("UPDATE execution_lease SET state='blocked',updated_at=? WHERE id=?",
                       (_now().isoformat(), candidate['id']))
            _record(db, 'blocked_capacity', lease_id=candidate['id'],
                    detail={'reason': 'lease_exceeds_window'})
            continue
        if _fits(db, candidate, candidate_window):
            db.execute("UPDATE execution_lease SET state='reserved',updated_at=? WHERE id=?",
                       (_now().isoformat(), candidate['id']))
            promoted.append(candidate['id'])
            _record(db, 'promote', lease_id=candidate['id'])
            if pending:
                _mark_skipped(db, pending, policy)
                pending = []
        else:
            if policy == 'strict_fifo':
                # The head cannot fit: every lease behind it keeps waiting.
                _record(db, 'wait_no_fit', lease_id=candidate['id'],
                        detail={'reason': 'head_does_not_fit', 'policy': policy})
                break
            pending.append(candidate)
    for candidate in pending:
        _record(db, 'wait_no_fit', lease_id=candidate['id'],
                detail={'reason': 'not_yet_overtaken', 'policy': policy})
    return promoted


def _promote(db, window_id):
    """Promote waiting leases in the scope a window belongs to.

    ``strict_fifo`` stops at the first lease that does not fit, leaving it and
    everything behind it waiting.  ``fit_backfill`` may pass that lease and
    admit smaller ones, but a lease only counts as "skipped" once something
    behind it is actually promoted; its ``wait_seq`` is never changed, so it
    keeps priority for the next capacity that becomes available.
    """
    window = _window(db, window_id)
    if window is None:
        return []
    if window['pool_id']:
        return _promote_pool(db, window['pool_id'])
    waiting = db.execute(
        "SELECT * FROM execution_lease WHERE state='waiting' AND window_id=? "
        "ORDER BY wait_seq,id", (window_id,)).fetchall()
    return _promote_scope(db, 'strict_fifo', waiting)


def _promote_pool(db, pool_id):
    pool = _pool(db, pool_id)
    if pool is None:
        return []
    waiting = db.execute(
        "SELECT * FROM execution_lease WHERE state='waiting' AND window_id IN "
        "(SELECT id FROM capacity_window WHERE pool_id=?) ORDER BY wait_seq,id",
        (pool_id,)).fetchall()
    return _promote_scope(db, pool['policy'], waiting)


# --------------------------------------------------------------------------
# public API
# --------------------------------------------------------------------------

def define_window(db_path, window_id, starts, ends, units,
                  pool_id=None, pool_units=None, promotion_policy=None):
    """Create or update a capacity window.

    Windows without ``pool_id`` keep the original per-window-only semantics.
    A pool's capacity and promotion policy are fixed when the pool is created;
    later declarations must match.  Updates are rejected if any already held
    lease would exceed either bound afterwards.
    """
    if not window_id:
        raise ValueError('invalid_capacity_window')
    start, end = _utc(starts), _utc(ends)
    if start >= end or units <= 0:
        raise ValueError('invalid_capacity_window')
    if pool_id is not None and not str(pool_id):
        raise ValueError('invalid_pool_id')
    if pool_id is None and (pool_units is not None or promotion_policy is not None):
        raise ValueError('pool_required')
    if pool_units is not None and pool_units <= 0:
        raise ValueError('invalid_pool_capacity')
    if promotion_policy is not None and promotion_policy not in POLICIES:
        raise ValueError('invalid_promotion_policy')

    db = _connect(db_path)
    db.execute('BEGIN IMMEDIATE')
    try:
        old = _window(db, window_id)
        target_pool = pool_id if pool_id is not None else (old['pool_id'] if old else None)
        pool_row = None
        if target_pool is not None:
            pool_row = _pool(db, target_pool)
            if pool_row is not None:
                if pool_units is not None and pool_units != pool_row['units']:
                    raise ValueError('pool_capacity_conflict')
                if promotion_policy is not None and promotion_policy != pool_row['policy']:
                    raise ValueError('pool_policy_conflict')
            else:
                if pool_units is None:
                    raise ValueError('pool_capacity_required')
                policy = promotion_policy or 'strict_fifo'
                db.execute('INSERT INTO capacity_pool(id,units,policy,created_at) VALUES (?,?,?,?)',
                           (target_pool, pool_units, policy, _now().isoformat()))
                pool_row = _pool(db, target_pool)

        stamp = _now().isoformat()
        if old is None:
            db.execute('INSERT INTO capacity_window(id,starts,ends,units,pool_id) '
                       'VALUES (?,?,?,?,?)',
                       (window_id, start.isoformat(), end.isoformat(), units, target_pool))
        else:
            db.execute('UPDATE capacity_window SET starts=?,ends=?,units=?,pool_id=? WHERE id=?',
                       (start.isoformat(), end.isoformat(), units, target_pool, window_id))

        # Any update must leave leases that already hold capacity legal: both
        # occupancy bounds must hold and their window must still be open now.
        if _window_used(db, window_id) > units:
            raise ValueError('window_update_invalidates_leases')
        held = db.execute(
            "SELECT 1 FROM execution_lease WHERE window_id=? AND state IN "
            "('reserved','active') LIMIT 1", (window_id,)).fetchone()
        now = _now()
        if held and (now < start or now >= end):
            raise ValueError('window_update_invalidates_leases')
        if target_pool is not None and _pool_used(db, target_pool) > pool_row['units']:
            raise ValueError('pool_update_invalidates_leases')

        _record(db, 'window_created' if old is None else 'window_updated',
                window_id=window_id, pool_id=target_pool,
                detail={'starts': start.isoformat(), 'ends': end.isoformat(),
                        'units': units, 'pool_units': pool_row['units'] if pool_row else None,
                        'policy': pool_row['policy'] if pool_row else None,
                        'at': stamp})
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return window_id


def reserve(db_path, lease_id, plan_digest, window_id, worker, units):
    """Reserve capacity or join the durable waiting queue.

    The whole read-check-write sequence runs under ``BEGIN IMMEDIATE`` so two
    overlapping windows can never both observe free pool capacity and commit.
    """
    if not lease_id or not worker or units <= 0:
        raise ValueError('invalid_lease')
    db = _connect(db_path)
    db.execute('BEGIN IMMEDIATE')
    try:
        window = _window(db, window_id)
        if not window:
            raise ValueError('unknown_window')
        old = db.execute('SELECT * FROM execution_lease WHERE id=?', (lease_id,)).fetchone()
        if old:
            if (old['plan_digest'], old['window_id'], old['worker'], old['units']) != \
                    (plan_digest, window_id, worker, units):
                raise ValueError('lease_identity_conflict')
            state = old['state']
            if state in ('blocked', 'released'):
                # A blocked/released lease is never revived, and never moved to
                # another window to regain capacity.
                _record(db, 'reserve_noop', lease_id=lease_id, detail={'state': state})
            elif not _plan_valid(db, old):
                db.execute("UPDATE execution_lease SET state='blocked',updated_at=? WHERE id=?",
                           (_now().isoformat(), lease_id))
                _record(db, 'blocked_stale_plan', lease_id=lease_id)
                _promote(db, window_id)
                state = 'blocked'
            # A replay of an existing request is otherwise a pure read: it must
            # not reorder the queue or promote anything on its own.
        else:
            plan = db.execute('SELECT * FROM plan WHERE digest=?', (plan_digest,)).fetchone()
            latest = db.execute('SELECT max(id) FROM run').fetchone()[0]
            if not plan or plan['run_id'] != latest:
                raise ValueError('unknown_or_stale_plan')
            now = _now()
            if now < _utc(window['starts']) or now >= _utc(window['ends']):
                raise ValueError('window_not_active')
            if units > window['units']:
                raise ValueError('lease_exceeds_window')
            target_hash = _target_hash(json.loads(plan['body']))
            stamp = now.isoformat()
            seq = db.execute('SELECT COALESCE(max(wait_seq),0)+1 FROM execution_lease').fetchone()[0]
            db.execute(
                'INSERT INTO execution_lease(id,plan_digest,run_id,window_id,worker,units,'
                'target_hash,state,wait_seq,skipped_count,created_at,updated_at) '
                "VALUES (?,?,?,?,?,?,?,'waiting',?,0,?,?)",
                (lease_id, plan_digest, latest, window_id, worker, units,
                 target_hash, seq, stamp, stamp))
            _record(db, 'enqueue', lease_id=lease_id)
            _promote(db, window_id)
            state = db.execute('SELECT state FROM execution_lease WHERE id=?',
                               (lease_id,)).fetchone()[0]

        db.commit()
        lease = db.execute('SELECT * FROM execution_lease WHERE id=?', (lease_id,)).fetchone()
        return {'lease': lease_id, 'state': state, 'window': window_id,
                'pool': window['pool_id'],
                'wait_position': _wait_position(db, lease) if lease else None,
                'skipped_count': lease['skipped_count'] if lease else 0}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def activate(db_path, lease_id):
    """Move a reserved lease to active after re-checking plan and capacity.

    Activating an already-active lease repeats the same checks, so a plan that
    went stale after activation still blocks the lease instead of silently
    keeping it live.
    """
    db = _connect(db_path)
    db.execute('BEGIN IMMEDIATE')
    try:
        row = db.execute('SELECT * FROM execution_lease WHERE id=?', (lease_id,)).fetchone()
        if not row:
            raise ValueError('unknown_lease')
        if row['state'] not in ('reserved', 'active'):
            raise ValueError('lease_not_reserved')
        stamp = _now().isoformat()
        if not _plan_valid(db, row):
            db.execute("UPDATE execution_lease SET state='blocked',updated_at=? WHERE id=?",
                       (stamp, lease_id))
            _record(db, 'blocked_stale_plan', lease_id=lease_id)
            _promote(db, row['window_id'])
            db.commit()
            return {'lease': lease_id, 'state': 'blocked'}
        if not _held_valid(db, row):
            db.execute("UPDATE execution_lease SET state='blocked',updated_at=? WHERE id=?",
                       (stamp, lease_id))
            _record(db, 'blocked_capacity', lease_id=lease_id)
            _promote(db, row['window_id'])
            db.commit()
            return {'lease': lease_id, 'state': 'blocked'}
        if row['state'] == 'reserved':
            db.execute("UPDATE execution_lease SET state='active',updated_at=? WHERE id=?",
                       (stamp, lease_id))
            _record(db, 'activate', lease_id=lease_id)
        db.commit()
        return {'lease': lease_id, 'state': 'active'}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def release(db_path, lease_id):
    """Release a held lease and promote waiting work under the pool policy."""
    db = _connect(db_path)
    db.execute('BEGIN IMMEDIATE')
    try:
        row = db.execute('SELECT * FROM execution_lease WHERE id=?', (lease_id,)).fetchone()
        if not row:
            raise ValueError('unknown_lease')
        if row['state'] == 'released':
            # Replaying a release must not free capacity twice or reorder the
            # queue, so it is a pure no-op.
            db.commit()
            return {'lease': lease_id, 'state': 'released', 'promoted': []}
        if row['state'] not in ('active', 'reserved'):
            raise ValueError('lease_not_releasable')
        db.execute("UPDATE execution_lease SET state='released',updated_at=? WHERE id=?",
                   (_now().isoformat(), lease_id))
        promoted = _promote(db, row['window_id'])
        _record(db, 'release', lease_id=lease_id, detail={'promoted': promoted})
        db.commit()
        return {'lease': lease_id, 'state': 'released', 'promoted': promoted}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def check_active_lease(db_path, lease_id, plan_digest):
    """Gate used by ``rules.action('apply')``.

    Verifies that the lease is active for the current plan *and* that the
    window/pool capacity decision it was admitted under still holds.
    """
    db = _connect(db_path)
    db.execute('BEGIN IMMEDIATE')
    try:
        row = db.execute('SELECT * FROM execution_lease WHERE id=?', (lease_id,)).fetchone()
        if not row or row['state'] != 'active' or row['plan_digest'] != plan_digest:
            raise ValueError('active_lease_required')
        if not _plan_valid(db, row):
            db.execute("UPDATE execution_lease SET state='blocked',updated_at=? WHERE id=?",
                       (_now().isoformat(), lease_id))
            _record(db, 'blocked_stale_plan', lease_id=lease_id)
            _promote(db, row['window_id'])
            db.commit()
            raise ValueError('stale_plan_blocked')
        if not _held_valid(db, row):
            db.execute("UPDATE execution_lease SET state='blocked',updated_at=? WHERE id=?",
                       (_now().isoformat(), lease_id))
            _record(db, 'blocked_capacity', lease_id=lease_id)
            _promote(db, row['window_id'])
            db.commit()
            raise ValueError('lease_capacity_invalid')
        db.commit()
        return True
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# --------------------------------------------------------------------------
# introspection and recovery
# --------------------------------------------------------------------------

def _scope_rows(db, window):
    if window['pool_id']:
        return db.execute(
            "SELECT * FROM execution_lease WHERE window_id IN "
            "(SELECT id FROM capacity_window WHERE pool_id=?)",
            (window['pool_id'],)).fetchall()
    return db.execute('SELECT * FROM execution_lease WHERE window_id=?',
                      (window['id'],)).fetchall()


def capacity_status(db_path, pool_id=None):
    """Queryable view of pool occupancy and durable waiting positions."""
    db = _connect(db_path)
    try:
        pools = []
        pool_rows = (db.execute('SELECT * FROM capacity_pool WHERE id=?', (pool_id,)).fetchall()
                     if pool_id else
                     db.execute('SELECT * FROM capacity_pool ORDER BY id').fetchall())
        for pool in pool_rows:
            leases = []
            windows = db.execute('SELECT * FROM capacity_window WHERE pool_id=? ORDER BY id',
                                 (pool['id'],)).fetchall()
            waiting = db.execute(
                "SELECT * FROM execution_lease WHERE state='waiting' AND window_id IN "
                "(SELECT id FROM capacity_window WHERE pool_id=?) ORDER BY wait_seq,id",
                (pool['id'],)).fetchall()
            seen = set()
            for window in windows:
                for lease in _scope_rows(db, window):
                    if lease['id'] in seen:
                        continue
                    seen.add(lease['id'])
                    leases.append({'id': lease['id'], 'window': lease['window_id'],
                                   'worker': lease['worker'], 'units': lease['units'],
                                   'state': lease['state'],
                                   'wait_position': _wait_position(db, lease)
                                   if lease['state'] == 'waiting' else None,
                                   'skipped_count': lease['skipped_count']})
            leases.sort(key=lambda item: (item['wait_position'] is None,
                                          item['wait_position'] or 0, item['id']))
            pools.append({'id': pool['id'], 'units': pool['units'],
                          'policy': pool['policy'],
                          'used': _pool_used(db, pool['id']),
                          'waiting': [x['id'] for x in waiting],
                          'leases': leases})
        windows = []
        for window in db.execute('SELECT * FROM capacity_window ORDER BY id'):
            if pool_id and window['pool_id'] != pool_id:
                continue
            windows.append({'id': window['id'], 'starts': window['starts'],
                            'ends': window['ends'], 'units': window['units'],
                            'pool': window['pool_id'],
                            'used': _window_used(db, window['id']),
                            'leases': [
                                {'id': lease['id'], 'state': lease['state'],
                                 'units': lease['units'],
                                 'wait_position': _wait_position(db, lease)
                                 if lease['state'] == 'waiting' else None,
                                 'skipped_count': lease['skipped_count']}
                                for lease in db.execute(
                                    'SELECT * FROM execution_lease WHERE window_id=? '
                                    'ORDER BY wait_seq,id', (window['id'],))]})
        return {'pools': pools, 'windows': windows}
    finally:
        db.close()


def capacity_audit(db_path, pool_id=None, lease_id=None, limit=None):
    """Return durable execution-audit rows, newest last."""
    db = _connect(db_path)
    try:
        sql = 'SELECT * FROM capacity_audit'
        clauses, params = [], []
        if pool_id:
            clauses.append('pool_id=?')
            params.append(pool_id)
        if lease_id:
            clauses.append('lease_id=?')
            params.append(lease_id)
        if clauses:
            sql += ' WHERE ' + ' AND '.join(clauses)
        sql += ' ORDER BY id'
        if limit:
            sql += ' LIMIT ?'
            params.append(int(limit))
        return [dict(row) for row in db.execute(sql, params).fetchall()]
    finally:
        db.close()


def check_invariants(db_path):
    """Return capacity invariant violations, if any."""
    db = _connect(db_path)
    try:
        problems = []
        for window in db.execute('SELECT * FROM capacity_window ORDER BY id'):
            used = _window_used(db, window['id'])
            if used > window['units']:
                problems.append({'kind': 'window_over_capacity', 'window': window['id'],
                                 'used': used, 'capacity': window['units']})
        for pool in db.execute('SELECT * FROM capacity_pool ORDER BY id'):
            used = _pool_used(db, pool['id'])
            if used > pool['units']:
                problems.append({'kind': 'pool_over_capacity', 'pool': pool['id'],
                                 'used': used, 'capacity': pool['units']})
        for lease in db.execute("SELECT * FROM execution_lease WHERE state IN ('reserved','active')"):
            if db.execute('SELECT 1 FROM capacity_window WHERE id=?',
                          (lease['window_id'],)).fetchone() is None:
                problems.append({'kind': 'lease_orphan_window', 'lease': lease['id']})
        # A lease occupies exactly one window by construction (``window_id`` is
        # a single column and ``id`` is the primary key), so no lease can be
        # counted in two windows at once.  Closed windows holding capacity are
        # still worth flagging: their interval is unusable.
        for lease in db.execute("SELECT * FROM execution_lease WHERE state IN ('reserved','active')"):
            window = db.execute('SELECT starts,ends FROM capacity_window WHERE id=?',
                                (lease['window_id'],)).fetchone()
            if window is None:
                continue
            if _utc(window['starts']) >= _utc(window['ends']):
                problems.append({'kind': 'lease_invalid_window', 'lease': lease['id']})
        return problems
    finally:
        db.close()


def _pool_overloaded_point(db, pool_id):
    """Largest simultaneous usage point for a pool, or None when within bound."""
    pool = _pool(db, pool_id)
    windows = db.execute('SELECT id,starts,ends FROM capacity_window WHERE pool_id=?',
                         (pool_id,)).fetchall()
    entries = [(_utc(w['starts']), _utc(w['ends']), _window_used(db, w['id']))
               for w in windows]
    points = {p for start, end, _ in entries for p in (start, end)}
    if not points:
        return None
    point = max(points, key=lambda p: sum(u for s, e, u in entries if s <= p < e))
    used = sum(u for s, e, u in entries if s <= point < e)
    if pool is not None and used > pool['units']:
        return point, used
    return None


def _overload(db):
    """Return a description of the first broken bound, or None."""
    for row in db.execute('SELECT * FROM capacity_window ORDER BY id'):
        if _window_used(db, row['id']) > row['units']:
            return ('window', row)
    for pool in db.execute('SELECT * FROM capacity_pool ORDER BY id'):
        if _pool_overloaded_point(db, pool['id']):
            return ('pool', pool)
    return None


def recover(db_path):
    """Defensive reconciliation after an interrupted process.

    Every mutation is already a single SQLite transaction, so a clean database
    is left untouched.  If external corruption or a partial upgrade did leave
    an over-committed state, the newest leases are demoted back to ``waiting``
    in their original queue positions until both bounds hold again.  Recovery
    never moves a lease to another window and never auto-promotes.
    """
    db = _connect(db_path)
    db.execute('BEGIN IMMEDIATE')
    demoted = []
    try:
        for _ in range(10000):
            offending = _overload(db)
            if offending is None:
                break
            if offending[0] == 'window':
                rows = db.execute(
                    "SELECT * FROM execution_lease WHERE window_id=? AND state IN "
                    "('reserved','active') "
                    "ORDER BY (state='active') ASC,wait_seq DESC,id DESC",
                    (offending[1]['id'],)).fetchall()
            else:
                point, _ = _pool_overloaded_point(db, offending[1]['id'])
                rows = db.execute(
                    "SELECT l.* FROM execution_lease l "
                    "JOIN capacity_window w ON w.id=l.window_id "
                    "WHERE w.pool_id=? AND l.state IN ('reserved','active') "
                    "ORDER BY (l.state='active') ASC,l.wait_seq DESC,l.id DESC",
                    (offending[1]['id'],)).fetchall()
                rows = [r for r in rows
                        if _utc(_window(db, r['window_id'])['starts']) <= point <
                        _utc(_window(db, r['window_id'])['ends'])]
            if not rows:
                break
            # Demotion only lowers occupancy, so the newest lease that was
            # actually contributing to the overloaded point is the victim.
            victim = rows[0]
            db.execute("UPDATE execution_lease SET state='waiting',updated_at=? WHERE id=?",
                       (_now().isoformat(), victim['id']))
            demoted.append(victim['id'])
            _record(db, 'recover_demote', lease_id=victim['id'],
                    detail={'kind': offending[0], 'scope': offending[1]['id']})
        _record(db, 'recover', detail={'demoted': demoted})
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return {'demoted': demoted, 'problems': check_invariants(db_path)}
