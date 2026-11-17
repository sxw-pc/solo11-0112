"""Regression tests for shared execution-pool capacity arbitration."""
import hashlib
import json
import multiprocessing
import os
import sqlite3
import threading
from pathlib import Path

import pytest

from backfillcomp.ingest import initialize, load
from backfillcomp.rules import evaluate, action
from backfillcomp.store import connect
from backfillcomp.schedule import (
    define_window, reserve, activate, release, capacity_status, capacity_audit,
    check_invariants, check_active_lease, recover,
)

FIX = Path(__file__).resolve().parent.parent / 'fixtures'
FAR_START = '2020-01-01T00:00:00Z'
FAR_END = '2099-01-01T00:00:00Z'


def ready(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    plan = tmp_path / 'plan.json'
    action('plan', db, out=plan)
    digest = hashlib.sha256(plan.read_bytes()).hexdigest()
    return db, plan, digest


def define_pool(db, pool_units=3, policy='strict_fifo', w1_units=3, w2_units=3):
    define_window(db, 'W1', FAR_START, FAR_END, w1_units,
                  pool_id='POOL', pool_units=pool_units, promotion_policy=policy)
    define_window(db, 'W2', '2020-06-01T00:00:00Z', '2099-06-01T00:00:00Z', w2_units,
                  pool_id='POOL')


def state(db, lease_id):
    row = connect(db).execute('SELECT state FROM execution_lease WHERE id=?',
                              (lease_id,)).fetchone()
    return row[0] if row else None


def seed_reserved(db, lease_id, window_id, units, seq):
    """Insert a held lease directly, for windows outside the current clock."""
    plan = connect(db).execute('SELECT body FROM plan LIMIT 1').fetchone()[0]
    target_hash = hashlib.sha256(
        json.dumps(json.loads(plan), sort_keys=True).encode()).hexdigest()
    con = connect(db)
    con.execute(
        'INSERT INTO execution_lease(id,plan_digest,run_id,window_id,worker,units,'
        'target_hash,state,wait_seq,skipped_count) '
        "SELECT ?,digest,run_id,?,?,?,?,'reserved',?,0 FROM plan LIMIT 1",
        (lease_id, window_id, 'seeded', units, target_hash, seq))
    con.commit()
    con.close()


# --------------------------------------------------------------------------
# overlapping windows share capacity
# --------------------------------------------------------------------------

def test_overlapping_windows_cannot_double_reserve_same_pool_capacity(tmp_path):
    db, plan, digest = ready(tmp_path)
    define_window(db, 'W1', FAR_START, FAR_END, 3, pool_id='POOL', pool_units=3)
    define_window(db, 'W2', '2020-06-01T00:00:00Z', '2099-06-01T00:00:00Z', 3, pool_id='POOL')

    assert reserve(db, 'A', digest, 'W1', 'worker-A', 2)['state'] == 'reserved'
    # W2 has 3 local units free, but the shared pool only has one left.
    result = reserve(db, 'B', digest, 'W2', 'worker-B', 2)
    assert result['state'] == 'waiting'
    assert state(db, 'B') == 'waiting'
    status = capacity_status(db, 'POOL')
    assert status['pools'][0]['used'] == 2
    assert status['pools'][0]['waiting'] == ['B']

    # Both cannot become active after each other either.
    assert activate(db, 'A')['state'] == 'active'
    with pytest.raises(ValueError, match='lease_not_reserved'):
        activate(db, 'B')
    with pytest.raises(ValueError, match='active_lease_required'):
        action('apply', db, sandbox=tmp_path / 'target', out=plan)
    # Applying with the active lease also releases it; the queued lease from the
    # other window is promoted into the freed pool capacity.
    action('apply', db, sandbox=tmp_path / 'target', out=plan, lease='A')
    assert state(db, 'A') == 'released'
    assert state(db, 'B') == 'reserved'
    assert capacity_status(db, 'POOL')['pools'][0]['used'] == 2
    assert check_invariants(db) == []


def test_recovery_demotes_until_occupied_point_fits(tmp_path):
    db, _, digest = ready(tmp_path)
    # Overlap layout where a naive "demote one lease" is not enough:
    # W1 [0, 7] holds 4 units and W2 [3, 9] holds 3; pool capacity is 5.
    define_pool(db, pool_units=5)
    con = connect(db)
    con.execute("UPDATE capacity_window SET starts=?,ends=? WHERE id='W1'",
                ('2020-01-01T00:00:00+00:00', '2099-01-01T00:00:00+00:00'))
    con.commit()
    con.close()
    seed_reserved(db, 'big1', 'W1', 3, 1)
    seed_reserved(db, 'big2', 'W2', 3, 2)
    seed_reserved(db, 'fresh', 'W2', 2, 3)
    assert check_invariants(db) != []
    result = recover(db)
    assert result['problems'] == []
    # The newest contributors are demoted first; the oldest keeps its way in.
    assert result['demoted'][0] == 'fresh'
    assert state(db, 'big1') == 'reserved'
    assert capacity_status(db, 'POOL')['pools'][0]['used'] <= 5


def test_recovery_leaves_a_clean_database_untouched(tmp_path):
    db, _, digest = ready(tmp_path)
    define_pool(db, pool_units=3)
    reserve(db, 'A', digest, 'W1', 'w', 2)
    before = capacity_status(db, 'POOL')['pools'][0]
    result = recover(db)
    assert result['demoted'] == [] and result['problems'] == []
    assert capacity_status(db, 'POOL')['pools'][0] == before
    assert state(db, 'A') == 'reserved'


def test_boundary_touching_windows_do_not_share_pool(tmp_path):
    db, _, digest = ready(tmp_path)
    define_window(db, 'W1', '2020-01-01T00:00:00Z', '2035-01-01T00:00:00Z', 2,
                  pool_id='POOL', pool_units=2)
    # Starts exactly when W1 ends: half-open intervals never overlap.
    define_window(db, 'W2', '2035-01-01T00:00:00Z', '2040-01-01T00:00:00Z', 2,
                  pool_id='POOL')
    assert reserve(db, 'A', digest, 'W1', 'worker-A', 2)['state'] == 'reserved'
    # W2 is not active yet, so seed its held lease to exercise the interval
    # arithmetic rather than the clock.
    seed_reserved(db, 'H2', 'W2', 2, 2)
    # Max simultaneous usage over both windows is 2, not the sum 4.
    assert capacity_status(db, 'POOL')['pools'][0]['used'] == 2
    assert check_invariants(db) == []


def test_window_units_remains_a_local_bound(tmp_path):
    db, _, digest = ready(tmp_path)
    define_window(db, 'W1', FAR_START, FAR_END, 1, pool_id='POOL', pool_units=5)
    with pytest.raises(ValueError, match='lease_exceeds_window'):
        reserve(db, 'B', digest, 'W1', 'worker-B', 2)
    assert reserve(db, 'A', digest, 'W1', 'worker-A', 1)['state'] == 'reserved'


def test_pool_capacity_and_policy_are_fixed_at_creation(tmp_path):
    db, _, _ = ready(tmp_path)
    define_window(db, 'W1', FAR_START, FAR_END, 3, pool_id='POOL', pool_units=3,
                  promotion_policy='fit_backfill')
    with pytest.raises(ValueError, match='pool_capacity_conflict'):
        define_window(db, 'W2', FAR_START, FAR_END, 3, pool_id='POOL', pool_units=4)
    with pytest.raises(ValueError, match='pool_policy_conflict'):
        define_window(db, 'W2', FAR_START, FAR_END, 3, pool_id='POOL', pool_units=3,
                      promotion_policy='strict_fifo')
    with pytest.raises(ValueError, match='invalid_capacity_window'):
        define_window(db, 'W3', FAR_END, FAR_START, 1)
    with pytest.raises(ValueError, match='window_timezone_required'):
        define_window(db, 'W3', '2020-01-01T00:00:00', FAR_END, 1)
    with pytest.raises(ValueError, match='pool_capacity_required'):
        define_window(db, 'W3', FAR_START, FAR_END, 1, pool_id='OTHER')
    with pytest.raises(ValueError, match='pool_required'):
        define_window(db, 'W3', FAR_START, FAR_END, 1, pool_units=2)
    # A later window in the same pool only needs to name it.
    define_window(db, 'W2', FAR_START, FAR_END, 3, pool_id='POOL')
    assert capacity_status(db, 'POOL')['pools'][0]['policy'] == 'fit_backfill'


def test_window_update_cannot_invalidate_held_leases(tmp_path):
    db, _, digest = ready(tmp_path)
    define_window(db, 'W1', FAR_START, FAR_END, 3)
    reserve(db, 'A', digest, 'W1', 'worker-A', 2)
    with pytest.raises(ValueError, match='window_update_invalidates_leases'):
        define_window(db, 'W1', FAR_START, FAR_END, 1)
    # A compatible extension is fine and idempotent.
    define_window(db, 'W1', FAR_START, FAR_END, 4)
    assert state(db, 'A') == 'reserved'
    # Attaching an existing window to a too-small pool is rejected too.
    with pytest.raises(ValueError, match='pool_update_invalidates_leases'):
        define_window(db, 'W1', FAR_START, FAR_END, 4, pool_id='POOL', pool_units=1)
    assert connect(db).execute('SELECT pool_id FROM capacity_window WHERE id=?',
                               ('W1',)).fetchone()[0] is None
    # Closing the window under a held lease would make it illegal.
    with pytest.raises(ValueError, match='window_update_invalidates_leases'):
        define_window(db, 'W1', '2020-01-01T00:00:00Z', '2021-01-01T00:00:00Z', 4)
    # Future-dating the start would too.
    with pytest.raises(ValueError, match='window_update_invalidates_leases'):
        define_window(db, 'W1', '2090-01-01T00:00:00Z', FAR_END, 4)
    assert state(db, 'A') == 'reserved'


def test_promotion_spans_windows_in_the_pool(tmp_path):
    db, _, digest = ready(tmp_path)
    define_window(db, 'W1', FAR_START, FAR_END, 3, pool_id='POOL', pool_units=3)
    define_window(db, 'W2', '2020-06-01T00:00:00Z', '2099-06-01T00:00:00Z', 3,
                  pool_id='POOL')
    assert reserve(db, 'hold', digest, 'W2', 'w0', 2)['state'] == 'reserved'
    assert reserve(db, 'head', digest, 'W1', 'w1', 2)['state'] == 'waiting'
    assert reserve(db, 'small', digest, 'W2', 'w2', 1)['state'] == 'waiting'
    # Freeing capacity in W2 promotes across the shared pool; the head in W1
    # blocks the smaller lease behind it under strict FIFO.
    assert release(db, 'hold')['promoted'] == ['head', 'small']
    assert check_invariants(db) == []


def test_promotion_leaves_closed_window_leases_waiting(tmp_path):
    db, _, digest = ready(tmp_path)
    define_window(db, 'W1', FAR_START, FAR_END, 3, pool_id='POOL', pool_units=3,
                  promotion_policy='fit_backfill')
    seed_reserved(db, 'hold', 'W1', 2, 1)
    # A waiting lease bound to an already-closed window must not be promoted.
    con = connect(db)
    con.execute("UPDATE capacity_window SET starts=?,ends=? WHERE id='W1'",
                ('2020-01-01T00:00:00+00:00', '2021-01-01T00:00:00+00:00'))
    con.execute(
        "INSERT INTO execution_lease(id,plan_digest,run_id,window_id,worker,units,"
        "target_hash,state,wait_seq,skipped_count) "
        "SELECT 'expired',plan_digest,run_id,'W1','w',1,target_hash,'waiting',2,0 "
        "FROM execution_lease WHERE id='hold'")
    con.commit()
    con.close()
    result = release(db, 'hold')
    assert 'expired' not in result['promoted']
    assert state(db, 'expired') == 'blocked'
    assert capacity_status(db, 'POOL')['pools'][0]['used'] == 0


# --------------------------------------------------------------------------
# promotion policies
# --------------------------------------------------------------------------

def test_strict_fifo_stops_at_head_that_does_not_fit(tmp_path):
    db, _, digest = ready(tmp_path)
    define_window(db, 'W1', FAR_START, FAR_END, 3, pool_id='POOL', pool_units=3,
                  promotion_policy='strict_fifo')
    assert reserve(db, 'hold', digest, 'W1', 'w0', 2)['state'] == 'reserved'
    assert reserve(db, 'head', digest, 'W1', 'w1', 2)['state'] == 'waiting'
    # One unit is free, but strict FIFO refuses to pass the head.
    assert reserve(db, 'small', digest, 'W1', 'w2', 1)['state'] == 'waiting'
    assert capacity_status(db, 'POOL')['pools'][0]['waiting'] == ['head', 'small']
    # Enough capacity returning promotes the head first, then the small one.
    con = connect(db)
    con.execute("UPDATE execution_lease SET units=1 WHERE id='hold'")
    con.commit()
    con.close()
    assert release(db, 'hold')['promoted'] == ['head', 'small']
    assert check_invariants(db) == []


def test_strict_fifo_head_is_never_overtaken_by_small_leases(tmp_path):
    db, _, digest = ready(tmp_path)
    define_window(db, 'W1', FAR_START, FAR_END, 3, pool_id='POOL', pool_units=3,
                  promotion_policy='strict_fifo')
    assert reserve(db, 'hold', digest, 'W1', 'w0', 3)['state'] == 'reserved'
    assert reserve(db, 'big', digest, 'W1', 'w1', 3)['state'] == 'waiting'
    for index in range(4):
        assert reserve(db, f'small{index}', digest, 'W1', f's{index}', 1)['state'] == 'waiting'
    # Small leases never held capacity, so they cannot be released; the head
    # stays first and receives the next release in full.
    for index in range(4):
        with pytest.raises(ValueError, match='lease_not_releasable'):
            release(db, f'small{index}')
    assert capacity_status(db, 'POOL')['pools'][0]['waiting'][0] == 'big'
    promoted = release(db, 'hold')['promoted']
    assert promoted[0] == 'big'
    assert state(db, 'big') == 'reserved'
    assert connect(db).execute(
        "SELECT skipped_count FROM execution_lease WHERE id='big'").fetchone()[0] == 0
    assert check_invariants(db) == []


def test_fit_backfill_skips_head_but_keeps_its_priority(tmp_path):
    db, _, digest = ready(tmp_path)
    define_window(db, 'W1', FAR_START, FAR_END, 3, pool_id='POOL', pool_units=3,
                  promotion_policy='fit_backfill')
    assert reserve(db, 'hold', digest, 'W1', 'w0', 2)['state'] == 'reserved'
    assert reserve(db, 'head', digest, 'W1', 'w1', 2)['state'] == 'waiting'
    # One unit is free: fit_backfill admits the smaller lease behind the head.
    assert reserve(db, 'small', digest, 'W1', 'w2', 1)['state'] == 'reserved'
    status = capacity_status(db, 'POOL')['pools'][0]
    head = [x for x in status['leases'] if x['id'] == 'head'][0]
    assert head['wait_position'] == 1
    assert head['skipped_count'] == 1

    # Newcomers cannot jump the skipped head either; while the head still does
    # not fit, they may be admitted behind it only when the policy allows.
    assert reserve(db, 'newcomer', digest, 'W1', 'w3', 2)['state'] == 'waiting'
    assert capacity_status(db, 'POOL')['pools'][0]['waiting'] == ['head', 'newcomer']

    # When enough capacity returns, the skipped head goes first.
    con = connect(db)
    con.execute("UPDATE execution_lease SET units=1 WHERE id='hold'")
    con.commit()
    con.close()
    promoted = release(db, 'hold')['promoted']
    assert promoted[0] == 'head'
    assert state(db, 'head') == 'reserved'
    assert state(db, 'small') == 'reserved'
    # Two units went to the head, so the larger newcomer still waits.
    assert state(db, 'newcomer') == 'waiting'
    assert release(db, 'small')['promoted'] == []
    # Releasing the head finally admits it; nothing behind it was ever
    # promoted ahead of the skipped lease.
    assert release(db, 'head')['promoted'] == ['newcomer']
    assert state(db, 'newcomer') == 'reserved'
    assert check_invariants(db) == []


def test_head_never_starves_under_continuous_small_leases(tmp_path):
    db, _, digest = ready(tmp_path)
    define_window(db, 'W1', FAR_START, FAR_END, 4, pool_id='POOL', pool_units=4,
                  promotion_policy='fit_backfill')
    assert reserve(db, 'hold', digest, 'W1', 'w0', 3)['state'] == 'reserved'
    assert reserve(db, 'head', digest, 'W1', 'w1', 3)['state'] == 'waiting'
    # The single free unit is repeatedly taken by small leases queued behind the
    # head; each release hands the unit to the next small lease, not to the head.
    for index in range(5):
        result = reserve(db, f'small{index}', digest, 'W1', f's{index}', 1)
        assert result['state'] in ('waiting', 'reserved')
    skipped_before = connect(db).execute(
        "SELECT skipped_count FROM execution_lease WHERE id='head'").fetchone()[0]
    assert skipped_before >= 1
    # Free the held 3 units: capacity is 1 used + 3 free, exactly enough for the
    # head, which must be the first promotion despite the newer small leases.
    promoted = release(db, 'hold')['promoted']
    assert promoted[0] == 'head'
    assert state(db, 'head') == 'reserved'
    assert check_invariants(db) == []


def test_replaying_requests_does_not_change_queue_or_promote_twice(tmp_path):
    db, _, digest = ready(tmp_path)
    define_pool(db, pool_units=2)
    assert reserve(db, 'A', digest, 'W1', 'w', 2)['state'] == 'reserved'
    assert reserve(db, 'B', digest, 'W2', 'w', 2)['state'] == 'waiting'
    assert reserve(db, 'C', digest, 'W1', 'w', 1)['state'] == 'waiting'
    first = capacity_status(db, 'POOL')['pools'][0]['waiting']
    for _ in range(3):
        assert reserve(db, 'B', digest, 'W2', 'w', 2)['state'] == 'waiting'
        assert reserve(db, 'C', digest, 'W1', 'w', 1)['state'] == 'waiting'
    assert capacity_status(db, 'POOL')['pools'][0]['waiting'] == first
    with pytest.raises(ValueError, match='lease_identity_conflict'):
        reserve(db, 'B', digest, 'W1', 'w', 2)

    # Strict FIFO promotes B (2 units) first; C no longer fits afterwards.
    assert release(db, 'A')['promoted'] == ['B']
    assert state(db, 'C') == 'waiting'
    audit_count = len(capacity_audit(db, lease_id='B'))
    assert release(db, 'A')['promoted'] == []
    assert len(capacity_audit(db, lease_id='B')) == audit_count
    assert state(db, 'B') == 'reserved'
    # Releasing the promoted lease moves the next queued lease along.
    assert release(db, 'B')['promoted'] == ['C']


# --------------------------------------------------------------------------
# plan invalidation and apply gate
# --------------------------------------------------------------------------

def test_stale_plan_blocked_lease_cannot_be_revived_by_changing_window(tmp_path):
    db, plan, digest = ready(tmp_path)
    define_pool(db, pool_units=4)
    assert reserve(db, 'A', digest, 'W1', 'w', 2)['state'] == 'reserved'
    evaluate(db)  # produces a new run; the old plan is now stale
    assert activate(db, 'A')['state'] == 'blocked'
    # The lease is still bound to its original window; a request that names a
    # different window is an identity conflict, not a way to regain capacity.
    with pytest.raises(ValueError, match='lease_identity_conflict'):
        reserve(db, 'A', digest, 'W2', 'w', 2)
    assert reserve(db, 'A', digest, 'W1', 'w', 2)['state'] == 'blocked'
    assert state(db, 'A') == 'blocked'
    with pytest.raises(ValueError, match='active_lease_required'):
        check_active_lease(db, 'A', digest)
    # And a brand new lease bound to the stale plan cannot be created either.
    with pytest.raises(ValueError, match='unknown_or_stale_plan'):
        reserve(db, 'B', digest, 'W2', 'w', 2)


def test_activation_rechecks_stale_plan_on_replay(tmp_path):
    db, _, digest = ready(tmp_path)
    define_window(db, 'W1', FAR_START, FAR_END, 2)
    assert reserve(db, 'A', digest, 'W1', 'w', 2)['state'] == 'reserved'
    assert activate(db, 'A')['state'] == 'active'
    assert activate(db, 'A')['state'] == 'active'  # idempotent while valid
    evaluate(db)
    # Replaying activate after a new run still blocks the stale lease.
    assert activate(db, 'A')['state'] == 'blocked'
    assert state(db, 'A') == 'blocked'


def test_apply_rechecks_pool_capacity_decision(tmp_path):
    db, plan, digest = ready(tmp_path)
    define_pool(db, pool_units=3)
    reserve(db, 'A', digest, 'W1', 'w', 2)
    assert activate(db, 'A')['state'] == 'active'
    # An out-of-band window shrink must invalidate the active decision.
    con = connect(db)
    con.execute("UPDATE capacity_window SET units=1 WHERE id='W1'")
    con.commit()
    con.close()
    with pytest.raises(ValueError, match='lease_capacity_invalid'):
        action('apply', db, sandbox=tmp_path / 'target', out=plan, lease='A')
    assert state(db, 'A') == 'blocked'


def test_unpooled_windows_keep_original_behaviour_and_can_coexist(tmp_path):
    db, _, digest = ready(tmp_path)
    define_window(db, 'legacy1', FAR_START, FAR_END, 2)
    define_window(db, 'legacy2', FAR_START, FAR_END, 2)
    assert reserve(db, 'A', digest, 'legacy1', 'w', 2)['state'] == 'reserved'
    assert reserve(db, 'B', digest, 'legacy2', 'w', 2)['state'] == 'reserved'
    assert reserve(db, 'C', digest, 'legacy1', 'w', 1)['state'] == 'waiting'
    assert capacity_status(db)['pools'] == []
    assert check_invariants(db) == []


# --------------------------------------------------------------------------
# audit and interruption
# --------------------------------------------------------------------------

def test_capacity_audit_is_queryable_and_records_decisions(tmp_path):
    db, _, digest = ready(tmp_path)
    define_pool(db, pool_units=3)
    reserve(db, 'A', digest, 'W1', 'w', 2)
    reserve(db, 'B', digest, 'W2', 'w', 2)
    assert activate(db, 'A')['state'] == 'active'
    release(db, 'A')
    rows = capacity_audit(db, pool_id='POOL')
    events = [row['event'] for row in rows]
    assert events[:6] == ['window_created', 'window_created', 'enqueue', 'promote',
                          'enqueue', 'wait_no_fit']
    assert 'activate' in events and 'release' in events and 'promote' in events
    blocked = [row for row in rows if row['event'] == 'wait_no_fit'][0]
    assert blocked['wait_position'] == 1 and blocked['pool_used'] == 2
    assert blocked['pool_capacity'] == 3
    for row in rows:
        assert row['pool_id'] == 'POOL'
        json.loads(row['detail'])


def test_interrupted_writer_leaves_consistent_state(tmp_path):
    db, _, digest = ready(tmp_path)
    define_pool(db, pool_units=2)
    assert reserve(db, 'A', digest, 'W1', 'w', 2)['state'] == 'reserved'
    # Simulate a writer that was killed after committing a lease insert but
    # before bookkeeping; SQLite's journal guarantees each row is all-or-nothing.
    con = connect(db)
    con.execute(
        'INSERT INTO execution_lease(id,plan_digest,run_id,window_id,worker,units,'
        'target_hash,state,wait_seq,skipped_count) '
        "SELECT 'Z',plan_digest,run_id,'W2','w',2,target_hash,'reserved',99,0 "
        "FROM execution_lease WHERE id='A'")
    con.commit()
    con.close()
    # The pool is now over-committed; recovery demotes the newest lease and
    # restores both bounds without moving anything to another window.
    assert check_invariants(db) != []
    result = recover(db)
    assert result['problems'] == []
    assert result['demoted'] == ['Z']
    assert state(db, 'Z') == 'waiting'
    assert state(db, 'A') == 'reserved'
    assert capacity_status(db, 'POOL')['pools'][0]['used'] == 2
    assert recover(db)['demoted'] == []


def _die_mid_transaction(db_path, digest, lease_id, script):
    """Open, run ``script`` inside an uncommitted transaction, then die."""
    con = connect(db_path)
    con.execute('BEGIN IMMEDIATE')
    con.execute(
        'INSERT INTO execution_lease(id,plan_digest,run_id,window_id,worker,units,'
        'target_hash,state,wait_seq,skipped_count) '
        "SELECT ?,digest,run_id,'W2','w',2,'x','waiting',500,0 FROM plan LIMIT 1",
        (lease_id,))
    os._exit(17)  # no commit: the connection dies with the process


def test_process_death_during_reserve_rolls_back_entirely(tmp_path):
    db, _, digest = ready(tmp_path)
    define_pool(db, pool_units=2)
    assert reserve(db, 'A', digest, 'W1', 'w', 2)['state'] == 'reserved'
    ctx = multiprocessing.get_context('spawn')
    worker = ctx.Process(target=_die_mid_transaction, args=(str(db), digest, 'Z', ''))
    worker.start()
    worker.join(timeout=60)
    assert worker.exitcode == 17
    # Neither the lease row nor any capacity change survived the interruption.
    assert state(db, 'Z') is None
    assert capacity_status(db, 'POOL')['pools'][0]['used'] == 2
    assert check_invariants(db) == []
    # And the pool still admits exactly the capacity that remains.
    assert reserve(db, 'B', digest, 'W2', 'w', 2)['state'] == 'waiting'

    # The same guarantee holds inside the transaction that performs promotion:
    # a killed process leaves the candidate waiting, not half-promoted.
    assert release(db, 'A')['promoted'] == ['B']


# --------------------------------------------------------------------------
# real cross-window concurrency
# --------------------------------------------------------------------------

def test_concurrent_reservations_across_windows_have_one_winner(tmp_path):
    db, _, digest = ready(tmp_path)
    define_pool(db, pool_units=2)
    barrier = threading.Barrier(2)
    outcomes = {}

    def contend(lease_id, window_id):
        barrier.wait()
        try:
            outcomes[lease_id] = reserve(str(db), lease_id, digest, window_id,
                                         'worker', 2)['state']
        except Exception as exc:  # pragma: no cover - surfaced in the assertion
            outcomes[lease_id] = 'error:' + str(exc)

    threads = [threading.Thread(target=contend, args=(lid, wid))
               for lid, wid in (('A', 'W1'), ('B', 'W2'))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert sorted(outcomes.values()) == ['reserved', 'waiting']
    assert capacity_status(db, 'POOL')['pools'][0]['used'] == 2
    assert check_invariants(db) == []


def test_promotion_is_atomic_when_writer_is_interrupted(tmp_path):
    db, _, digest = ready(tmp_path)
    define_pool(db, pool_units=2)
    assert reserve(db, 'A', digest, 'W1', 'w', 2)['state'] == 'reserved'
    assert reserve(db, 'B', digest, 'W2', 'w', 2)['state'] == 'waiting'
    con = connect(db)
    con.execute(
        "CREATE TRIGGER injected_crash BEFORE UPDATE ON execution_lease "
        "WHEN OLD.state='waiting' AND NEW.state='reserved' "
        "BEGIN SELECT RAISE(ABORT,'injected'); END")
    con.commit()
    con.close()
    # The release transaction fails while promoting: nothing is half-written.
    with pytest.raises(sqlite3.IntegrityError, match='injected'):
        release(db, 'A')
    assert state(db, 'A') == 'reserved'
    assert state(db, 'B') == 'waiting'
    assert capacity_status(db, 'POOL')['pools'][0]['used'] == 2
    assert check_invariants(db) == []

    con = connect(db)
    con.execute('DROP TRIGGER injected_crash')
    con.commit()
    con.close()
    assert release(db, 'A')['promoted'] == ['B']
    assert check_invariants(db) == []


def test_cli_exposes_pool_window_reservations_and_audit(tmp_path):
    from backfillcomp.__main__ import main
    db, plan, _ = ready(tmp_path)
    assert capture(main, ['window', '--db', str(db), '--id', 'W1', '--starts', FAR_START,
                          '--ends', FAR_END, '--units', '3', '--pool', 'POOL',
                          '--pool-units', '3', '--policy', 'fit_backfill']).strip() == 'W1'
    main(['window', '--db', str(db), '--id', 'W2', '--starts', FAR_START,
          '--ends', FAR_END, '--units', '3', '--pool', 'POOL'])
    main(['reserve', '--db', str(db), '--id', 'A', '--plan', str(plan),
          '--window', 'W1', '--worker', 'w', '--units', '2'])
    main(['reserve', '--db', str(db), '--id', 'B', '--plan', str(plan),
          '--window', 'W2', '--worker', 'w', '--units', '2'])
    status = json.loads(capture(main, ['capacity-status', '--db', str(db),
                                       '--pool', 'POOL']))
    assert status['pools'][0]['waiting'] == ['B']
    main(['release-lease', '--db', str(db), '--id', 'A'])
    audit = json.loads(capture(main, ['capacity-audit', '--db', str(db),
                                      '--pool', 'POOL']))
    assert [row['event'] for row in audit][-4:] == ['enqueue', 'wait_no_fit', 'promote',
                                                    'release']
    recovered = json.loads(capture(main, ['recover', '--db', str(db)]))
    assert recovered['problems'] == []


def capture(func, argv):
    import contextlib
    import io
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        func(argv)
    return buffer.getvalue()


# --------------------------------------------------------------------------
# old database upgrade
# --------------------------------------------------------------------------

LEGACY_SCHEMA = """
CREATE TABLE IF NOT EXISTS capacity_window(
 id TEXT PRIMARY KEY, starts TEXT NOT NULL, ends TEXT NOT NULL, units INTEGER NOT NULL CHECK(units>0));
CREATE TABLE IF NOT EXISTS execution_lease(
 id TEXT PRIMARY KEY, plan_digest TEXT NOT NULL, run_id INTEGER NOT NULL REFERENCES run(id),
 window_id TEXT NOT NULL REFERENCES capacity_window(id), worker TEXT NOT NULL,
 units INTEGER NOT NULL, target_hash TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('waiting','reserved','active','released','blocked')));
"""


def test_legacy_window_database_upgrades_in_place(tmp_path):
    db, plan, digest = ready(tmp_path)
    con = sqlite3.connect(db)
    con.executescript(LEGACY_SCHEMA)
    con.execute("INSERT INTO capacity_window VALUES ('OLD',?,?,2)", (FAR_START, FAR_END))
    # A waiting lease from the old release has no queue position yet.
    target_hash = hashlib.sha256(
        json.dumps(json.loads(plan.read_text(encoding='utf-8')), sort_keys=True).encode()
    ).hexdigest()
    con.execute(
        "INSERT INTO execution_lease SELECT 'LEGACY',digest,run_id,'OLD','w',1,?,"
        "'waiting' FROM plan LIMIT 1", (target_hash,))
    con.commit()
    con.close()

    # Legacy windows keep working with no pool, and the schema gains queue
    # positions and audit columns.
    # The pre-existing waiting lease keeps the head of the queue and takes the
    # first freed capacity, so A (2 units) cannot fit beside it.
    assert reserve(db, 'A', digest, 'OLD', 'w', 2)['state'] == 'waiting'
    assert state(db, 'LEGACY') == 'reserved'
    assert capacity_status(db)['windows'][0]['pool'] is None
    waiting = [x for x in capacity_status(db)['windows'][0]['leases']
               if x['state'] == 'waiting']
    assert waiting[0]['id'] == 'A' and waiting[0]['wait_position'] == 1
    con = connect(db)
    con.execute("UPDATE execution_lease SET units=1 WHERE id='LEGACY'")
    con.commit()
    con.close()
    assert release(db, 'LEGACY')['promoted'] == ['A']
    columns = {row[1] for row in connect(db).execute('PRAGMA table_info(execution_lease)')}
    assert {'wait_seq', 'skipped_count'} <= columns
    assert connect(db).execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='capacity_audit'"
    ).fetchone()
