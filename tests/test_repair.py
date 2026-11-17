"""故障注入测试：两个提交点之间中断后的恢复裁定、路线、发布与租约竞争。"""
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import pytest

from backfillcomp import rules, repair
from backfillcomp.ingest import initialize, load
from backfillcomp.rules import evaluate, action, verify_target
from backfillcomp.schedule import define_window, reserve, activate
from backfillcomp.publish import check, publish
from backfillcomp.report import repair_report
from backfillcomp.store import connect

FIX = Path(__file__).resolve().parent.parent / 'fixtures'


class CrashOnCommitGap:
    """模拟进程在目标事务提交后、控制事务提交前被杀死。"""
    def __init__(self):
        self.fired = 0

    def __call__(self):
        self.fired += 1
        raise RuntimeError('injected_crash_between_commits')


def build(db, plan, window=True):
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    action('plan', db, out=plan)
    digest = hashlib.sha256(plan.read_bytes()).hexdigest()
    if window:
        define_window(db, 'W1', '2020-01-01T00:00:00Z', '2099-01-01T00:00:00Z', 2)
        reserve(db, 'L1', digest, 'W1', 'worker-A', 2)
        activate(db, 'L1')
    return digest


def interrupt(db, plan, sandbox, lease='L1'):
    crash = CrashOnCommitGap()
    old = rules._fault_between_commits
    rules._fault_between_commits = crash
    try:
        with pytest.raises(RuntimeError, match='injected_crash'):
            action('apply', db, sandbox=sandbox, out=plan, lease=lease)
    finally:
        rules._fault_between_commits = old
    return crash


def target_con(target):
    return sqlite3.connect(target / 'ledger.db')


def test_interruption_leaves_target_applied_but_control_execution_missing(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    digest = build(db, plan)
    sandbox = tmp_path / 'sandbox'
    crash = interrupt(db, plan, sandbox)
    assert crash.fired == 1
    con = target_con(sandbox)
    assert con.execute("SELECT count(*) FROM backfill_audit WHERE plan_digest=? AND state='applied'",
                       (digest,)).fetchone()[0] == 2
    con.close()
    cdb = connect(db)
    assert cdb.execute('SELECT count(*) FROM execution').fetchone()[0] == 0
    assert cdb.execute("SELECT state FROM execution_lease WHERE id='L1'").fetchone()[0] == 'active'
    cdb.close()
    # verify_target 只凭目标侧就能报已应用，但控制库缺口仍需恢复裁定。
    assert all(x['status'] == '已应用' for x in verify_target(db, sandbox))
    result = repair.inspect(db, sandbox, plan, 'CASE-1', worker='worker-A', lease_id='L1')
    assert result['recoverable'] is True
    assert result['routes'] == {'forward': True, 'rollback': True}
    assert result['blockers'] == []


def test_forward_route_backfills_execution_and_completes_lease(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan)
    sandbox = tmp_path / 'sandbox'
    interrupt(db, plan, sandbox)
    repair.inspect(db, sandbox, plan, 'CASE-F', worker='worker-A', lease_id='L1')
    out = repair.decide(db, sandbox, plan, 'CASE-F', 'forward', worker='worker-A', lease_id='L1')
    assert out['status'] == 'forwarded' and out['lease_released'] == 'L1'
    cdb = connect(db)
    assert cdb.execute("SELECT count(*) FROM execution WHERE action='apply'").fetchone()[0] == 2
    assert cdb.execute("SELECT state FROM execution_lease WHERE id='L1'").fetchone()[0] == 'released'
    case = cdb.execute('SELECT * FROM repair_case WHERE id=?', ('CASE-F',)).fetchone()
    assert case['status'] == 'forwarded' and case['route'] == 'forward' and case['decided_at']
    cdb.close()
    expected = json.loads((FIX / 'consumer-expected.json').read_text(encoding='utf-8'))
    assert check(db, sandbox / 'ledger.db', 'G1', 'consumer-A', expected)['status'] == 'ready'
    assert publish(db, sandbox / 'ledger.db', 'G1')['status'] == 'published'


def test_repeated_decide_same_route_is_idempotent(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan)
    sandbox = tmp_path / 'sandbox'
    interrupt(db, plan, sandbox)
    first_inspect = repair.inspect(db, sandbox, plan, 'CASE-F', worker='worker-A', lease_id='L1')
    # 冻结证据：相同现场重复 inspect 产生相同证据哈希。
    again_inspect = repair.inspect(db, sandbox, plan, 'CASE-F', worker='worker-A', lease_id='L1')
    assert again_inspect['evidence_hash'] == first_inspect['evidence_hash']
    first = repair.decide(db, sandbox, plan, 'CASE-F', 'forward', worker='worker-A', lease_id='L1')
    second = repair.decide(db, sandbox, plan, 'CASE-F', 'forward', worker='worker-A', lease_id='L1')
    assert second['replayed'] is True and second['status'] == first['status']
    assert connect(db).execute("SELECT count(*) FROM execution WHERE action='apply'").fetchone()[0] == 2


def test_opposite_route_after_decision_is_rejected(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan)
    sandbox = tmp_path / 'sandbox'
    interrupt(db, plan, sandbox)
    repair.inspect(db, sandbox, plan, 'CASE-F', worker='worker-A', lease_id='L1')
    repair.decide(db, sandbox, plan, 'CASE-F', 'forward', worker='worker-A', lease_id='L1')
    with pytest.raises(ValueError, match='repair_case_already_decided'):
        repair.decide(db, sandbox, plan, 'CASE-F', 'rollback', worker='worker-A', lease_id='L1')


def test_rollback_route_restores_old_values_and_blocks_publication(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan)
    sandbox = tmp_path / 'sandbox'
    interrupt(db, plan, sandbox)
    repair.inspect(db, sandbox, plan, 'CASE-R', worker='worker-A', lease_id='L1')
    out = repair.decide(db, sandbox, plan, 'CASE-R', 'rollback', worker='worker-A', lease_id='L1')
    assert out['status'] == 'rolled_back'
    con = target_con(sandbox)
    assert con.execute("SELECT computed FROM ledger WHERE id='r1'").fetchone()[0] == '10.00'
    assert con.execute("SELECT computed FROM ledger WHERE id='r2'").fetchone()[0] == '20.00'
    assert con.execute("SELECT count(*) FROM backfill_audit WHERE state='rolled_back'").fetchone()[0] == 2
    assert con.execute("SELECT count(*) FROM backfill_audit WHERE state='applied'").fetchone()[0] == 0
    con.close()
    cdb = connect(db)
    assert cdb.execute("SELECT count(*) FROM execution WHERE action='rollback'").fetchone()[0] == 2
    assert cdb.execute("SELECT state FROM execution_lease WHERE id='L1'").fetchone()[0] == 'released'
    cdb.close()
    assert all(x['status'] == '未应用' for x in verify_target(db, sandbox))
    expected = json.loads((FIX / 'consumer-expected.json').read_text(encoding='utf-8'))
    gate = check(db, sandbox / 'ledger.db', 'G2', 'consumer-A', expected)
    assert gate['status'] == 'uncertain' and any('unproven' in p for p in gate['problems'])
    # 回退幂等：重复回退不再次改账，不重复写 execution。
    repair.decide(db, sandbox, plan, 'CASE-R', 'rollback', worker='worker-A', lease_id='L1')
    assert connect(db).execute("SELECT count(*) FROM execution WHERE action='rollback'").fetchone()[0] == 2


def test_published_consumer_forbids_rollback_even_via_control_only_change(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan)
    sandbox = tmp_path / 'sandbox'
    interrupt(db, plan, sandbox)
    expected = json.loads((FIX / 'consumer-expected.json').read_text(encoding='utf-8'))
    # 中断现场下目标已 applied，门禁本身可以判 ready 并发布。
    gate = check(db, sandbox / 'ledger.db', 'G1', 'consumer-A', expected)
    assert gate['status'] == 'ready'
    publish(db, sandbox / 'ledger.db', 'G1')
    result = repair.inspect(db, sandbox, plan, 'CASE-P', worker='worker-A', lease_id='L1')
    assert result['recoverable'] is False
    assert result['routes'] == {'forward': False, 'rollback': False}
    assert any(b.startswith('published_consumer:') for b in result['blockers'])
    with pytest.raises(ValueError, match='rollback_forbidden_published'):
        repair.decide(db, sandbox, plan, 'CASE-P', 'rollback', worker='worker-A', lease_id='L1')
    # 即便维护者手工把控制库案例改成 open，决定时刻重新取证仍会拦住回退。
    cdb = connect(db)
    cdb.execute("UPDATE repair_case SET status='open' WHERE id='CASE-P'")
    cdb.commit(); cdb.close()
    with pytest.raises(ValueError, match='rollback_forbidden_published'):
        repair.decide(db, sandbox, plan, 'CASE-P', 'rollback', worker='worker-A', lease_id='L1')
    # 前进路线在发布后仍被视为阻断：已发布消费者的事实不允许自动恢复改写。
    with pytest.raises(ValueError, match='published_consumer'):
        repair.decide(db, sandbox, plan, 'CASE-P', 'forward', worker='worker-A', lease_id='L1')


def test_lease_contention_foreign_worker_blocks_and_other_plan_cannot_preempt(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    digest = build(db, plan)
    sandbox = tmp_path / 'sandbox'
    interrupt(db, plan, sandbox)
    # 另一工作者不持有 L1：不能恢复别人的活动租约。
    result = repair.inspect(db, sandbox, plan, 'CASE-X', worker='worker-B', lease_id='L1')
    assert result['recoverable'] is False
    assert any(b.startswith('lease_foreign_worker:') for b in result['blockers'])
    with pytest.raises(ValueError, match='lease_foreign_worker'):
        repair.decide(db, sandbox, plan, 'CASE-X', 'forward', worker='worker-B', lease_id='L1')
    # 新计划的租约无法借用同一窗口：L1 仍 active 占用容量。
    other_plan = tmp_path / 'other-plan.json'
    action('plan', db, out=other_plan)
    other_digest = hashlib.sha256(other_plan.read_bytes()).hexdigest()
    assert reserve(db, 'L2', other_digest, 'W1', 'worker-B', 1)['state'] == 'waiting'
    # 原租约恢复完成后释放容量，等待者才被提升——抢占被阻止。
    repair.inspect(db, sandbox, plan, 'CASE-A', worker='worker-A', lease_id='L1')
    out = repair.decide(db, sandbox, plan, 'CASE-A', 'forward', worker='worker-A', lease_id='L1')
    assert out['promoted'] == ['L2']
    assert connect(db).execute("SELECT state FROM execution_lease WHERE id='L2'").fetchone()[0] == 'reserved'


def test_third_party_rewrite_blocks_recovery(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan)
    sandbox = tmp_path / 'sandbox'
    interrupt(db, plan, sandbox)
    con = target_con(sandbox)
    con.execute("UPDATE ledger SET computed='777.00' WHERE id='r2'")
    con.commit(); con.close()
    result = repair.inspect(db, sandbox, plan, 'CASE-T')
    assert result['status'] == 'blocked'
    assert any(b.startswith('third_party_rewrite:r2') for b in result['blockers'])
    assert result['routes'] == {'forward': False, 'rollback': False}
    with pytest.raises(ValueError, match='repair_blocked'):
        repair.decide(db, sandbox, plan, 'CASE-T', 'forward')


def test_audit_contradiction_blocks_recovery(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    digest = build(db, plan)
    sandbox = tmp_path / 'sandbox'
    interrupt(db, plan, sandbox)
    con = target_con(sandbox)
    # 目标值保持新值，但审计被第三方改成 rolled_back：值与审计互相矛盾。
    con.execute("UPDATE backfill_audit SET state='rolled_back' WHERE subject='r1' AND plan_digest=?",
                (digest,))
    con.commit(); con.close()
    result = repair.inspect(db, sandbox, plan, 'CASE-A')
    assert any(b.startswith('audit_contradiction:r1') for b in result['blockers'])
    assert result['recoverable'] is False


def test_plan_changed_after_interruption_blocks_recovery(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan)
    sandbox = tmp_path / 'sandbox'
    interrupt(db, plan, sandbox)
    repair.inspect(db, sandbox, plan, 'CASE-1', worker='worker-A', lease_id='L1')
    # 规则重算产生新 run，旧计划不再是最新运行的计划。
    evaluate(db)
    with pytest.raises(ValueError, match='plan_changed'):
        repair.decide(db, sandbox, plan, 'CASE-1', 'forward', worker='worker-A', lease_id='L1')
    result = repair.inspect(db, sandbox, plan, 'CASE-1', worker='worker-A', lease_id='L1')
    assert 'plan_changed' in result['blockers'] and result['recoverable'] is False


def test_recovery_without_capacity_windows_needs_no_lease(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan, window=False)
    sandbox = tmp_path / 'sandbox'
    crash = CrashOnCommitGap()
    old = rules._fault_between_commits
    rules._fault_between_commits = crash
    try:
        with pytest.raises(RuntimeError):
            action('apply', db, sandbox=sandbox, out=plan)
    finally:
        rules._fault_between_commits = old
    result = repair.inspect(db, sandbox, plan, 'CASE-N')
    assert result['recoverable'] is True
    out = repair.decide(db, sandbox, plan, 'CASE-N', 'forward')
    assert out['status'] == 'forwarded' and out['lease_released'] is None


def test_rollback_crash_between_target_and_control_is_reentrant(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan)
    sandbox = tmp_path / 'sandbox'
    interrupt(db, plan, sandbox)
    repair.inspect(db, sandbox, plan, 'CASE-R', worker='worker-A', lease_id='L1')
    # 在回退路线的目标提交后、控制提交前再次中断：半数行已 rolled_back，租约仍 active。
    calls = {'n': 0}

    def fault():
        calls['n'] += 1
        raise RuntimeError('injected_crash_during_rollback')

    old = repair._fault_after_target
    repair._fault_after_target = fault
    try:
        with pytest.raises(RuntimeError, match='injected_crash_during_rollback'):
            repair.decide(db, sandbox, plan, 'CASE-R', 'rollback', worker='worker-A', lease_id='L1')
    finally:
        repair._fault_after_target = old
    con = target_con(sandbox)
    states = dict(con.execute('SELECT subject,state FROM backfill_audit').fetchall())
    con.close()
    assert set(states.values()) == {'rolled_back'}
    assert connect(db).execute('SELECT count(*) FROM execution').fetchone()[0] == 0
    # 同一恢复命令重复执行：完成剩余控制侧工作，不留下半数状态。
    out = repair.decide(db, sandbox, plan, 'CASE-R', 'rollback', worker='worker-A', lease_id='L1')
    assert out['status'] == 'rolled_back'
    cdb = connect(db)
    assert cdb.execute("SELECT count(*) FROM execution WHERE action='rollback'").fetchone()[0] == 2
    assert cdb.execute("SELECT state FROM execution_lease WHERE id='L1'").fetchone()[0] == 'released'
    cdb.close()
    con = target_con(sandbox)
    assert con.execute("SELECT count(*) FROM backfill_audit WHERE state='rolled_back'").fetchone()[0] == 2
    assert con.execute("SELECT count(*) FROM backfill_audit WHERE state='applied'").fetchone()[0] == 0
    con.close()


def test_forward_decision_failure_leaves_no_half_state(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan)
    sandbox = tmp_path / 'sandbox'
    interrupt(db, plan, sandbox)
    repair.inspect(db, sandbox, plan, 'CASE-F', worker='worker-A', lease_id='L1')
    # 决定时不提供租约身份却配置了容量窗口：控制事务整体失败，案例与租约保持原状。
    with pytest.raises(ValueError, match='active_lease_required'):
        repair.decide(db, sandbox, plan, 'CASE-F', 'forward')
    cdb = connect(db)
    assert cdb.execute('SELECT count(*) FROM execution').fetchone()[0] == 0
    assert cdb.execute("SELECT state FROM execution_lease WHERE id='L1'").fetchone()[0] == 'active'
    assert cdb.execute("SELECT status FROM repair_case WHERE id='CASE-F'").fetchone()[0] == 'open'
    cdb.close()
    con = target_con(sandbox)
    assert con.execute("SELECT count(*) FROM backfill_audit WHERE state='applied'").fetchone()[0] == 2
    con.close()


def test_partial_control_execution_blocks_automatic_recovery(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan)
    sandbox = tmp_path / 'sandbox'
    interrupt(db, plan, sandbox)
    # 手工制造控制库只记录了一半 execution 的不一致现场。
    item = json.loads(plan.read_text())[0]
    cdb = connect(db)
    cdb.execute('INSERT INTO execution(run_id,subject,action,before_hash,after_state) VALUES (1,?,?,?,?)',
                (item['subject'], 'apply', repair.item_hash(item), item['new']))
    cdb.commit(); cdb.close()
    result = repair.inspect(db, sandbox, plan, 'CASE-H')
    assert 'partial_execution' in result['blockers']
    assert result['recoverable'] is False
    with pytest.raises(ValueError, match='partial_execution'):
        repair.decide(db, sandbox, plan, 'CASE-H', 'forward', worker='worker-A', lease_id='L1')


def test_gate_adjudication_follows_audit_state_change(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan)
    sandbox = tmp_path / 'sandbox'
    interrupt(db, plan, sandbox)
    expected = json.loads((FIX / 'consumer-expected.json').read_text(encoding='utf-8'))
    assert check(db, sandbox / 'ledger.db', 'G1', 'consumer-A', expected)['status'] == 'ready'
    repair.inspect(db, sandbox, plan, 'CASE-R', worker='worker-A', lease_id='L1')
    repair.decide(db, sandbox, plan, 'CASE-R', 'rollback', worker='worker-A', lease_id='L1')
    # 同一门禁 id 随审计 applied→rolled_back 重新裁定为 uncertain。
    flipped = check(db, sandbox / 'ledger.db', 'G1', 'consumer-A', expected)
    assert flipped['status'] == 'uncertain'
    # 前进路线重新 apply 后再核对，门禁再次跟随到 ready（租约已释放，重新预留）。
    digest = hashlib.sha256(plan.read_bytes()).hexdigest()
    reserve(db, 'L3', digest, 'W1', 'worker-A', 2)
    activate(db, 'L3')
    action('apply', db, sandbox=sandbox, out=plan, lease='L3')
    ready_again = check(db, sandbox / 'ledger.db', 'G1', 'consumer-A', expected)
    assert ready_again['status'] == 'ready'


def test_migrate_is_idempotent_on_preexisting_control_db(tmp_path):
    db = tmp_path / 'old.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    repair.migrate(db)
    repair.migrate(db)
    cdb = connect(db)
    for table in ('repair_case', 'repair_row', 'execution_lease', 'publication_gate'):
        assert cdb.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    cdb.close()


def test_matching_value_without_applied_audit_is_not_recoverable(tmp_path):
    """不能只凭目标值相等选择路线：缺 applied 审计的新值必须阻断。"""
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan, window=False)
    import shutil
    sandbox = tmp_path / 'sandbox'
    sandbox.mkdir()
    shutil.copy2(FIX / 'ledger.db', sandbox / 'ledger.db')
    con = target_con(sandbox)
    con.execute('CREATE TABLE IF NOT EXISTS backfill_audit(subject TEXT, plan_digest TEXT, state TEXT, '
                'before_value TEXT, after_value TEXT)')
    con.execute("UPDATE ledger SET computed='11.00' WHERE id='r1'")
    con.execute("UPDATE ledger SET computed='23.00' WHERE id='r2'")
    con.commit(); con.close()
    result = repair.inspect(db, sandbox, plan, 'CASE-U')
    assert any(b.startswith('untracked_value:') for b in result['blockers'])
    assert result['recoverable'] is False
    with pytest.raises(ValueError, match='repair_blocked'):
        repair.decide(db, sandbox, plan, 'CASE-U', 'forward')


def test_missing_target_and_missing_audit_table_block_recovery(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan, window=False)
    # 目标库完全不存在。
    result = repair.inspect(db, tmp_path / 'empty-sandbox', plan, 'CASE-M')
    assert 'target_missing' in result['blockers']
    assert result['recoverable'] is False
    # 目标库存在但没有 backfill_audit 表：值不能充当执行证据。
    import shutil
    sandbox = tmp_path / 'bare-sandbox'
    sandbox.mkdir()
    shutil.copy2(FIX / 'ledger.db', sandbox / 'ledger.db')
    result = repair.inspect(db, sandbox, plan, 'CASE-B')
    assert 'target_audit_missing' in result['blockers']
    assert result['recoverable'] is False
    with pytest.raises(ValueError, match='repair_blocked'):
        repair.decide(db, sandbox, plan, 'CASE-B', 'forward')


def test_repair_report_contains_both_sides_and_blockers(tmp_path):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan)
    sandbox = tmp_path / 'sandbox'
    interrupt(db, plan, sandbox)
    repair.inspect(db, sandbox, plan, 'CASE-1', worker='worker-A', lease_id='L1')
    repair.decide(db, sandbox, plan, 'CASE-1', 'forward', worker='worker-A', lease_id='L1')
    out = tmp_path / 'repair.json'
    data = repair_report(db, out)
    case = data['repair_cases'][0]
    assert case['id'] == 'CASE-1' and case['status'] == 'forwarded'
    assert {r['subject'] for r in case['rows']} == {'r1', 'r2'}
    assert case['evidence']['executions'] and case['evidence']['leases']
    assert out.with_suffix('.md').read_text(encoding='utf-8').startswith('# 中断恢复裁定报告')


def test_repair_cli_offline_commands(tmp_path, monkeypatch):
    db, plan = tmp_path / 'c.db', tmp_path / 'plan.json'
    build(db, plan)
    sandbox = tmp_path / 'sandbox'
    interrupt(db, plan, sandbox)
    from backfillcomp.__main__ import main
    monkeypatch.setattr(sys, 'argv', ['backfillcomp', 'migrate', '--db', str(db)])
    main()
    monkeypatch.setattr(sys, 'argv', ['backfillcomp', 'repair-inspect', '--db', str(db),
                                      '--sandbox', str(sandbox), '--plan', str(plan),
                                      '--id', 'CASE-CLI', '--worker', 'worker-A', '--lease', 'L1'])
    main()
    monkeypatch.setattr(sys, 'argv', ['backfillcomp', 'repair-decide', '--db', str(db),
                                      '--sandbox', str(sandbox), '--plan', str(plan),
                                      '--id', 'CASE-CLI', '--route', 'forward',
                                      '--worker', 'worker-A', '--lease', 'L1'])
    main()
    monkeypatch.setattr(sys, 'argv', ['backfillcomp', 'repair-report', '--db', str(db),
                                      '--out', str(tmp_path / 'cli-report.json')])
    main()
    assert json.loads((tmp_path / 'cli-report.json').read_text(encoding='utf-8'))['repair_cases']
