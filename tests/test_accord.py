import json
import sqlite3
from pathlib import Path

import pytest

from backfillcomp.ingest import initialize,load
from backfillcomp.rules import evaluate,action
from backfillcomp.publish import (prepare_accord,recheck_accord,publish_accord,
                                  accord_status,list_accords,consumer_versions,rejections)
from backfillcomp.report import export
from backfillcomp.store import connect

FIX=Path(__file__).resolve().parent.parent/'fixtures'

def ready(tmp_path):
    db=tmp_path/'control.db';initialize(db,FIX/'config.json');load(db,FIX/'events.jsonl');evaluate(db)
    plan=tmp_path/'plan.json';action('plan',db,out=plan)
    target=Path(action('apply',db,sandbox=tmp_path/'target',out=plan))
    return db,target

def contract(route='atomic',version=1,dependencies=None,members=None):
    return {'route':route,'version':version,
            'members':members or [
                {'consumer':'A','scope':['r1'],'expected':{'r1':'11.00'}},
                {'consumer':'B','scope':['r2'],'expected':{'r2':'23.00'}}],
            'dependencies':dependencies or {}}

def abc(route='atomic',version=1):
    return {'route':route,'version':version,
            'members':[
                {'consumer':'A','scope':['r1'],'expected':{'r1':'11.00'}},
                {'consumer':'B','scope':['r2'],'expected':{'r2':'23.00'}},
                {'consumer':'C','scope':['r2'],'expected':{'r2':'23.00'}}],
            'dependencies':{'C':['B']}}

def break_audit(target,subject):
    con=sqlite3.connect(target);con.execute('DELETE FROM backfill_audit WHERE subject=?',(subject,));con.commit();con.close()

def test_atomic_route_blocks_everyone_when_one_member_uncertain(tmp_path):
    db,target=ready(tmp_path);break_audit(target,'r2')
    status=prepare_accord(db,target,abc('atomic'),accord_id='ACC')
    assert {m['consumer']:m['readiness'] for m in status['members']}=={'A':'ready','B':'uncertain','C':'uncertain'}
    result=publish_accord(db,target,'ACC')
    assert result['status']=='blocked'
    assert result['published_groups']==[]
    assert {m['consumer'] for m in accord_status(db,'ACC')['members'] if m['published_at']}==set()

def test_partitioned_route_publishes_only_independent_ready_group(tmp_path):
    db,target=ready(tmp_path);break_audit(target,'r2')
    prepare_accord(db,target,abc('partitioned'),accord_id='ACC')
    result=publish_accord(db,target,'ACC')
    assert result['status']=='partially_published'
    assert result['published_groups']==['A']
    # B and C share subject r2 and C depends on B: they must move as one blocked group.
    assert [g['members'] for g in result['blocked_groups']]==[['B','C']]
    versions={v['consumer']:v for v in consumer_versions(db)}
    assert set(versions)=={'A'} and versions['A']['consistent']

def test_partitioned_route_never_exposes_mixed_version(tmp_path):
    db,target=ready(tmp_path);break_audit(target,'r2')
    prepare_accord(db,target,abc('partitioned'),accord_id='ACC')
    publish_accord(db,target,'ACC')
    # Publishing C alone is refused because C sits in the B+C group.
    result=publish_accord(db,target,'ACC',consumers=['C'])
    assert result['status']=='partially_published'
    assert result['published_groups']==['A']
    c=[g for g in result['blocked_groups'] if 'C' in g['group']][0]
    assert c['status']=='uncertain'
    assert not any(m['consumer']=='C' and m['published_at'] for m in accord_status(db,'ACC')['members'])

def test_shared_subject_conflicting_expectations_are_rejected(tmp_path):
    db,target=ready(tmp_path)
    bad=contract(members=[
        {'consumer':'A','scope':['r1','r2'],'expected':{'r1':'11.00','r2':'23.00'}},
        {'consumer':'B','scope':['r2'],'expected':{'r2':'99.00'}}])
    with pytest.raises(ValueError,match='consumer_expectation_conflict'):
        prepare_accord(db,target,bad,accord_id='ACC')
    assert list_accords(db)==[]

def test_shared_subject_consistent_expectations_form_one_group(tmp_path):
    db,target=ready(tmp_path)
    shared={'route':'partitioned','version':1,'members':[
        {'consumer':'A','scope':['r1','r2'],'expected':{'r1':'11.00','r2':'23.00'}},
        {'consumer':'B','scope':['r2'],'expected':{'r2':'23.00'}}]}
    status=prepare_accord(db,target,shared,accord_id='ACC')
    assert len(status['groups'])==1
    assert status['groups'][0]['members']==['A','B']

def test_duplicate_consumer_identity_is_rejected(tmp_path):
    db,target=ready(tmp_path)
    bad=contract(members=[{'consumer':'A','scope':['r1'],'expected':{'r1':'11.00'}},
                          {'consumer':'A','scope':['r2'],'expected':{'r2':'23.00'}}])
    with pytest.raises(ValueError,match='duplicate_consumer_identity'):
        prepare_accord(db,target,bad,accord_id='ACC')

def test_absent_member_is_tracked_and_blocks_dependants(tmp_path):
    db,target=ready(tmp_path)
    # A depends on Z, but Z is not a declared member of the accord.
    status=prepare_accord(db,target,contract('partitioned',dependencies={'A':['Z']}),
                          accord_id='ACC')
    member={m['consumer']:m for m in status['members']}
    assert member['A']['readiness']=='uncertain'
    assert member['A']['reasons']==[['dependency_member_absent','Z']]
    result=publish_accord(db,target,'ACC')
    # B is independent of the missing member, so only A is held back.
    assert result['status']=='partially_published'
    assert result['published_groups']==['B']
    assert [g['members'] for g in result['blocked_groups']]==[['A']]
    assert any('dependency_member_absent' in reason for g in result['blocked_groups'] for reason in g['reasons'])

def test_absent_expected_subject_is_traced(tmp_path):
    db,target=ready(tmp_path)
    status=prepare_accord(db,target,contract(members=[
        {'consumer':'A','scope':['r1','r9'],'expected':{'r1':'11.00'}}]),accord_id='ACC')
    member=status['members'][0]
    assert member['readiness']=='uncertain'
    assert ['consumer_expectation_absent','r9'] in member['reasons']
    stored=connect(db).execute("SELECT code,detail FROM accord_reason WHERE accord_key='ACC:v1'").fetchall()
    assert any(row['code']=='consumer_expectation_absent' and row['detail']=='r9' for row in stored)

def test_target_drift_after_prepare_is_stale_on_commit(tmp_path):
    db,target=ready(tmp_path)
    prepare_accord(db,target,contract(),accord_id='ACC')
    con=sqlite3.connect(target);con.execute("UPDATE ledger SET computed='999.00' WHERE id='r2'");con.commit();con.close()
    result=publish_accord(db,target,'ACC')
    assert result['status']=='stale'
    assert any('target_drift' in reason for reason in result['reasons'])
    assert not any(m['published_at'] for m in result['members'])

def test_recheck_after_rule_change_makes_stale_accord_non_publishable(tmp_path):
    db,target=ready(tmp_path)
    prepare_accord(db,target,contract(),accord_id='ACC')
    # A new run with a fresh plan changes the shared plan digest.
    evaluate(db);plan=tmp_path/'plan2.json';action('plan',db,out=plan)
    status=recheck_accord(db,target,'ACC')
    assert status['state']=='stale'
    assert any('plan_changed' in reason for reason in status['reasons'])
    result=publish_accord(db,target,'ACC')
    assert result['status']=='stale'

def test_readiness_is_readjudicated_when_target_changes(tmp_path):
    db,target=ready(tmp_path)
    status=prepare_accord(db,target,contract(),accord_id='ACC')
    assert all(m['readiness']=='ready' for m in status['members'])
    break_audit(target,'r1')
    status=recheck_accord(db,target,'ACC')
    assert {m['consumer']:m['readiness'] for m in status['members']}['A']=='uncertain'
    assert status['state']=='stale'

def test_repeated_prepare_does_not_rewrite_frozen_members(tmp_path):
    db,target=ready(tmp_path)
    first=prepare_accord(db,target,contract(),accord_id='ACC')
    digest=first['member_digest'];target_hash=first['target_hash']
    second=prepare_accord(db,target,contract(),accord_id='ACC')
    assert second['member_digest']==digest and second['target_hash']==target_hash
    assert second['version']==1

def test_conflicting_reprepare_under_same_version_is_rejected(tmp_path):
    db,target=ready(tmp_path)
    prepare_accord(db,target,contract(),accord_id='ACC')
    changed=contract(members=[{'consumer':'A','scope':['r1'],'expected':{'r1':'12.00'}}])
    with pytest.raises(ValueError,match='accord_identity_conflict'):
        prepare_accord(db,target,changed,accord_id='ACC')

def test_published_accord_is_immutable_and_new_version_is_a_new_record(tmp_path):
    db,target=ready(tmp_path)
    prepare_accord(db,target,contract(),accord_id='ACC')
    assert publish_accord(db,target,'ACC')['status']=='published'
    frozen=accord_status(db,'ACC')
    # Rewriting expected after publication cannot alter the frozen record.
    newer=prepare_accord(db,target,contract(version=2),accord_id='ACC')
    assert newer['version']==2
    historical=accord_status(db,'ACC',version=1)
    assert historical['members']==frozen['members']
    assert historical['target_hash']==frozen['target_hash'] and historical['state']=='published'
    assert [a['version'] for a in list_accords(db,'ACC')]==[1,2]
    changed_version_one=contract(members=[{'consumer':'A','scope':['r1'],'expected':{'r1':'12.00'}}])
    with pytest.raises(ValueError,match='accord_identity_conflict'):
        prepare_accord(db,target,changed_version_one,accord_id='ACC')

def test_repeated_publish_is_idempotent(tmp_path):
    db,target=ready(tmp_path)
    prepare_accord(db,target,contract(),accord_id='ACC')
    first=publish_accord(db,target,'ACC')
    second=publish_accord(db,target,'ACC')
    assert first['status']=='published' and second['status']=='published'
    status=accord_status(db,'ACC')
    assert status['published_at']==accord_status(db,'ACC')['published_at']
    assert sum(1 for m in status['members'] if m['published_at'])==2

def test_route_is_fixed_before_prepare(tmp_path):
    db,target=ready(tmp_path)
    with pytest.raises(ValueError,match='invalid_accord_route'):
        prepare_accord(db,target,{**contract(),'route':'eventual'},accord_id='ACC')

def test_report_reports_consumer_version_consistency(tmp_path):
    db,target=ready(tmp_path)
    prepare_accord(db,target,contract('partitioned'),accord_id='ACC')
    publish_accord(db,target,'ACC')
    data=json.loads(export(db,tmp_path/'report.json').read_text(encoding='utf-8'))
    consumers={c['consumer']:c for c in data['publication']['consumers']}
    assert consumers['A']['consistent'] and consumers['B']['consistent']
    assert data['publication']['accords'][0]['state']=='published'

def test_report_flags_mixed_versions_for_same_consumer(tmp_path):
    db,target=ready(tmp_path)
    prepare_accord(db,target,contract('partitioned',version=1,
        members=[{'consumer':'A','scope':['r1'],'expected':{'r1':'11.00'}}]),accord_id='ACC')
    publish_accord(db,target,'ACC')
    # A new rule run produces a different shared plan and therefore a different
    # frozen snapshot for the same consumer in accord version 2.
    con=connect(db);cfg=json.loads(con.execute('SELECT config_raw FROM batch').fetchone()[0])
    cfg['new_multiplier']='1.20'
    con.execute('UPDATE batch SET config_raw=?',(json.dumps(cfg).encode(),));con.commit();con.close()
    evaluate(db);plan=tmp_path/'plan2.json';action('plan',db,out=plan)
    target2=Path(action('apply',db,sandbox=tmp_path/'target2',out=plan))
    prepare_accord(db,target2,contract('partitioned',version=2,
        members=[{'consumer':'A','scope':['r1'],'expected':{'r1':'12.00'}}]),accord_id='ACC')
    assert publish_accord(db,target2,'ACC')['status']=='published'
    data=json.loads(export(db,tmp_path/'report.json').read_text(encoding='utf-8'))
    a=[c for c in data['publication']['consumers'] if c['consumer']=='A'][0]
    assert not a['consistent'] and a['reason']=='mixed_target_snapshots'
    assert len(a['published_target_hashes'])==2

def test_accord_store_tracks_dependencies_and_snapshot(tmp_path):
    db,target=ready(tmp_path)
    prepare_accord(db,target,abc('partitioned'),accord_id='ACC')
    con=connect(db)
    edges={(r['consumer'],r['depends_on']) for r in con.execute('SELECT * FROM accord_dependency')}
    snapshots={r['subject'] for r in con.execute('SELECT * FROM accord_snapshot')}
    assert edges=={('C','B')}
    assert snapshots=={'r1','r2'}
    con.close()

def test_publish_is_not_allowed_on_target_that_never_existed(tmp_path):
    db=tmp_path/'control.db';initialize(db,FIX/'config.json');load(db,FIX/'events.jsonl');evaluate(db)
    plan=tmp_path/'plan.json';action('plan',db,out=plan)
    missing=tmp_path/'nowhere'/'ledger.db'
    status=prepare_accord(db,missing,contract(),accord_id='ACC')
    assert all(m['readiness']=='uncertain' for m in status['members'])
    assert publish_accord(db,missing,'ACC')['status']=='stale'

def test_consumer_scope_must_cover_expectations(tmp_path):
    db,target=ready(tmp_path)
    with pytest.raises(ValueError,match='consumer_scope_mismatch'):
        prepare_accord(db,target,{**contract(),'members':[
            {'consumer':'A','scope':['r1'],'expected':{'r1':'11.00','r2':'23.00'}}]},accord_id='ACC')

def test_rejected_prepare_leaves_traceable_reason(tmp_path):
    db,target=ready(tmp_path)
    with pytest.raises(ValueError,match='consumer_expectation_conflict'):
        prepare_accord(db,target,contract(members=[
            {'consumer':'A','scope':['r1','r2'],'expected':{'r1':'11.00','r2':'23.00'}},
            {'consumer':'B','scope':['r2'],'expected':{'r2':'99.00'}}]),accord_id='ACC')
    trail=rejections(db,'ACC')
    assert [row['code'] for row in trail]==['consumer_expectation_conflict']

def test_frozen_member_and_snapshot_cannot_be_rewritten_in_store(tmp_path):
    db,target=ready(tmp_path)
    first=prepare_accord(db,target,contract(),accord_id='ACC')
    con=connect(db)
    with pytest.raises(sqlite3.IntegrityError,match='accord_member_frozen'):
        con.execute("UPDATE accord_member SET expected='{\"r1\":\"99.00\"}' WHERE accord_key='ACC:v1' AND consumer='A'")
        con.commit()
    con.rollback()
    with pytest.raises(sqlite3.IntegrityError,match='accord_snapshot_frozen'):
        con.execute("UPDATE accord_snapshot SET value='99.00' WHERE accord_key='ACC:v1'")
        con.commit()
    con.rollback()
    with pytest.raises(sqlite3.IntegrityError,match='accord_identity_frozen'):
        con.execute("DELETE FROM publication_accord WHERE accord_key='ACC:v1'")
        con.commit()
    con.rollback();con.close()
    assert accord_status(db,'ACC',version=1)['member_digest']==first['member_digest']

def test_explicit_version_argument_overrides_contract(tmp_path):
    db,target=ready(tmp_path)
    status=prepare_accord(db,target,contract(version=1),accord_id='ACC',version=3)
    assert status['version']==3 and status['accord_key']=='ACC:v3'

def test_published_agreement_records_later_drift_without_rewriting(tmp_path):
    db,target=ready(tmp_path)
    prepare_accord(db,target,contract(),accord_id='ACC')
    publish_accord(db,target,'ACC')
    frozen=accord_status(db,'ACC')
    con=sqlite3.connect(target);con.execute("UPDATE ledger SET computed='999.00' WHERE id='r1'");con.commit();con.close()
    result=publish_accord(db,target,'ACC')
    assert result['status']=='published'
    assert any('target_drift' in item for item in result['drift'])
    after=accord_status(db,'ACC')
    assert after['state']=='published'
    assert after['members']==frozen['members']
    assert after['target_hash']==frozen['target_hash']

def test_rollback_execution_state_reopens_readiness_and_groups(tmp_path):
    db,target=ready(tmp_path);plan=tmp_path/'plan.json'
    prepare_accord(db,target,contract('partitioned'),accord_id='ACC')
    assert all(m['readiness']=='ready' for m in accord_status(db,'ACC')['members'])
    action('rollback',db,sandbox=tmp_path/'target',out=plan)
    status=recheck_accord(db,target,'ACC')
    assert {m['consumer']:m['readiness'] for m in status['members']}=={'A':'uncertain','B':'uncertain'}
    assert all(g['status']!='ready' for g in status['groups'])
    assert publish_accord(db,target,'ACC')['status']=='stale'

def test_duplicate_identity_rejection_is_traceable(tmp_path):
    db,target=ready(tmp_path)
    bad=contract(members=[{'consumer':'A','scope':['r1'],'expected':{'r1':'11.00'}},
                          {'consumer':'A','scope':['r2'],'expected':{'r2':'23.00'}}])
    with pytest.raises(ValueError,match='duplicate_consumer_identity'):
        prepare_accord(db,target,bad,accord_id='ACC')
    assert [row['code'] for row in rejections(db,'ACC')]==['duplicate_consumer_identity']
    assert list_accords(db)==[]

def test_old_single_consumer_gate_ready_is_not_accord_fact(tmp_path):
    from backfillcomp.publish import check
    db,target=ready(tmp_path)
    expected={'r1':'11.00','r2':'23.00'}
    assert check(db,target,'G1','consumer-A',expected)['status']=='ready'
    # The target is then changed after the gate was recorded.
    con=sqlite3.connect(target);con.execute("UPDATE ledger SET computed='999.00' WHERE id='r2'");con.commit();con.close()
    status=prepare_accord(db,target,contract(),accord_id='ACC')
    assert {m['consumer']:m['readiness'] for m in status['members']}=={'A':'ready','B':'blocked'}
    assert ['target_drift','r2'] in status['members'][1]['reasons']
    # A prepared accord is blocked, not published, even though G1 stayed ready.
    assert publish_accord(db,target,'ACC')['status']=='blocked'
    # The legacy gate remains recorded as ready but never authorises the accord.
    con=connect(db)
    assert con.execute("SELECT status FROM publication_gate WHERE id='G1'").fetchone()[0]=='ready'
    con.close()
