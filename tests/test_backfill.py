import json,sqlite3
from pathlib import Path
import pytest
from backfillcomp.ingest import initialize,load
from backfillcomp.rules import evaluate,action,_money,verify_target
from backfillcomp.report import export,verify
from backfillcomp.store import connect
FIX=Path(__file__).resolve().parents[1]/'fixtures'
def setup(tmp):
    db=tmp/'x.db';initialize(db,FIX/'config.json');load(db,FIX/'events.jsonl');return db
def test_demo(tmp_path):assert evaluate(setup(tmp_path))=={'r1':'可补偿','r2':'可补偿','r3':'待核'}
def test_money_round():assert _money('1.005','1.00')=='1.01'
def test_idempotent(tmp_path):assert load(setup(tmp_path),FIX/'events.jsonl')==0
def test_drift_detected(tmp_path,monkeypatch):
    db=setup(tmp_path);snapshot=tmp_path/'bad.db';import shutil;shutil.copy2(FIX/'ledger.db',snapshot)
    con=sqlite3.connect(snapshot);con.execute("UPDATE ledger SET computed='99.00' WHERE id='r1'");con.commit();con.close()
    monkeypatch.setattr('backfillcomp.rules._snapshot',lambda cfg:snapshot)
    assert evaluate(db)['r1']=='待核'
def test_dependency_cycle(tmp_path):
    db=setup(tmp_path);con=connect(db);cfg=json.loads(con.execute('SELECT config_raw FROM batch').fetchone()[0]);cfg['dependencies']['r1']=['r2'];con.execute('UPDATE batch SET config_raw=?',(json.dumps(cfg).encode(),));con.commit();con.close()
    assert evaluate(db)['r1']=='待核'
def test_deterministic_plan_and_apply_rollback(tmp_path):
    db=setup(tmp_path);evaluate(db);p=tmp_path/'plan.json';action('plan',db,out=p);assert len(json.loads(p.read_text()))==2
    target=action('apply',db,sandbox=tmp_path/'sandbox',out=p)
    con=sqlite3.connect(target);assert con.execute("SELECT computed FROM ledger WHERE id='r1'").fetchone()[0]=='11.00';con.close()
    assert all(x['status']=='已应用' for x in verify_target(db,tmp_path/'sandbox'))
    action('apply',db,sandbox=tmp_path/'sandbox',out=p)
    action('rollback',db,sandbox=tmp_path/'sandbox',out=p)
    con=sqlite3.connect(target);assert con.execute("SELECT computed FROM ledger WHERE id='r1'").fetchone()[0]=='10.00';con.close()
def test_apply_atomic_on_conflict(tmp_path):
    db=setup(tmp_path);evaluate(db);p=tmp_path/'plan.json';action('plan',db,out=p)
    target=tmp_path/'sandbox'/'ledger.db';target.parent.mkdir();import shutil;shutil.copy2(FIX/'ledger.db',target)
    con=sqlite3.connect(target);con.execute("UPDATE ledger SET computed='99.00' WHERE id='r2'");con.commit();con.close()
    with pytest.raises(ValueError,match='optimistic_conflict'):action('apply',db,sandbox=target.parent,out=p)
    con=sqlite3.connect(target);assert con.execute("SELECT computed FROM ledger WHERE id='r1'").fetchone()[0]=='10.00';con.close()
def test_report_verify(tmp_path):
    db=setup(tmp_path);evaluate(db);p=export(db,tmp_path/'report.json');assert p.with_suffix('.md').exists() and verify(db)==[]
def test_downstream_delta_and_order(tmp_path):
    db=setup(tmp_path);evaluate(db);p=tmp_path/'p.json';action('plan',db,out=p);items=json.loads(p.read_text());assert [x['subject'] for x in items]==['r1','r2'];assert items[1]['new']=='23.00'
def test_missing_dependency_blocks(tmp_path):
    db=setup(tmp_path);con=connect(db);cfg=json.loads(con.execute('SELECT config_raw FROM batch').fetchone()[0]);cfg['dependencies']['r2']=['missing'];con.execute('UPDATE batch SET config_raw=?',(json.dumps(cfg).encode(),));con.commit();con.close()
    assert evaluate(db)['r2']=='待核'
def test_no_change_multiplier(tmp_path):
    db=setup(tmp_path);con=connect(db);cfg=json.loads(con.execute('SELECT config_raw FROM batch').fetchone()[0]);cfg['new_multiplier']='1.00';con.execute('UPDATE batch SET config_raw=?',(json.dumps(cfg).encode(),));con.commit();con.close()
    assert evaluate(db)['r1']=='不变'
def test_external_snapshot_option(tmp_path):
    db=setup(tmp_path);import shutil;snap=tmp_path/'other.db';shutil.copy2(FIX/'ledger.db',snap)
    con=connect(db);cfg=json.loads(con.execute('SELECT config_raw FROM batch').fetchone()[0]);cfg['snapshot_path']=str(snap);con.execute('UPDATE batch SET config_raw=?',(json.dumps(cfg).encode(),));con.commit();con.close()
    assert evaluate(db)['r1']=='可补偿'
def test_rollback_rejects_later_change(tmp_path):
    db=setup(tmp_path);evaluate(db);p=tmp_path/'p.json';action('plan',db,out=p);target=action('apply',db,sandbox=tmp_path/'sandbox',out=p)
    con=sqlite3.connect(target);con.execute("UPDATE ledger SET computed='99.00' WHERE id='r2'");con.commit();con.close()
    with pytest.raises(ValueError,match='optimistic_conflict'):action('rollback',db,sandbox=tmp_path/'sandbox',out=p)
def test_plan_tamper_blocked(tmp_path):
    db=setup(tmp_path);evaluate(db);p=tmp_path/'p.json';action('plan',db,out=p);items=json.loads(p.read_text());items[0]['new']='999.00';p.write_text(json.dumps(items))
    with pytest.raises(ValueError,match='plan_tampered'):action('apply',db,sandbox=tmp_path/'sandbox',out=p)
def test_stale_plan_cannot_apply_after_new_decision(tmp_path):
    db=setup(tmp_path);evaluate(db);p=tmp_path/'p.json';action('plan',db,out=p)
    evaluate(db)
    with pytest.raises(ValueError,match='plan_tampered_or_stale'):
        action('apply',db,sandbox=tmp_path/'sandbox',out=p)
    assert not (tmp_path/'sandbox'/'ledger.db').exists()

def test_rollback_requires_target_transaction_proof(tmp_path):
    db=setup(tmp_path);evaluate(db);p=tmp_path/'p.json';action('plan',db,out=p)
    target=tmp_path/'sandbox'/'ledger.db';target.parent.mkdir()
    import shutil;shutil.copy2(FIX/'ledger.db',target)
    with pytest.raises(ValueError,match='rollback_without_applied_plan'):
        action('rollback',db,sandbox=target.parent,out=p)
    assert sqlite3.connect(target).execute("SELECT computed FROM ledger WHERE id='r1'").fetchone()[0]=='10.00'
    action('apply',db,sandbox=target.parent,out=p)
    con=sqlite3.connect(target);assert con.execute("SELECT count(*) FROM backfill_audit WHERE state='applied'").fetchone()[0]==2;con.close()
    action('rollback',db,sandbox=target.parent,out=p)
    action('rollback',db,sandbox=target.parent,out=p)
def test_verify_target_states(tmp_path):
    db=setup(tmp_path);evaluate(db);import shutil;root=tmp_path/'sandbox';root.mkdir();target=root/'ledger.db';shutil.copy2(FIX/'ledger.db',target)
    assert all(x['status']=='未应用' for x in verify_target(db,root))
    con=sqlite3.connect(target);con.execute("UPDATE ledger SET computed='99.00' WHERE id='r1'");con.commit();con.close()
    assert any(x['status']=='不确定' for x in verify_target(db,root))

def test_matching_value_without_plan_audit_is_not_proof(tmp_path):
    db=setup(tmp_path);evaluate(db);p=tmp_path/'p.json';action('plan',db,out=p)
    import shutil;root=tmp_path/'sandbox';root.mkdir();target=root/'ledger.db';shutil.copy2(FIX/'ledger.db',target)
    planned={x['subject']:x for x in json.loads(p.read_text(encoding='utf-8'))}
    con=sqlite3.connect(target);con.execute('UPDATE ledger SET computed=? WHERE id=?',(planned['r1']['new'],'r1'));con.commit();con.close()
    result={x['subject']:x['status'] for x in verify_target(db,root)}
    assert result['r1']=='不确定'
    with pytest.raises(ValueError,match='untracked_applied_value'):
        action('apply',db,sandbox=root,out=p)
