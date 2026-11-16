import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from backfillcomp.ingest import initialize,load
from backfillcomp.rules import evaluate,action
from backfillcomp.schedule import define_window,reserve,activate,release
from backfillcomp.publish import check,publish

FIX=Path(__file__).resolve().parent.parent/'fixtures'

def ready(tmp_path):
    db=tmp_path/'control.db';initialize(db,FIX/'config.json');load(db,FIX/'events.jsonl');evaluate(db)
    plan=tmp_path/'plan.json';action('plan',db,out=plan)
    digest=hashlib.sha256(plan.read_bytes()).hexdigest()
    return db,plan,digest

def test_capacity_contention_changes_who_may_execute(tmp_path):
    db,plan,digest=ready(tmp_path)
    define_window(db,'W1','2020-01-01T00:00:00Z','2099-01-01T00:00:00Z',2)
    assert reserve(db,'L1',digest,'W1','worker-A',2)['state']=='reserved'
    assert reserve(db,'L2',digest,'W1','worker-B',1)['state']=='waiting'
    with pytest.raises(ValueError,match='lease_not_reserved'):activate(db,'L2')
    assert activate(db,'L1')['state']=='active'
    with pytest.raises(ValueError,match='active_lease_required'):
        action('apply',db,sandbox=tmp_path/'target',out=plan)
    action('apply',db,sandbox=tmp_path/'target',out=plan,lease='L1')
    assert reserve(db,'L2',digest,'W1','worker-B',1)['state']=='reserved'

def test_stale_plan_blocks_lease_activation(tmp_path):
    db,plan,digest=ready(tmp_path)
    define_window(db,'W1','2020-01-01T00:00:00Z','2099-01-01T00:00:00Z',1)
    reserve(db,'L1',digest,'W1','worker-A',1)
    evaluate(db)
    assert activate(db,'L1')['state']=='blocked'

def test_future_window_does_not_grant_capacity(tmp_path):
    db,_,digest=ready(tmp_path)
    define_window(db,'future','2090-01-01T00:00:00Z','2099-01-01T00:00:00Z',1)
    with pytest.raises(ValueError,match='window_not_active'):
        reserve(db,'L1',digest,'future','worker-A',1)

def test_publication_requires_applied_audit_and_consumer_parity(tmp_path):
    db,plan,_=ready(tmp_path)
    target=tmp_path/'target'/'ledger.db'
    expected=json.loads((FIX/'consumer-expected.json').read_text(encoding='utf-8'))
    assert check(db,target,'G0','consumer-A',expected)['status']=='uncertain'
    action('apply',db,sandbox=target.parent,out=plan)
    assert check(db,target,'G1','consumer-A',expected)['status']=='ready'
    assert publish(db,target,'G1')['status']=='published'
    assert check(db,target,'G2','consumer-A',{'r1':'11.00','r2':'22.00'})['status']=='blocked'

def test_publication_rechecks_target_at_commit(tmp_path):
    db,plan,_=ready(tmp_path)
    target=Path(action('apply',db,sandbox=tmp_path/'target',out=plan))
    expected={'r1':'11.00','r2':'23.00'}
    assert check(db,target,'G1','consumer-A',expected)['status']=='ready'
    con=sqlite3.connect(target);con.execute("UPDATE ledger SET computed='999.00' WHERE id='r2'");con.commit();con.close()
    assert publish(db,target,'G1')['status']=='stale'
