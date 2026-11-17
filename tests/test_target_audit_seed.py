from pathlib import Path
import sqlite3
from backfillcomp.ingest import initialize,load
from backfillcomp.rules import evaluate,action
from backfillcomp.target_audit import register
from backfillcomp.store import connect
import pytest

FIXTURES=Path(__file__).resolve().parents[1]/'fixtures'

def ready(tmp_path):
    db=tmp_path/'control.db';initialize(db,FIXTURES/'config.json');load(db,FIXTURES/'events.jsonl');evaluate(db)
    plan=tmp_path/'plan.json';action('plan',db,out=plan)
    target=Path(action('apply',db,sandbox=tmp_path/'sandbox',out=plan))
    with sqlite3.connect(target) as conn:
        for i in range(4,10):conn.execute('INSERT INTO ledger VALUES (?,?,?,?)',(f'r{i}','2026-01','10.00','10.00'))
        conn.execute("INSERT INTO ledger VALUES ('r10','2026-01','100.00','100.00')")
    return db,target

def test_audit_case_freezes_real_target_and_deterministic_draw(tmp_path):
    db,target=ready(tmp_path)
    got=register(db,target,'audit-01',FIXTURES/'target-audit.json')
    assert got['population_size']==10 and set(got['sampled'])=={f'r{i}' for i in range(1,9)}
    assert register(db,target,'audit-01',FIXTURES/'target-audit.json')==got
    with connect(db) as conn:assert conn.execute('SELECT COUNT(*) FROM target_audit_case').fetchone()[0]==1

def test_audit_case_rejects_target_without_applied_plan(tmp_path):
    db=tmp_path/'empty.db';target=tmp_path/'empty-target.db'
    with sqlite3.connect(target) as conn:
        conn.execute('CREATE TABLE ledger(id TEXT,partition TEXT,amount TEXT,computed TEXT)')
        conn.execute('CREATE TABLE backfill_audit(subject TEXT,plan_digest TEXT,state TEXT,before_value TEXT,after_value TEXT)')
    with pytest.raises(ValueError,match='bad_audit_population'):
        register(db,target,'audit-01',FIXTURES/'target-audit.json')
