from pathlib import Path
import json,sqlite3
from backfillcomp.ingest import initialize,load
from backfillcomp.rate_case import register
from backfillcomp.rules import evaluate
from backfillcomp.store import connect
import pytest

FIXTURES=Path(__file__).resolve().parents[1]/'fixtures'

def ready(tmp_path):
    snapshot=tmp_path/'ledger.db'
    with sqlite3.connect(snapshot) as target:target.executescript((FIXTURES/'rate-ledger.sql').read_text())
    cfg=json.loads((FIXTURES/'rate-config.json').read_text());cfg['snapshot_path']=str(snapshot)
    config=tmp_path/'config.json';config.write_text(json.dumps(cfg));db=tmp_path/'control.db'
    initialize(db,config);assert load(db,FIXTURES/'rate-events.jsonl')==2
    assert evaluate(db)=={'r1':'可补偿','r2':'可补偿'}
    return db

def test_freezes_evaluated_two_row_case_without_envelope(tmp_path):
    db=ready(tmp_path);got=register(db,'rate-01',FIXTURES/'rate-case.json')
    assert got['subjects']==2
    assert register(db,'rate-01',FIXTURES/'rate-case.json')==got
    with connect(db) as conn:
        assert conn.execute('SELECT COUNT(*) FROM rate_case').fetchone()[0]==1
        assert conn.execute('SELECT COUNT(*) FROM decision').fetchone()[0]==2

def test_unevaluated_case_rejected(tmp_path):
    db=tmp_path/'empty.db'
    with pytest.raises(ValueError,match='rate_run_not_ready'):register(db,'rate-01',FIXTURES/'rate-case.json')
