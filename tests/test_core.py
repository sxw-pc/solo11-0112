import hashlib,json
from pathlib import Path
from backfillcomp.ingest import initialize,load
from backfillcomp.rules import evaluate
from backfillcomp.store import connect
from backfillcomp.report import export,verify
FIX=Path(__file__).resolve().parents[1]/'fixtures'
def setup(tmp):
    db=tmp/'core.db';initialize(db,FIX/'config.json');load(db,FIX/'events.jsonl');return db
def test_raw_bytes_and_hash(tmp_path):
    db=setup(tmp_path);row=connect(db).execute('SELECT raw,digest FROM source_file LIMIT 1').fetchone();assert hashlib.sha256(row['raw']).hexdigest()==row['digest']
def test_bad_line_keeps_evidence(tmp_path):
    db=setup(tmp_path);p=tmp_path/'bad.jsonl';p.write_text('{bad json',encoding='utf-8');assert load(db,p)==0
    assert connect(db).execute("SELECT count(*) FROM issue WHERE code='parse_error'").fetchone()[0]==1
def test_run_decision_evidence_transaction(tmp_path):
    db=setup(tmp_path);evaluate(db);con=connect(db);rid=con.execute('SELECT max(id) FROM run').fetchone()[0]
    assert con.execute('SELECT count(*) FROM decision WHERE run_id=?',(rid,)).fetchone()[0]>0
    assert con.execute('SELECT count(*) FROM evidence WHERE run_id=?',(rid,)).fetchone()[0]>0
    assert con.execute('SELECT status FROM run WHERE id=?',(rid,)).fetchone()[0] in ('已判定','待核')
def test_report_references_sources(tmp_path):
    db=setup(tmp_path);evaluate(db);p=export(db,tmp_path/'r.json');data=json.loads(p.read_text(encoding='utf-8'));assert data['sources'] and data['decisions'] and not verify(db)
