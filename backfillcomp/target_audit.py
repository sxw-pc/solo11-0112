"""Freeze a synthetic independent business-review draw of a target ledger."""
import hashlib,json,sqlite3
from pathlib import Path
from .store import connect

def register(db_path,target_path,case_id,fixture):
    if not case_id:raise ValueError('missing_audit_case_id')
    source=Path(fixture).resolve();raw=source.read_bytes();doc=json.loads(raw)
    target=sqlite3.connect(target_path);target.row_factory=sqlite3.Row
    try:
        rows={r['id']:dict(r) for r in target.execute('SELECT * FROM ledger')}
        target_audits=[dict(r) for r in target.execute('SELECT * FROM backfill_audit')]
    finally:target.close()
    if len(rows)!=doc['population_size'] or not target_audits:
        raise ValueError('bad_audit_population')
    sampled=sorted(rows,key=lambda rid:hashlib.sha256(f"{doc['draw_seed']}|{rid}".encode()).hexdigest())[:doc['draw_count']]
    reviews=doc['manual_reviews']
    if set(sampled)!=set(reviews) or any(reviews[rid]!='correct' for rid in sampled):
        raise ValueError('audit_draw_or_review_mismatch')
    db=connect(db_path)
    try:
        with db:
            if not db.execute("SELECT 1 FROM execution WHERE action='apply' LIMIT 1").fetchone():
                raise ValueError('target_not_applied')
            payload=json.dumps({'fixture':str(source),'digest':hashlib.sha256(raw).hexdigest(),
                                'doc':doc,'target_rows':rows,'target_audits':target_audits,
                                'sampled':sampled},sort_keys=True,separators=(',',':'))
            old=db.execute('SELECT payload FROM target_audit_case WHERE id=?',(case_id,)).fetchone()
            if old and old['payload']!=payload:raise ValueError('target_audit_case_conflict')
            db.execute('INSERT OR IGNORE INTO target_audit_case VALUES (?,?)',(case_id,payload))
    finally:db.close()
    return {'case_id':case_id,'digest':hashlib.sha256(raw).hexdigest(),
            'population_size':len(rows),'sampled':sampled}
