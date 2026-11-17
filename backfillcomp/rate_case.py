"""Freeze an evaluated correction plan and synthetic rate-risk contract."""
import hashlib,json,sqlite3
from pathlib import Path
from .rules import _cfg,_snapshot
from .store import connect

def register(db_path,case_id,fixture):
    if not case_id:raise ValueError('missing_rate_case_id')
    source=Path(fixture).resolve();raw=source.read_bytes();doc=json.loads(raw)
    if not doc.get('subjects') or not doc.get('rate_bounds') or not doc.get('models'):
        raise ValueError('bad_rate_contract')
    db=connect(db_path)
    try:
        with db:
            run=db.execute('SELECT id,status FROM run ORDER BY id DESC LIMIT 1').fetchone()
            if not run or run['status']!='已判定':raise ValueError('rate_run_not_ready')
            cfg=_cfg(db);target=sqlite3.connect(_snapshot(cfg));target.row_factory=sqlite3.Row
            try:rows={r['id']:dict(r) for r in target.execute('SELECT * FROM ledger')}
            finally:target.close()
            decisions=[]
            for subject in doc['subjects']:
                decision=db.execute('SELECT status,detail FROM decision WHERE run_id=? AND subject=?',(run['id'],subject)).fetchone()
                if not decision or decision['status']!='可补偿' or subject not in rows:
                    raise ValueError('rate_subject_not_ready')
                decisions.append((subject,decision['detail'],rows[subject]))
            payload=json.dumps({'fixture':str(source),'digest':hashlib.sha256(raw).hexdigest(),
                                'doc':doc,'run_id':run['id'],
                                'config_hash':hashlib.sha256(json.dumps(cfg,sort_keys=True).encode()).hexdigest(),
                                'decisions':decisions},sort_keys=True,separators=(',',':'))
            old=db.execute('SELECT payload FROM rate_case WHERE id=?',(case_id,)).fetchone()
            if old and old['payload']!=payload:raise ValueError('rate_case_conflict')
            db.execute('INSERT OR IGNORE INTO rate_case VALUES (?,?)',(case_id,payload))
    finally:db.close()
    return {'case_id':case_id,'digest':hashlib.sha256(raw).hexdigest(),'subjects':len(decisions)}
