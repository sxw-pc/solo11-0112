import hashlib,json
from pathlib import Path
from .store import connect
def hash_bytes(raw):return hashlib.sha256(raw).hexdigest()
def initialize(db_path,config_path):
    raw=Path(config_path).read_bytes();config=json.loads(raw);db=connect(db_path)
    bid=config['batch_id'];digest=hash_bytes(raw)
    with db:
        row=db.execute('SELECT config_hash FROM batch WHERE id=?',(bid,)).fetchone()
        if row and row['config_hash']!=digest:raise ValueError('batch_config_conflict')
        db.execute('INSERT OR IGNORE INTO batch VALUES (?,?,?)',(bid,digest,raw))
        for key in config['subjects']:
            db.execute('INSERT OR IGNORE INTO subject VALUES (?,?)',(key,bid))
        rule=config.get('rule_version','v1')
        db.execute('INSERT OR IGNORE INTO rule_version VALUES (?,?,?)',(rule,json.dumps(config,sort_keys=True),digest))
    db.close();return bid
def load(db_path,path):
    raw=Path(path).read_bytes();db=connect(db_path)
    batch=db.execute('SELECT id FROM batch LIMIT 1').fetchone()
    if not batch:raise ValueError('not_initialized')
    if db.execute('SELECT 1 FROM source_file WHERE batch_id=? AND digest=?',(batch['id'],hash_bytes(raw))).fetchone():return 0
    count=0
    with db:
        sid=db.execute('INSERT INTO source_file(batch_id,name,digest,raw) VALUES (?,?,?,?)',(batch['id'],str(path),hash_bytes(raw),raw)).lastrowid
        for line_no,line in enumerate(raw.decode('utf-8-sig').splitlines(),1):
            try:
                item=json.loads(line);key=str(item['event_key']);subject=str(item['subject']);kind=str(item['kind'])
                if not db.execute('SELECT 1 FROM subject WHERE id=?',(subject,)).fetchone():raise ValueError('unknown_subject')
                digest=hash_bytes(line.encode());old=db.execute('SELECT digest FROM raw_event WHERE event_key=?',(key,)).fetchone()
                if old:
                    if old['digest']!=digest:db.execute('INSERT INTO issue(source_id,line_no,subject,code,detail) VALUES (?,?,?,?,?)',(sid,line_no,subject,'identity_conflict',key))
                    continue
                db.execute('INSERT INTO raw_event(source_id,line_no,event_key,subject,kind,at,payload,digest) VALUES (?,?,?,?,?,?,?,?)',(sid,line_no,key,subject,kind,item.get('at'),json.dumps(item,ensure_ascii=False),digest));count+=1
            except (ValueError,KeyError,TypeError) as exc:
                db.execute('INSERT INTO issue(source_id,line_no,code,detail) VALUES (?,?,?,?)',(sid,line_no,'parse_error',str(exc)))
    db.close();return count
