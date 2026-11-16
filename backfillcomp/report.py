import json
from pathlib import Path
from .store import connect
def export(db_path,out):
    db=connect(db_path);out=Path(out);out.parent.mkdir(parents=True,exist_ok=True)
    run=db.execute('SELECT * FROM run ORDER BY id DESC LIMIT 1').fetchone()
    if not run:raise ValueError('no_run')
    rid=run['id'];data={'run':dict(run),'sources':[{'id':x['id'],'name':x['name'],'digest':x['digest']} for x in db.execute('SELECT * FROM source_file ORDER BY id')],
      'decisions':[dict(x) for x in db.execute('SELECT * FROM decision WHERE run_id=? ORDER BY subject',(rid,))],
      'issues':[dict(x) for x in db.execute('SELECT * FROM issue WHERE run_id=? OR run_id IS NULL ORDER BY id',(rid,))],
      'evidence':[dict(x) for x in db.execute('SELECT * FROM evidence WHERE run_id=? ORDER BY id',(rid,))]}
    data['executions']=[dict(x) for x in db.execute('SELECT * FROM execution ORDER BY id')]
    out.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
    md=out.with_suffix('.md');md.write_text('# 离线判定报告\n\n'+f'运行 {rid}：{run["status"]}\n\n'+'\n'.join(f'- {x["subject"]}: {x["status"]}，{x["detail"]}' for x in data['decisions']),encoding='utf-8')
    db.close();return out
def verify(db_path):
    db=connect(db_path);problems=[]
    for s in db.execute('SELECT * FROM source_file'):
        from .ingest import hash_bytes
        if hash_bytes(s['raw'])!=s['digest']:problems.append(f'source:{s["id"]}')
    for d in db.execute('SELECT * FROM decision'):
        if not db.execute('SELECT 1 FROM evidence WHERE run_id=? AND subject=?',(d['run_id'],d['subject'])).fetchone():problems.append(f'decision:{d["id"]}')
    db.close();return problems
