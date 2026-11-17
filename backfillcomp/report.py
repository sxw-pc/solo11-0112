import json
from pathlib import Path
from .store import connect
def _accord_report(db):
    """Published version per consumer plus cross-consumer consistency."""
    rows=db.execute('''SELECT m.consumer,m.published_at,m.published_target_hash,m.readiness,
                              a.accord_id,a.version,a.route,a.state,g.status AS group_status,
                              g.group_key AS group_key
                       FROM accord_member m JOIN publication_accord a USING(accord_key)
                       LEFT JOIN accord_group g ON g.accord_key=m.accord_key
                            AND instr(g.members,'"'||m.consumer||'"')>0
                       ORDER BY a.accord_id,a.version,m.consumer''').fetchall()
    consumers={}
    for row in rows:
        item=consumers.setdefault(row['consumer'],{'consumer':row['consumer'],'published':[],
                                                   'groups':[],'readiness':row['readiness']})
        item['readiness']=row['readiness']
        item['groups'].append({'accord_id':row['accord_id'],'version':row['version'],
                               'group':row['group_key'],'status':row['group_status']})
        if row['published_at']:
            item['published'].append({'accord_id':row['accord_id'],'version':row['version'],
                                      'route':row['route'],'state':row['state'],
                                      'group_status':row['group_status'],
                                      'target_hash':row['published_target_hash']})
    result=[]
    for consumer in sorted(consumers):
        entry=consumers[consumer]
        hashes={x['target_hash'] for x in entry['published']}
        entry['published_target_hashes']=sorted(hashes)
        entry['consistent']=len(hashes)<=1
        entry['reason']=None if len(hashes)<=1 else 'mixed_target_snapshots'
        # Group-level adjudication determines whether the consumer's whole
        # contract is out, even when some earlier version already shipped.
        entry['fully_published']=all(g['status']=='published' for g in entry['groups']) and bool(entry['groups'])
        entry['blocked_groups']=[g for g in entry['groups'] if g['status']!='published']
        result.append(entry)
    accords=[]
    for row in db.execute('SELECT accord_id,version,route,state,target_hash FROM publication_accord ORDER BY accord_id,version'):
        accords.append({'accord_id':row['accord_id'],'version':row['version'],'route':row['route'],
                        'state':row['state'],'target_hash':row['target_hash'],
                        'groups':[{'group':g['group_key'],'members':json.loads(g['members']),
                                   'status':g['status']}
                                  for g in db.execute('SELECT * FROM accord_group WHERE accord_key=? ORDER BY group_key',
                                                      (f'{row["accord_id"]}:v{row["version"]}',))],
                        'members':[{'consumer':m['consumer'],'readiness':m['readiness'],
                                    'published':bool(m['published_at'])}
                                   for m in db.execute('SELECT * FROM accord_member WHERE accord_key=? ORDER BY consumer',
                                                       (f'{row["accord_id"]}:v{row["version"]}',))]})
    return {'consumers':result,'accords':accords}
def export(db_path,out):
    db=connect(db_path);out=Path(out);out.parent.mkdir(parents=True,exist_ok=True)
    run=db.execute('SELECT * FROM run ORDER BY id DESC LIMIT 1').fetchone()
    if not run:raise ValueError('no_run')
    rid=run['id'];data={'run':dict(run),'sources':[{'id':x['id'],'name':x['name'],'digest':x['digest']} for x in db.execute('SELECT * FROM source_file ORDER BY id')],
      'decisions':[dict(x) for x in db.execute('SELECT * FROM decision WHERE run_id=? ORDER BY subject',(rid,))],
      'issues':[dict(x) for x in db.execute('SELECT * FROM issue WHERE run_id=? OR run_id IS NULL ORDER BY id',(rid,))],
      'evidence':[dict(x) for x in db.execute('SELECT * FROM evidence WHERE run_id=? ORDER BY id',(rid,))]}
    data['executions']=[dict(x) for x in db.execute('SELECT * FROM execution ORDER BY id')]
    data['publication']=_accord_report(db)
    out.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
    md=out.with_suffix('.md');md.write_text('# 离线判定报告\n\n'+f'运行 {rid}：{run["status"]}\n\n'+'\n'.join(f'- {x["subject"]}: {x["status"]}，{x["detail"]}' for x in data['decisions'])
        +'\n\n## 消费方发布\n\n'+'\n'.join(
            f'- {x["consumer"]}: readiness {x["readiness"]}，'+
            (f'一致（{len(x["published_target_hashes"])} 个快照）' if x['consistent'] else f'版本不一致（{x["reason"]}）')
            for x in data['publication']['consumers']),encoding='utf-8')
    db.close();return out
def verify(db_path):
    db=connect(db_path);problems=[]
    from .ingest import hash_bytes
    for batch in db.execute('SELECT id,config_raw,config_hash FROM batch'):
        if hash_bytes(batch['config_raw'])!=batch['config_hash']:
            problems.append(f'batch_config:{batch["id"]}')
    sources={s['id']:dict(s) for s in db.execute('SELECT * FROM source_file')}
    for s in sources.values():
        from .ingest import hash_bytes
        if hash_bytes(s['raw'])!=s['digest']:problems.append(f'source:{s["id"]}')
    events={e['id']:dict(e) for e in db.execute('SELECT * FROM raw_event')}
    for e in events.values():
        try:
            lines=sources[e['source_id']]['raw'].decode('utf-8-sig').splitlines()
            original=lines[e['line_no']-1]
            if hash_bytes(original.encode('utf-8'))!=e['digest'] or json.loads(original)!=json.loads(e['payload']):
                problems.append(f'event:{e["id"]}')
        except (KeyError,IndexError,UnicodeError,ValueError):
            problems.append(f'event:{e["id"]}')
    for ev in db.execute('SELECT * FROM evidence WHERE event_id IS NOT NULL'):
        source_event=events.get(ev['event_id'])
        if not source_event or source_event['source_id']!=ev['source_id']:
            problems.append(f'evidence:{ev["id"]}')
    for d in db.execute('SELECT * FROM decision'):
        if not db.execute('SELECT 1 FROM evidence WHERE run_id=? AND subject=?',(d['run_id'],d['subject'])).fetchone():problems.append(f'decision:{d["id"]}')
        if db.execute('SELECT 1 FROM raw_event WHERE subject=?',(d['subject'],)).fetchone() and not db.execute(
            'SELECT 1 FROM evidence WHERE run_id=? AND subject=? AND event_id IS NOT NULL',(d['run_id'],d['subject'])).fetchone():
            problems.append(f'decision_source:{d["id"]}')
    db.close();return problems
