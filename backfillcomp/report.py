import json
from pathlib import Path
from .store import connect
def repair_report(db_path,out=None):
    """展示目标与控制两侧证据、恢复路线、每行最终状态及阻断原因。"""
    db=connect(db_path)
    cases=[dict(x) for x in db.execute('SELECT * FROM repair_case ORDER BY created_at,id')]
    for case in cases:
        case['evidence']=json.loads(case['evidence'])
        case['blockers']=json.loads(case['blockers'])
        case['rows']=[dict(x) for x in db.execute(
            'SELECT * FROM repair_row WHERE case_id=? ORDER BY subject',(case['id'],))]
    db.close()
    data={'repair_cases':cases}
    if out:
        out=Path(out);out.parent.mkdir(parents=True,exist_ok=True)
        out.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
        lines=['# 中断恢复裁定报告','']
        for case in cases:
            ev=case['evidence']
            lines += [f"## 案例 {case['id']}：{case['status']}",
                      f"- 计划摘要：{case['plan_digest'][:12]}…，run {case['run_id']}，证据哈希 {case['evidence_hash'][:12]}…",
                      f"- 目标库：{ev['target_path']}，冻结于 {ev['frozen_at']}",
                      f"- 可选路线：{ev['routes']}，选定路线：{case['route'] or '未选择'}",
                      f"- 阻断原因：{case['blockers'] or '无'}",
                      '- 逐行最终状态：']
            for row in case['rows']:
                lines.append(f"  - {row['subject']}：{row['row_state']}，实际 {row['actual_value']}，审计 {row['audit_state']}（期望 {row['expected_old']} → {row['expected_new']}）")
            lines.append('')
        out.with_suffix('.md').write_text('\n'.join(lines),encoding='utf-8')
    return data
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
