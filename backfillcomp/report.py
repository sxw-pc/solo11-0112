import json,sqlite3
from decimal import Decimal,ROUND_HALF_UP,ROUND_DOWN
from pathlib import Path
from .store import connect
CENT=Decimal('0.01')
ZERO=Decimal('0.00')
def _cents(value):return str(value+ZERO)
def _money(value,multiplier):return str((Decimal(str(value))*Decimal(str(multiplier))).quantize(CENT,rounding=ROUND_HALF_UP))
def _snapshot(cfg):
    if cfg and cfg.get('snapshot_path'):return Path(cfg['snapshot_path']).resolve()
    return Path(__file__).resolve().parent.parent/'fixtures'/'ledger.db'
def export(db_path,out):
    db=connect(db_path);out=Path(out);out.parent.mkdir(parents=True,exist_ok=True)
    run=db.execute('SELECT * FROM run ORDER BY id DESC LIMIT 1').fetchone()
    if not run:raise ValueError('no_run')
    rid=run['id'];data={'run':dict(run),'sources':[{'id':x['id'],'name':x['name'],'digest':x['digest']} for x in db.execute('SELECT * FROM source_file ORDER BY id')],
      'decisions':[dict(x) for x in db.execute('SELECT * FROM decision WHERE run_id=? ORDER BY subject',(rid,))],
      'issues':[dict(x) for x in db.execute('SELECT * FROM issue WHERE run_id=? OR run_id IS NULL ORDER BY id',(rid,))],
      'evidence':[dict(x) for x in db.execute('SELECT * FROM evidence WHERE run_id=? ORDER BY id',(rid,))]}
    data['allocations']={'contracts':[dict(x) for x in db.execute('SELECT * FROM allocation_contract WHERE run_id=? ORDER BY source',(rid,))],
      'shares':[dict(x) for x in db.execute('SELECT * FROM allocation_share WHERE run_id=? ORDER BY source,parent,child',(rid,))],
      'carryovers':[dict(x) for x in db.execute('SELECT * FROM carryover_balance WHERE run_id=? ORDER BY source',(rid,))]}
    data['executions']=[dict(x) for x in db.execute('SELECT * FROM execution ORDER BY id')]
    out.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
    md=out.with_suffix('.md');md.write_text('# 离线判定报告\n\n'+f'运行 {rid}：{run["status"]}\n\n'
      +'\n'.join(f'- {x["subject"]}: {x["status"]}，{x["detail"]}' for x in data['decisions'])
      +'\n'.join(f'- 待结转余额 {x["source"]}: {x["amount"]}（{x["state"]}）' for x in data['allocations']['carryovers']),encoding='utf-8')
    db.close();return out
def contracts_state(db_path):
    db=connect(db_path)
    run=db.execute('SELECT * FROM run ORDER BY id DESC LIMIT 1').fetchone()
    if not run:raise ValueError('no_run')
    rid=run['id']
    state={'run':rid,'status':run['status'],
      'contracts':[dict(x) for x in db.execute('SELECT * FROM allocation_contract WHERE run_id=? ORDER BY source',(rid,))],
      'shares':[dict(x) for x in db.execute('SELECT * FROM allocation_share WHERE run_id=? ORDER BY source,parent,child',(rid,))],
      'carryovers':[dict(x) for x in db.execute('SELECT * FROM carryover_balance WHERE run_id=? ORDER BY source',(rid,))]}
    db.close();return state
def _recompute(db):
    """Independently recompute every allocation contribution and remainder from
    source revision events and allocation edges; never trusts stored details."""
    cfg=json.loads(db.execute('SELECT config_raw FROM batch LIMIT 1').fetchone()[0])
    con=sqlite3.connect(_snapshot(cfg));con.row_factory=sqlite3.Row
    rows={r['id']:dict(r) for r in con.execute('SELECT * FROM ledger')};con.close()
    revisions={}
    for event in db.execute("SELECT id,subject,event_key,payload FROM raw_event WHERE kind='correction'"):
        if json.loads(event['payload']).get('reason')=='rule_revision':revisions.setdefault(event['subject'],[]).append(dict(event))
    subjects=cfg['subjects'];old={};new={};ok={}
    for key in subjects:
        row=rows.get(key);fine=True
        if len(revisions.get(key,[]))!=1:fine=False
        if not row:fine=False
        else:
            old[key]=_money(row['amount'],cfg['old_multiplier']);new[key]=_money(row['amount'],cfg['new_multiplier'])
            if row['computed']!=old[key] or row['partition'] not in cfg['ranges']:fine=False
        ok[key]=fine
    raw=cfg.get('allocations') or []
    if isinstance(raw,dict):raw=[{'source':k,**v} for k,v in raw.items()]
    contracts=[];seen_sources=set()
    for item in raw:
        source=item.get('source');problems=[]
        contribution=item.get('contribution','once');remainder=item.get('remainder','priority')
        if contribution not in ('once','each'):problems.append('contribution')
        if remainder not in ('priority','carryover'):problems.append('remainder')
        if not source or source not in subjects:problems.append('source')
        if source in seen_sources:problems.append('conflict')
        seen_sources.add(source)
        revs=revisions.get(source,[]) if source else []
        if item.get('event_key') and (len(revs)!=1 or revs[0]['event_key']!=item['event_key']):problems.append('event')
        edges=item.get('edges') or [];total=Decimal('0');weight_bad=False;seen_edges=set()
        for edge in edges:
            parent=edge.get('parent',source);child=edge.get('child')
            if parent!=source:problems.append('parent')
            if not child or child not in subjects:problems.append('child')
            if (parent,child) in seen_edges:problems.append('duplicate')
            seen_edges.add((parent,child))
            weight=edge.get('weight')
            if weight is None:
                if contribution=='once':weight_bad=True;problems.append('weight')
            else:
                try:
                    parsed=Decimal(str(weight))
                    if parsed<0:weight_bad=True;problems.append('weight')
                    total+=parsed
                except ArithmeticError:weight_bad=True;problems.append('weight')
            parts=edge.get('partitions')
            if parts is not None and child in rows and rows[child]['partition'] not in parts:problems.append('partition')
        if contribution=='once' and not weight_bad and total!=Decimal('1'):problems.append('total')
        contracts.append({'source':source,'contribution':contribution,'remainder':remainder,'edges':edges,'problems':problems})
    for contract in contracts:
        if contract['problems']:
            if contract['source'] in ok:ok[contract['source']]=False
            for edge in contract['edges']:
                if edge.get('child') in ok:ok[edge['child']]=False
    deps=cfg.get('dependencies') or {};graph={}
    for child,parents in deps.items():graph.setdefault(child,[]).extend(parents)
    for contract in contracts:
        for edge in contract['edges']:
            child=edge.get('child');parent=edge.get('parent') or contract['source']
            if child in subjects and parent:graph.setdefault(child,[]).append(parent)
    visiting=set();visited=set();cycles=set()
    def visit(node):
        if node in visiting:cycles.update(visiting);return
        if node in visited:return
        visiting.add(node)
        for parent in graph.get(node,[]):visit(parent)
        visiting.remove(node);visited.add(node)
    for node in graph:visit(node)
    for node in cycles:
        if node in ok:ok[node]=False
    for child,parents in deps.items():
        for parent in parents:
            if parent not in rows and child in ok:ok[child]=False
    shares=[];carryovers=[];once_sources=set()
    for contract in contracts:
        if contract['problems']:continue
        source=contract['source']
        if not ok.get(source):
            for edge in contract['edges']:
                if edge.get('child') in ok:ok[edge['child']]=False
            continue
        total=Decimal(new[source])-Decimal(old[source]);edges=contract['edges']
        if contract['contribution']=='once':
            once_sources.add(source)
            amounts=[(total*Decimal(str(edge.get('weight')))).quantize(CENT,rounding=ROUND_DOWN) for edge in edges]
            rest=total-sum(amounts,Decimal('0'))
            if rest and contract['remainder']=='priority':
                top=min(range(len(edges)),key=lambda i:(edges[i].get('priority') if edges[i].get('priority') is not None else 1<<30,str(edges[i].get('child'))))
                amounts[top]+=rest
            if rest and contract['remainder']=='carryover':carryovers.append({'source':source,'amount':_cents(rest)})
            for edge,share in zip(edges,amounts):
                shares.append({'source':source,'parent':edge.get('parent',source),'child':edge.get('child'),
                               'weight':str(edge.get('weight')),'share':_cents(share)})
        else:
            for edge in edges:
                weight=edge.get('weight')
                share=total if weight is None else (total*Decimal(str(weight))).quantize(CENT,rounding=ROUND_HALF_UP)
                shares.append({'source':source,'parent':edge.get('parent',source),'child':edge.get('child'),
                               'weight':None if weight is None else str(weight),'share':_cents(share)})
    entries={key:[] for key in subjects}
    for key in subjects:
        if ok.get(key) and key in old:
            rev=revisions[key][0]
            entries[key].append({'source':key,'event_key':rev['event_key'],'edge':None,'amount':Decimal(new[key])-Decimal(old[key]),'dedup':False})
    for share in shares:
        child=share['child']
        if child in entries:
            revs=revisions.get(share['source'],[])
            entries[child].append({'source':share['source'],'event_key':revs[0]['event_key'] if revs else None,
                                   'edge':(share['source'],share['parent'],child),'amount':Decimal(share['share']),
                                   'dedup':share['source'] in once_sources})
    eligible={key:ok.get(key,False) for key in subjects};order=[];seen=set()
    def walk(node):
        if node in seen or node in cycles:return
        seen.add(node)
        for parent in sorted(deps.get(node,[])):walk(parent)
        order.append(node)
    for key in sorted(subjects):walk(key)
    for child in order:
        if not eligible.get(child):continue
        if any(not eligible.get(parent) for parent in deps.get(child,[])):eligible[child]=False;continue
        for parent in deps.get(child,[]):
            for entry in entries[parent]:
                if entry['edge'] is None and entry['source']==parent and parent in once_sources:continue
                entries[child].append(entry)
        merged=[];lineage=set()
        for entry in entries[child]:
            if entry['dedup']:
                if entry['edge'] in lineage:continue
                lineage.add(entry['edge'])
            merged.append(entry)
        entries[child]=merged
    final={};sources={}
    for key in subjects:
        if not eligible.get(key) or key not in old:continue
        delta=sum((entry['amount'] for entry in entries[key] if not (entry['edge'] is None and entry['source']==key)),Decimal('0'))
        final[key]=_cents((Decimal(new[key])+delta).quantize(CENT))
        merged={}
        for entry in entries[key]:
            slot=merged.setdefault((entry['source'],entry['event_key']),{'source':entry['source'],'event_key':entry['event_key'],'amount':Decimal('0')})
            slot['amount']+=entry['amount']
        sources[key]=[{'source':slot['source'],'event_key':slot['event_key'],'amount':_cents(slot['amount'].quantize(CENT))}
                      for slot in sorted(merged.values(),key=lambda x:(str(x['source']),str(x['event_key'])))]
    return {'shares':shares,'carryovers':carryovers,'old':old,'new':final,'sources':sources,'eligible':eligible}
def _verify_allocations(db):
    latest=db.execute('SELECT max(id) FROM run').fetchone()[0]
    if not latest:return []
    rec=_recompute(db);problems=[]
    persisted={(r['source'],r['parent'],r['child'],r['weight'],r['share']) for r in db.execute('SELECT * FROM allocation_share WHERE run_id=?',(latest,))}
    expected={(s['source'],s['parent'],s['child'],s['weight'],s['share']) for s in rec['shares']}
    for item in sorted(expected-persisted):problems.append(f'allocation_share_missing:{item[0]}->{item[2]}')
    for item in sorted(persisted-expected):problems.append(f'allocation_share_mismatch:{item[0]}->{item[2]}')
    carried={(r['source'],r['amount']) for r in db.execute('SELECT * FROM carryover_balance WHERE run_id=?',(latest,))}
    expected_carried={(c['source'],c['amount']) for c in rec['carryovers']}
    for item in sorted(expected_carried-carried):problems.append(f'carryover_missing:{item[0]}')
    for item in sorted(carried-expected_carried):problems.append(f'carryover_mismatch:{item[0]}')
    for d in db.execute('SELECT subject,status,detail FROM decision WHERE run_id=?',(latest,)):
        detail=json.loads(d['detail'])
        if d['status'] in ('可补偿','不变') and detail.get('plan'):
            key=d['subject']
            if key not in rec['new'] or detail['plan'].get('new')!=rec['new'][key] or detail['plan'].get('old')!=rec['old'].get(key):
                problems.append(f'decision_value:{key}')
            elif detail['plan'].get('sources')!=rec['sources'].get(key):problems.append(f'decision_sources:{key}')
        if d['status']=='待核' and rec['eligible'].get(d['subject']):problems.append(f'decision_status:{d["subject"]}')
    plan_row=db.execute('SELECT body FROM plan WHERE run_id=? ORDER BY id DESC LIMIT 1',(latest,)).fetchone()
    if plan_row:
        body=json.loads(plan_row['body'])
        if {item['subject'] for item in body}!={key for key in rec['new'] if rec['new'][key]!=rec['old'][key]}:
            problems.append('plan_scope')
        for item in body:
            key=item['subject']
            if key not in rec['new'] or item.get('new')!=rec['new'][key] or item.get('old')!=rec['old'].get(key):
                problems.append(f'plan_value:{key}')
            elif item.get('sources')!=rec['sources'].get(key):problems.append(f'plan_sources:{key}')
    return problems
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
    problems+=_verify_allocations(db)
    db.close();return problems
