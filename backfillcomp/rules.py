import json,hashlib,sqlite3,shutil
from decimal import Decimal,ROUND_HALF_UP,ROUND_DOWN
from pathlib import Path
from .store import connect
CENT=Decimal('0.01')
ZERO=Decimal('0.00')
def _cents(value):return str(value+ZERO)
def _cfg(db):return json.loads(db.execute('SELECT config_raw FROM batch LIMIT 1').fetchone()[0])
def _snapshot(cfg=None):
    if cfg and cfg.get('snapshot_path'):return Path(cfg['snapshot_path']).resolve()
    return Path(__file__).resolve().parent.parent/'fixtures'/'ledger.db'
def _money(value,multiplier):return str((Decimal(str(value))*Decimal(str(multiplier))).quantize(CENT,rounding=ROUND_HALF_UP))
def _revision_events(db):
    events={}
    for event in db.execute("SELECT id,source_id,subject,event_key,payload FROM raw_event WHERE kind='correction'"):
        events.setdefault(event['subject'],[]).append(dict(event))
    return events
def _revisions(events):
    return {key:[e for e in items if json.loads(e['payload']).get('reason')=='rule_revision'] for key,items in events.items()}
def _raw_contracts(cfg):
    raw=cfg.get('allocations') or []
    if isinstance(raw,dict):raw=[{'source':key,**value} for key,value in raw.items()]
    return raw
def _validate_contracts(cfg,rows,revisions):
    """Declarative allocation contracts: freeze weight, source revision event and
    effective partitions for every parent->child relation of a revision source."""
    contracts=[];by_source={}
    for raw in _raw_contracts(cfg):
        source=raw.get('source');issues=[]
        contract={'source':source,'event_key':raw.get('event_key'),
                  'contribution':raw.get('contribution','once'),'remainder':raw.get('remainder','priority'),
                  'edges':[],'issues':issues,'event':None}
        if contract['contribution'] not in ('once','each'):issues.append('allocation_contribution_invalid')
        if contract['remainder'] not in ('priority','carryover'):issues.append('allocation_remainder_invalid')
        if not source or source not in cfg['subjects']:
            issues.append('allocation_unknown_source')
        else:
            revs=revisions.get(source,[])
            event=revs[0] if len(revs)==1 else None
            contract['event']=event
            if contract['event_key'] and (not event or event['event_key']!=contract['event_key']):
                issues.append('allocation_event_mismatch')
            if not contract['event_key'] and event:contract['event_key']=event['event_key']
        seen_edges=set();total=Decimal('0')
        for edge in raw.get('edges') or []:
            parent=edge.get('parent',source);child=edge.get('child')
            entry={'parent':parent,'child':child,'weight':edge.get('weight'),
                   'priority':edge.get('priority'),'partitions':edge.get('partitions')}
            contract['edges'].append(entry)
            if parent!=source:issues.append('allocation_parent_mismatch')
            if not child or child not in cfg['subjects']:issues.append('allocation_unknown_child')
            if (parent,child) in seen_edges:issues.append('allocation_edge_duplicate')
            seen_edges.add((parent,child))
            if entry['weight'] is None:
                if contract['contribution']=='once':issues.append('allocation_weight_missing')
            else:
                try:
                    weight=Decimal(str(entry['weight']))
                    if weight<0:issues.append('allocation_weight_negative')
                    total+=weight
                except ArithmeticError:
                    issues.append('allocation_weight_invalid')
            parts=entry['partitions']
            if parts is not None and child in rows and rows[child]['partition'] not in parts:
                issues.append('allocation_partition_mismatch')
        if contract['contribution']=='once' and not any(c.endswith('weight_missing') or c.endswith('weight_invalid') or c.endswith('weight_negative') for c in issues):
            if total!=Decimal('1'):issues.append('allocation_weight_total')
        contract['issues']=[c for c in dict.fromkeys(issues)]
        if source in by_source:
            by_source[source]['issues'].append('allocation_contract_conflict')
            contract['issues'].append('allocation_contract_conflict')
        else:by_source[source]=contract
        contracts.append(contract)
    return contracts
def _split_once(total,edges):
    """Conservative cent split: floor each weighted share, then the sub-cent
    remainder goes to the highest stable business priority receiver, or is kept
    as an independent carryover balance. Both modes conserve the total exactly."""
    floors=[(total*Decimal(str(edge['weight']))).quantize(CENT,rounding=ROUND_DOWN) for edge in edges]
    remainder=total-sum(floors,Decimal('0'))
    return floors,remainder
def _top_priority(edges):
    return min(range(len(edges)),key=lambda i:(edges[i]['priority'] if edges[i]['priority'] is not None else 1<<30,str(edges[i]['child'])))
def evaluate(path):
    db=connect(path);cfg=_cfg(db);source=sqlite3.connect(_snapshot(cfg));source.row_factory=sqlite3.Row
    rows={r['id']:dict(r) for r in source.execute('SELECT * FROM ledger')};source.close();result={};issues={};plans={}
    correction_events=_revision_events(db);revisions=_revisions(correction_events)
    for key,spec in cfg['subjects'].items():
        row=rows.get(key);codes=[]
        if len(revisions.get(key,[]))!=1:codes.append('correction_evidence_missing_or_ambiguous')
        if not row:codes.append('missing_row')
        else:
            old=_money(row['amount'],cfg['old_multiplier'])
            if row['computed']!=old:codes.append('source_drift')
            new=_money(row['amount'],cfg['new_multiplier'])
            plans[key]={'old':old,'new':new,'amount':row['amount'],'partition':row['partition']}
            if row['partition'] not in cfg['ranges']:codes.append('out_of_scope')
        issues[key]=codes;result[key]='待核' if codes else ('不变' if plans[key]['old']==plans[key]['new'] else '可补偿')
    contracts=_validate_contracts(cfg,rows,revisions)
    for contract in contracts:
        if not contract['issues']:continue
        if contract['source'] in issues:
            issues[contract['source']].extend(c for c in contract['issues'] if c not in issues[contract['source']])
            result[contract['source']]='待核'
        for edge in contract['edges']:
            child=edge['child']
            if child in issues and 'allocation_contract_invalid' not in issues[child]:
                issues[child].append('allocation_contract_invalid');result[child]='待核'
    deps={child:list(parents) for child,parents in cfg.get('dependencies',{}).items()}
    graph={child:list(parents) for child,parents in deps.items()}
    for contract in contracts:
        for edge in contract['edges']:
            if edge['child'] in cfg['subjects'] and edge['parent']:
                graph.setdefault(edge['child'],[]).append(edge['parent'])
    visiting=set();visited=set();cycles=set()
    def visit(node):
        if node in visiting:cycles.update(visiting);return
        if node in visited:return
        visiting.add(node)
        for parent in graph.get(node,[]):visit(parent)
        visiting.remove(node);visited.add(node)
    for node in graph:visit(node)
    for node in cycles:
        issues.setdefault(node,[]).append('dependency_cycle');result[node]='待核'
    for child,parents in deps.items():
        for parent in parents:
            if parent not in rows:issues.setdefault(child,[]).append('missing_dependency');result[child]='待核'
    shares=[];carryovers=[];once_sources=set()
    for contract in contracts:
        if contract['issues']:continue
        src=contract['source']
        if src not in plans or issues.get(src):
            for edge in contract['edges']:
                child=edge['child']
                if child in issues and 'allocation_source_blocked' not in issues[child]:
                    issues[child].append('allocation_source_blocked');result[child]='待核'
            continue
        total=Decimal(plans[src]['new'])-Decimal(plans[src]['old'])
        event=contract['event'] or {}
        if contract['contribution']=='once':
            once_sources.add(src)
            amounts,remainder=_split_once(total,contract['edges'])
            if remainder and contract['remainder']=='priority':amounts[_top_priority(contract['edges'])]+=remainder
            if remainder and contract['remainder']=='carryover':
                carryovers.append({'source':src,'event_id':event.get('id'),'event_key':contract['event_key'],'amount':_cents(remainder)})
            for edge,share in zip(contract['edges'],amounts):
                shares.append({'source':src,'event_id':event.get('id'),'event_key':contract['event_key'],
                               'parent':edge['parent'],'child':edge['child'],'weight':str(edge['weight']),
                               'priority':edge['priority'],'share':_cents(share)})
        else:
            for edge in contract['edges']:
                share=total if edge['weight'] is None else (total*Decimal(str(edge['weight']))).quantize(CENT,rounding=ROUND_HALF_UP)
                shares.append({'source':src,'event_id':event.get('id'),'event_key':contract['event_key'],
                               'parent':edge['parent'],'child':edge['child'],
                               'weight':None if edge['weight'] is None else str(edge['weight']),
                               'priority':edge['priority'],'share':_cents(share)})
    entries={key:[] for key in cfg['subjects']}
    for key in cfg['subjects']:
        if key in plans and not issues.get(key):
            rev=revisions[key][0]
            entries[key].append({'source':key,'event_id':rev['id'],'event_key':rev['event_key'],'edge':None,
                                 'amount':Decimal(plans[key]['new'])-Decimal(plans[key]['old']),'dedup':False})
    for share in shares:
        child=share['child']
        if child in entries:
            entries[child].append({'source':share['source'],'event_id':share['event_id'],'event_key':share['event_key'],
                                   'edge':(share['source'],share['parent'],child),'amount':Decimal(share['share']),
                                   'dedup':share['source'] in once_sources})
    order=[];seen=set()
    def walk(node):
        if node in seen or node in cycles:return
        seen.add(node)
        for parent in sorted(deps.get(node,[])):walk(parent)
        order.append(node)
    for key in sorted(cfg['subjects']):walk(key)
    for child in order:
        if child not in plans or issues.get(child):continue
        if any(parent not in plans or issues.get(parent) for parent in deps.get(child,[])):
            issues[child].append('dependency_blocked');result[child]='待核';continue
        for parent in deps.get(child,[]):
            for entry in entries[parent]:
                if entry['edge'] is None and entry['source']==parent and parent in once_sources:continue
                entries[child].append(entry)
        merged=[];lineage={}
        for entry in entries[child]:
            if entry['dedup']:
                key=(entry['event_id'],entry['edge'])
                if key in lineage:
                    if lineage[key]['amount']!=entry['amount']:
                        issues[child].append('allocation_lineage_conflict');result[child]='待核'
                    continue
                lineage[key]=entry
            merged.append(entry)
        entries[child]=merged
    for key in cfg['subjects']:
        if key not in plans or issues.get(key):continue
        delta=sum((entry['amount'] for entry in entries[key] if not (entry['edge'] is None and entry['source']==key)),Decimal('0'))
        plans[key]['new']=_cents((Decimal(plans[key]['new'])+delta).quantize(CENT))
        merged={}
        for entry in entries[key]:
            slot=merged.setdefault((entry['source'],entry['event_key']),{'source':entry['source'],'event_key':entry['event_key'],'amount':Decimal('0')})
            slot['amount']+=entry['amount']
        plans[key]['sources']=[{'source':slot['source'],'event_key':slot['event_key'],'amount':_cents(slot['amount'].quantize(CENT))}
                               for slot in sorted(merged.values(),key=lambda x:(str(x['source']),str(x['event_key'])))]
        result[key]='不变' if plans[key]['new']==plans[key]['old'] else '可补偿'
    incoming={}
    for share in shares:incoming.setdefault(share['child'],[]).append(share)
    retained={}
    for balance in carryovers:retained.setdefault(balance['source'],[]).append(balance)
    with db:
        rid=db.execute("INSERT INTO run(batch_id,status) VALUES (?, '已校验')",(cfg['batch_id'],)).lastrowid
        for contract in contracts:
            body=json.dumps({'source':contract['source'],'event_key':contract['event_key'],'contribution':contract['contribution'],
                             'remainder':contract['remainder'],'edges':contract['edges']},ensure_ascii=False,sort_keys=True)
            db.execute('INSERT OR IGNORE INTO allocation_contract(run_id,source,event_id,contribution,remainder,status,digest,body) VALUES (?,?,?,?,?,?,?,?)',
                       (rid,str(contract['source'] or ''),(contract['event'] or {}).get('id'),contract['contribution'],contract['remainder'],
                        'invalid' if contract['issues'] else 'frozen',hashlib.sha256(body.encode()).hexdigest(),body))
        for share in shares:
            db.execute('INSERT INTO allocation_share(run_id,source,event_id,parent,child,weight,priority,share) VALUES (?,?,?,?,?,?,?,?)',
                       (rid,share['source'],share['event_id'],share['parent'],share['child'],share['weight'],share['priority'],share['share']))
        for balance in carryovers:
            db.execute('INSERT INTO carryover_balance(run_id,source,event_id,amount,state) VALUES (?,?,?,?,?)',
                       (rid,balance['source'],balance['event_id'],balance['amount'],'pending'))
        for key,status in result.items():
            detail=json.dumps({'plan':plans.get(key),'issues':issues[key],'dependencies':deps.get(key,[]),
                               'allocations':[{'source':s['source'],'event_key':s['event_key'],'parent':s['parent'],
                                               'weight':s['weight'],'share':s['share']} for s in incoming.get(key,[])],
                               'carryover':[{'source':b['source'],'event_key':b['event_key'],'amount':b['amount']} for b in retained.get(key,[])],
                               'correction_event_ids':[e['id'] for e in correction_events.get(key,[])]},ensure_ascii=False)
            for code in issues[key]:db.execute('INSERT INTO issue(run_id,subject,code,detail) VALUES (?,?,?,?)',(rid,key,code,code))
            db.execute('INSERT INTO evidence(run_id,subject,rule_id,detail) VALUES (?,?,?,?)',(rid,key,cfg['rule_version'],detail))
            for event in correction_events.get(key,[]):
                db.execute('INSERT INTO evidence(run_id,subject,source_id,event_id,rule_id,detail) VALUES (?,?,?,?,?,?)',
                           (rid,key,event['source_id'],event['id'],cfg['rule_version'],'correction_source'))
            db.execute('INSERT INTO decision(run_id,subject,status,detail) VALUES (?,?,?,?)',(rid,key,status,detail))
        db.execute("UPDATE run SET status=CASE WHEN EXISTS(SELECT 1 FROM decision WHERE run_id=? AND status='待核') THEN '待核' ELSE '已判定' END,ended_at=CURRENT_TIMESTAMP WHERE id=?",(rid,rid))
    db.close();return result
def _parents_map(cfg):
    parents={}
    for child,items in cfg.get('dependencies',{}).items():parents.setdefault(child,[]).extend(items)
    for contract in _raw_contracts(cfg):
        for edge in contract.get('edges') or []:
            parent=edge.get('parent') or contract.get('source');child=edge.get('child')
            if parent and child:parents.setdefault(child,[]).append(parent)
    return parents
def _plan(db):
    rows=db.execute('SELECT subject,status,detail FROM decision WHERE run_id=(SELECT max(id) FROM run) ORDER BY subject').fetchall()
    if not rows:raise ValueError('no_decisions')
    plan=[{'subject':r['subject'],**json.loads(r['detail'])['plan']} for r in rows if r['status']=='可补偿']
    cfg=_cfg(db);by_id={x['subject']:x for x in plan};ordered=[];visited=set();parents=_parents_map(cfg)
    def visit(key):
        if key in visited:return
        for parent in parents.get(key,[]):
            if parent in by_id:visit(parent)
        visited.add(key);ordered.append(by_id[key])
    for key in sorted(by_id):visit(key)
    return ordered
def action(command,path,sandbox=None,out=None,lease=None):
    db=connect(path);cfg=_cfg(db)
    if command=='recheck':return evaluate(path)
    if command=='plan':
        plan=_plan(db);target=Path(out or 'backfill-plan.json');body=json.dumps(plan,ensure_ascii=False,indent=2);target.write_bytes(body.encode('utf-8'))
        with db:db.execute('INSERT OR IGNORE INTO plan(run_id,digest,body) VALUES ((SELECT max(id) FROM run),?,?)',(hashlib.sha256(body.encode()).hexdigest(),body))
        db.close();return str(target)
    if command in ('apply','rollback'):
        if not sandbox:raise ValueError('sandbox_required')
        root=Path(sandbox).resolve();root.mkdir(parents=True,exist_ok=True);target=root/'ledger.db'
        if target.is_symlink():raise ValueError('unsafe_target_symlink')
        body=Path(out or 'backfill-plan.json').read_text(encoding='utf-8')
        plan_digest=hashlib.sha256(body.encode()).hexdigest()
        latest=db.execute('SELECT max(id) FROM run').fetchone()[0]
        if not db.execute('SELECT 1 FROM plan WHERE digest=? AND body=? AND run_id=?',(plan_digest,body,latest)).fetchone():
            raise ValueError('plan_tampered_or_stale')
        if command=='apply' and db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='capacity_window'").fetchone():
            if db.execute('SELECT 1 FROM capacity_window LIMIT 1').fetchone():
                if not lease or not db.execute("SELECT 1 FROM execution_lease WHERE id=? AND plan_digest=? AND state='active'",(lease,plan_digest)).fetchone():
                    raise ValueError('active_lease_required')
        if not target.exists():
            if command=='rollback':raise ValueError('target_missing')
            shutil.copy2(_snapshot(cfg),target)
        plan=json.loads(body);con=sqlite3.connect(target)
        try:
            with con:
                con.execute('CREATE TABLE IF NOT EXISTS backfill_audit(subject TEXT NOT NULL, plan_digest TEXT NOT NULL, state TEXT NOT NULL, before_value TEXT NOT NULL, after_value TEXT NOT NULL, PRIMARY KEY(subject,plan_digest))')
                for item in (plan if command=='apply' else reversed(plan)):
                    expected=item['old'] if command=='apply' else item['new'];replacement=item['new'] if command=='apply' else item['old']
                    row=con.execute('SELECT amount,computed FROM ledger WHERE id=?',(item['subject'],)).fetchone()
                    if not row or row[0]!=item['amount'] or row[1] not in (expected,replacement):raise ValueError('optimistic_conflict')
                    audit=con.execute('SELECT state FROM backfill_audit WHERE subject=? AND plan_digest=?',(item['subject'],plan_digest)).fetchone()
                    if command=='rollback' and audit and audit[0]=='rolled_back' and row[1]==replacement:
                        continue
                    if command=='rollback' and (not audit or audit[0]!='applied'):
                        raise ValueError('rollback_without_applied_plan')
                    if command=='apply' and row[1]==replacement and (not audit or audit[0]!='applied'):
                        raise ValueError('untracked_applied_value')
                    if row[1]==replacement:continue
                    con.execute('UPDATE ledger SET computed=? WHERE id=?',(replacement,item['subject']))
                    con.execute('INSERT INTO backfill_audit(subject,plan_digest,state,before_value,after_value) VALUES (?,?,?,?,?) ON CONFLICT(subject,plan_digest) DO UPDATE SET state=excluded.state',
                                (item['subject'],plan_digest,'applied' if command=='apply' else 'rolled_back',item['old'],item['new']))
            with db:
                for item in plan:
                    state=item['new'] if command=='apply' else item['old']
                    db.execute('INSERT OR IGNORE INTO execution(run_id,subject,action,before_hash,after_state) VALUES ((SELECT max(id) FROM run),?,?,?,?)',(item['subject'],command,hashlib.sha256(json.dumps(item,sort_keys=True).encode()).hexdigest(),state))
        finally:con.close();db.close()
        if command=='apply' and lease:
            from .schedule import release
            release(path,lease)
        return str(target)
    raise ValueError('unsupported_action')
def verify_target(path,sandbox):
    db=connect(path);target=Path(sandbox).resolve()/'ledger.db'
    if not target.exists():return [{'status':'不确定','reason':'target_missing'}]
    con=sqlite3.connect(target);results=[]
    row=db.execute('SELECT digest FROM plan WHERE run_id=(SELECT max(id) FROM run) ORDER BY id DESC LIMIT 1').fetchone()
    plan_digest=row['digest'] if row else None
    has_audit=bool(con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='backfill_audit'").fetchone())
    for item in _plan(db):
        row=con.execute('SELECT computed FROM ledger WHERE id=?',(item['subject'],)).fetchone()
        value=row[0] if row else None
        audit=con.execute('SELECT state FROM backfill_audit WHERE subject=? AND plan_digest=?',(item['subject'],plan_digest)).fetchone() if has_audit and plan_digest else None
        status='已应用' if value==item['new'] and audit and audit[0]=='applied' else (
            '未应用' if value==item['old'] and (not audit or audit[0]=='rolled_back') else '不确定')
        results.append({'subject':item['subject'],'status':status,'expected_old':item['old'],'expected_new':item['new'],'actual':value,'plan_digest':hashlib.sha256(json.dumps(item,sort_keys=True).encode()).hexdigest()})
    con.close();db.close();return results
