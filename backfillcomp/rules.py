import json,hashlib,sqlite3,shutil
from decimal import Decimal,ROUND_HALF_UP
from pathlib import Path
from .store import connect
def _cfg(db):return json.loads(db.execute('SELECT config_raw FROM batch LIMIT 1').fetchone()[0])
def _snapshot(cfg=None):
    if cfg and cfg.get('snapshot_path'):return Path(cfg['snapshot_path']).resolve()
    return Path(__file__).resolve().parent.parent/'fixtures'/'ledger.db'
def _money(value,multiplier):return str((Decimal(str(value))*Decimal(str(multiplier))).quantize(Decimal('0.01'),rounding=ROUND_HALF_UP))
def evaluate(path):
    db=connect(path);cfg=_cfg(db);source=sqlite3.connect(_snapshot(cfg));source.row_factory=sqlite3.Row
    rows={r['id']:dict(r) for r in source.execute('SELECT * FROM ledger')};source.close();result={};issues={};plans={}
    correction_events={}
    for event in db.execute("SELECT id,source_id,subject,payload FROM raw_event WHERE kind='correction'"):
        correction_events.setdefault(event['subject'],[]).append(dict(event))
    for key,spec in cfg['subjects'].items():
        row=rows.get(key);codes=[]
        corrections=[e for e in correction_events.get(key,[]) if json.loads(e['payload']).get('reason')=='rule_revision']
        if len(corrections)!=1:codes.append('correction_evidence_missing_or_ambiguous')
        if not row:codes.append('missing_row')
        else:
            old=_money(row['amount'],cfg['old_multiplier'])
            if row['computed']!=old:codes.append('source_drift')
            new=_money(row['amount'],cfg['new_multiplier'])
            plans[key]={'old':old,'new':new,'amount':row['amount'],'partition':row['partition']}
            if row['partition'] not in cfg['ranges']:codes.append('out_of_scope')
        issues[key]=codes;result[key]='待核' if codes else ('不变' if plans[key]['old']==plans[key]['new'] else '可补偿')
    graph=cfg.get('dependencies',{})
    visiting=set();visited=set();cycles=set()
    def visit(node):
        if node in visiting:cycles.update(visiting);return
        if node in visited:return
        visiting.add(node)
        for parent in graph.get(node,[]):visit(parent)
        visiting.remove(node);visited.add(node)
    for node in graph:visit(node)
    for node in cycles:issues.setdefault(node,[]).append('dependency_cycle');result[node]='待核'
    for child,parents in graph.items():
        for parent in parents:
            if parent not in rows:issues.setdefault(child,[]).append('missing_dependency');result[child]='待核'
    completed=set()
    def propagate(child):
        if child in completed or child in cycles:return
        for parent in graph.get(child,[]):
            if parent in graph:propagate(parent)
        if child in plans and not issues.get(child):
            for parent in graph.get(child,[]):
                if parent not in plans or issues.get(parent):issues[child].append('dependency_blocked');result[child]='待核';break
                delta=Decimal(plans[parent]['new'])-Decimal(plans[parent]['old'])
                plans[child]['new']=str((Decimal(plans[child]['new'])+delta).quantize(Decimal('0.01')))
            if not issues.get(child):result[child]='不变' if plans[child]['new']==plans[child]['old'] else '可补偿'
        completed.add(child)
    for child in graph:propagate(child)
    with db:
        rid=db.execute("INSERT INTO run(batch_id,status) VALUES (?, '已校验')",(cfg['batch_id'],)).lastrowid
        for key,status in result.items():
            detail=json.dumps({'plan':plans.get(key),'issues':issues[key],'dependencies':graph.get(key,[]),
                               'correction_event_ids':[e['id'] for e in correction_events.get(key,[])]},ensure_ascii=False)
            for code in issues[key]:db.execute('INSERT INTO issue(run_id,subject,code,detail) VALUES (?,?,?,?)',(rid,key,code,code))
            db.execute('INSERT INTO evidence(run_id,subject,rule_id,detail) VALUES (?,?,?,?)',(rid,key,cfg['rule_version'],detail))
            for event in correction_events.get(key,[]):
                db.execute('INSERT INTO evidence(run_id,subject,source_id,event_id,rule_id,detail) VALUES (?,?,?,?,?,?)',
                           (rid,key,event['source_id'],event['id'],cfg['rule_version'],'correction_source'))
            db.execute('INSERT INTO decision(run_id,subject,status,detail) VALUES (?,?,?,?)',(rid,key,status,detail))
        db.execute("UPDATE run SET status=CASE WHEN EXISTS(SELECT 1 FROM decision WHERE run_id=? AND status='待核') THEN '待核' ELSE '已判定' END,ended_at=CURRENT_TIMESTAMP WHERE id=?",(rid,rid))
    db.close();return result
def _plan(db):
    rows=db.execute('SELECT subject,status,detail FROM decision WHERE run_id=(SELECT max(id) FROM run) ORDER BY subject').fetchall()
    if not rows:raise ValueError('no_decisions')
    plan=[{'subject':r['subject'],**json.loads(r['detail'])['plan']} for r in rows if r['status']=='可补偿']
    cfg=_cfg(db);by_id={x['subject']:x for x in plan};ordered=[];visited=set()
    def visit(key):
        if key in visited:return
        for parent in cfg.get('dependencies',{}).get(key,[]):
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
