import json,sqlite3,subprocess,sys
from decimal import Decimal,ROUND_HALF_UP
from pathlib import Path
import pytest
from backfillcomp.ingest import initialize,load
from backfillcomp.rules import evaluate,action
from backfillcomp.report import export,verify,contracts_state
from backfillcomp.store import connect
ROOT=Path(__file__).resolve().parents[1]
def world(tmp_path,amounts,deps=None,allocations=None,ranges=('2026-01',),partitions=None,
          old_multiplier='1.00',new_multiplier='1.10',drop_events=(),extra_events=()):
    partitions=partitions or {}
    tmp_path.mkdir(parents=True,exist_ok=True)
    snap=tmp_path/'snap.db';con=sqlite3.connect(snap)
    con.execute('CREATE TABLE ledger(id TEXT PRIMARY KEY, partition TEXT, amount TEXT, computed TEXT)')
    for key,amount in amounts.items():
        computed=str((Decimal(amount)*Decimal(old_multiplier)).quantize(Decimal('0.01'),rounding=ROUND_HALF_UP))
        con.execute('INSERT INTO ledger VALUES (?,?,?,?)',(key,partitions.get(key,'2026-01'),amount,computed))
    con.commit();con.close()
    cfg={'batch_id':'alloc-test','rule_version':'v3','old_multiplier':old_multiplier,'new_multiplier':new_multiplier,
         'ranges':list(ranges),'subjects':{key:{'partition':partitions.get(key,'2026-01')} for key in amounts},
         'snapshot_path':str(snap)}
    if deps is not None:cfg['dependencies']=deps
    if allocations is not None:cfg['allocations']=allocations
    config=tmp_path/'config.json';config.write_text(json.dumps(cfg),encoding='utf-8')
    db=tmp_path/'control.db';initialize(db,config)
    lines=[]
    for key in amounts:
        if key in drop_events:continue
        lines.append(json.dumps({'event_key':f'c-{key}','subject':key,'kind':'correction','at':'2026-03-01','reason':'rule_revision'}))
    lines+= [json.dumps(e) for e in extra_events]
    events=tmp_path/'events.jsonl';events.write_text('\n'.join(lines),encoding='utf-8');load(db,events)
    return db
def rewrite_config(db,**changes):
    from backfillcomp.ingest import hash_bytes
    con=connect(db);cfg=json.loads(con.execute('SELECT config_raw FROM batch').fetchone()[0])
    cfg.update(changes);raw=json.dumps(cfg).encode()
    con.execute('UPDATE batch SET config_raw=?,config_hash=?',(raw,hash_bytes(raw)));con.commit();con.close()
def plan_items(db,tmp_path,name='plan.json'):
    out=tmp_path/name;action('plan',db,out=out);return json.loads(out.read_text(encoding='utf-8'))
def test_single_parent_without_contract_stays_legacy(tmp_path):
    db=world(tmp_path,{'r1':'10.00','r2':'20.00'},deps={'r2':['r1']})
    assert evaluate(db)=={'r1':'可补偿','r2':'可补偿'}
    items=plan_items(db,tmp_path)
    assert [x['subject'] for x in items]==['r1','r2'] and items[1]['new']=='23.00'
    assert items[1]['sources']==[{'source':'r1','event_key':'c-r1','amount':'1.00'},
                                 {'source':'r2','event_key':'c-r2','amount':'2.00'}]
    assert verify(db)==[]
def test_fork_split_conserves_to_the_cent(tmp_path):
    alloc=[{'source':'r1','remainder':'priority','edges':[
        {'child':'r2','weight':'0.5','priority':1,'partitions':['2026-01']},
        {'child':'r3','weight':'0.5','priority':2,'partitions':['2026-01']}]}]
    db=world(tmp_path,{'r1':'0.15','r2':'0.04','r3':'0.04'},allocations=alloc)
    assert evaluate(db)=={'r1':'可补偿','r2':'可补偿','r3':'可补偿'}
    con=connect(db)
    shares={r['child']:r['share'] for r in con.execute('SELECT child,share FROM allocation_share')}
    assert shares=={'r2':'0.01','r3':'0.01'}
    assert sum(Decimal(s) for s in shares.values())==Decimal('0.02')
    items={x['subject']:x for x in plan_items(db,tmp_path)}
    assert items['r2']['new']=='0.05' and items['r3']['new']=='0.05'
    assert verify(db)==[]
def test_diamond_join_counts_source_once(tmp_path):
    alloc=[{'source':'r1','remainder':'priority','edges':[
        {'child':'r2','weight':'0.5','priority':1,'partitions':['2026-01']},
        {'child':'r3','weight':'0.5','priority':2,'partitions':['2026-01']}]}]
    db=world(tmp_path,{'r1':'0.05','r2':'0.04','r3':'0.04','r4':'0.04'},
             deps={'r4':['r2','r3']},allocations=alloc)
    assert evaluate(db)=={'r1':'可补偿','r2':'可补偿','r3':'不变','r4':'可补偿'}
    items={x['subject']:x for x in plan_items(db,tmp_path)}
    assert items['r2']['new']=='0.05' and items['r4']['new']=='0.05'
    r1_share=[s for s in items['r4']['sources'] if s['source']=='r1']
    assert r1_share==[{'source':'r1','event_key':'c-r1','amount':'0.01'}]
    assert [x['subject'] for x in plan_items(db,tmp_path)]==['r1','r2','r4']
    assert verify(db)==[]
def test_remainder_priority_gives_top_receiver_the_cent(tmp_path):
    alloc=[{'source':'r1','remainder':'priority','edges':[
        {'child':'r2','weight':'0.5','priority':1,'partitions':['2026-01']},
        {'child':'r3','weight':'0.5','priority':2,'partitions':['2026-01']}]}]
    db=world(tmp_path,{'r1':'0.05','r2':'0.04','r3':'0.04'},allocations=alloc)
    assert evaluate(db)=={'r1':'可补偿','r2':'可补偿','r3':'不变'}
    con=connect(db)
    shares={r['child']:r['share'] for r in con.execute('SELECT child,share FROM allocation_share')}
    assert shares=={'r2':'0.01','r3':'0.00'}
    assert con.execute('SELECT count(*) FROM carryover_balance').fetchone()[0]==0
    assert sum(Decimal(s) for s in shares.values())==Decimal('0.01')
    assert [x['subject'] for x in plan_items(db,tmp_path)]==['r1','r2']
    assert verify(db)==[]
def test_remainder_carryover_keeps_balance_out_of_receivers(tmp_path):
    alloc=[{'source':'r1','remainder':'carryover','edges':[
        {'child':'r2','weight':'0.5','priority':1,'partitions':['2026-01']},
        {'child':'r3','weight':'0.5','priority':2,'partitions':['2026-01']}]}]
    db=world(tmp_path,{'r1':'0.05','r2':'0.04','r3':'0.04'},allocations=alloc)
    assert evaluate(db)=={'r1':'可补偿','r2':'不变','r3':'不变'}
    con=connect(db)
    shares={r['child']:r['share'] for r in con.execute('SELECT child,share FROM allocation_share')}
    assert shares=={'r2':'0.00','r3':'0.00'}
    balance=con.execute('SELECT source,amount,state FROM carryover_balance').fetchone()
    assert (balance['source'],balance['amount'],balance['state'])==('r1','0.01','pending')
    assert sum(Decimal(s) for s in shares.values())+Decimal(balance['amount'])==Decimal('0.01')
    assert [x['subject'] for x in plan_items(db,tmp_path)]==['r1']
    state=contracts_state(db)
    assert state['carryovers'][0]['amount']=='0.01'
    assert verify(db)==[]
def test_missing_weight_blocks_contract_and_plan(tmp_path):
    alloc=[{'source':'r1','edges':[{'child':'r2','partitions':['2026-01']},{'child':'r3','weight':'0.5','partitions':['2026-01']}]}]
    db=world(tmp_path,{'r1':'0.05','r2':'0.04','r3':'0.04'},allocations=alloc)
    assert evaluate(db)=={'r1':'待核','r2':'待核','r3':'待核'}
    con=connect(db)
    assert con.execute("SELECT 1 FROM issue WHERE subject='r1' AND code='allocation_weight_missing'").fetchone()
    assert con.execute("SELECT 1 FROM issue WHERE subject='r2' AND code='allocation_contract_invalid'").fetchone()
    assert plan_items(db,tmp_path)==[]
def test_negative_weight_and_total_mismatch_block(tmp_path):
    alloc=[{'source':'r1','edges':[{'child':'r2','weight':'-0.5'},{'child':'r3','weight':'1.5'}]}]
    db=world(tmp_path/'a',{'r1':'0.05','r2':'0.04','r3':'0.04'},allocations=alloc)
    assert evaluate(db)['r1']=='待核'
    assert connect(db).execute("SELECT 1 FROM issue WHERE code='allocation_weight_negative'").fetchone()
    alloc2=[{'source':'r1','edges':[{'child':'r2','weight':'0.4'},{'child':'r3','weight':'0.5'}]}]
    db2=world(tmp_path/'b',{'r1':'0.05','r2':'0.04','r3':'0.04'},allocations=alloc2)
    assert evaluate(db2)['r1']=='待核'
    assert connect(db2).execute("SELECT 1 FROM issue WHERE code='allocation_weight_total'").fetchone()
    assert plan_items(db2,tmp_path/'b')==[]
def test_cycle_through_allocation_edge_blocks(tmp_path):
    alloc=[{'source':'r1','edges':[{'child':'r2','weight':'1.0','partitions':['2026-01']}]}]
    db=world(tmp_path,{'r1':'0.05','r2':'0.04'},deps={'r1':['r2']},allocations=alloc)
    result=evaluate(db)
    assert result=={'r1':'待核','r2':'待核'}
    assert connect(db).execute("SELECT 1 FROM issue WHERE code='dependency_cycle'").fetchone()
    assert plan_items(db,tmp_path)==[]
def test_cross_partition_edge_rejected_then_accepted(tmp_path):
    alloc=[{'source':'r1','edges':[{'child':'r2','weight':'1.0','partitions':['2026-01']}]}]
    amounts={'r1':'0.05','r2':'0.04'}
    db=world(tmp_path,amounts,allocations=alloc,ranges=('2026-01','2026-02'),partitions={'r2':'2026-02'})
    assert evaluate(db)=={'r1':'待核','r2':'待核'}
    assert connect(db).execute("SELECT 1 FROM issue WHERE code='allocation_partition_mismatch'").fetchone()
    assert plan_items(db,tmp_path)==[]
    alloc[0]['edges'][0]['partitions']=['2026-01','2026-02']
    rewrite_config(db,allocations=alloc)
    assert evaluate(db)=={'r1':'可补偿','r2':'可补偿'}
    items={x['subject']:x for x in plan_items(db,tmp_path,'p2.json')}
    assert items['r2']['new']=='0.05'
    assert verify(db)==[]
def test_weight_change_causally_rewrites_frozen_values(tmp_path):
    alloc=[{'source':'r1','remainder':'priority','edges':[
        {'child':'r2','weight':'0.5','priority':1},{'child':'r3','weight':'0.5','priority':2}]}]
    db=world(tmp_path,{'r1':'0.15','r2':'0.04','r3':'0.04'},allocations=alloc)
    evaluate(db);first={x['subject']:x for x in plan_items(db,tmp_path,'a.json')}
    assert first['r2']['new']=='0.05' and first['r3']['new']=='0.05'
    alloc[0]['edges'][0]['weight']='0.75';alloc[0]['edges'][1]['weight']='0.25'
    rewrite_config(db,allocations=alloc)
    assert evaluate(db)=={'r1':'可补偿','r2':'可补偿','r3':'不变'}
    second={x['subject']:x for x in plan_items(db,tmp_path,'b.json')}
    assert second['r2']['new']=='0.06' and 'r3' not in second
    con=connect(db)
    detail=json.loads(con.execute("SELECT detail FROM decision WHERE subject='r3' ORDER BY run_id DESC LIMIT 1").fetchone()[0])
    assert detail['plan']['new']=='0.04'
    assert verify(db)==[]
def test_repeated_evaluation_is_idempotent(tmp_path):
    alloc=[{'source':'r1','remainder':'carryover','edges':[
        {'child':'r2','weight':'0.5','priority':1},{'child':'r3','weight':'0.5','priority':2}]}]
    db=world(tmp_path,{'r1':'0.05','r2':'0.04','r3':'0.04'},allocations=alloc)
    first=evaluate(db);second=evaluate(db)
    assert first==second
    con=connect(db)
    runs=[r['id'] for r in con.execute('SELECT id FROM run ORDER BY id')]
    assert len(runs)==2
    for query in ('SELECT source,parent,child,share FROM allocation_share WHERE run_id=? ORDER BY source,parent,child',
                  'SELECT source,amount,state FROM carryover_balance WHERE run_id=? ORDER BY source'):
        per_run=[[tuple(x) for x in con.execute(query,(r,))] for r in runs]
        assert per_run[0]==per_run[1] and per_run[0]
    digest=lambda: __import__('hashlib').sha256((tmp_path/'p.json').read_bytes()).hexdigest()
    action('plan',db,out=tmp_path/'p.json');first_digest=digest()
    evaluate(db);action('plan',db,out=tmp_path/'p.json')
    assert digest()==first_digest
    assert verify(db)==[]
def test_frozen_plan_and_executions_not_rewritten_by_new_weights(tmp_path):
    alloc=[{'source':'r1','remainder':'priority','edges':[
        {'child':'r2','weight':'0.5','priority':1},{'child':'r3','weight':'0.5','priority':2}]}]
    db=world(tmp_path,{'r1':'0.15','r2':'0.04','r3':'0.04'},allocations=alloc)
    evaluate(db);plan=tmp_path/'p.json';action('plan',db,out=plan)
    target=action('apply',db,sandbox=tmp_path/'sandbox',out=plan)
    con=connect(db)
    executions=con.execute('SELECT subject,action,after_state FROM execution').fetchall()
    assert {(r['subject'],r['after_state']) for r in executions}=={('r1','0.17'),('r2','0.05'),('r3','0.05')}
    audit=sqlite3.connect(target);assert audit.execute("SELECT after_value FROM backfill_audit WHERE subject='r2'").fetchone()[0]=='0.05';audit.close()
    alloc[0]['edges'][0]['weight']='0.9';alloc[0]['edges'][1]['weight']='0.1'
    rewrite_config(db,allocations=alloc)
    evaluate(db)
    con=connect(db)
    assert {(r['subject'],r['after_state']) for r in con.execute('SELECT subject,action,after_state FROM execution')}=={('r1','0.17'),('r2','0.05'),('r3','0.05')}
    with pytest.raises(ValueError,match='plan_tampered_or_stale'):
        action('apply',db,sandbox=tmp_path/'sandbox2',out=plan)
    fresh={x['subject']:x for x in plan_items(db,tmp_path,'new.json')}
    assert fresh['r2']['new']=='0.06' and 'r3' not in fresh
    con=connect(db)
    assert con.execute("SELECT share FROM allocation_share WHERE child='r2' ORDER BY run_id DESC LIMIT 1").fetchone()[0]=='0.02'
    audit=sqlite3.connect(target);assert audit.execute("SELECT after_value FROM backfill_audit WHERE subject='r2'").fetchone()[0]=='0.05';audit.close()
    assert verify(db)==[]
def test_each_mode_counts_per_edge_without_conservation(tmp_path):
    alloc=[{'source':'r1','contribution':'each','edges':[{'child':'r2'},{'child':'r3'}]}]
    db=world(tmp_path,{'r1':'0.05','r2':'0.04','r3':'0.04','r4':'0.04'},
             deps={'r4':['r2','r3']},allocations=alloc)
    assert evaluate(db)=={'r1':'可补偿','r2':'可补偿','r3':'可补偿','r4':'可补偿'}
    items={x['subject']:x for x in plan_items(db,tmp_path)}
    assert items['r2']['new']=='0.05' and items['r3']['new']=='0.05'
    assert items['r4']['new']=='0.06'
    assert verify(db)==[]
def test_duplicate_share_via_two_paths_counts_once(tmp_path):
    alloc=[{'source':'r1','remainder':'priority','edges':[{'child':'r2','weight':'1.0','priority':1}]}]
    db=world(tmp_path,{'r1':'0.05','r2':'0.04','r3':'0.04','r4':'0.04','r5':'0.04'},
             deps={'r3':['r2'],'r4':['r2'],'r5':['r3','r4']},allocations=alloc)
    assert evaluate(db)=={'r1':'可补偿','r2':'可补偿','r3':'可补偿','r4':'可补偿','r5':'可补偿'}
    items={x['subject']:x for x in plan_items(db,tmp_path)}
    assert items['r5']['new']=='0.05'
    assert [s for s in items['r5']['sources'] if s['source']=='r1'][0]['amount']=='0.01'
    assert verify(db)==[]
def test_contract_edge_and_dependency_on_same_pair_do_not_double_count(tmp_path):
    alloc=[{'source':'r1','remainder':'priority','edges':[{'child':'r2','weight':'1.0','priority':1}]}]
    db=world(tmp_path,{'r1':'0.05','r2':'0.04'},deps={'r2':['r1']},allocations=alloc)
    assert evaluate(db)=={'r1':'可补偿','r2':'可补偿'}
    items={x['subject']:x for x in plan_items(db,tmp_path)}
    assert items['r2']['new']=='0.05'
    assert [s for s in items['r2']['sources'] if s['source']=='r1'][0]['amount']=='0.01'
    assert verify(db)==[]
def test_negative_revision_delta_allocates_and_conserves(tmp_path):
    alloc=[{'source':'r1','remainder':'priority','edges':[
        {'child':'r2','weight':'0.5','priority':1},{'child':'r3','weight':'0.5','priority':2}]}]
    db=world(tmp_path,{'r1':'0.05','r2':'0.04','r3':'0.04'},allocations=alloc,
             old_multiplier='1.10',new_multiplier='1.00')
    assert evaluate(db)=={'r1':'可补偿','r2':'可补偿','r3':'不变'}
    con=connect(db)
    shares={r['child']:r['share'] for r in con.execute('SELECT child,share FROM allocation_share')}
    assert shares=={'r2':'-0.01','r3':'0.00'}
    assert sum(Decimal(s) for s in shares.values())==Decimal('-0.01')
    items={x['subject']:x for x in plan_items(db,tmp_path)}
    assert items['r1']['new']=='0.05' and items['r2']['new']=='0.03'
    assert verify(db)==[]
def test_revision_event_removal_cascades_to_receivers(tmp_path):
    alloc=[{'source':'r1','remainder':'priority','edges':[
        {'child':'r2','weight':'0.5','priority':1},{'child':'r3','weight':'0.5','priority':2}]}]
    db=world(tmp_path/'missing',{'r1':'0.05','r2':'0.04','r3':'0.04'},allocations=alloc,drop_events=('r1',))
    assert evaluate(db)=={'r1':'待核','r2':'待核','r3':'待核'}
    con=connect(db)
    assert con.execute("SELECT 1 FROM issue WHERE subject='r2' AND code='allocation_source_blocked'").fetchone()
    assert plan_items(db,tmp_path/'missing')==[]
    restored=world(tmp_path/'restored',{'r1':'0.05','r2':'0.04','r3':'0.04'},allocations=alloc)
    assert evaluate(restored)=={'r1':'可补偿','r2':'可补偿','r3':'不变'}
    assert [x['subject'] for x in plan_items(restored,tmp_path/'restored')]==['r1','r2']
    assert verify(restored)==[]
def test_report_independently_recomputes_contributions(tmp_path):
    alloc=[{'source':'r1','remainder':'carryover','edges':[
        {'child':'r2','weight':'0.5','priority':1},{'child':'r3','weight':'0.5','priority':2}]}]
    db=world(tmp_path,{'r1':'0.05','r2':'0.04','r3':'0.04'},allocations=alloc)
    evaluate(db);plan_items(db,tmp_path)
    assert verify(db)==[]
    con=connect(db)
    con.execute("UPDATE allocation_share SET share='99.00' WHERE child='r2' AND run_id=(SELECT max(id) FROM run)");con.commit();con.close()
    assert any(p.startswith('allocation_share_mismatch:') or p.startswith('allocation_share_missing:') for p in verify(db))
    con=connect(db)
    con.execute("UPDATE allocation_share SET share='0.00' WHERE child='r2' AND run_id=(SELECT max(id) FROM run)")
    con.execute("UPDATE carryover_balance SET amount='0.02' WHERE run_id=(SELECT max(id) FROM run)");con.commit();con.close()
    assert any(p.startswith('carryover_mismatch:') for p in verify(db))
    con=connect(db)
    con.execute("UPDATE carryover_balance SET amount='0.01' WHERE run_id=(SELECT max(id) FROM run)")
    con.execute("UPDATE decision SET detail=json_set(detail,'$.plan.new','99.00') WHERE subject='r2' AND run_id=(SELECT max(id) FROM run)");con.commit();con.close()
    assert any(p.startswith('decision_value:r2') for p in verify(db))
    con=connect(db)
    rid=con.execute('SELECT max(id) FROM run').fetchone()[0]
    detail=json.loads(con.execute("SELECT detail FROM decision WHERE subject='r2' AND run_id=?",(rid,)).fetchone()[0])
    detail['plan']['new']='0.04'
    con.execute("UPDATE decision SET detail=? WHERE subject='r2' AND run_id=?",(json.dumps(detail,ensure_ascii=False),rid))
    body=json.loads(con.execute("SELECT body FROM plan WHERE run_id=?",(rid,)).fetchone()[0])
    body[0]['sources']=[];body[0]['new']='99.00'
    con.execute("UPDATE plan SET body=? WHERE run_id=?",(json.dumps(body,ensure_ascii=False),rid));con.commit();con.close()
    problems=verify(db)
    assert any(p.startswith('plan_value:') or p.startswith('plan_sources:') for p in problems)
def test_export_carries_contract_state(tmp_path):
    alloc=[{'source':'r1','remainder':'carryover','edges':[
        {'child':'r2','weight':'0.5','priority':1},{'child':'r3','weight':'0.5','priority':2}]}]
    db=world(tmp_path,{'r1':'0.05','r2':'0.04','r3':'0.04'},allocations=alloc)
    evaluate(db);out=export(db,tmp_path/'report.json')
    data=json.loads(out.read_text(encoding='utf-8'))
    assert data['allocations']['contracts'][0]['status']=='frozen'
    assert len(data['allocations']['shares'])==2
    assert data['allocations']['carryovers'][0]['amount']=='0.01'
    assert '待结转余额 r1: 0.01' in out.with_suffix('.md').read_text(encoding='utf-8')
def test_cli_contracts_and_verify(tmp_path):
    alloc=[{'source':'r1','remainder':'carryover','edges':[
        {'child':'r2','weight':'0.5','priority':1},{'child':'r3','weight':'0.5','priority':2}]}]
    db=world(tmp_path,{'r1':'0.05','r2':'0.04','r3':'0.04'},allocations=alloc)
    evaluate(db)
    out=subprocess.run([sys.executable,'-m','backfillcomp','contracts','--db',str(db)],
                       capture_output=True,text=True,cwd=ROOT)
    assert out.returncode==0,out.stderr
    state=json.loads(out.stdout)
    assert [s['child'] for s in state['shares']]==['r2','r3']
    assert state['carryovers'][0]['amount']=='0.01'
    out=subprocess.run([sys.executable,'-m','backfillcomp','verify','--db',str(db)],
                       capture_output=True,text=True,cwd=ROOT)
    assert out.returncode==0 and "'integrity_problems': []" in out.stdout
