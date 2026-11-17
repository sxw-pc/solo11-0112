"""Downstream publication gate and multi-consumer publication accord.

A single-consumer ``check``/``publish`` gate remains for compatibility. A
publication accord binds several consumers that share one backfill plan to a
single frozen target snapshot and one adjudication of readiness. Publication is
either atomic (every member must be ready) or partitioned (only independent
groups without shared subjects or downstream references may publish).
"""
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .store import connect

SCHEMA="""
CREATE TABLE IF NOT EXISTS publication_gate(
 id TEXT PRIMARY KEY, consumer TEXT NOT NULL, run_id INTEGER NOT NULL REFERENCES run(id),
 plan_digest TEXT NOT NULL, target_hash TEXT NOT NULL, expected_hash TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('ready','blocked','uncertain','published','stale')),
 detail TEXT NOT NULL);
"""

ROUTES=('atomic','partitioned')
UNCERTAIN_CODES={'target_missing','missing','unproven','consumer_expectation_absent',
                 'subject_not_in_plan','dependency_member_absent','expected_absent'}

def _hash(value):return hashlib.sha256(json.dumps(value,sort_keys=True).encode()).hexdigest()

def _now():return datetime.now(timezone.utc).isoformat()

def _pairs(reasons):
    """Normalise mixed reason shapes to ``(code, detail)`` tuples."""
    out=[]
    for item in reasons or []:
        if isinstance(item,(list,tuple)) and len(item)==2:out.append((str(item[0]),str(item[1])))
        elif isinstance(item,str):
            code,_,detail=item.partition(':')
            out.append((code,detail))
        elif isinstance(item,dict):out.append((str(item.get('code','')),str(item.get('detail',''))))
    return out

def _reason_codes(reasons):return [code for code,_ in _pairs(reasons)]

# ---------------------------------------------------------------- legacy gate

def _target_snapshot(target,plan):
    if not Path(target).exists():return None
    db=sqlite3.connect(target)
    try:
        has_audit=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='backfill_audit'").fetchone()
        rows=[]
        for item in plan:
            row=db.execute('SELECT amount,computed FROM ledger WHERE id=?',(item['subject'],)).fetchone()
            audit=db.execute('SELECT state FROM backfill_audit WHERE subject=? AND plan_digest=?',
                             (item['subject'],item['_plan_digest'])).fetchone() if has_audit else None
            rows.append({'subject':item['subject'],'row':row,'audit':audit[0] if audit else None})
        return rows
    finally:db.close()

def check(db_path,target,gate_id,consumer,expected):
    """Independent consumer view must match each compensated value and audit."""
    db=connect(db_path);db.executescript(SCHEMA)
    run=db.execute('SELECT max(id) FROM run').fetchone()[0]
    plan_row=db.execute('SELECT * FROM plan WHERE run_id=? ORDER BY id DESC LIMIT 1',(run,)).fetchone()
    if not plan_row:raise ValueError('plan_required')
    plan=[{**x,'_plan_digest':plan_row['digest']} for x in json.loads(plan_row['body'])]
    if set(expected)!={x['subject'] for x in plan}:raise ValueError('consumer_scope_mismatch')
    snapshot=_target_snapshot(target,plan)
    problems=[]
    if snapshot is None:problems.append('target_missing')
    else:
        for item,seen in zip(plan,snapshot):
            if seen['row'] is None:problems.append(f"missing:{item['subject']}")
            elif seen['audit']!='applied':problems.append(f"unproven:{item['subject']}")
            elif seen['row'][1]!=item['new']:problems.append(f"target_drift:{item['subject']}")
            elif expected[item['subject']]!=seen['row'][1]:problems.append(f"consumer_diff:{item['subject']}")
    status='uncertain' if any(x.startswith(('target_missing','unproven','missing')) for x in problems) else ('blocked' if problems else 'ready')
    target_hash=_hash(snapshot)
    expected_hash=_hash(expected)
    old=db.execute('SELECT * FROM publication_gate WHERE id=?',(gate_id,)).fetchone()
    if old:
        if (old['consumer'],old['run_id'],old['plan_digest'],old['expected_hash'])!=(consumer,run,plan_row['digest'],expected_hash):
            raise ValueError('gate_identity_conflict')
        return dict(old)
    with db:
        db.execute('INSERT INTO publication_gate VALUES (?,?,?,?,?,?,?,?)',
                   (gate_id,consumer,run,plan_row['digest'],target_hash,expected_hash,status,json.dumps(problems)))
    db.close();return {'gate':gate_id,'status':status,'problems':problems}

def publish(db_path,target,gate_id):
    db=connect(db_path);db.executescript(SCHEMA)
    with db:
        gate=db.execute('SELECT * FROM publication_gate WHERE id=?',(gate_id,)).fetchone()
        if not gate:raise ValueError('unknown_gate')
        if gate['status']=='published':return {'gate':gate_id,'status':'published'}
        if gate['status']!='ready':raise ValueError('gate_not_ready')
        plan_row=db.execute('SELECT body FROM plan WHERE digest=?',(gate['plan_digest'],)).fetchone()
        plan=[{**x,'_plan_digest':gate['plan_digest']} for x in json.loads(plan_row['body'])]
        latest=db.execute('SELECT max(id) FROM run').fetchone()[0]
        live=_hash(_target_snapshot(target,plan))
        if latest!=gate['run_id'] or live!=gate['target_hash']:
            db.execute("UPDATE publication_gate SET status='stale' WHERE id=?",(gate_id,))
            return {'gate':gate_id,'status':'stale'}
        db.execute("UPDATE publication_gate SET status='published' WHERE id=?",(gate_id,))
    db.close();return {'gate':gate_id,'status':'published'}

# ------------------------------------------------------------ accord internals

def _latest_plan(db):
    run=db.execute('SELECT max(id) FROM run').fetchone()[0]
    row=db.execute('SELECT * FROM plan WHERE run_id=? ORDER BY id DESC LIMIT 1',(run,)).fetchone()
    if not row:raise ValueError('plan_required')
    return run,row,[{**x,'_plan_digest':row['digest']} for x in json.loads(row['body'])]

def _normalise_contract(contract):
    """Validate a consumer contract and return a canonical, digestible form."""
    if not isinstance(contract,dict):raise ValueError('accord_contract_invalid')
    route=contract.get('route')
    if route not in ROUTES:raise ValueError('invalid_accord_route')
    members=contract.get('members')
    if not isinstance(members,list) or not members:raise ValueError('accord_members_required')
    seen=set();norm=[]
    for item in members:
        consumer=str(item.get('consumer') or '').strip()
        if not consumer:raise ValueError('consumer_identity_required')
        if consumer in seen:raise ValueError('duplicate_consumer_identity')
        raw_scope=item.get('scope')
        if not isinstance(raw_scope,list) or not raw_scope:raise ValueError('consumer_scope_required')
        scope=[str(x) for x in raw_scope]
        if len(set(scope))!=len(scope):raise ValueError('duplicate_scope_identity')
        raw_expected=item.get('expected')
        if raw_expected is None:raw_expected={}
        if not isinstance(raw_expected,dict):raise ValueError('consumer_expected_invalid')
        expected={str(k):str(v) for k,v in raw_expected.items()}
        if not set(expected)<=set(scope):raise ValueError('consumer_scope_mismatch')
        seen.add(consumer)
        norm.append({'consumer':consumer,'scope':sorted(scope),
                     'expected':{k:expected[k] for k in sorted(expected)}})
    owners={}
    for member in norm:
        for subject,value in member['expected'].items():
            if subject in owners and owners[subject][1]!=value:raise ValueError('consumer_expectation_conflict')
            owners[subject]=(member['consumer'],value)
    raw_deps=contract.get('dependencies') or {}
    if not isinstance(raw_deps,dict):raise ValueError('accord_dependencies_invalid')
    deps=[]
    for consumer,parents in raw_deps.items():
        consumer=str(consumer)
        if not isinstance(parents,list):raise ValueError('accord_dependencies_invalid')
        for parent in parents:
            parent=str(parent)
            if consumer==parent:raise ValueError('accord_dependency_self')
            deps.append((consumer,parent))
    deps=sorted(set(deps))
    version=contract.get('version')
    if isinstance(version,bool) or not isinstance(version,int) or version<=0:raise ValueError('accord_version_required')
    return {'route':route,'version':version,'members':norm,'dependencies':deps,
            'disabled':sorted(str(x) for x in (contract.get('disabled') or []))}

def _contract_digest(norm):
    return _hash({'route':norm['route'],'version':norm['version'],
                  'members':norm['members'],'dependencies':norm['dependencies'],
                  'disabled':norm['disabled']})

def _scope_subjects(norm):
    subjects=set()
    for member in norm['members']:
        subjects.update(member['scope'])
        subjects.update(member['expected'])
    return subjects

def _accord_snapshot(target,subjects,plan_digest,plan_new):
    """Freeze what the target says about every subject the accord touches."""
    if not Path(target).exists():return None
    con=sqlite3.connect(target)
    try:
        has_audit=con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='backfill_audit'").fetchone()
        rows={}
        for subject in sorted(subjects):
            row=con.execute('SELECT amount,computed FROM ledger WHERE id=?',(subject,)).fetchone()
            audit=con.execute('SELECT state FROM backfill_audit WHERE subject=? AND plan_digest=?',
                              (subject,plan_digest)).fetchone() if has_audit else None
            rows[subject]={'value':row[1] if row else None,
                           'amount':row[0] if row else None,
                           'audit':audit[0] if audit else None,
                           'plan_new':plan_new.get(subject)}
        return rows
    finally:con.close()

def _snapshot_hash(snapshot):return _hash(snapshot) if snapshot is not None else _hash(None)

def _member_readiness(member,snapshot,plan_subjects,plan_new,has_target):
    """Adjudicate one consumer's expected values against the frozen facts."""
    reasons=[]
    expected=member['expected']
    for subject in member['scope']:
        if subject not in expected:
            reasons.append(('consumer_expectation_absent',subject));continue
        if subject not in plan_subjects:
            reasons.append(('subject_not_in_plan',subject));continue
        if not has_target:
            reasons.append(('target_missing',subject));continue
        seen=snapshot.get(subject) or {}
        if seen.get('value') is None:
            reasons.append(('missing',subject))
        elif seen.get('audit')!='applied':
            reasons.append(('unproven',subject))
        elif seen.get('value')!=plan_new.get(subject):
            reasons.append(('target_drift',subject))
        elif expected[subject]!=seen.get('value'):
            reasons.append(('consumer_diff',subject))
    for subject in expected:
        if subject not in member['scope']:reasons.append(('consumer_scope_mismatch',subject))
    if any(code in UNCERTAIN_CODES for code,_ in reasons):status='uncertain'
    elif reasons:status='blocked'
    else:status='ready'
    return status,reasons

def _dependency_edges(norm):
    consumers={m['consumer'] for m in norm['members']}
    edges=set()
    for consumer,parent in norm['dependencies']:
        if consumer in consumers and parent in consumers:edges.add((consumer,parent))
    return edges

def _group_components(norm):
    """Connected components over shared subjects and consumer dependencies."""
    consumers=sorted(m['consumer'] for m in norm['members'])
    parent={c:c for c in consumers}
    def find(x):
        while parent[x]!=x:parent[x]=parent[parent[x]];x=parent[x]
        return x
    def union(a,b):
        ra,rb=find(a),find(b)
        if ra!=rb:parent[rb]=ra
    owner={}
    for member in norm['members']:
        for subject in member['scope']:
            if subject in owner:union(owner[subject],member['consumer'])
            else:owner[subject]=member['consumer']
    for consumer,dependency in _dependency_edges(norm):union(consumer,dependency)
    buckets={}
    for consumer in consumers:buckets.setdefault(find(consumer),[]).append(consumer)
    groups=[sorted(items) for items in buckets.values()]
    if norm['route']=='atomic':
        return [{'group':'*','members':consumers}]
    groups.sort()
    return [{'group':'+'.join(items),'members':items} for items in groups]

def _group_status(consumers,readinesses):
    statuses={readinesses[c] for c in consumers}
    if 'uncertain' in statuses:return 'uncertain'
    if 'blocked' in statuses:return 'blocked'
    return 'ready'

def _group_published(db,accord_key,consumers):
    if not consumers:return False
    marks=db.execute('SELECT count(*) FROM accord_member WHERE accord_key=? AND published_at IS NOT NULL AND consumer IN (%s)'
                     % ','.join('?'*len(consumers)),(accord_key,*consumers)).fetchone()[0]
    return marks==len(consumers)

def _accord_row(db,accord_id,version=None):
    if version is None:
        row=db.execute('SELECT * FROM publication_accord WHERE accord_id=? ORDER BY version DESC LIMIT 1',(accord_id,)).fetchone()
    else:
        row=db.execute('SELECT * FROM publication_accord WHERE accord_id=? AND version=?',(accord_id,version)).fetchone()
    if not row:raise ValueError('unknown_accord')
    return row

def _record_reasons(db,accord_key,entries):
    existing=set()
    for row in db.execute('SELECT consumer,code,detail FROM accord_reason WHERE accord_key=?',(accord_key,)):
        existing.add((row['consumer'] or '',row['code'],row['detail']))
    added=False
    for consumer,code,detail in entries:
        key=(consumer or '',code,str(detail))
        if key in existing:continue
        db.execute('INSERT INTO accord_reason(accord_key,consumer,code,detail) VALUES (?,?,?,?)',
                   (accord_key,consumer or None,code,str(detail)))
        existing.add(key);added=True
    return added

def _member_dict(db,accord_key):
    result={}
    for row in db.execute('SELECT * FROM accord_member WHERE accord_key=? ORDER BY consumer',(accord_key,)):
        result[row['consumer']]={'consumer':row['consumer'],'scope':json.loads(row['scope']),
                                 'expected':json.loads(row['expected']),'expected_hash':row['expected_hash'],
                                 'readiness':row['readiness'],'reasons':json.loads(row['reasons']),
                                 'published_at':row['published_at'],'published_target_hash':row['published_target_hash']}
    return result

def _group_dict(db,accord_key):
    result=[]
    for row in db.execute('SELECT * FROM accord_group WHERE accord_key=? ORDER BY group_key',(accord_key,)):
        result.append({'group':row['group_key'],'members':json.loads(row['members']),'status':row['status'],
                       'reasons':json.loads(row['reasons']),'published_at':row['published_at'],
                       'target_hash':row['target_hash']})
    return result

def _status_dict(db,row):
    members=_member_dict(db,row['accord_key'])
    groups=_group_dict(db,row['accord_key'])
    reasons=json.loads(row['reasons'] or '[]')
    return {'accord_id':row['accord_id'],'version':row['version'],'accord_key':row['accord_key'],
            'route':row['route'],'state':row['state'],'plan_digest':row['plan_digest'],'run_id':row['run_id'],
            'target_hash':row['target_hash'],'member_digest':row['member_digest'],
            'expected_digest':row['expected_digest'],'contract_digest':row['contract_digest'],
            'members':[members[k] for k in sorted(members)],
            'groups':groups,'reasons':reasons,
            'published_at':row['published_at']}

def _stored_plan(db,row):
    plan_row=db.execute('SELECT * FROM plan WHERE digest=?',(row['plan_digest'],)).fetchone()
    body=json.loads(plan_row['body']) if plan_row else []
    return [{**x,'_plan_digest':row['plan_digest']} for x in body]

def _live_facts(db,row,target):
    latest_run=db.execute('SELECT max(id) FROM run').fetchone()[0]
    current=db.execute('SELECT digest FROM plan WHERE run_id=? ORDER BY id DESC LIMIT 1',(latest_run,)).fetchone()
    current_digest=current['digest'] if current else None
    plan=_stored_plan(db,row)
    plan_new={x['subject']:x['new'] for x in plan}
    # The frozen subject set defines drift: the plan may contain unrelated
    # subjects, but only this accord's scope is part of its snapshot identity.
    subjects={x['subject'] for x in db.execute('SELECT subject FROM accord_snapshot WHERE accord_key=?',(row['accord_key'],))}
    for member in db.execute('SELECT scope,expected FROM accord_member WHERE accord_key=?',(row['accord_key'],)):
        subjects.update(json.loads(member['scope']));subjects.update(json.loads(member['expected']))
    snapshot=_accord_snapshot(target,subjects,row['plan_digest'],plan_new)
    return {'latest_run':latest_run,'current_digest':current_digest,
            'plan':plan,'plan_subjects':set(plan_new),'plan_new':plan_new,
            'snapshot':snapshot,'target_hash':_snapshot_hash(snapshot)}

def _recompute(db,row,target,record=True):
    """Re-adjudicate member readiness and publishable groups from live facts."""
    facts=_live_facts(db,row,target)
    members=_member_dict(db,row['accord_key'])
    has_target=facts['snapshot'] is not None
    readiness={};entries=[]
    for consumer,member in members.items():
        status,reasons=_member_readiness(member,facts['snapshot'],facts['plan_subjects'],
                                         facts['plan_new'],has_target)
        # A declared downstream reference to an absent member cannot be planned.
        for dependency in [x['depends_on'] for x in db.execute(
                'SELECT depends_on FROM accord_dependency WHERE accord_key=? AND consumer=?',(row['accord_key'],consumer))]:
            if dependency not in members:
                status='uncertain';reasons.append(('dependency_member_absent',dependency))
        readiness[consumer]=status
        members[consumer]['readiness']=status;members[consumer]['reasons']=reasons
        if record:
            db.execute('UPDATE accord_member SET readiness=?,reasons=? WHERE accord_key=? AND consumer=?',
                       (status,json.dumps(reasons),row['accord_key'],consumer))
            entries.extend((consumer,code,detail) for code,detail in reasons)
    norm={'route':row['route'],
          'members':[{'consumer':c,'scope':members[c]['scope'],'expected':members[c]['expected']} for c in sorted(members)],
          'dependencies':[(x['consumer'],x['depends_on']) for x in db.execute(
              'SELECT consumer,depends_on FROM accord_dependency WHERE accord_key=? ORDER BY consumer,depends_on',(row['accord_key'],))]}
    groups=[]
    for spec in _group_components(norm):
        status='published' if _group_published(db,row['accord_key'],spec['members']) else _group_status(spec['members'],readiness)
        reasons=[]
        for consumer in spec['members']:
            for code,detail in members[consumer]['reasons']:
                if code in ('consumer_diff','target_drift','subject_not_in_plan','unproven',
                            'missing','consumer_expectation_absent','dependency_member_absent'):
                    reasons.append((f'{consumer}:{code}',detail))
        for consumer,parent in _dependency_edges(norm):
            if consumer in spec['members'] and parent in spec['members'] and readiness[parent]!='ready':
                reasons.append((f'{consumer}:dependency_not_ready',parent))
        groups.append({'group':spec['group'],'members':spec['members'],'status':status,'reasons':reasons})
        if record:
            db.execute('''INSERT INTO accord_group(accord_key,group_key,members,status,reasons)
                          VALUES (?,?,?,?,?)
                          ON CONFLICT(accord_key,group_key) DO UPDATE SET
                            status=excluded.status,reasons=excluded.reasons''',
                       (row['accord_key'],spec['group'],json.dumps(spec['members']),status,
                        json.dumps([f'{code}:{detail}' for code,detail in reasons])))
        if status!='ready':entries.append((None,f'group_{status}',spec['group']))
    if record and entries:_record_reasons(db,row['accord_key'],entries)
    return {'readiness':readiness,'groups':groups,'facts':facts}

def _reject(db_path,accord_id,version,error):
    """Persist why an accord could not be prepared, so conflicts stay traceable."""
    code=str(error);detail=''
    if len(code)>64:code,detail=code[:64],code[64:]
    try:
        db=connect(db_path)
        try:
            with db:
                db.execute('INSERT INTO accord_rejection(accord_id,version,code,detail) VALUES (?,?,?,?)',
                           (accord_id,int(version) if isinstance(version,int) and not isinstance(version,bool) else None,
                            code,detail))
        finally:db.close()
    except Exception:pass

def prepare_accord(db_path,target,contract,accord_id=None,version=None):
    """Freeze members, expected digest and target snapshot for a consumer accord."""
    raw_id=accord_id or (contract.get('accord_id') if isinstance(contract,dict) else None)
    try:
        if version is not None:
            if isinstance(version,bool) or int(version)<=0:raise ValueError('accord_version_required')
            contract={**contract,'version':int(version)}
        return _prepare_accord(db_path,target,contract,accord_id)
    except ValueError as error:
        _reject(db_path,str(raw_id or ''),version if version is not None
                else (contract.get('version') if isinstance(contract,dict) else None),error)
        raise

def rejections(db_path,accord_id=None):
    db=connect(db_path)
    try:
        if accord_id is None:
            rows=db.execute('SELECT * FROM accord_rejection ORDER BY id').fetchall()
        else:rows=db.execute('SELECT * FROM accord_rejection WHERE accord_id=? ORDER BY id',(accord_id,)).fetchall()
        return [dict(row) for row in rows]
    finally:db.close()

def _prepare_accord(db_path,target,contract,accord_id=None):
    norm=_normalise_contract(contract)
    accord_id=str(accord_id or contract.get('accord_id') or '').strip()
    if not accord_id:raise ValueError('accord_id_required')
    contract_digest=_contract_digest(norm)
    accord_key=f'{accord_id}:v{norm["version"]}'
    db=connect(db_path)
    try:
        existing=db.execute('SELECT * FROM publication_accord WHERE accord_key=?',(accord_key,)).fetchone()
        if existing:
            if existing['contract_digest']!=contract_digest:
                raise ValueError('accord_identity_conflict')
            return _refresh_duplicate(db,existing,target)
        latest_version=db.execute('SELECT COALESCE(max(version),0) FROM publication_accord WHERE accord_id=?',
                                  (accord_id,)).fetchone()[0]
        if norm['version']<=latest_version:raise ValueError('accord_version_conflict')
        run,plan_row,plan=_latest_plan(db)
        plan_new={x['subject']:x['new'] for x in plan}
        plan_subjects=set(plan_new)
        subjects=set(_scope_subjects(norm))
        snapshot=_accord_snapshot(target,subjects,plan_row['digest'],plan_new)
        has_target=snapshot is not None
        target_hash=_snapshot_hash(snapshot)
        members=[];entries=[];readiness={}
        for spec in norm['members']:
            status,reasons=_member_readiness(spec,snapshot,plan_subjects,plan_new,has_target)
            for consumer,parent in norm['dependencies']:
                if consumer==spec['consumer'] and parent not in {m['consumer'] for m in norm['members']}:
                    status='uncertain';reasons.append(('dependency_member_absent',parent))
            readiness[spec['consumer']]=status
            members.append({**spec,'status':status,'reasons':reasons})
            entries.extend((spec['consumer'],code,detail) for code,detail in reasons)
        member_digest=_hash([{'consumer':m['consumer'],'scope':m['scope'],'expected':m['expected']} for m in members])
        expected_digest=_hash({m['consumer']:m['expected'] for m in members})
        state='prepared'
        with db:
            db.execute('''INSERT INTO publication_accord
                (accord_key,accord_id,version,route,plan_digest,run_id,target_path,target_hash,
                 member_digest,expected_digest,contract_digest,contract,state,reasons)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (accord_key,accord_id,norm['version'],norm['route'],plan_row['digest'],run,str(target),
                 target_hash,member_digest,expected_digest,contract_digest,
                 json.dumps({'route':norm['route'],'members':norm['members'],
                             'dependencies':norm['dependencies']},sort_keys=True),
                 state,'[]'))
            for member in members:
                db.execute('''INSERT INTO accord_member
                    (accord_key,consumer,scope,expected,expected_hash,readiness,reasons)
                    VALUES (?,?,?,?,?,?,?)''',
                    (accord_key,member['consumer'],json.dumps(member['scope']),json.dumps(member['expected']),
                     _hash(member['expected']),member['status'],
                     json.dumps([f'{code}:{detail}' for code,detail in member['reasons']])))
            for consumer,parent in norm['dependencies']:
                db.execute('INSERT OR IGNORE INTO accord_dependency(accord_key,consumer,depends_on) VALUES (?,?,?)',
                           (accord_key,consumer,parent))
            for subject,seen in (snapshot or {}).items():
                db.execute('INSERT INTO accord_snapshot(accord_key,subject,value,audit_state,plan_new) VALUES (?,?,?,?,?)',
                           (accord_key,subject,seen['value'],seen['audit'],seen['plan_new']))
            row=db.execute('SELECT * FROM publication_accord WHERE accord_key=?',(accord_key,)).fetchone()
            _recompute(db,row,target)
            if entries:_record_reasons(db,accord_key,entries)
        return _status_dict(db,db.execute('SELECT * FROM publication_accord WHERE accord_key=?',(accord_key,)).fetchone())
    finally:db.close()

def _refresh_duplicate(db,row,target):
    """A repeated prepare never rewrites frozen data; it only reports live drift."""
    with db:
        facts=_live_facts(db,row,target)
        entries=[]
        if facts['latest_run']!=row['run_id'] or facts['current_digest']!=row['plan_digest']:
            entries.append((None,'plan_changed',f'{row["run_id"]}->{facts["latest_run"]}'))
        if facts['target_hash']!=row['target_hash']:
            entries.append((None,'target_drift','snapshot'))
        if entries:_record_reasons(db,row['accord_key'],entries)
        if entries and row['state'] not in ('published','partially_published'):
            summary=json.loads(row['reasons'] or '[]')
            for _,code,detail in entries:
                label=f'{code}:{detail}' if detail else code
                if label not in summary:summary.append(label)
            db.execute('UPDATE publication_accord SET state=?,reasons=? WHERE accord_key=?',
                       ('stale',json.dumps(summary),row['accord_key']))
            _recompute(db,db.execute('SELECT * FROM publication_accord WHERE accord_key=?',(row['accord_key'],)).fetchone(),target)
    return _status_dict(db,db.execute('SELECT * FROM publication_accord WHERE accord_key=?',(row['accord_key'],)).fetchone())

def recheck_accord(db_path,target,accord_id,version=None):
    """Re-adjudicate readiness and publishable groups against current rules/target."""
    db=connect(db_path)
    try:
        row=_accord_row(db,accord_id,version)
        with db:
            facts=_live_facts(db,row,target)
            entries=[]
            if facts['latest_run']!=row['run_id'] or facts['current_digest']!=row['plan_digest']:
                entries.append((None,'plan_changed',f'{row["run_id"]}->{facts["latest_run"]}'))
            if facts['target_hash']!=row['target_hash']:
                entries.append((None,'target_drift','snapshot'))
            if entries:
                summary=json.loads(row['reasons'] or '[]')
                for _,code,detail in entries:
                    label=f'{code}:{detail}' if detail else code
                    if label not in summary:summary.append(label)
                state=row['state'] if row['state'] in ('published','partially_published') else 'stale'
                db.execute('UPDATE publication_accord SET state=?,reasons=? WHERE accord_key=?',
                           (state,json.dumps(summary),row['accord_key']))
                _record_reasons(db,row['accord_key'],entries)
            _recompute(db,db.execute('SELECT * FROM publication_accord WHERE accord_key=?',(row['accord_key'],)).fetchone(),target)
            row=db.execute('SELECT * FROM publication_accord WHERE accord_key=?',(row['accord_key'],)).fetchone()
            status=_status_dict(db,row)
            status['live_target_hash']=facts['target_hash']
            status['live_plan_digest']=facts['current_digest']
        return status
    finally:db.close()

def publish_accord(db_path,target,accord_id,version=None,consumers=None):
    """Publish a whole accord (atomic) or its independent ready groups."""
    db=connect(db_path)
    try:
        row=_accord_row(db,accord_id,version)
        members={m['consumer']:m for m in _member_dict(db,row['accord_key']).values()}
        if consumers is not None:
            consumers=list(consumers)
            unknown=[c for c in consumers if c not in members]
            if unknown:raise ValueError('unknown_accord_member')
        with db:
            row=db.execute('SELECT * FROM publication_accord WHERE accord_key=?',(row['accord_key'],)).fetchone()
            facts=_live_facts(db,row,target)
            changed=[]
            if facts['latest_run']!=row['run_id'] or facts['current_digest']!=row['plan_digest']:
                changed.append(('plan_changed',f'{row["run_id"]}->{facts["latest_run"]}'))
            if facts['target_hash']!=row['target_hash'] or facts['snapshot'] is None:
                changed.append(('target_drift','snapshot') if facts['snapshot'] is not None
                               else ('target_missing','snapshot'))
            if changed:
                summary=json.loads(row['reasons'] or '[]')
                for code,detail in changed:
                    label=f'{code}:{detail}' if detail else code
                    if label not in summary:summary.append(label)
                # A published agreement keeps its member set and frozen snapshot;
                # later drift is reported as an observation, never a rewrite.
                if row['state']=='published':
                    db.execute('UPDATE publication_accord SET reasons=? WHERE accord_key=?',
                               (json.dumps(summary),row['accord_key']))
                    _record_reasons(db,row['accord_key'],[(None,code,detail) for code,detail in changed])
                    status=_status_dict(db,db.execute('SELECT * FROM publication_accord WHERE accord_key=?',(row['accord_key'],)).fetchone())
                    status['status']='published'
                    status['drift']=[f'{code}:{detail}' for code,detail in changed]
                    return status
                db.execute('UPDATE publication_accord SET state=?,reasons=? WHERE accord_key=?',
                           ('stale',json.dumps(summary),row['accord_key']))
                _record_reasons(db,row['accord_key'],[(None,code,detail) for code,detail in changed])
                status=_status_dict(db,db.execute('SELECT * FROM publication_accord WHERE accord_key=?',(row['accord_key'],)).fetchone())
                status['status']='stale'
                return status
            _recompute(db,row,target)
            row=db.execute('SELECT * FROM publication_accord WHERE accord_key=?',(row['accord_key'],)).fetchone()
            if row['state']=='published':
                status=_status_dict(db,row);status['status']='published';return status
            groups=_group_dict(db,row['accord_key'])
            requested_groups=None
            if consumers is not None:
                requested_groups={g['group'] for g in groups if set(g['members'])&set(consumers)}
            target_hash=row['target_hash']
            published_groups=[];blocked_groups=[]
            for group in groups:
                group=dict(group)
                already=_group_published(db,row['accord_key'],group['members'])
                if already:group['status']='published'
                if already:
                    published_groups.append(group['group']);continue
                selected=requested_groups is None or group['group'] in requested_groups
                if not selected:continue
                if group['status']=='ready':
                    stamp=_now()
                    db.execute("UPDATE accord_group SET status='published',published_at=?,target_hash=? WHERE accord_key=? AND group_key=?",
                               (stamp,target_hash,row['accord_key'],group['group']))
                    for consumer in group['members']:
                        db.execute('UPDATE accord_member SET published_at=?,published_target_hash=? WHERE accord_key=? AND consumer=?',
                                   (stamp,target_hash,row['accord_key'],consumer))
                        members[consumer]['published_at']=stamp;members[consumer]['published_target_hash']=target_hash
                    published_groups.append(group['group'])
                else:
                    blocked_groups.append({'group':group['group'],'members':group['members'],
                                           'status':group['status'],'reasons':group['reasons']})
                    _record_reasons(db,row['accord_key'],[(None,f'group_{group["status"]}',group['group'])])
            remaining=db.execute("SELECT count(*) FROM accord_group WHERE accord_key=? AND status!='published'",
                                 (row['accord_key'],)).fetchone()[0]
            if remaining==0:state='published';published_at=_now()
            elif published_groups:state='partially_published';published_at=None
            else:state='blocked';published_at=None
            db.execute('UPDATE publication_accord SET state=?,published_at=COALESCE(published_at,?) WHERE accord_key=?',
                       (state,published_at,row['accord_key']))
            status=_status_dict(db,db.execute('SELECT * FROM publication_accord WHERE accord_key=?',(row['accord_key'],)).fetchone())
            status['status']='published' if state=='published' else ('partially_published' if state=='partially_published' else 'blocked')
            status['published_groups']=sorted(set(published_groups))
            status['blocked_groups']=blocked_groups
            return status
    finally:db.close()

def accord_status(db_path,accord_id,version=None):
    db=connect(db_path)
    try:return _status_dict(db,_accord_row(db,accord_id,version))
    finally:db.close()

def list_accords(db_path,accord_id=None):
    db=connect(db_path)
    try:
        if accord_id:
            rows=db.execute('SELECT * FROM publication_accord WHERE accord_id=? ORDER BY version',(accord_id,)).fetchall()
        else:rows=db.execute('SELECT * FROM publication_accord ORDER BY accord_id,version').fetchall()
        return [_status_dict(db,row) for row in rows]
    finally:db.close()

def consumer_versions(db_path):
    """Per-consumer publication history; mixed target snapshots are inconsistent."""
    db=connect(db_path)
    try:
        rows=db.execute('''SELECT m.consumer,m.published_at,m.published_target_hash,
                                  a.accord_id,a.version,a.route
                           FROM accord_member m JOIN publication_accord a USING(accord_key)
                           WHERE m.published_at IS NOT NULL ORDER BY m.consumer,a.accord_id,a.version''').fetchall()
        by_consumer={}
        for row in rows:
            by_consumer.setdefault(row['consumer'],[]).append({
                'accord_id':row['accord_id'],'version':row['version'],'route':row['route'],
                'target_hash':row['published_target_hash']})
        result=[]
        for consumer in sorted(by_consumer):
            history=by_consumer[consumer]
            hashes={x['target_hash'] for x in history}
            result.append({'consumer':consumer,'published':history,
                           'published_target_hashes':sorted(hashes),
                           'consistent':len(hashes)<=1,
                           'reason':None if len(hashes)<=1 else 'mixed_target_snapshots'})
        return result
    finally:db.close()
