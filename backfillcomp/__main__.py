import argparse,tempfile
from pathlib import Path
from .ingest import initialize,load
from .rules import evaluate, action
from .report import export,verify

def _demo_repair(work,fixture):
    """在显式沙箱中演示两个提交点之间中断后的两条恢复路线。"""
    import hashlib,json,sqlite3
    from . import rules
    from .schedule import define_window,reserve,activate
    from .publish import check
    from .repair import inspect as repair_inspect, decide as repair_decide
    from .report import repair_report
    expected=json.loads((fixture/'consumer-expected.json').read_text(encoding='utf-8'))

    def build(prefix):
        cdb=work/f'{prefix}-control.db'
        initialize(cdb,fixture/'config.json');load(cdb,fixture/'events.jsonl');evaluate(cdb)
        plan=work/f'{prefix}-plan.json';action('plan',cdb,out=plan)
        digest=hashlib.sha256(plan.read_bytes()).hexdigest()
        define_window(cdb,'W1','2020-01-01T00:00:00Z','2099-01-01T00:00:00Z',2)
        reserve(cdb,'L1',digest,'W1','worker-A',2);activate(cdb,'L1')
        sandbox=work/f'{prefix}-sandbox'
        def crash():raise RuntimeError('injected_crash_between_commits')
        old=rules._fault_between_commits;rules._fault_between_commits=crash
        try:
            try:action('apply',cdb,sandbox=sandbox,out=plan,lease='L1')
            except RuntimeError as exc:print({'injected_interruption':str(exc),'case':prefix})
        finally:rules._fault_between_commits=old
        con=sqlite3.connect(sandbox/'ledger.db')
        assert con.execute("SELECT count(*) FROM backfill_audit WHERE state='applied'").fetchone()[0]==2;con.close()
        return cdb,plan,sandbox

    # 路线一：前进——补记控制库 execution，补偿成为已完成，发布门禁可判 ready。
    fdb,fplan,fsandbox=build('forward')
    print(repair_inspect(fdb,fsandbox,fplan,'CASE-F',worker='worker-A',lease_id='L1'))
    print(repair_decide(fdb,fsandbox,fplan,'CASE-F','forward',worker='worker-A',lease_id='L1'))
    print({'forward_idempotent':repair_decide(fdb,fsandbox,fplan,'CASE-F','forward',worker='worker-A',lease_id='L1')})
    print({'publication_after_forward':check(fdb,fsandbox/'ledger.db','G1','consumer-A',expected)})

    # 路线二：回退——逆序恢复旧值并记录 rolled_back，发布门禁只能 uncertain/blocked。
    rdb,rplan,rsandbox=build('rollback')
    print(repair_inspect(rdb,rsandbox,rplan,'CASE-R',worker='worker-A',lease_id='L1'))
    print(repair_decide(rdb,rsandbox,rplan,'CASE-R','rollback',worker='worker-A',lease_id='L1'))
    print({'publication_after_rollback':check(rdb,rsandbox/'ledger.db','G2','consumer-A',expected)})
    print(repair_report(rdb,work/'repair-report.json'))
    print({'repair_report':str(work/'repair-report.json')})

def main():
    p=argparse.ArgumentParser(description='离线证据判定工具');sub=p.add_subparsers(dest='command',required=True)
    d=sub.add_parser('demo');d.add_argument('--workdir')
    i=sub.add_parser('init');i.add_argument('--db',required=True);i.add_argument('--config',required=True)
    l=sub.add_parser('load');l.add_argument('--db',required=True);l.add_argument('path')
    e=sub.add_parser('evaluate');e.add_argument('--db',required=True)
    r=sub.add_parser('report');r.add_argument('--db',required=True);r.add_argument('--out',required=True)
    v=sub.add_parser('verify');v.add_argument('--db',required=True);v.add_argument('--sandbox');v.add_argument('--out')
    for name in ('plan','recheck','apply','rollback'):
        x=sub.add_parser(name);x.add_argument('--db',required=True);x.add_argument('--sandbox');x.add_argument('--out');x.add_argument('--lease')
    w=sub.add_parser('window');w.add_argument('--db',required=True);w.add_argument('--id',required=True);w.add_argument('--starts',required=True);w.add_argument('--ends',required=True);w.add_argument('--units',required=True,type=int)
    rs=sub.add_parser('reserve');rs.add_argument('--db',required=True);rs.add_argument('--id',required=True);rs.add_argument('--plan',required=True);rs.add_argument('--window',required=True);rs.add_argument('--worker',required=True);rs.add_argument('--units',required=True,type=int)
    for name in ('activate','release-lease'):
        x=sub.add_parser(name);x.add_argument('--db',required=True);x.add_argument('--id',required=True)
    pc=sub.add_parser('publication-check');pc.add_argument('--db',required=True);pc.add_argument('--target',required=True);pc.add_argument('--id',required=True);pc.add_argument('--consumer',required=True);pc.add_argument('--expected',required=True)
    pb=sub.add_parser('publish');pb.add_argument('--db',required=True);pb.add_argument('--target',required=True);pb.add_argument('--id',required=True)
    mg=sub.add_parser('migrate');mg.add_argument('--db',required=True)
    ri=sub.add_parser('repair-inspect');ri.add_argument('--db',required=True);ri.add_argument('--sandbox',required=True);ri.add_argument('--plan',required=True);ri.add_argument('--id',required=True);ri.add_argument('--worker');ri.add_argument('--lease')
    rd=sub.add_parser('repair-decide');rd.add_argument('--db',required=True);rd.add_argument('--sandbox',required=True);rd.add_argument('--plan',required=True);rd.add_argument('--id',required=True);rd.add_argument('--route',required=True,choices=('forward','rollback'));rd.add_argument('--worker');rd.add_argument('--lease')
    rr=sub.add_parser('repair-report');rr.add_argument('--db',required=True);rr.add_argument('--out')
    a=p.parse_args()
    if a.command=='demo':
        work=Path(a.workdir or tempfile.mkdtemp(prefix='evidence-'));work.mkdir(parents=True,exist_ok=True);db=work/'ledger.db';fixture=Path(__file__).resolve().parent.parent/'fixtures'
        initialize(db,fixture/'config.json');load(db,fixture/'events.jsonl');print(evaluate(db));print(export(db,work/'report.json'))
        _demo_repair(work,fixture)
    elif a.command=='init':print(initialize(a.db,a.config))
    elif a.command=='load':print(load(a.db,a.path))
    elif a.command=='evaluate':print(evaluate(a.db))
    elif a.command=='report':print(export(a.db,a.out))
    elif a.command=='verify':
        from . import rules
        checks=verify(a.db)
        if hasattr(rules,'verify_decisions'):checks+=rules.verify_decisions(a.db)
        target=rules.verify_target(a.db,a.sandbox) if a.sandbox and hasattr(rules,'verify_target') else []
        if a.out:
            import json
            out=Path(a.out);out.parent.mkdir(parents=True,exist_ok=True)
            out.write_text(json.dumps({'integrity_problems':checks,'target':target},ensure_ascii=False,indent=2),encoding='utf-8')
            out.with_suffix('.md').write_text('# 独立对账\n\n'+'\n'.join(f'- {x}' for x in target),encoding='utf-8')
        print({'integrity_problems':checks,'target':target})
    elif a.command=='window':
        from .schedule import define_window
        print(define_window(a.db,a.id,a.starts,a.ends,a.units))
    elif a.command=='reserve':
        import hashlib
        from .schedule import reserve
        print(reserve(a.db,a.id,hashlib.sha256(Path(a.plan).read_bytes()).hexdigest(),a.window,a.worker,a.units))
    elif a.command in ('activate','release-lease'):
        from .schedule import activate,release
        print((activate if a.command=='activate' else release)(a.db,a.id))
    elif a.command=='publication-check':
        import json
        from .publish import check
        print(check(a.db,a.target,a.id,a.consumer,json.loads(Path(a.expected).read_text(encoding='utf-8'))))
    elif a.command=='publish':
        from .publish import publish
        print(publish(a.db,a.target,a.id))
    elif a.command=='migrate':
        from .repair import migrate
        print(migrate(a.db))
    elif a.command=='repair-inspect':
        from .repair import inspect
        print(inspect(a.db,a.sandbox,a.plan,a.id,worker=a.worker,lease_id=a.lease))
    elif a.command=='repair-decide':
        from .repair import decide
        print(decide(a.db,a.sandbox,a.plan,a.id,a.route,worker=a.worker,lease_id=a.lease))
    elif a.command=='repair-report':
        from .report import repair_report
        print(repair_report(a.db,a.out))
    else:print(action(a.command,a.db,sandbox=getattr(a,'sandbox',None),out=getattr(a,'out',None),lease=getattr(a,'lease',None)))
if __name__=='__main__':main()
