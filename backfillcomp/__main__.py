import argparse,json,tempfile
from pathlib import Path
from .ingest import initialize,load
from .rules import evaluate, action
from .report import export,verify
def main(argv=None):
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
    w.add_argument('--pool');w.add_argument('--pool-units',type=int);w.add_argument('--policy',choices=('strict_fifo','fit_backfill'))
    rs=sub.add_parser('reserve');rs.add_argument('--db',required=True);rs.add_argument('--id',required=True);rs.add_argument('--plan',required=True);rs.add_argument('--window',required=True);rs.add_argument('--worker',required=True);rs.add_argument('--units',required=True,type=int)
    for name in ('activate','release-lease'):
        x=sub.add_parser(name);x.add_argument('--db',required=True);x.add_argument('--id',required=True)
    cs=sub.add_parser('capacity-status');cs.add_argument('--db',required=True);cs.add_argument('--pool')
    ca=sub.add_parser('capacity-audit');ca.add_argument('--db',required=True);ca.add_argument('--pool');ca.add_argument('--lease');ca.add_argument('--limit',type=int)
    rc=sub.add_parser('recover');rc.add_argument('--db',required=True)
    pc=sub.add_parser('publication-check');pc.add_argument('--db',required=True);pc.add_argument('--target',required=True);pc.add_argument('--id',required=True);pc.add_argument('--consumer',required=True);pc.add_argument('--expected',required=True)
    pb=sub.add_parser('publish');pb.add_argument('--db',required=True);pb.add_argument('--target',required=True);pb.add_argument('--id',required=True)
    a=p.parse_args(argv)
    if a.command=='demo':
        work=Path(a.workdir or tempfile.mkdtemp(prefix='evidence-'));work.mkdir(parents=True,exist_ok=True);db=work/'ledger.db';fixture=Path(__file__).resolve().parent.parent/'fixtures'
        initialize(db,fixture/'config.json');load(db,fixture/'events.jsonl');print(evaluate(db));print(export(db,work/'report.json'))
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
            out=Path(a.out);out.parent.mkdir(parents=True,exist_ok=True)
            out.write_text(json.dumps({'integrity_problems':checks,'target':target},ensure_ascii=False,indent=2),encoding='utf-8')
            out.with_suffix('.md').write_text('# 独立对账\n\n'+'\n'.join(f'- {x}' for x in target),encoding='utf-8')
        print({'integrity_problems':checks,'target':target})
    elif a.command=='window':
        from .schedule import define_window
        print(define_window(a.db,a.id,a.starts,a.ends,a.units,pool_id=a.pool,
                            pool_units=a.pool_units,promotion_policy=a.policy))
    elif a.command=='reserve':
        import hashlib
        from .schedule import reserve
        print(reserve(a.db,a.id,hashlib.sha256(Path(a.plan).read_bytes()).hexdigest(),a.window,a.worker,a.units))
    elif a.command in ('activate','release-lease'):
        from .schedule import activate,release
        print((activate if a.command=='activate' else release)(a.db,a.id))
    elif a.command=='capacity-status':
        from .schedule import capacity_status
        print(json.dumps(capacity_status(a.db,a.pool),ensure_ascii=False))
    elif a.command=='capacity-audit':
        from .schedule import capacity_audit
        print(json.dumps(capacity_audit(a.db,a.pool,a.lease,a.limit),ensure_ascii=False))
    elif a.command=='recover':
        from .schedule import recover
        print(json.dumps(recover(a.db),ensure_ascii=False))
    elif a.command=='publication-check':
        from .publish import check
        print(check(a.db,a.target,a.id,a.consumer,json.loads(Path(a.expected).read_text(encoding='utf-8'))))
    elif a.command=='publish':
        from .publish import publish
        print(publish(a.db,a.target,a.id))
    else:print(action(a.command,a.db,sandbox=getattr(a,'sandbox',None),out=getattr(a,'out',None),lease=getattr(a,'lease',None)))
if __name__=='__main__':main()
