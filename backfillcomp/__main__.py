import argparse,tempfile
from pathlib import Path
from .ingest import initialize,load
from .rules import evaluate, action
from .report import export,verify
def main():
    p=argparse.ArgumentParser(description='离线证据判定工具');sub=p.add_subparsers(dest='command',required=True)
    d=sub.add_parser('demo');d.add_argument('--workdir')
    i=sub.add_parser('init');i.add_argument('--db',required=True);i.add_argument('--config',required=True)
    er=sub.add_parser('energy-record');er.add_argument('--db',required=True);er.add_argument('--input',required=True)
    es=sub.add_parser('energy-summary');es.add_argument('--db',required=True);es.add_argument('--id',required=True)
    ea=sub.add_parser('energy-attribute');ea.add_argument('--db',required=True);ea.add_argument('--id',required=True);ea.add_argument('--contract',required=True)
    ev=sub.add_parser('energy-verify');ev.add_argument('--db',required=True);ev.add_argument('--id');ev.add_argument('--contract')
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
    a=p.parse_args()
    if a.command=='demo':
        work=Path(a.workdir or tempfile.mkdtemp(prefix='evidence-'));work.mkdir(parents=True,exist_ok=True);db=work/'ledger.db';fixture=Path(__file__).resolve().parent.parent/'fixtures'
        initialize(db,fixture/'config.json');load(db,fixture/'events.jsonl');print(evaluate(db));print(export(db,work/'report.json'))
        from .energy import CONTRACTS,attribute,record as energy_record,verify_energy
        energy_record(db,fixture/'energy-meter.json')
        for contract in CONTRACTS:
            ledger=attribute(db,'meter-S1',contract)
            print(contract,ledger['status'],ledger['totals'])
        print('energy_verify',verify_energy(db))
    elif a.command=='init':print(initialize(a.db,a.config))
    elif a.command=='energy-record':
        from .energy import record
        print(record(a.db,a.input))
    elif a.command=='energy-summary':
        from .energy import meter_summary
        print(meter_summary(a.db,a.id))
    elif a.command=='energy-attribute':
        from .energy import attribute
        print(attribute(a.db,a.id,a.contract))
    elif a.command=='energy-verify':
        from .energy import verify_energy
        print(verify_energy(a.db,a.id,a.contract))
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
    else:print(action(a.command,a.db,sandbox=getattr(a,'sandbox',None),out=getattr(a,'out',None),lease=getattr(a,'lease',None)))
if __name__=='__main__':main()
