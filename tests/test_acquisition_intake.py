import json
from pathlib import Path

import pytest

from backfillcomp.acquisition import evaluate_manual, record
from backfillcomp.ingest import initialize, load


FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def setup(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'acquisition-config.json')
    load(db, FIX / 'acquisition-base.jsonl')
    record(db, FIX / 'acquisition-pool.json')
    return db


def test_manual_acquisition_replays_existing_pipeline(tmp_path):
    db = setup(tmp_path)
    assert record(db, FIX / 'acquisition-pool.json') == 'acq-P1'
    result = evaluate_manual(db, FIX / 'acquisition-manual.json')
    assert result == evaluate_manual(db, FIX / 'acquisition-manual.json')
    assert result['cost'] == 5
    assert result['ready'] == ['a', 'b', 'c', 'd']
    assert result['weighted_ready'] == 4
    assert result['statuses']['e'] == '待核'
    weighted = evaluate_manual(db, FIX / 'acquisition-manual-weighted.json')
    assert weighted['cost'] == 4
    assert weighted['ready'] == ['e']
    assert weighted['weighted_ready'] == 10


def test_competing_evidence_can_reduce_readiness(tmp_path):
    db = setup(tmp_path)
    plan = {'plan_id':'duplicate-A','pool_id':'acq-P1','selected':['F1','F2','F3']}
    path = tmp_path / 'manual.json'
    path.write_text(json.dumps(plan),encoding='utf-8')
    result = evaluate_manual(db,path)
    assert result['statuses']['a'] == '待核'
    assert result['statuses']['d'] == '待核'
    plan['selected'] = ['F5']
    path.write_text(json.dumps(plan),encoding='utf-8')
    with pytest.raises(ValueError,match='acquisition_plan_conflict'):
        evaluate_manual(db,path)
