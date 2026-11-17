import pytest
from pathlib import Path

from backfillcomp.causality import manual_diff,record_pair
from backfillcomp.ingest import initialize,load
from backfillcomp.rules import evaluate
from backfillcomp.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def test_pair_freezes_real_status_flip(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db,FIX / 'config.json')
    load(db,FIX / 'events.jsonl')
    assert evaluate(db)['r2'] == '可补偿'
    load(db,FIX / 'causality-dup-r1.jsonl')
    load(db,FIX / 'causality-dup-r2.jsonl')
    assert evaluate(db)['r2'] == '待核'
    assert record_pair(db,'pair-1',1,2,'r2') == 'pair-1'
    assert record_pair(db,'pair-1',1,2,'r2') == 'pair-1'
    diff = manual_diff(db,'pair-1')
    assert diff['changed_subjects'] == ['r1','r2']
    assert diff['target_before'] == '可补偿'
    assert diff['target_after'] == '待核'
    assert len(diff['new_event_ids']) == 2


def test_pair_identity_conflict(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db,FIX / 'config.json')
    load(db,FIX / 'events.jsonl')
    evaluate(db)
    load(db,FIX / 'causality-dup-r1.jsonl')
    evaluate(db)
    record_pair(db,'pair-1',1,2,'r2')
    con = connect(db)
    con.execute("UPDATE raw_event SET digest='tampered' WHERE event_key='dup-r1'")
    con.commit()
    con.close()
    with pytest.raises(ValueError,match='causality_pair_conflict'):
        record_pair(db,'pair-1',1,2,'r2')
