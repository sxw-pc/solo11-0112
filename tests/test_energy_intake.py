import json
from pathlib import Path

import pytest

from backfillcomp.energy import meter_summary, record
from backfillcomp.ingest import initialize


FIX = Path(__file__).resolve().parent.parent / 'fixtures'


def test_meter_summary_reconstructs_reset_and_totals(tmp_path):
    db = tmp_path / 'ledger.db'
    initialize(db, FIX / 'config.json')
    assert record(db, FIX / 'energy-meter.json') == 'meter-S1'
    assert record(db, FIX / 'energy-meter.json') == 'meter-S1'
    result = meter_summary(db, 'meter-S1')
    assert [slot['energy_wh'] for slot in result['intervals']] == [1000, 4000, 6000, 12000]
    assert result['total_wh'] == 23000
    assert (result['total_grams_num'], result['total_grams_den']) == (2300, 1)


def test_meter_same_id_conflict_keeps_first_evidence(tmp_path):
    db = tmp_path / 'ledger.db'
    initialize(db, FIX / 'config.json')
    record(db, FIX / 'energy-meter.json')
    changed = json.loads((FIX / 'energy-meter.json').read_text(encoding='utf-8'))
    changed['slots'][3]['intensity_g_per_kwh'] = 200
    path = tmp_path / 'changed.json'
    path.write_text(json.dumps(changed), encoding='utf-8')
    with pytest.raises(ValueError, match='energy_source_conflict'):
        record(db, path)
