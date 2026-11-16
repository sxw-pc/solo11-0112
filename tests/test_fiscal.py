import json
from pathlib import Path

import pytest

from backfillcomp import fiscal


FIXTURE = Path(__file__).resolve().parent.parent / 'fixtures' / 'fiscal-periods.json'


def test_fiscal_calendar_freezes_source(tmp_path):
    db = tmp_path / 'ledger.db'
    assert fiscal.register(db, 'calendar-1', FIXTURE) == 3
    assert fiscal.register(db, 'calendar-1', FIXTURE) == 0
    rows = fiscal.list_periods(db, 'calendar-1')
    assert [row['state'] for row in rows] == ['closed', 'open', 'open']
    changed = json.loads(FIXTURE.read_text())
    changed[0]['closed_at'] = '2026-03-02T00:00:00Z'
    other = tmp_path / 'changed.json'
    other.write_text(json.dumps(changed))
    with pytest.raises(ValueError, match='identity_conflict'):
        fiscal.register(db, 'calendar-1', other)


def test_closed_period_requires_utc_time(tmp_path):
    body = [{'period': '2026-01', 'state': 'closed', 'closed_at': '2026-03-01T00:00:00'}]
    other = tmp_path / 'bad.json'
    other.write_text(json.dumps(body))
    with pytest.raises(ValueError, match='timezone_required'):
        fiscal.register(tmp_path / 'ledger.db', 'calendar-1', other)
