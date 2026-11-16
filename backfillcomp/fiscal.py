"""Freeze a local accounting-period calendar without deciding compensation."""
import hashlib
import json
import re
from pathlib import Path

from .store import connect
from .schedule import _utc


def register(db_path, calendar_id, file_path):
    if not calendar_id:
        raise ValueError('calendar_id_required')
    raw = Path(file_path).read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    body = json.loads(raw)
    if not isinstance(body, list) or not body:
        raise ValueError('fiscal_periods_required')
    periods = []
    seen = set()
    for row in body:
        if not isinstance(row, dict):
            raise ValueError('fiscal_period_invalid')
        period = row.get('period')
        state = row.get('state')
        closed_at = row.get('closed_at')
        if not isinstance(period, str) or not re.fullmatch(r'\d{4}-(?:0[1-9]|1[0-2])', period):
            raise ValueError('fiscal_period_invalid')
        if period in seen or state not in {'open', 'closed'}:
            raise ValueError('fiscal_period_duplicate_or_state_invalid')
        if state == 'closed':
            if not isinstance(closed_at, str):
                raise ValueError('fiscal_close_time_required')
            _utc(closed_at)
        elif closed_at is not None:
            raise ValueError('open_period_cannot_have_close_time')
        seen.add(period)
        periods.append((calendar_id, period, state, closed_at))
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT digest FROM fiscal_calendar WHERE id=?', (calendar_id,)).fetchone()
            if old:
                if old['digest'] != digest:
                    raise ValueError('fiscal_calendar_identity_conflict')
                return 0
            db.execute('INSERT INTO fiscal_calendar VALUES (?,?,?)', (calendar_id, digest, raw))
            db.executemany('INSERT INTO fiscal_period VALUES (?,?,?,?)', periods)
        return len(periods)
    finally:
        db.close()


def list_periods(db_path, calendar_id):
    db = connect(db_path)
    try:
        return [dict(row) for row in db.execute(
            'SELECT p.period,p.state,p.closed_at,c.digest AS calendar_digest '
            'FROM fiscal_period p JOIN fiscal_calendar c ON c.id=p.calendar_id '
            'WHERE p.calendar_id=? ORDER BY p.period', (calendar_id,))]
    finally:
        db.close()
