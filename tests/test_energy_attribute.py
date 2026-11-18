import hashlib
import json
from fractions import Fraction
from pathlib import Path

import pytest

from backfillcomp.energy import (
    CONTRACTS, attribute, meter_summary, record, verify_energy)
from backfillcomp.ingest import initialize, load
from backfillcomp.report import export, verify
from backfillcomp.rules import evaluate
from backfillcomp.store import connect

FIX = Path(__file__).resolve().parent.parent / 'fixtures'


def frac(num, den=1):
    return {'num': num, 'den': den}


def share(value):
    return Fraction(value['num'], value['den'])


def setup_db(tmp_path, fixture='energy-meter.json'):
    db = tmp_path / 'ledger.db'
    initialize(db, FIX / 'config.json')
    record(db, FIX / fixture)
    return db


def write_doc(tmp_path, doc):
    path = tmp_path / f"{doc['source_id']}.json"
    path.write_text(json.dumps(doc), encoding='utf-8')
    return path


def base_doc(source_id, boundaries, slots, calibration, resets=None, jobs=None):
    return {
        'source_id': source_id,
        'batch_id': 'backfill-demo',
        'unit': 'Wh integer; gCO2e per kWh project-local synthetic factor',
        'jobs': jobs or [{'id': 'A'}, {'id': 'B'}],
        'boundaries': [{'id': f'T{i}', 'reading_wh': r} for i, r in enumerate(boundaries)],
        'resets': resets or [],
        'slots': slots,
        'coalition_calibration_wh': calibration,
    }


def insert_doctored_source(db, source_id, mutate):
    doc = json.loads((FIX / 'energy-meter.json').read_text(encoding='utf-8'))
    doc['source_id'] = source_id
    mutate(doc)
    raw = json.dumps(doc).encode('utf-8')
    con = connect(db)
    with con:
        con.execute('INSERT INTO energy_source VALUES (?,?,?,?)',
                    (source_id, 'backfill-demo', hashlib.sha256(raw).hexdigest(), raw))
    con.close()
    return doc


# 1. 复位衔接与总量守恒

def test_reset_bridges_and_totals(tmp_path):
    db = setup_db(tmp_path)
    summary = meter_summary(db, 'meter-S1')
    assert [i['energy_wh'] for i in summary['intervals']] == [1000, 4000, 6000, 12000]
    assert summary['total_wh'] == 23000
    for contract in CONTRACTS:
        ledger = attribute(db, 'meter-S1', contract)
        assert ledger['status'] == '已归属'
        assert [s['energy_wh'] for s in ledger['slots']] == [1000, 4000, 6000, 12000]
        # T3 后的复位证明 old 111000 -> new 0 衔接 S4，而不是负耗能或猜测复位
        assert ledger['slots'][3]['coalition'] == 'A,B'
        assert ledger['totals']['energy_wh'] == 23000
        assert ledger['totals']['grams'] == frac(2300)
        for slot in ledger['slots']:
            shares = sum((share(v) for v in slot['job_shares_wh'].values()), Fraction(0))
            assert shares + share(slot['platform_wh']) == slot['energy_wh']
            grams = sum((share(v) for v in slot['job_grams'].values()), Fraction(0))
            assert grams + share(slot['platform_grams']) == share(slot['slot_grams'])


# 2. 双合同在同一来源下产生不同逐作业账且总量守恒

def test_direct_platform_overhead_contract(tmp_path):
    db = setup_db(tmp_path)
    ledger = attribute(db, 'meter-S1', 'direct_platform_overhead')
    totals = ledger['totals']
    assert totals['jobs']['A']['wh'] == frac(6000)
    assert totals['jobs']['B']['wh'] == frac(10000)
    assert totals['platform']['wh'] == frac(7000)
    assert totals['jobs']['A']['grams'] == frac(600)
    assert totals['jobs']['B']['grams'] == frac(1000)
    assert totals['platform']['grams'] == frac(700)
    s4 = ledger['slots'][3]
    assert s4['job_shares_wh'] == {'A': frac(3000), 'B': frac(5000)}
    assert s4['platform_wh'] == frac(4000)
    assert s4['platform_terms'] == [
        {'kind': 'idle', 'marginal_wh': 1000},
        {'kind': 'shared_residual', 'marginal_wh': 3000}]


def test_shapley_shared_overhead_contract(tmp_path):
    db = setup_db(tmp_path)
    ledger = attribute(db, 'meter-S1', 'shapley_shared_overhead')
    totals = ledger['totals']
    assert totals['jobs']['A']['wh'] == frac(7500)
    assert totals['jobs']['B']['wh'] == frac(11500)
    assert totals['platform']['wh'] == frac(4000)
    assert totals['jobs']['A']['grams'] == frac(750)
    assert totals['jobs']['B']['grams'] == frac(1150)
    assert totals['platform']['grams'] == frac(400)
    s4 = ledger['slots'][3]
    assert s4['job_shares_wh'] == {'A': frac(4500), 'B': frac(6500)}
    assert s4['platform_wh'] == frac(1000)
    terms = {t['over']: t for t in s4['marginal_terms']['A']}
    assert terms[''] == {'over': '', 'weight': frac(1, 2), 'marginal_wh': 3000}
    assert terms['B'] == {'over': 'B', 'weight': frac(1, 2), 'marginal_wh': 6000}


def test_contracts_change_per_job_ledger_not_totals(tmp_path):
    db = setup_db(tmp_path)
    direct = attribute(db, 'meter-S1', 'direct_platform_overhead')
    shapley = attribute(db, 'meter-S1', 'shapley_shared_overhead')
    assert direct['totals']['jobs']['A']['wh'] != shapley['totals']['jobs']['A']['wh']
    assert direct['totals']['platform']['wh'] != shapley['totals']['platform']['wh']
    for ledger in (direct, shapley):
        jobs_wh = sum((share(t['wh']) for t in ledger['totals']['jobs'].values()), Fraction(0))
        jobs_g = sum((share(t['grams']) for t in ledger['totals']['jobs'].values()), Fraction(0))
        assert jobs_wh + share(ledger['totals']['platform']['wh']) == 23000
        assert jobs_g + share(ledger['totals']['platform']['grams']) == 2300
    con = connect(db)
    assert con.execute('SELECT count(*) FROM energy_attribution').fetchone()[0] == 2
    con.close()


# 3. 多作业并发、两次复位、非加性校准与逐槽强度

def test_multi_job_nonadditive_calibration(tmp_path):
    db = setup_db(tmp_path, 'energy-meter-multi.json')
    summary = meter_summary(db, 'meter-multi')
    assert [i['energy_wh'] for i in summary['intervals']] == [1000, 3000, 4000, 6000, 2000]
    assert summary['total_wh'] == 16000
    assert (summary['total_grams_num'], summary['total_grams_den']) == (1920, 1)

    direct = attribute(db, 'meter-multi', 'direct_platform_overhead')
    assert direct['status'] == '已归属'
    # E(AB)=3000 高于 E(A)+E(B)-E(∅)=2300：共享开销 700 由平台承担，不是均分也不是单独功耗相加
    s2 = direct['slots'][1]
    assert s2['platform_terms'][1] == {'kind': 'shared_residual', 'marginal_wh': 700}
    assert s2['platform_wh'] == frac(1700)
    totals = direct['totals']
    assert totals['jobs']['A']['wh'] == frac(1500)
    assert totals['jobs']['B']['wh'] == frac(2400)
    assert totals['jobs']['C']['wh'] == frac(3000)
    assert totals['jobs']['D']['wh'] == frac(1500)
    assert totals['platform']['wh'] == frac(7600)
    assert totals['platform']['grams'] == frac(874)
    # 逐槽强度换算：平台 7600Wh 若乘全期平均强度 120g/kWh 得 912g，与逐槽结果 874g 不同
    assert totals['jobs']['A']['grams'] == frac(185)
    assert share(totals['jobs']['A']['grams']) != Fraction(1500 * 120, 1000)

    shapley = attribute(db, 'meter-multi', 'shapley_shared_overhead')
    s4 = shapley['slots'][3]
    assert s4['active_jobs'] == ['A', 'B', 'C', 'D']
    assert s4['job_shares_wh']['A'] == frac(2300, 3)
    assert s4['job_shares_wh']['B'] == frac(3400, 3)
    assert s4['job_shares_wh']['C'] == frac(3400, 3)
    assert s4['job_shares_wh']['D'] == frac(5900, 3)
    # 正确的阶乘权重 |S|!(n-|S|-1)!/n!，而非平均人数
    terms = {t['over']: t for t in s4['marginal_terms']['A']}
    assert terms[''] == {'over': '', 'weight': frac(1, 4), 'marginal_wh': 500}
    assert terms['B'] == {'over': 'B', 'weight': frac(1, 12), 'marginal_wh': 1200}
    assert terms['B,C,D'] == {'over': 'B,C,D', 'weight': frac(1, 4), 'marginal_wh': 600}
    totals = shapley['totals']
    assert totals['jobs']['A']['wh'] == frac(7150, 3)
    assert totals['jobs']['B']['wh'] == frac(3400)
    assert totals['jobs']['C']['wh'] == frac(3250)
    assert totals['jobs']['D']['wh'] == frac(5900, 3)
    assert totals['platform']['wh'] == frac(5000)
    assert totals['jobs']['A']['grams'] == frac(881, 3)
    assert totals['jobs']['D']['grams'] == frac(295)
    assert totals['platform']['grams'] == frac(540)
    assert totals['energy_wh'] == 16000
    assert totals['grams'] == frac(1920)


# 4. 缺证待核、版本与单位

def test_missing_calibration_is_pending_not_zero(tmp_path):
    db = setup_db(tmp_path, 'energy-meter-incomplete.json')
    for contract in CONTRACTS:
        ledger = attribute(db, 'meter-incomplete', contract)
        assert ledger['status'] == '待核'
        assert ledger['totals'] is None
        assert ledger['slots'] == []
        codes = [issue['code'] for issue in ledger['issues']]
        assert 'missing_calibration' in codes


def test_unexplained_counter_drop_is_pending(tmp_path):
    db = tmp_path / 'ledger.db'
    initialize(db, FIX / 'config.json')
    doc = base_doc('meter-drop', [5000, 4000],
                   [{'id': 'S1', 'active_jobs': [], 'intensity_g_per_kwh': 100}],
                   {'': 1000})
    record(db, write_doc(tmp_path, doc))
    ledger = attribute(db, 'meter-drop', 'direct_platform_overhead')
    assert ledger['status'] == '待核'
    assert [i['code'] for i in ledger['issues']] == ['unexplained_counter_drop']
    with pytest.raises(ValueError, match='energy_unexplained_counter_drop'):
        meter_summary(db, 'meter-drop')


def test_reset_identity_conflict_is_pending(tmp_path):
    db = setup_db(tmp_path)
    insert_doctored_source(db, 'meter-bad-reset',
                           lambda doc: doc['resets'][0].update(old_wh=111001))
    ledger = attribute(db, 'meter-bad-reset', 'direct_platform_overhead')
    assert ledger['status'] == '待核'
    codes = [i['code'] for i in ledger['issues']]
    assert 'reset_identity_conflict' in codes
    # 复位证明失效后读数回落不得当作负耗能，也不得猜测复位
    assert 'unexplained_counter_drop' in codes


def test_missing_intensity_is_pending(tmp_path):
    db = setup_db(tmp_path)
    insert_doctored_source(db, 'meter-no-intensity',
                           lambda doc: doc['slots'][1].pop('intensity_g_per_kwh'))
    ledger = attribute(db, 'meter-no-intensity', 'direct_platform_overhead')
    assert ledger['status'] == '待核'
    assert [i['code'] for i in ledger['issues']] == ['missing_intensity']


def test_active_set_mismatch_is_pending(tmp_path):
    db = setup_db(tmp_path)
    insert_doctored_source(db, 'meter-unknown-job',
                           lambda doc: doc['slots'][1].update(active_jobs=['A', 'Z']))
    ledger = attribute(db, 'meter-unknown-job', 'direct_platform_overhead')
    assert ledger['status'] == '待核'
    assert [i['code'] for i in ledger['issues']] == ['active_set_mismatch']


def test_negative_residual_not_fabricated(tmp_path):
    db = tmp_path / 'ledger.db'
    initialize(db, FIX / 'config.json')
    doc = base_doc('meter-subadditive', [0, 8000],
                   [{'id': 'S1', 'active_jobs': ['A', 'B'], 'intensity_g_per_kwh': 100}],
                   {'': 1000, 'A': 4000, 'B': 6000, 'A,B': 8000})
    record(db, write_doc(tmp_path, doc))
    direct = attribute(db, 'meter-subadditive', 'direct_platform_overhead')
    assert direct['status'] == '待核'
    assert [i['code'] for i in direct['issues']] == ['negative_platform_residual']
    assert direct['totals'] is None
    shapley = attribute(db, 'meter-subadditive', 'shapley_shared_overhead')
    assert shapley['status'] == '已归属'
    assert shapley['totals']['jobs']['A']['wh'] == frac(2500)
    assert shapley['totals']['jobs']['B']['wh'] == frac(4500)
    assert shapley['totals']['platform']['wh'] == frac(1000)


def test_calibration_not_monotonic_is_pending(tmp_path):
    db = tmp_path / 'ledger.db'
    initialize(db, FIX / 'config.json')
    doc = base_doc('meter-nonmonotone', [0, 3000],
                   [{'id': 'S1', 'active_jobs': ['A', 'B'], 'intensity_g_per_kwh': 100}],
                   {'': 1000, 'A': 4000, 'B': 6000, 'A,B': 3000})
    record(db, write_doc(tmp_path, doc))
    ledger = attribute(db, 'meter-nonmonotone', 'shapley_shared_overhead')
    assert ledger['status'] == '待核'
    assert 'calibration_not_monotonic' in [i['code'] for i in ledger['issues']]


def test_observed_calibration_mismatch_is_pending(tmp_path):
    db = tmp_path / 'ledger.db'
    initialize(db, FIX / 'config.json')
    doc = base_doc('meter-mismatch', [0, 5000],
                   [{'id': 'S1', 'active_jobs': ['A'], 'intensity_g_per_kwh': 100}],
                   {'': 1000, 'A': 4000})
    record(db, write_doc(tmp_path, doc))
    ledger = attribute(db, 'meter-mismatch', 'direct_platform_overhead')
    assert ledger['status'] == '待核'
    assert [i['code'] for i in ledger['issues']] == ['observed_calibration_mismatch']


def test_shapley_max_four_active_jobs(tmp_path):
    db = tmp_path / 'ledger.db'
    initialize(db, FIX / 'config.json')
    doc = base_doc('meter-five', [0, 9000],
                   [{'id': 'S1', 'active_jobs': ['A', 'B', 'C', 'D', 'E'],
                     'intensity_g_per_kwh': 100}],
                   {'': 1000},
                   jobs=[{'id': j} for j in 'ABCDE'])
    record(db, write_doc(tmp_path, doc))
    ledger = attribute(db, 'meter-five', 'shapley_shared_overhead')
    assert ledger['status'] == '待核'
    assert [i['code'] for i in ledger['issues']] == ['too_many_active_jobs']


def test_replay_idempotent_conflict_and_versions(tmp_path):
    db = setup_db(tmp_path)
    first = attribute(db, 'meter-S1', 'direct_platform_overhead')
    assert attribute(db, 'meter-S1', 'direct_platform_overhead') == first
    assert record(db, FIX / 'energy-meter.json') == 'meter-S1'
    changed = json.loads((FIX / 'energy-meter.json').read_text(encoding='utf-8'))
    changed['coalition_calibration_wh']['A,B'] = 12001
    path = tmp_path / 'changed.json'
    path.write_text(json.dumps(changed), encoding='utf-8')
    with pytest.raises(ValueError, match='energy_source_conflict'):
        record(db, path)
    # 新读数/校准只能以新来源版本入账，旧归属账不可回写
    record(db, FIX / 'energy-meter-multi.json')
    attribute(db, 'meter-multi', 'direct_platform_overhead')
    con = connect(db)
    rows = con.execute('SELECT source_id, contract, version, body FROM energy_attribution'
                       ' ORDER BY source_id').fetchall()
    assert [(r['source_id'], r['contract'], r['version']) for r in rows] == [
        ('meter-S1', 'direct_platform_overhead', 1),
        ('meter-multi', 'direct_platform_overhead', 1)]
    assert json.loads(rows[0]['body']) == first
    raw = con.execute("SELECT raw, digest FROM energy_source WHERE source_id='meter-S1'").fetchone()
    assert hashlib.sha256(raw['raw']).hexdigest() == raw['digest']
    assert raw['raw'] == (FIX / 'energy-meter.json').read_bytes()
    con.close()


def test_unknown_contract_and_source(tmp_path):
    db = setup_db(tmp_path)
    with pytest.raises(ValueError, match='energy_unknown_contract'):
        attribute(db, 'meter-S1', 'equal_split')
    with pytest.raises(ValueError, match='energy_unknown_source'):
        attribute(db, 'meter-missing', 'direct_platform_overhead')


# 5. 独立 verify：从冻结原始读数复算并发现篡改

def test_verify_clean_after_attribution(tmp_path):
    db = setup_db(tmp_path)
    record(db, FIX / 'energy-meter-multi.json')
    record(db, FIX / 'energy-meter-incomplete.json')
    for source in ('meter-S1', 'meter-multi', 'meter-incomplete'):
        for contract in CONTRACTS:
            attribute(db, source, contract)
    assert verify_energy(db) == []
    assert verify_energy(db, source_id='meter-S1') == []
    assert verify_energy(db, contract='shapley_shared_overhead') == []


def tamper_body(db, source_id, contract, mutate):
    con = connect(db)
    row = con.execute(
        'SELECT body FROM energy_attribution WHERE source_id=? AND contract=?',
        (source_id, contract)).fetchone()
    body = json.loads(row['body'])
    mutate(body)
    with con:
        con.execute('UPDATE energy_attribution SET body=? WHERE source_id=? AND contract=?',
                    (json.dumps(body, ensure_ascii=False, sort_keys=True), source_id, contract))
    con.close()


def test_verify_detects_swapped_job_shares(tmp_path):
    db = setup_db(tmp_path)
    attribute(db, 'meter-S1', 'shapley_shared_overhead')

    def swap(body):
        shares = body['slots'][3]['job_shares_wh']
        shares['A'], shares['B'] = shares['B'], shares['A']
    tamper_body(db, 'meter-S1', 'shapley_shared_overhead', swap)
    problems = verify_energy(db)
    assert problems == ['energy_attribution:meter-S1:shapley_shared_overhead']


def test_verify_detects_tampered_platform_residual(tmp_path):
    db = setup_db(tmp_path)
    attribute(db, 'meter-S1', 'direct_platform_overhead')

    def shrink(body):
        body['slots'][3]['platform_wh'] = frac(3000)
    tamper_body(db, 'meter-S1', 'direct_platform_overhead', shrink)
    assert verify_energy(db) == ['energy_attribution:meter-S1:direct_platform_overhead']


def test_verify_detects_wrong_slot_intensity(tmp_path):
    db = setup_db(tmp_path)
    attribute(db, 'meter-S1', 'direct_platform_overhead')

    def bump(body):
        body['slots'][0]['intensity_g_per_kwh'] = 120
    tamper_body(db, 'meter-S1', 'direct_platform_overhead', bump)
    assert verify_energy(db) == ['energy_attribution:meter-S1:direct_platform_overhead']


def test_verify_detects_missed_reset_in_raw(tmp_path):
    db = setup_db(tmp_path)
    attribute(db, 'meter-S1', 'direct_platform_overhead')
    attribute(db, 'meter-S1', 'shapley_shared_overhead')
    con = connect(db)
    doc = json.loads((FIX / 'energy-meter.json').read_text(encoding='utf-8'))
    doc['resets'] = []
    with con:
        con.execute("UPDATE energy_source SET raw=? WHERE source_id='meter-S1'",
                    (json.dumps(doc).encode('utf-8'),))
    con.close()
    problems = verify_energy(db)
    assert 'energy_source:meter-S1' in problems
    assert 'energy_attribution:meter-S1:direct_platform_overhead' in problems
    assert 'energy_attribution:meter-S1:shapley_shared_overhead' in problems


def test_verify_detects_rewritten_boundary_reading(tmp_path):
    db = setup_db(tmp_path)
    attribute(db, 'meter-S1', 'direct_platform_overhead')
    con = connect(db)
    doc = json.loads((FIX / 'energy-meter.json').read_text(encoding='utf-8'))
    doc['boundaries'][1]['reading_wh'] = 101500
    raw = json.dumps(doc).encode('utf-8')
    with con:
        con.execute("UPDATE energy_source SET raw=?, digest=? WHERE source_id='meter-S1'",
                    (raw, hashlib.sha256(raw).hexdigest()))
    con.close()
    assert verify_energy(db) == ['energy_attribution:meter-S1:direct_platform_overhead']


# 兼容：归属账不影响既有补偿编排与报告

def test_energy_does_not_alter_compensation_flow(tmp_path):
    db = tmp_path / 'ledger.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    record(db, FIX / 'energy-meter.json')
    attribute(db, 'meter-S1', 'direct_platform_overhead')
    assert evaluate(db) == {'r1': '可补偿', 'r2': '可补偿', 'r3': '待核'}
    report = export(db, tmp_path / 'report.json')
    data = json.loads(report.read_text(encoding='utf-8'))
    assert [x['source_id'] for x in data['energy_attributions']] == ['meter-S1']
    assert verify(db) == []
