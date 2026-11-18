"""Causal diagnosis certificates: contracts, sandbox replays and verification."""
import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from backfillcomp.causality import (
    assess, manual_diff, record_pair, verify_certificate,
    NECESSARY, SUFFICIENT, MAX_UNITS, MAX_SUBJECTS, MAX_EDGES,
)
from backfillcomp.ingest import initialize, load
from backfillcomp.rules import evaluate
from backfillcomp.report import verify as report_verify
from backfillcomp.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'
CASES = FIX / 'causality-cases'


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _case_config(tmp_path, case):
    """Copy the case config into the test dir and point it at its snapshot."""
    src = CASES / case
    cfg_path = tmp_path / 'config.json'
    cfg = json.loads((src / 'config.json').read_text(encoding='utf-8'))
    cfg['snapshot_path'] = str((src / 'ledger.db').resolve())
    cfg_path.write_text(json.dumps(cfg, ensure_ascii=False), encoding='utf-8')
    return src, cfg_path


def _pair_from_case(tmp_path, case, units, target, pair='p'):
    src, cfg_path = _case_config(tmp_path, case)
    db = tmp_path / 'control.db'
    initialize(db, cfg_path)
    load(db, src / 'baseline.jsonl')
    run1 = evaluate(db)
    for unit in units:
        load(db, src / unit)
    run2 = evaluate(db)
    record_pair(db, pair, 1, 2, target)
    return db, run1, run2


def _unit_names(cert):
    return [[s['event_keys'] for s in group['subset_sources']]
            for group in cert['findings'][SUFFICIENT]]


# --------------------------------------------------------------------------- #
# Acceptance: real double-contract counterexample on the original fixtures
# --------------------------------------------------------------------------- #

def test_acceptance_dup_sources_two_runs_and_pair(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    assert evaluate(db) == {'r1': '可补偿', 'r2': '可补偿', 'r3': '待核'}
    load(db, FIX / 'causality-dup-r1.jsonl')
    load(db, FIX / 'causality-dup-r2.jsonl')
    assert evaluate(db)['r2'] == '待核'
    assert record_pair(db, 'pair-1', 1, 2, 'r2') == 'pair-1'
    diff = manual_diff(db, 'pair-1')
    assert diff['changed_subjects'] == ['r1', 'r2']
    assert len(diff['new_event_ids']) == 2


def test_acceptance_four_subset_replays_for_target_r2(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    load(db, FIX / 'causality-dup-r1.jsonl')
    load(db, FIX / 'causality-dup-r2.jsonl')
    evaluate(db)
    record_pair(db, 'pair-1', 1, 2, 'r2')
    cert = assess(db, 'pair-1', NECESSARY)
    assert cert['status'] == 'complete'
    assert cert['search'] == {'strategy': 'full_enumeration',
                              'subsets_total': 4, 'subsets_evaluated': 4,
                              'complete': True}
    by_mask = {c['subset_mask']: c['target_status'] for c in cert['counterfactuals']}
    # No new source -> baseline; either duplicate alone -> flip; both -> flip.
    assert by_mask == {0: '可补偿', 1: '待核', 2: '待核', 3: '待核'}


def test_necessary_contract_reports_none_and_escalation(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    load(db, FIX / 'causality-dup-r1.jsonl')
    load(db, FIX / 'causality-dup-r2.jsonl')
    evaluate(db)
    record_pair(db, 'pair-1', 1, 2, 'r2')
    cert = assess(db, 'pair-1', NECESSARY)
    # No single source is alone necessary: must say none, never pick one.
    assert cert['findings'] == {NECESSARY: []}
    checklist = cert['investigation_checklist']
    assert len(checklist) == 1 and checklist[0]['action'] == 'escalate_review'
    assert checklist[0]['reason'] == 'no_single_necessary_source'


def test_sufficient_contract_lists_both_independent_minimal_causes(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    load(db, FIX / 'causality-dup-r1.jsonl')
    load(db, FIX / 'causality-dup-r2.jsonl')
    evaluate(db)
    record_pair(db, 'pair-1', 1, 2, 'r2')
    cert = assess(db, 'pair-1', SUFFICIENT)
    groups = _unit_names(cert)
    # Both {dup-r1} and {dup-r2} must be reported, not just the first one.
    assert groups == [[['dup-r1']], [['dup-r2']]]
    assert all(len(g) == 1 for g in groups)
    checklist = cert['investigation_checklist']
    assert [x['reason'] for x in checklist] == [
        'independent_minimal_sufficient_change',
        'independent_minimal_sufficient_change']
    # The parent propagation path r2 -> r1 is attached to every checklist item.
    assert checklist[0]['parent_dependency_chain'] == [
        {'subject': 'r1', 'via': ['r2'], 'cycle': False}]


def test_dup_r1_flips_r2_through_parent_dependency(tmp_path):
    """The dup-r1 source flips r2 even though it carries no r2 event."""
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    load(db, FIX / 'causality-dup-r1.jsonl')
    load(db, FIX / 'causality-dup-r2.jsonl')
    evaluate(db)
    record_pair(db, 'pair-1', 1, 2, 'r2')
    cert = assess(db, 'pair-1', SUFFICIENT)
    dup_r1_group = next(g for g in cert['findings'][SUFFICIENT]
                        if g['subset_sources'][0]['event_keys'] == ['dup-r1'])
    mask = cert['counterfactuals'][[
        c['subset_mask'] for c in cert['counterfactuals']
        if c['subset_source_ids'] == dup_r1_group['subset_source_ids']][0]]
    assert mask['target_status'] == '待核'
    # r1 itself is 待核 inside that replay and propagates the block to r2.
    assert mask['all_subject_statuses']['r1'] == '待核'
    assert mask['all_subject_statuses']['r2'] == '待核'
    assert mask['parent_dependency_chain'][0]['subject'] == 'r1'


def test_certificate_is_immutable_and_byte_replay_idempotent(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    load(db, FIX / 'causality-dup-r1.jsonl')
    load(db, FIX / 'causality-dup-r2.jsonl')
    evaluate(db)
    record_pair(db, 'pair-1', 1, 2, 'r2')
    first = assess(db, 'pair-1', SUFFICIENT)
    second = assess(db, 'pair-1', SUFFICIENT)
    assert first['digest'] == second['digest']
    assert first == second
    stored = connect(db).execute(
        'SELECT body FROM causality_certificate WHERE pair_id=? AND contract=?',
        ('pair-1', SUFFICIENT)).fetchone()[0]
    assert json.loads(stored)['digest'] == first['digest']


# --------------------------------------------------------------------------- #
# Multi-event source unit: whole file is the atomic intervention
# --------------------------------------------------------------------------- #

def test_multi_event_file_is_one_atomic_unit(tmp_path):
    db, run1, run2 = _pair_from_case(
        tmp_path, 'multi-event', ['unit-both.jsonl'], 'm2')
    assert run1['m2'] == '可补偿' and run2['m2'] == '待核'
    cert = assess(db, 'p', SUFFICIENT)
    assert cert['status'] == 'complete'
    groups = cert['findings'][SUFFICIENT]
    assert len(groups) == 1
    # Both event keys travel together; there is no per-line cherry-picking.
    assert groups[0]['subset_sources'][0]['event_keys'] == ['u-m1', 'u-m2']
    assert [(c['subset_mask'], c['target_status'])
            for c in cert['counterfactuals']] == [(0, '可补偿'), (1, '待核')]
    assert verify_certificate(db, 'p', SUFFICIENT)['ok'] is True


# --------------------------------------------------------------------------- #
# Non-monotonic: improvement unit + worsening unit on one target flip
# --------------------------------------------------------------------------- #

def test_nonmonotonic_improvement_and_worsening_share_one_flip(tmp_path):
    db, run1, run2 = _pair_from_case(
        tmp_path, 'nonmonotonic',
        ['improve-s2.jsonl', 'worsen-s1.jsonl'], 's2')
    assert run1['s2'] == '待核' and run2['s2'] == '可补偿'
    nec = assess(db, 'p', NECESSARY)
    suf = assess(db, 'p', SUFFICIENT)
    # Neither unit is alone necessary (the full set minus either still flips).
    assert nec['findings'] == {NECESSARY: []}
    # Two independent minimal sufficient causes, including the worsening unit
    # acting through the s1 -> s2 dependency.
    assert _unit_names(suf) == [[['add-s2']], [['dup-s1']]]
    statuses = {c['subset_mask']: c['all_subject_statuses']
                for c in nec['counterfactuals']}
    assert statuses[0] == {'s1': '待核', 's2': '待核'}
    assert statuses[1] == {'s1': '可补偿', 's2': '可补偿'}
    assert statuses[2] == {'s1': '待核', 's2': '可补偿'}
    assert statuses[3] == {'s1': '待核', 's2': '可补偿'}
    assert verify_certificate(db, 'p', NECESSARY)['ok'] is True
    assert verify_certificate(db, 'p', SUFFICIENT)['ok'] is True


# --------------------------------------------------------------------------- #
# Incomparable runs / source conflicts
# --------------------------------------------------------------------------- #

def test_same_event_key_variant_is_indeterminate_not_pending(tmp_path):
    db, run1, run2 = _pair_from_case(
        tmp_path, 'same-key-variant', ['variant.jsonl'], 'k1')
    assert run1['k1'] == run2['k1'] == '可补偿'
    cert = assess(db, 'p', NECESSARY)
    assert cert['status'] == 'indeterminate'
    assert cert['comparability']['indeterminate_problems'] == [
        'unattributable_identity_conflict']
    assert cert['findings'] == {NECESSARY: None}
    assert verify_certificate(db, 'p', NECESSARY)['ok'] is True


def test_missing_raw_bytes_is_indeterminate(tmp_path):
    db, _, _ = _pair_from_case(
        tmp_path, 'missing-bytes', ['unit-q1.jsonl'], 'q1')
    con = connect(db)
    # Simulate loss of the original bytes for the newly loaded source unit.
    con.execute("UPDATE source_file SET raw=CAST('' AS BLOB) WHERE id=("
                'SELECT max(id) FROM source_file)')
    con.commit()
    con.close()
    cert = assess(db, 'p', NECESSARY)
    assert cert['status'] == 'indeterminate'
    assert 'source_raw_bytes_missing' in cert['comparability']['indeterminate_problems']


def test_baseline_event_disappeared_is_unsupported(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    load(db, FIX / 'causality-dup-r1.jsonl')
    load(db, FIX / 'causality-dup-r2.jsonl')
    evaluate(db)
    record_pair(db, 'pair-1', 1, 2, 'r2')
    con = connect(db)
    con.execute('DELETE FROM evidence WHERE run_id=1 AND event_id IS NOT NULL')
    con.commit()
    con.close()
    cert = assess(db, 'pair-1', NECESSARY)
    assert cert['status'] == 'unsupported'
    assert 'run_evidence_drift' in cert['comparability']['unsupported_problems']


def test_config_snapshot_drift_is_unsupported(tmp_path):
    from backfillcomp.ingest import hash_bytes
    db, _, _ = _pair_from_case(
        tmp_path, 'multi-event', ['unit-both.jsonl'], 'm2')
    con = connect(db)
    row = con.execute('SELECT config_raw,config_hash FROM batch').fetchone()
    cfg = json.loads(row['config_raw'])
    cfg['new_multiplier'] = '1.20'
    new_raw = json.dumps(cfg).encode()
    new_hash = hash_bytes(new_raw)
    # Rewrite the live config with an internally consistent self-hash and
    # rule-version digest; the only remaining signal is that the frozen pair's
    # config digest no longer matches the live configuration identity.
    con.execute('UPDATE batch SET config_raw=?, config_hash=?', (new_raw, new_hash))
    con.execute('UPDATE rule_version SET digest=? WHERE id=?',
                (new_hash, cfg['rule_version']))
    con.commit()
    con.close()
    cert = assess(db, 'p', NECESSARY)
    assert cert['status'] == 'unsupported'
    assert 'config_snapshot_drift' in cert['comparability']['unsupported_problems']


def test_ledger_snapshot_drift_is_unsupported(tmp_path):
    src, cfg_path = _case_config(tmp_path, 'multi-event')
    # Work on a private snapshot copy so checked-in fixtures stay untouched.
    snapshot = tmp_path / 'ledger-snapshot.db'
    shutil.copy2(src / 'ledger.db', snapshot)
    cfg = json.loads(cfg_path.read_text(encoding='utf-8'))
    cfg['snapshot_path'] = str(snapshot)
    cfg_path.write_text(json.dumps(cfg, ensure_ascii=False), encoding='utf-8')
    db = tmp_path / 'control.db'
    initialize(db, cfg_path)
    load(db, src / 'baseline.jsonl')
    evaluate(db)
    load(db, src / 'unit-both.jsonl')
    evaluate(db)
    record_pair(db, 'p', 1, 2, 'm2')
    # Drift the snapshot file only after the pair froze its digest.
    con = sqlite3.connect(snapshot)
    con.execute("UPDATE ledger SET computed='99.00' WHERE id='m1'")
    con.commit()
    con.close()
    cert = assess(db, 'p', NECESSARY)
    assert cert['status'] == 'unsupported'
    assert 'ledger_snapshot_drift' in cert['comparability']['unsupported_problems']


def test_old_source_mutation_is_unsupported(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    load(db, FIX / 'causality-dup-r1.jsonl')
    evaluate(db)
    record_pair(db, 'pair-1', 1, 2, 'r2')
    # Mutate the original baseline source's stored raw bytes after the freeze.
    con = connect(db)
    con.execute("UPDATE source_file SET raw=CAST(raw || char(10) AS BLOB) WHERE id=1")
    con.commit()
    con.close()
    cert = assess(db, 'pair-1', NECESSARY)
    assert cert['status'] == 'unsupported'
    problems = cert['comparability']['unsupported_problems']
    assert 'source_raw_mismatch' in problems or 'report_verify_failed' in problems


def test_no_status_flip_is_unsupported(tmp_path):
    db, _, _ = _pair_from_case(tmp_path, 'cycle', ['unit-y1.jsonl'], 'y1')
    cert = assess(db, 'p', NECESSARY)
    assert cert['status'] == 'unsupported'
    assert cert['comparability']['reason'] == 'no_target_status_flip'
    assert cert['counterfactuals'] == [] and cert['findings'] == {NECESSARY: None}


def test_cycle_dependency_is_recorded_in_frozen_decisions(tmp_path):
    db, run1, run2 = _pair_from_case(
        tmp_path, 'cycle', ['unit-y1.jsonl'], 'y1')
    assert run1['y1'] == run2['y1'] == '待核'
    con = connect(db)
    detail = json.loads(con.execute(
        "SELECT detail FROM decision WHERE run_id=2 AND subject='y1'").fetchone()[0])
    con.close()
    assert 'dependency_cycle' in detail['issues']


# --------------------------------------------------------------------------- #
# Search completeness limits
# --------------------------------------------------------------------------- #

def test_scale_over_subject_limit_is_indeterminate_without_fake_completeness(tmp_path):
    db, run1, run2 = _pair_from_case(
        tmp_path, 'scale', ['unit-z00.jsonl'], 'z00')
    assert run1['z00'] == '可补偿' and run2['z00'] == '待核'
    cert = assess(db, 'p', NECESSARY)
    assert cert['status'] == 'indeterminate'
    assert cert['comparability']['reason'] == 'search_scale_limit_exceeded'
    assert cert['search']['complete'] is False
    assert cert['search']['subsets_evaluated'] == 0
    assert cert['search']['limits'] == {
        'new_source_units': 1, 'subjects': MAX_SUBJECTS + 1,
        'dependency_edges': 0, 'max_units': MAX_UNITS,
        'max_subjects': MAX_SUBJECTS, 'max_edges': MAX_EDGES}
    # No counterfactual claim is emitted when the search cannot run completely.
    assert cert['counterfactuals'] == [] and cert['findings'] == {NECESSARY: None}


# --------------------------------------------------------------------------- #
# Independent verification detects each tampering mode
# --------------------------------------------------------------------------- #

def test_verify_recomputes_and_matches_complete_certificates(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    load(db, FIX / 'causality-dup-r1.jsonl')
    load(db, FIX / 'causality-dup-r2.jsonl')
    evaluate(db)
    record_pair(db, 'pair-1', 1, 2, 'r2')
    for contract in (NECESSARY, SUFFICIENT):
        assess(db, 'pair-1', contract)
        result = verify_certificate(db, 'pair-1', contract)
        assert result['ok'] is True and result['problems'] == []
        assert result['stored_digest'] == result['recomputed_digest']


def test_verify_detects_missing_second_sufficient_cause(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    load(db, FIX / 'causality-dup-r1.jsonl')
    load(db, FIX / 'causality-dup-r2.jsonl')
    evaluate(db)
    record_pair(db, 'pair-1', 1, 2, 'r2')
    assess(db, 'pair-1', SUFFICIENT)
    con = connect(db)
    body = json.loads(con.execute(
        'SELECT body FROM causality_certificate WHERE pair_id=? AND contract=?',
        ('pair-1', SUFFICIENT)).fetchone()[0])
    body['findings'][SUFFICIENT] = body['findings'][SUFFICIENT][:1]  # drop dup-r2
    con.execute('UPDATE causality_certificate SET body=? WHERE pair_id=? AND contract=?',
                (json.dumps(body, ensure_ascii=False), 'pair-1', SUFFICIENT))
    con.commit()
    con.close()
    result = verify_certificate(db, 'pair-1', SUFFICIENT)
    assert result['ok'] is False
    assert 'finding_mismatch' in result['problems']


def test_verify_detects_wrongly_chosen_subset(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    load(db, FIX / 'causality-dup-r1.jsonl')
    load(db, FIX / 'causality-dup-r2.jsonl')
    evaluate(db)
    record_pair(db, 'pair-1', 1, 2, 'r2')
    assess(db, 'pair-1', SUFFICIENT)
    con = connect(db)
    body = json.loads(con.execute(
        'SELECT body FROM causality_certificate WHERE pair_id=? AND contract=?',
        ('pair-1', SUFFICIENT)).fetchone()[0])
    # Forge a counterfactual status on the empty subset.
    body['counterfactuals'][0]['target_status'] = '待核'
    con.execute('UPDATE causality_certificate SET body=? WHERE pair_id=? AND contract=?',
                (json.dumps(body, ensure_ascii=False), 'pair-1', SUFFICIENT))
    con.commit()
    con.close()
    result = verify_certificate(db, 'pair-1', SUFFICIENT)
    assert result['ok'] is False
    assert any(p.startswith('counterfactual_status_mismatch:mask0')
               for p in result['problems'])


def test_verify_detects_forged_source_digest(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    load(db, FIX / 'causality-dup-r1.jsonl')
    load(db, FIX / 'causality-dup-r2.jsonl')
    evaluate(db)
    record_pair(db, 'pair-1', 1, 2, 'r2')
    assess(db, 'pair-1', SUFFICIENT)
    con = connect(db)
    body = json.loads(con.execute(
        'SELECT body FROM causality_certificate WHERE pair_id=? AND contract=?',
        ('pair-1', SUFFICIENT)).fetchone()[0])
    body['counterfactuals'][1]['subset_sources'][0]['digest'] = 'deadbeef'
    con.execute('UPDATE causality_certificate SET body=? WHERE pair_id=? AND contract=?',
                (json.dumps(body, ensure_ascii=False), 'pair-1', SUFFICIENT))
    con.commit()
    con.close()
    result = verify_certificate(db, 'pair-1', SUFFICIENT)
    assert result['ok'] is False
    assert any(p.startswith('forged_source:mask') for p in result['problems'])


def test_verify_detects_modified_historical_run(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    load(db, FIX / 'causality-dup-r1.jsonl')
    load(db, FIX / 'causality-dup-r2.jsonl')
    evaluate(db)
    record_pair(db, 'pair-1', 1, 2, 'r2')
    assess(db, 'pair-1', NECESSARY)
    con = connect(db)
    con.execute("UPDATE decision SET status='可补偿' WHERE run_id=2 AND subject='r2'")
    con.commit()
    con.close()
    result = verify_certificate(db, 'pair-1', NECESSARY)
    assert result['ok'] is False
    assert 'run_evidence_drift' in result['problems']


def test_verify_detects_omitted_upstream_propagation(tmp_path):
    db, _, _ = _pair_from_case(
        tmp_path, 'nonmonotonic',
        ['improve-s2.jsonl', 'worsen-s1.jsonl'], 's2')
    assess(db, 'p', NECESSARY)
    con = connect(db)
    body = json.loads(con.execute(
        'SELECT body FROM causality_certificate WHERE pair_id=? AND contract=?',
        ('p', NECESSARY)).fetchone()[0])
    # Erase the s1 status from a replay so upstream propagation is hidden.
    entry = next(c for c in body['counterfactuals'] if c['subset_mask'] == 2)
    entry['all_subject_statuses'].pop('s1')
    con.execute('UPDATE causality_certificate SET body=? WHERE pair_id=? AND contract=?',
                (json.dumps(body, ensure_ascii=False), 'p', NECESSARY))
    con.commit()
    con.close()
    result = verify_certificate(db, 'p', NECESSARY)
    assert result['ok'] is False
    assert any(p.startswith('upstream_propagation_mismatch:mask2')
               for p in result['problems'])


def test_verify_unknown_certificate_raises(tmp_path):
    db, _, _ = _pair_from_case(
        tmp_path, 'multi-event', ['unit-both.jsonl'], 'm2')
    with pytest.raises(ValueError, match='causality_unknown_certificate'):
        verify_certificate(db, 'p', NECESSARY)


def test_report_verify_still_passes_on_causal_databases(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    load(db, FIX / 'causality-dup-r1.jsonl')
    load(db, FIX / 'causality-dup-r2.jsonl')
    evaluate(db)
    record_pair(db, 'pair-1', 1, 2, 'r2')
    assert report_verify(db) == []


def test_parse_failure_inside_a_unit_is_indeterminate(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    unit = tmp_path / 'mixed.jsonl'
    unit.write_text(
        json.dumps({'event_key': 'dup-r1', 'subject': 'r1', 'kind': 'correction',
                    'at': '2026-03-02', 'reason': 'rule_revision'}) + '\n{broken\n',
        encoding='utf-8')
    load(db, unit)
    evaluate(db)
    record_pair(db, 'p', 1, 2, 'r2')
    cert = assess(db, 'p', NECESSARY)
    assert cert['status'] == 'indeterminate'
    assert cert['comparability']['indeterminate_problems'] == [
        'unattributable_identity_conflict']
    # A parse failure is never accepted as a normal "pending" decision.
    assert cert['findings'] == {NECESSARY: None}
    assert verify_certificate(db, 'p', NECESSARY)['ok'] is True


def test_same_bytes_idempotent_but_variant_text_conflicts(tmp_path):
    db = tmp_path / 'control.db'
    initialize(db, FIX / 'config.json')
    load(db, FIX / 'events.jsonl')
    evaluate(db)
    load(db, FIX / 'causality-dup-r1.jsonl')
    evaluate(db)
    record_pair(db, 'p', 1, 2, 'r2')
    # Re-freezing identical bytes is idempotent.
    assert record_pair(db, 'p', 1, 2, 'r2') == 'p'
    # A different payload under an already frozen event key conflicts.
    con = connect(db)
    con.execute("UPDATE raw_event SET payload=?, digest='x' WHERE event_key='dup-r1'",
                (json.dumps({'event_key': 'dup-r1', 'subject': 'r1',
                             'kind': 'correction', 'at': 'other'}, ensure_ascii=False),))
    con.commit()
    con.close()
    with pytest.raises(ValueError, match='causality_pair_conflict'):
        record_pair(db, 'p', 1, 2, 'r2')
