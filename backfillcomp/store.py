import sqlite3
from pathlib import Path
SCHEMA="""
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS batch(id TEXT PRIMARY KEY, config_hash TEXT NOT NULL, config_raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS source_file(id INTEGER PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batch(id), name TEXT NOT NULL, digest TEXT NOT NULL, raw BLOB NOT NULL, UNIQUE(batch_id,digest));
CREATE TABLE IF NOT EXISTS raw_event(id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL REFERENCES source_file(id), line_no INTEGER NOT NULL, event_key TEXT NOT NULL, subject TEXT NOT NULL, kind TEXT NOT NULL, at TEXT, payload TEXT NOT NULL, digest TEXT NOT NULL, UNIQUE(source_id,line_no));
CREATE TABLE IF NOT EXISTS subject(id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batch(id));
CREATE TABLE IF NOT EXISTS rule_version(id TEXT PRIMARY KEY, body TEXT NOT NULL, digest TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS run(id INTEGER PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batch(id), status TEXT NOT NULL, started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, ended_at TEXT);
CREATE TABLE IF NOT EXISTS issue(id INTEGER PRIMARY KEY, run_id INTEGER REFERENCES run(id), source_id INTEGER REFERENCES source_file(id), line_no INTEGER, subject TEXT, code TEXT NOT NULL, detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS evidence(id INTEGER PRIMARY KEY, run_id INTEGER NOT NULL REFERENCES run(id), subject TEXT NOT NULL, source_id INTEGER REFERENCES source_file(id), event_id INTEGER REFERENCES raw_event(id), rule_id TEXT REFERENCES rule_version(id), detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS decision(id INTEGER PRIMARY KEY, run_id INTEGER NOT NULL REFERENCES run(id), subject TEXT NOT NULL, status TEXT NOT NULL, detail TEXT NOT NULL, UNIQUE(run_id,subject));
CREATE TABLE IF NOT EXISTS execution(id INTEGER PRIMARY KEY, run_id INTEGER REFERENCES run(id), subject TEXT NOT NULL, action TEXT NOT NULL, before_hash TEXT, after_state TEXT NOT NULL, at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(subject,action,before_hash));
CREATE TABLE IF NOT EXISTS plan(id INTEGER PRIMARY KEY, run_id INTEGER NOT NULL REFERENCES run(id), digest TEXT NOT NULL UNIQUE, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS publication_accord(
 accord_key TEXT PRIMARY KEY, accord_id TEXT NOT NULL, version INTEGER NOT NULL,
 route TEXT NOT NULL CHECK(route IN ('atomic','partitioned')),
 plan_digest TEXT NOT NULL, run_id INTEGER NOT NULL,
 target_path TEXT NOT NULL, target_hash TEXT NOT NULL,
 member_digest TEXT NOT NULL, expected_digest TEXT NOT NULL, contract_digest TEXT NOT NULL,
 contract TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('prepared','published','partially_published','blocked','stale')),
 reasons TEXT NOT NULL,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, published_at TEXT,
 UNIQUE(accord_id, version));
CREATE TABLE IF NOT EXISTS accord_member(
 accord_key TEXT NOT NULL REFERENCES publication_accord(accord_key), consumer TEXT NOT NULL,
 scope TEXT NOT NULL, expected TEXT NOT NULL, expected_hash TEXT NOT NULL,
 readiness TEXT NOT NULL CHECK(readiness IN ('ready','blocked','uncertain')), reasons TEXT NOT NULL,
 published_at TEXT, published_target_hash TEXT, PRIMARY KEY(accord_key,consumer));
CREATE TABLE IF NOT EXISTS accord_dependency(
 accord_key TEXT NOT NULL REFERENCES publication_accord(accord_key),
 consumer TEXT NOT NULL, depends_on TEXT NOT NULL,
 PRIMARY KEY(accord_key,consumer,depends_on));
CREATE TABLE IF NOT EXISTS accord_snapshot(
 accord_key TEXT NOT NULL REFERENCES publication_accord(accord_key), subject TEXT NOT NULL,
 value TEXT, audit_state TEXT, plan_new TEXT, PRIMARY KEY(accord_key,subject));
CREATE TABLE IF NOT EXISTS accord_group(
 accord_key TEXT NOT NULL REFERENCES publication_accord(accord_key), group_key TEXT NOT NULL,
 members TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('ready','blocked','uncertain','published')),
 reasons TEXT NOT NULL, published_at TEXT, target_hash TEXT, PRIMARY KEY(accord_key,group_key));
CREATE TABLE IF NOT EXISTS accord_reason(
 id INTEGER PRIMARY KEY, accord_key TEXT NOT NULL REFERENCES publication_accord(accord_key),
 consumer TEXT, code TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '',
 at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS accord_rejection(
 id INTEGER PRIMARY KEY, accord_id TEXT NOT NULL, version INTEGER,
 code TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '',
 at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TRIGGER IF NOT EXISTS accord_member_frozen BEFORE UPDATE OF scope,expected,expected_hash ON accord_member
 BEGIN SELECT RAISE(ABORT,'accord_member_frozen'); END;
CREATE TRIGGER IF NOT EXISTS accord_member_no_delete BEFORE DELETE ON accord_member
 BEGIN SELECT RAISE(ABORT,'accord_member_frozen'); END;
CREATE TRIGGER IF NOT EXISTS accord_snapshot_frozen BEFORE UPDATE ON accord_snapshot
 BEGIN SELECT RAISE(ABORT,'accord_snapshot_frozen'); END;
CREATE TRIGGER IF NOT EXISTS accord_snapshot_no_delete BEFORE DELETE ON accord_snapshot
 BEGIN SELECT RAISE(ABORT,'accord_snapshot_frozen'); END;
CREATE TRIGGER IF NOT EXISTS accord_identity_frozen
 BEFORE UPDATE OF accord_id,version,route,plan_digest,run_id,target_hash,member_digest,expected_digest,
                  contract_digest,contract ON publication_accord
 BEGIN SELECT RAISE(ABORT,'accord_identity_frozen'); END;
CREATE TRIGGER IF NOT EXISTS accord_no_delete BEFORE DELETE ON publication_accord
 BEGIN SELECT RAISE(ABORT,'accord_identity_frozen'); END;
"""
def connect(path):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(path);db.row_factory=sqlite3.Row;db.executescript(SCHEMA);return db
