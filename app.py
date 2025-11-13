"""
CU Trial Data Validator - single-file prototype
Implements: validators, pipeline, tracker (SQLite), error logger, CLI and sample file generator.
This is a single-file runnable prototype you can split into modules later.
"""

import re
import csv
import sqlite3
import hashlib
import uuid
import json
from datetime import datetime, timezone
from pathlib import Path
import argparse
import shutil
import sys

# ----------------------------- Configuration -----------------------------
INCOMING = Path("data/incoming")
ARCHIVE = Path("data/archive")
REJECTED = Path("data/rejected")
LOGS = Path("logs/errors.jsonl")
DB_PATH = Path("app/seen.db")

EXPECTED_HEADERS = ["batch_id", "timestamp"] + [f"reading{i}" for i in range(1, 11)]
FILENAME_RX = re.compile(r"^MED_DATA_(\d{14})\.csv$")
TIMESTAMP_RX = re.compile(r"^\d{2}:\d{2}:\d{2}$")

# ensure dirs
for p in (INCOMING, ARCHIVE, REJECTED, LOGS.parent, DB_PATH.parent):
    p.mkdir(parents=True, exist_ok=True)

# ----------------------------- Error Logger -----------------------------
class ErrorLogger:
    def __init__(self, path=LOGS):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _sha256(self, file_path):
        if not file_path:
            return None
        h = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(1 << 20), b''):
                h.update(chunk)
        return h.hexdigest()

    def emit(self, filename, rule, message, row=None, path=None, meta=None, guid=None):
        rec = {
            "guid": guid or str(uuid.uuid1()),
            "filename": filename,
            "sha256": self._sha256(path) if path else None,
            "rule": rule,
            "message": message,
            "row": row,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "meta": meta or {}
        }
        with open(self.path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

logger = ErrorLogger()

# ----------------------------- Tracker (SQLite) -----------------------------
class Tracker:
    SCHEMA = """
    CREATE TABLE IF NOT EXISTS seen_files (
      id INTEGER PRIMARY KEY,
      filename TEXT,
      sha256 TEXT UNIQUE,
      first_seen_ts TEXT
    );
    """

    def __init__(self, db_path=DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.execute(self.SCHEMA)
        self.conn.commit()

    def hash_file(self, path: Path):
        h = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(1 << 20), b''):
                h.update(chunk)
        return h.hexdigest()

    def is_seen(self, sha: str):
        cur = self.conn.execute("SELECT 1 FROM seen_files WHERE sha256=?", (sha,))
        return cur.fetchone() is not None

    def mark_seen(self, filename: str, sha: str):
        self.conn.execute(
            "INSERT OR IGNORE INTO seen_files(filename, sha256, first_seen_ts) VALUES (?, ?, datetime('now'))",
            (filename, sha)
        )
        self.conn.commit()

tracker = Tracker()

# ----------------------------- Validators -----------------------------
class ValidationResult:
    def __init__(self, ok=True, issues=None, meta=None):
        self.ok = ok
        self.issues = issues or []
        self.meta = meta or {}

class BaseValidator:
    name = "base"
    def validate(self, filename: str, path: Path, context=None) -> ValidationResult:
        raise NotImplementedError

class FilenameValidator(BaseValidator):
    name = "filename_format"
    def validate(self, filename, path, context=None):
        m = FILENAME_RX.match(filename)
        if not m:
            return ValidationResult(False, [{"rule": self.name, "message": "invalid filename format"}], {})
        ts = m.group(1)
        try:
            dt = datetime.strptime(ts, "%Y%m%d%H%M%S")
        except ValueError:
            return ValidationResult(False, [{"rule": self.name, "message": "invalid datetime in filename"}], {})
        return ValidationResult(True, [], {"dt": dt, "ts": ts})

class NonEmptyValidator(BaseValidator):
    name = "non_empty"
    def validate(self, filename, path, context=None):
        if path.stat().st_size == 0:
            return ValidationResult(False, [{"rule": self.name, "message": "file is empty"}], {})
        return ValidationResult(True, [], {})

class CSVParseValidator(BaseValidator):
    name = "csv_parse"
    def validate(self, filename, path, context=None):
        try:
            with open(path, newline='') as f:
                sn = f.read(4096)
                f.seek(0)
                reader = csv.reader(f)
                for _ in range(2):
                    next(reader, None)
        except Exception as e:
            return ValidationResult(False, [{"rule": self.name, "message": f"CSV parse error: {e}"}], {})
        return ValidationResult(True, [], {})

class HeaderValidator(BaseValidator):
    name = "header_check"
    def validate(self, filename, path, context=None):
        with open(path, newline='') as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                return ValidationResult(False, [{"rule": self.name, "message": "missing header"}], {})
        if header != EXPECTED_HEADERS:
            return ValidationResult(False, [{"rule": self.name, "message": f"header mismatch: {header}"}], {})
        return ValidationResult(True, [], {})

class RowShapeValidator(BaseValidator):
    name = "row_shape"
    def validate(self, filename, path, context=None):
        issues = []
        with open(path, newline='') as f:
            reader = csv.reader(f)
            next(reader, None)
            for i, row in enumerate(reader, start=2):
                if len(row) != len(EXPECTED_HEADERS):
                    issues.append({"rule": self.name, "message": f"row has {len(row)} cols", "row": i})
        return ValidationResult(len(issues) == 0, issues, {})

class ValueDomainValidator(BaseValidator):
    name = "value_domain"
    def validate(self, filename, path, context=None):
        issues = []
        with open(path, newline='') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, start=2):
                try:
                    bid = int(row.get('batch_id', '').strip())
                    if bid <= 0:
                        issues.append({"rule": self.name, "message": "batch_id not positive", "row": i})
                except Exception:
                    issues.append({"rule": self.name, "message": "batch_id not integer", "row": i})
                ts = row.get('timestamp', '').strip()
                if not TIMESTAMP_RX.match(ts):
                    issues.append({"rule": self.name, "message": "bad timestamp format", "row": i})
                for col in EXPECTED_HEADERS[2:]:
                    val = row.get(col, '').strip()
                    try:
                        fval = float(val)
                        if fval >= 10.0:
                            issues.append({"rule": self.name, "message": f"{col} out of range: {fval}", "row": i})
                    except Exception:
                        issues.append({"rule": self.name, "message": f"{col} not float", "row": i})
        return ValidationResult(len(issues) == 0, issues, {})

class DuplicateBatchValidator(BaseValidator):
    name = "duplicate_batch_id"
    def validate(self, filename, path, context=None):
        seen = set()
        issues = []
        with open(path, newline='') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, start=2):
                bid = row.get('batch_id', '').strip()
                if bid in seen:
                    issues.append({"rule": self.name, "message": f"duplicate batch_id: {bid}", "row": i})
                else:
                    seen.add(bid)
        return ValidationResult(len(issues) == 0, issues, {})

class UniquenessValidator(BaseValidator):
    name = "file_uniqueness"
    def validate(self, filename, path, context=None):
        sha = tracker.hash_file(path)
        if tracker.is_seen(sha):
            return ValidationResult(False, [{"rule": self.name, "message": "duplicate file (sha256)"}], {"sha": sha})
        return ValidationResult(True, [], {"sha": sha})

# ----------------------------- Pipeline -----------------------------
DEFAULT_PIPELINE = [
    FilenameValidator(),
    NonEmptyValidator(),
    CSVParseValidator(),
    HeaderValidator(),
    RowShapeValidator(),
    ValueDomainValidator(),
    DuplicateBatchValidator(),
    UniquenessValidator(),
]

class Pipeline:
    def __init__(self, validators=None, dry_run=False, no_move=False, archive_path=None):
        self.validators = validators or DEFAULT_PIPELINE
        self.dry_run = dry_run
        self.no_move = no_move
        self.archive_path = Path(archive_path) if archive_path else ARCHIVE

    def process_file(self, path: Path):
        filename = path.name
        context = {}
        all_issues = []
        sha = None
        for v in self.validators:
            try:
                res = v.validate(filename, path, context=context)
            except Exception as e:
                logger.emit(filename, getattr(v, 'name', 'validator_error'), f"validator exception: {e}", path=path)
                return False
            if res.meta:
                context.update(res.meta)
            if not res.ok:
                for issue in res.issues:
                    logger.emit(filename, issue.get('rule', v.name), issue.get('message'), row=issue.get('row'), path=path, meta=context)
                all_issues.extend(res.issues)
                break
        if all_issues:
            if not self.dry_run and not self.no_move:
                dest = REJECTED / datetime.now().strftime('%Y/%m/%d')
                dest.mkdir(parents=True, exist_ok=True)
                shutil.move(str(path), str(dest / filename))
            return False
        else:
            sha = context.get('sha') or tracker.hash_file(path)
            if not self.dry_run and not self.no_move:
                tracker.mark_seen(filename, sha)
                dt = context.get('dt', datetime.now())
                dest = self.archive_path / dt.strftime('%Y/%m/%d')
                dest.mkdir(parents=True, exist_ok=True)
                shutil.move(str(path), str(dest / filename))
            return True

# ----------------------------- CLI / Utilities -----------------------------
def process_folder(path: Path, dry_run=False, verbose=False, no_move=False, archive_path=None):
    p = Path(path)
    if not p.exists():
        print("Path does not exist:", p)
        sys.exit(1)
    files = sorted([x for x in p.iterdir() if x.is_file()])
    pipeline = Pipeline(dry_run=dry_run, no_move=no_move, archive_path=archive_path)
    stats = {"total": 0, "valid": 0, "invalid": 0}
    for f in files:
        stats['total'] += 1
        ok = pipeline.process_file(f)
        if ok:
            stats['valid'] += 1
            if verbose:
                print(f"ACCEPTED: {f.name}")
        else:
            stats['invalid'] += 1
            if verbose:
                print(f"REJECTED: {f.name}")
    print(json.dumps(stats, indent=2))
    # exit with 1 if invalid files found (for CI/CD)
    if stats['invalid'] > 0:
        sys.exit(1)

# sample generator helper
SAMPLE_DIR = Path('samples')
SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

def generate_sample_valid(filename: str):
    path = SAMPLE_DIR / filename
    dt = datetime.now().strftime('%Y%m%d%H%M%S')
    fname = f"MED_DATA_{dt}.csv"
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(EXPECTED_HEADERS)
        for i in range(1, 6):
            row = [i, '12:00:00'] + [f"{(i*0.1):.3f}" for _ in range(10)]
            writer.writerow(row)
    print('sample written to', path)
    return path

# ----------------------------- Entrypoint -----------------------------
def main():
    ap = argparse.ArgumentParser(description='CU Trial Data Validator - prototype')
    ap.add_argument('command', choices=['process', 'gen-sample'], help='command')
    ap.add_argument('--path', default=str(INCOMING), help='folder to process')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--verbose', action='store_true')
    ap.add_argument('--sample-name', default='valid.csv')
    ap.add_argument('--no-move', action='store_true', help='Do not move files (useful for testing)')
    ap.add_argument('--archive-path', default=None, help='Custom archive directory path')

    args = ap.parse_args()

    if args.command == 'process':
        process_folder(Path(args.path), dry_run=args.dry_run, verbose=args.verbose,
                       no_move=args.no_move, archive_path=args.archive_path)
    elif args.command == 'gen-sample':
        generate_sample_valid(args.sample_name)

if __name__ == '__main__':
    main()
