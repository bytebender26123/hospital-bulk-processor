"""CSV parsing and validation.

Pure and synchronous (no I/O), so the same code backs both ``POST /hospitals/bulk`` and
``POST /hospitals/bulk/validate``. A file with any error is rejected as a whole *before*
anything is sent upstream: a half-imported CSV is worse than a rejected one.
"""

import csv
import io
import re
from dataclasses import asdict, dataclass, field
from typing import Any

from app.core.exceptions import AppError
from app.domain.models import Fingerprint, HospitalRecord

REQUIRED_COLUMNS = ("name", "address")
OPTIONAL_COLUMNS = ("phone",)
KNOWN_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS
MAX_FIELD_LENGTH = 255

# Deliberately permissive: the upstream accepts any string, so a strange phone is a warning.
_PHONE_PATTERN = re.compile(r"^\+?[0-9][0-9 ()\-.]{2,24}(\s*(x|ext\.?)\s*\d{1,6})?$", re.IGNORECASE)


@dataclass(frozen=True)
class CsvIssue:
    message: str
    row: int | None = None
    field: str | None = None


@dataclass
class CsvParseResult:
    records: list[HospitalRecord] = field(default_factory=list)
    errors: list[CsvIssue] = field(default_factory=list)
    warnings: list[CsvIssue] = field(default_factory=list)
    total_rows: int = 0

    @property
    def is_valid(self) -> bool:
        return not self.errors


class CsvValidationError(AppError):
    status_code = 422

    def __init__(self, result: CsvParseResult) -> None:
        super().__init__("CSV validation failed")
        self.result = result

    def to_dict(self) -> dict[str, Any]:
        return {
            "detail": self.detail,
            "errors": [asdict(issue) for issue in self.result.errors],
            "warnings": [asdict(issue) for issue in self.result.warnings],
        }


def parse_hospital_csv(content: bytes, *, max_rows: int) -> CsvParseResult:
    """Parse ``name,address[,phone]`` rows. ``row`` numbers are 1-based, header excluded."""
    result = CsvParseResult()

    try:
        text = content.decode("utf-8-sig")  # tolerate the BOM Excel adds
    except UnicodeDecodeError:
        result.errors.append(CsvIssue("File is not valid UTF-8 text"))
        return result
    if not text.strip():
        result.errors.append(CsvIssue("CSV file is empty"))
        return result

    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        header = next((row for row in reader if any(cell.strip() for cell in row)), None)
        if header is None:
            result.errors.append(CsvIssue("CSV file is empty"))
            return result

        columns = [cell.strip().lower() for cell in header]
        if not _validate_header(columns, result):
            return result
        index = {name: i for i, name in enumerate(columns)}

        seen: dict[Fingerprint, int] = {}
        for raw in reader:
            if not any(cell.strip() for cell in raw):
                continue  # blank lines are harmless
            result.total_rows += 1
            if result.total_rows > max_rows:
                continue  # keep counting so the error can report the real size
            record = _parse_row(result.total_rows, raw, columns, index, result)
            if record is None:
                continue
            if (first := seen.get(record.fingerprint)) is not None:
                result.warnings.append(
                    CsvIssue(f"Duplicate of row {first}; both will be created", row=record.row)
                )
            else:
                seen[record.fingerprint] = record.row
            result.records.append(record)
    except csv.Error as exc:
        result.errors.append(CsvIssue(f"Malformed CSV near line {reader.line_num}: {exc}"))
        return result

    if result.total_rows == 0:
        result.errors.append(CsvIssue("CSV contains a header but no hospital rows"))
    elif result.total_rows > max_rows:
        result.errors.append(
            CsvIssue(f"CSV contains {result.total_rows} hospitals; the maximum is {max_rows}")
        )
    return result


def _validate_header(columns: list[str], result: CsvParseResult) -> bool:
    duplicates = sorted({c for c in columns if c and columns.count(c) > 1})
    if duplicates:
        result.errors.append(CsvIssue(f"Duplicate column(s): {', '.join(duplicates)}"))
    missing = [c for c in REQUIRED_COLUMNS if c not in columns]
    if missing:
        result.errors.append(
            CsvIssue(
                f"Missing required column(s): {', '.join(missing)}. "
                f"Expected header: {','.join(KNOWN_COLUMNS)}"
            )
        )
    unknown = [c for c in columns if c not in KNOWN_COLUMNS]
    if unknown:
        result.warnings.append(CsvIssue(f"Unknown column(s) ignored: {', '.join(unknown)}"))
    return not result.errors


def _parse_row(
    row_number: int,
    raw: list[str],
    columns: list[str],
    index: dict[str, int],
    result: CsvParseResult,
) -> HospitalRecord | None:
    if len(raw) > len(columns):
        result.errors.append(
            CsvIssue(f"Expected {len(columns)} fields, found {len(raw)}", row=row_number)
        )
        return None

    def cell(column: str) -> str:
        i = index.get(column)
        return raw[i].strip() if i is not None and i < len(raw) else ""

    values = {column: cell(column) for column in KNOWN_COLUMNS}
    errors = [
        CsvIssue(f"{column} is required", row=row_number, field=column)
        for column in REQUIRED_COLUMNS
        if not values[column]
    ]
    errors += [
        CsvIssue(f"{column} exceeds {MAX_FIELD_LENGTH} characters", row=row_number, field=column)
        for column, value in values.items()
        if len(value) > MAX_FIELD_LENGTH
    ]
    if errors:
        result.errors.extend(errors)
        return None

    phone = values["phone"] or None
    if phone and not _PHONE_PATTERN.match(phone):
        result.warnings.append(
            CsvIssue(
                f"phone '{phone}' does not look like a phone number", row=row_number, field="phone"
            )
        )
    return HospitalRecord(
        row=row_number, name=values["name"], address=values["address"], phone=phone
    )
