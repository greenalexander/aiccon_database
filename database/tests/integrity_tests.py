"""
database/tests/test_integrity.py

Integrity checks on the aiccon.duckdb file produced by build_db.py.

These are not unit tests of Python code — they are data quality assertions
on the database itself. They check that:
    - All expected tables and views exist
    - Fact tables have rows (not empty)
    - Foreign key relationships are satisfied (no orphan keys)
    - No impossible values (negative counts, null required fields)
    - Dimension tables are fully populated
    - Known indicator codes are present

Run after every build_db.py execution:
    python -m database.tests.test_integrity

Run against a specific database file:
    python -m database.tests.test_integrity --db path/to/aiccon.duckdb

Adding checks for a new domain:
    1. Add a check_fact_{domain}() function following the pattern below
    2. Register it in DOMAIN_CHECKS at the bottom of this file
    3. Enable the domain in settings.yaml — the check is skipped automatically
       if the domain is disabled, so no conditional logic needed here
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from ingestion.loaders.base_loader import get_logger, load_config
from ingestion.loaders.sharepoint_loader import database_path

logger = get_logger("database.tests")


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    name:    str
    passed:  bool
    message: str
    detail:  str = ""


@dataclass
class TestReport:
    results: list[CheckResult] = field(default_factory=list)

    def add(self, result: CheckResult) -> None:
        self.results.append(result)
        icon = "✓" if result.passed else "✗"
        lvl  = logger.info if result.passed else logger.warning
        lvl(f"  {icon} {result.name}: {result.message}")
        if result.detail and not result.passed:
            logger.warning(f"      {result.detail}")

    @property
    def n_passed(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def n_failed(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    @property
    def all_passed(self) -> bool:
        return self.n_failed == 0


# ── Helper ────────────────────────────────────────────────────────────────────

def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> any:
    return con.execute(sql).fetchone()[0]


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    result = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [name]
    ).fetchone()[0]
    return result > 0


def _view_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    result = con.execute(
        "SELECT COUNT(*) FROM information_schema.views WHERE table_name = ?",
        [name]
    ).fetchone()[0]
    return result > 0


# ── Shared dimension checks ───────────────────────────────────────────────────
# These run regardless of which domains are active.

def check_dimensions(con: duckdb.DuckDBPyConnection, report: TestReport) -> None:
    """Verify all shared dimension tables exist and are populated."""

    logger.info("── Dimension checks ─────────────────────────────")

    required_tables = [
        "dim_geography", "dim_time", "dim_source",
        "dim_legal_form", "dim_indicator",
    ]
    for table in required_tables:
        exists = _table_exists(con, table)
        report.add(CheckResult(
            name=f"table_exists:{table}",
            passed=exists,
            message="exists" if exists else "MISSING",
        ))
        if not exists:
            continue

        n = _scalar(con, f"SELECT COUNT(*) FROM {table}")
        report.add(CheckResult(
            name=f"table_populated:{table}",
            passed=n > 0,
            message=f"{n:,} rows",
            detail=f"{table} is empty — check populate_dimensions() in build_db.py",
        ))

    # dim_geography: Italy national row must exist
    if _table_exists(con, "dim_geography"):
        has_italy = _scalar(con, "SELECT COUNT(*) FROM dim_geography WHERE nuts_code = 'IT'") > 0
        report.add(CheckResult(
            name="dim_geography:italy_row",
            passed=has_italy,
            message="IT row present" if has_italy else "IT row MISSING",
            detail="nuts_istat.csv may not have loaded correctly",
        ))

        # All NUTS2 Italian regions should be present (20 regions = 21 rows incl Trentino split)
        n_nuts2_it = _scalar(con, """
            SELECT COUNT(*) FROM dim_geography
            WHERE nuts_level = 2 AND country_code = 'IT'
        """)
        report.add(CheckResult(
            name="dim_geography:italian_regions",
            passed=n_nuts2_it >= 20,
            message=f"{n_nuts2_it} Italian NUTS2 regions",
            detail="Expected at least 20 — check nuts_istat.csv",
        ))

    # dim_time: must cover at least 2010–2024
    if _table_exists(con, "dim_time"):
        has_range = _scalar(con, """
            SELECT COUNT(*) FROM dim_time
            WHERE year BETWEEN 2010 AND 2024
        """)
        report.add(CheckResult(
            name="dim_time:covers_2010_2024",
            passed=has_range == 15,
            message=f"{has_range}/15 years present (2010–2024)",
        ))

    # dim_indicator: known social economy codes must be present
    if _table_exists(con, "dim_indicator"):
        expected_indicators = [
            "volunteering_rate",
            "association_membership_rate",
            "n_employed",
            "n_local_units",
        ]
        for code in expected_indicators:
            present = _scalar(con,
                f"SELECT COUNT(*) FROM dim_indicator WHERE indicator_code = '{code}'"
            ) > 0
            report.add(CheckResult(
                name=f"dim_indicator:{code}",
                passed=present,
                message="present" if present else "MISSING — add to dimensions.sql",
            ))


# ── Domain-specific checks ────────────────────────────────────────────────────
# Each function checks one domain's fact table and associated views.
# Adding a new domain: copy check_fact_social_economy(), replace the domain
# name, table name, view names, and expected indicator codes.

def check_fact_social_economy(con: duckdb.DuckDBPyConnection, report: TestReport) -> None:
    """Integrity checks for the social economy domain."""

    logger.info("── Social economy checks ────────────────────────")

    table = "fact_social_economy"
    if not _table_exists(con, table):
        report.add(CheckResult(
            name=f"table_exists:{table}",
            passed=False,
            message="MISSING — run build_db.py first",
        ))
        return

    # Row count
    n_total = _scalar(con, f"SELECT COUNT(*) FROM {table}")
    report.add(CheckResult(
        name="fact_social_economy:row_count",
        passed=n_total > 0,
        message=f"{n_total:,} rows",
        detail="Fact table is empty — check merge_social_economy.py output",
    ))

    if n_total == 0:
        return

    # No null required keys
    for key_col in ["geo_key", "time_key", "source_key", "indicator_key"]:
        n_null = _scalar(con, f"SELECT COUNT(*) FROM {table} WHERE {key_col} IS NULL")
        report.add(CheckResult(
            name=f"fact_social_economy:no_null_{key_col}",
            passed=n_null == 0,
            message=f"{n_null} null values" if n_null > 0 else "no nulls",
            detail=f"{n_null} rows have null {key_col} — key resolution failed for these rows",
        ))

    # No null or negative values
    n_null_val = _scalar(con, f"SELECT COUNT(*) FROM {table} WHERE value IS NULL")
    report.add(CheckResult(
        name="fact_social_economy:no_null_value",
        passed=n_null_val == 0,
        message=f"{n_null_val} null values" if n_null_val > 0 else "no nulls",
    ))

    n_neg = _scalar(con, f"SELECT COUNT(*) FROM {table} WHERE value < 0")
    report.add(CheckResult(
        name="fact_social_economy:no_negative_values",
        passed=n_neg == 0,
        message=f"{n_neg} negative values" if n_neg > 0 else "no negatives",
        detail="Negative counts or rates are likely parsing errors",
    ))

    # No orphan foreign keys (all keys resolve to a dimension row)
    orphan_checks = [
        ("geo_key",       "dim_geography",  "geo_key"),
        ("time_key",      "dim_time",       "time_key"),
        ("source_key",    "dim_source",     "source_key"),
        ("indicator_key", "dim_indicator",  "indicator_key"),
    ]
    for fk_col, dim_table, dim_pk in orphan_checks:
        n_orphan = _scalar(con, f"""
            SELECT COUNT(*) FROM {table} f
            LEFT JOIN {dim_table} d ON f.{fk_col} = d.{dim_pk}
            WHERE d.{dim_pk} IS NULL
        """)
        report.add(CheckResult(
            name=f"fact_social_economy:no_orphan_{fk_col}",
            passed=n_orphan == 0,
            message=f"{n_orphan} orphan rows" if n_orphan > 0 else "all keys resolve",
            detail=f"{n_orphan} rows in {table} have {fk_col} not in {dim_table}",
        ))

    # Both sources present
    for source in ["istat", "eurostat"]:
        n_source = _scalar(con, f"""
            SELECT COUNT(*) FROM {table} f
            JOIN dim_source s ON f.source_key = s.source_key
            WHERE s.source_id = '{source}'
        """)
        report.add(CheckResult(
            name=f"fact_social_economy:source_{source}",
            passed=n_source > 0,
            message=f"{n_source:,} rows",
            detail=f"No rows from {source} — check ingestion and merge steps",
        ))

    # Expected indicator codes present
    for code in ["volunteering_rate", "association_membership_rate", "n_employed"]:
        n_ind = _scalar(con, f"""
            SELECT COUNT(*) FROM {table} f
            JOIN dim_indicator i ON f.indicator_key = i.indicator_key
            WHERE i.indicator_code = '{code}'
        """)
        report.add(CheckResult(
            name=f"fact_social_economy:indicator_{code}",
            passed=n_ind > 0,
            message=f"{n_ind:,} rows",
            detail=f"No rows for indicator '{code}' — check ISTAT_DATASET_META in merge_social_economy.py",
        ))

    # Italian rows present at NUTS0 and NUTS2
    for level, label in [(0, "national"), (2, "regional")]:
        n_it = _scalar(con, f"""
            SELECT COUNT(*) FROM {table} f
            JOIN dim_geography g ON f.geo_key = g.geo_key
            WHERE g.country_code = 'IT' AND g.nuts_level = {level}
        """)
        report.add(CheckResult(
            name=f"fact_social_economy:italy_{label}",
            passed=n_it > 0,
            message=f"{n_it:,} rows",
            detail=f"No Italian {label}-level rows — check geo harmonisation",
        ))

    # Views exist and are queryable
    expected_views = [
        "vw_social_economy",
        "vw_se_volunteering_national",
        "vw_se_volunteering_regional",
        "vw_se_associationism_national",
        "vw_se_employment_eu",
        "vw_se_local_units_regional",
    ]
    for view in expected_views:
        exists = _view_exists(con, view)
        if exists:
            # Also check the view actually returns rows
            n_view = _scalar(con, f"SELECT COUNT(*) FROM {view}")
            report.add(CheckResult(
                name=f"view_populated:{view}",
                passed=n_view > 0,
                message=f"{n_view:,} rows",
                detail=f"View exists but returns 0 rows — check view definition in views.sql",
            ))
        else:
            report.add(CheckResult(
                name=f"view_exists:{view}",
                passed=False,
                message="MISSING — check views.sql",
            ))


# ── Stub checks for future domains ───────────────────────────────────────────
# When you add a new domain:
# 1. Copy check_fact_social_economy() above
# 2. Replace table name, view names, source names, and indicator codes
# 3. Add it to DOMAIN_CHECKS below

def check_fact_immigration(con: duckdb.DuckDBPyConnection, report: TestReport) -> None:
    """Stub — fill in when immigration domain is built."""
    table = "fact_immigration"
    if not _table_exists(con, table):
        # Table not yet populated — skip silently rather than failing
        logger.info("── Immigration checks: table not yet populated, skipping ──")
        return
    n = _scalar(con, f"SELECT COUNT(*) FROM {table}")
    report.add(CheckResult(
        name="fact_immigration:row_count",
        passed=n > 0,
        message=f"{n:,} rows",
    ))
    # TODO: add detailed checks when domain is built


def check_fact_welfare(con, report): pass     # stub
def check_fact_poverty(con, report): pass     # stub
def check_fact_labour(con, report):  pass     # stub
def check_fact_sdg(con, report):     pass     # stub


# ── Domain check registry ─────────────────────────────────────────────────────
# Maps domain name → check function.
# Checks are skipped automatically if the domain is disabled in settings.yaml.

DOMAIN_CHECKS = {
    "social_economy": check_fact_social_economy,
    "immigration":    check_fact_immigration,
    "welfare":        check_fact_welfare,
    "poverty":        check_fact_poverty,
    "labour":         check_fact_labour,
    "sdg":            check_fact_sdg,
}


# ── Main ──────────────────────────────────────────────────────────────────────

def run_checks(db_path: Path, config: dict) -> TestReport:
    """
    Connect to the DuckDB file and run all applicable checks.

    Checks for disabled domains are skipped.
    Returns a TestReport with results for all checks that ran.
    """
    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found at {db_path}. Run build_db.py first."
        )

    logger.info(f"Running integrity checks on {db_path}")
    con = duckdb.connect(str(db_path), read_only=True)
    report = TestReport()

    # Always run dimension checks
    check_dimensions(con, report)

    # Run domain checks for active domains
    active_domains = [
        d for d in DOMAIN_CHECKS
        if config.get("domains", {}).get(d, {}).get("enabled", False)
    ]

    for domain in active_domains:
        check_fn = DOMAIN_CHECKS.get(domain)
        if check_fn:
            check_fn(con, report)

    con.close()

    # Print summary
    logger.info(f"\n{'═' * 50}")
    logger.info(f"Results: {report.n_passed} passed, {report.n_failed} failed")
    if report.all_passed:
        logger.info("All checks passed ✓")
    else:
        logger.warning(
            f"{report.n_failed} check(s) failed — review warnings above "
            "before connecting PowerBI."
        )
    logger.info(f"{'═' * 50}")

    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run integrity checks on the aiccon DuckDB database."
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="Path to .duckdb file. Defaults to SharePoint database/ folder.",
    )
    parser.add_argument(
        "--config", type=str, default=None,
        help="Path to settings.yaml (default: auto-detected).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args   = parse_args()
    config = load_config(settings_path=args.config)

    if args.db:
        db_path = Path(args.db)
    else:
        db_path = database_path(config) / config["sharepoint"]["database_filename"]

    report = run_checks(db_path, config)
    sys.exit(0 if report.all_passed else 1)