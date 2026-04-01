"""
ingestion/loaders/base_loader.py

Shared utilities for all API source scripts (eurostat.py, istat.py, etc.).
Handles HTTP fetching with retry logic, DataFrame normalisation, and writing
raw parquet to the locally synced SharePoint folder.
"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_log, after_log

load_dotenv("config/.env")

logger = logging.getLogger(__name__)


# ── CONFIG ────────────────────────────────────────────────────────────────────

# Loaded from config/.env — add a line like:
_root = os.getenv("SHAREPOINT_ROOT")
if not _root:
    raise EnvironmentError(
        "SHAREPOINT_ROOT is not set. Add it to config/.env — "
        "it should be the path to your locally synced SharePoint folder."
    )

SHAREPOINT_ROOT = Path(_root)
RAW_DIR = SHAREPOINT_ROOT / "data" / "raw"

# HTTP request settings
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3             # total attempts before giving up
WAIT_MIN_SECONDS = 2        # first retry waits 2s, then 4s, then 8s


# ── HTTP SESSION ──────────────────────────────────────────────────────────────

_session = requests.Session()


# ── FETCH ─────────────────────────────────────────────────────────────────────

@retry(
    retry=retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout)),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=WAIT_MIN_SECONDS),
    before=before_log(logger, logging.DEBUG),
    after=after_log(logger, logging.WARNING),
    reraise=True,
)
def fetch_json(url: str, params: dict | None = None) -> dict:
    """
    Fetch a JSON endpoint and return the parsed response body.

    Retries up to MAX_RETRIES times on connection errors and timeouts,
    with exponential backoff (2s, 4s, 8s). HTTP errors (4xx, 5xx) are
    raised immediately without retrying, as they indicate a real problem
    (wrong URL, dataset renamed, etc.) that won't resolve itself.

    Args:
        url:    Full URL to fetch.
        params: Optional query parameters to append.

    Returns:
        Parsed JSON response as a dict.

    Raises:
        RuntimeError: If the request fails after all retries.
    """
    logger.info("Fetching: %s  params=%s", url, params)
    try:
        response = _session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(
            f"HTTP error fetching {url}: {exc.response.status_code} {exc.response.reason}"
        ) from exc


@retry(
    retry=retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout)),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=WAIT_MIN_SECONDS),
    before=before_log(logger, logging.DEBUG),
    after=after_log(logger, logging.WARNING),
    reraise=True,
)
def fetch_xml(url: str, params: dict | None = None) -> str:
    """
    Fetch an XML endpoint and return the raw response text.
    Same retry and error handling as fetch_json.

    Returns:
        Raw response body as a string.

    Raises:
        RuntimeError: If the request fails after all retries.
    """
    logger.info("Fetching XML: %s  params=%s", url, params)
    try:
        response = _session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.text
    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(
            f"HTTP error fetching {url}: {exc.response.status_code} {exc.response.reason}"
        ) from exc


# ── NORMALISE ─────────────────────────────────────────────────────────────────

def add_extracted_at(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add an extracted_at column (UTC timestamp) to a DataFrame.
    This is written into every raw parquet file so we always know when
    data was pulled, even if the source doesn't provide its own update date.
    """
    df = df.copy()
    df["extracted_at"] = datetime.now(timezone.utc).isoformat()
    return df


# ── WRITE PARQUET ─────────────────────────────────────────────────────────────

def save_raw_parquet(df: pd.DataFrame, domain: str, filename: str) -> Path:
    """
    Write a DataFrame to the raw layer of the locally synced SharePoint folder.

    Files are written to:
        <SHAREPOINT_ROOT>/data/raw/<domain>/<filename>.parquet

    An extracted_at column is added automatically if not already present.

    Args:
        df:       DataFrame to save.
        domain:   Domain subfolder name, e.g. "social_economy".
        filename: File name without extension, e.g. "eurostat_nama_10r_3empers".

    Returns:
        Path to the written parquet file.

    Raises:
        RuntimeError: If the write fails (e.g. SharePoint folder not synced).
    """
    if "extracted_at" not in df.columns:
        df = add_extracted_at(df)

    out_dir = RAW_DIR / domain
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{filename}.parquet"

    try:
        df.to_parquet(out_path, index=False)
        logger.info("Saved %d rows → %s", len(df), out_path)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to write parquet to {out_path}. "
            f"Check that the SharePoint folder is synced and SHAREPOINT_ROOT is set correctly.\n"
            f"Original error: {exc}"
        ) from exc

    return out_path