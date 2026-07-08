from __future__ import annotations


import datetime as dt
import http.client
import logging
import os
import time
import urllib.error
import urllib.request
from zoneinfo import ZoneInfo

import boto3
from boto3.s3.transfer import TransferConfig

LOG = logging.getLogger()
LOG.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# --- Configuration (all overridable via environment variables) ---------------

S3_BUCKET = os.environ.get("S3_BUCKET", "stateabc")
S3_PREFIX = os.environ.get("S3_PREFIX", "newyork").strip("/")
FILE_PREFIX = os.environ.get("FILE_PREFIX", "FRL_")          # TTB-style prefix
DATE_FORMAT = os.environ.get("DATE_FORMAT", "%m-%d-%Y")      # 07-06-2026
RUN_TIMEZONE = os.environ.get("RUN_TIMEZONE", "America/New_York")  # #3
SOCRATA_DOMAIN = os.environ.get("SOCRATA_DOMAIN", "data.ny.gov")
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN")      # optional, for throttling
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "300"))    # seconds, per read
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
BACKOFF_BASE = float(os.environ.get("BACKOFF_BASE", "2"))    # seconds; 2,4,8...


_RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


DATASETS = {
    "Active": (
        os.environ.get("ACTIVE_DATASET_ID", "9s3h-dpkz"),    # Current Liquor Authority Active Licenses
        "activelicenses",
        f"{FILE_PREFIX}Active_Licenses_List.csv",
    ),
    "Pending": (
        os.environ.get("PENDING_DATASET_ID", "f8i8-k2gm"),   # Current SLA Pending Licenses
        "pendinglicenses",
        f"{FILE_PREFIX}Pending_Licenses_List.csv",
    ),
    "Inactive": (
    
        os.environ.get("INACTIVE_DATASET_ID", "6dg3-2z7i"),  # Current SLA Inactive Licenses
        "inactivelicenses",
        f"{FILE_PREFIX}Inactive_Licenses_List.csv",
    ),
}

_S3 = boto3.client("s3")


_TRANSFER_CFG = TransferConfig(
    multipart_threshold=16 * 1024 * 1024,   # 16 MB
    multipart_chunksize=16 * 1024 * 1024,   # 16 MB
    use_threads=False,
)


class _PeekedStream:
    """Wrap an HTTP response so bytes already peeked are served before the rest.

    Lets us sniff the first chunk (to reject HTML error pages) without losing it,
    then hand a single continuous read()-able stream to S3 upload_fileobj.
    """

    def __init__(self, peeked: bytes, rest) -> None:
        self._peeked = peeked
        self._rest = rest

    def read(self, amt: int | None = None) -> bytes:
        if self._peeked:
            if amt is None:
                out = self._peeked + self._rest.read()
                self._peeked = b""
                return out
            if amt <= len(self._peeked):
                out, self._peeked = self._peeked[:amt], self._peeked[amt:]
                return out
            out = self._peeked + self._rest.read(amt - len(self._peeked))
            self._peeked = b""
            return out
        return self._rest.read(amt)


def _export_url(dataset_id: str) -> str:
    """Socrata CSV export endpoint -- the equivalent of the site's Download CSV.

    NOTE: this is the /api/views/{id}/rows.csv export, which returns the FULL
    dataset. Do NOT switch to /resource/{id}.csv -- that endpoint silently caps
    at 1,000 rows unless you paginate.
    """
    return (
        f"https://{SOCRATA_DOMAIN}/api/views/{dataset_id}"
        f"/rows.csv?accessType=DOWNLOAD"
    )


def _stream_dataset_to_s3(name: str, dataset_id: str, key: str) -> int:
    """Download one dataset and stream it to s3://S3_BUCKET/<key>. Returns bytes."""
    url = _export_url(dataset_id)
    last_err: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "Accept": "text/csv",
                    "User-Agent": "nysla-s3-mirror/2.0 (+lambda)",
                },
            )
            if SOCRATA_APP_TOKEN:
                req.add_header("X-App-Token", SOCRATA_APP_TOKEN)

            LOG.info("[%s] GET %s (attempt %d/%d)", name, url, attempt, MAX_RETRIES)
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                # Server-declared size, when present (often absent on chunked
                # transfers) -- used as an end-to-end integrity check below. #6
                declared = resp.headers.get("Content-Length")
                declared_len = int(declared) if declared and declared.isdigit() else None

                # Peek the first bytes to confirm we got CSV, not an HTML error page.
                head = resp.read(512)
                if not head:
                    raise ValueError("empty response body")
                if head.lstrip()[:1] == b"<":
                    raise ValueError("response looks like HTML, not CSV")

                stream = _PeekedStream(head, resp)
                _S3.upload_fileobj(
                    stream,
                    S3_BUCKET,
                    key,
                    ExtraArgs={"ContentType": "text/csv"},
                    Config=_TRANSFER_CFG,
                )

            size = _S3.head_object(Bucket=S3_BUCKET, Key=key)["ContentLength"]
            if size < 100:
                raise ValueError(f"uploaded object suspiciously small ({size} bytes)")
            if declared_len is not None and size != declared_len:
                # Truncated / partial download -- reject so we don't overwrite
                # good data with a short file. #6
                raise ValueError(
                    f"size mismatch: uploaded {size} != Content-Length {declared_len}"
                )

            LOG.info("[%s] wrote s3://%s/%s (%d bytes)", name, S3_BUCKET, key, size)
            return size

        except urllib.error.HTTPError as err:
            last_err = err
            if err.code not in _RETRYABLE_STATUS:
                # Hard client error (403/404/...) -- retrying won't help. #4
                LOG.error("[%s] non-retryable HTTP %s: %s", name, err.code, err)
                break
            LOG.warning("[%s] attempt %d failed (HTTP %s): %s",
                        name, attempt, err.code, err)
        except (urllib.error.URLError, http.client.IncompleteRead,
                ValueError, OSError) as err:  # IncompleteRead added in v2. #5
            last_err = err
            LOG.warning("[%s] attempt %d failed: %s", name, attempt, err)

        if attempt < MAX_RETRIES:
            backoff = BACKOFF_BASE * (2 ** (attempt - 1))   # 2s, 4s, 8s... #4
            LOG.info("[%s] backing off %.1fs before retry", name, backoff)
            time.sleep(backoff)

    raise RuntimeError(f"[{name}] failed after {MAX_RETRIES} attempts: {last_err}")


def lambda_handler(event, context):
    """Entry point.

    Writes each dataset into its own subfolder under one dated folder:

        s3://stateabc/newyork/07-06-2026/activelicenses/FRL_Active_Licenses_List.csv
        s3://stateabc/newyork/07-06-2026/pendinglicenses/FRL_Pending_Licenses_List.csv
        s3://stateabc/newyork/07-06-2026/inactivelicenses/FRL_Inactive_Licenses_List.csv

    The dated folder is stamped in Eastern time (RUN_TIMEZONE) so a late-evening
    run never lands in the next day's folder. An optional
    {"run_date": "MM-DD-YYYY"} in the event allows backfills / re-runs into a
    specific dated folder.
    """
    run_date = (event or {}).get("run_date") or dt.datetime.now(
        ZoneInfo(RUN_TIMEZONE)
    ).strftime(DATE_FORMAT)
    folder = "/".join(p for p in (S3_PREFIX, run_date) if p)
    LOG.info("Mirroring NYS SLA licenses to s3://%s/%s/", S3_BUCKET, folder)

    written: dict[str, dict] = {}
    errors: dict[str, str] = {}

    for name, (dataset_id, subfolder, filename) in DATASETS.items():
        key = f"{folder}/{subfolder}/{filename}"
        try:
            written[name] = {
                "key": key,
                "bytes": _stream_dataset_to_s3(name, dataset_id, key),
            }
        except Exception as err:  # noqa: BLE001 -- record and continue to next dataset
            errors[name] = str(err)
            LOG.error("[%s] %s", name, err)

    summary = {
        "date": run_date,
        "bucket": S3_BUCKET,
        "folder": folder,
        "written": written,
        "errors": errors,
    }

    if errors:
        raise RuntimeError(f"One or more datasets failed: {summary}")

    LOG.info("Done: %s", summary)
    return summary
