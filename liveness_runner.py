#!/usr/bin/env python3
"""
liveness_runner.py — batch runner for HyperVerge checkLiveness.

Reads a CSV with columns `riderId,image` (image = S3 presigned URL), downloads each
image into memory, sends it to checkLiveness with transactionId = riderId, and writes
every response field to CSV. One run = one batch file.

USAGE
    export HV_APP_ID=xxxx
    export HV_APP_KEY=yyyy
    python3 liveness_runner.py --input batch_01.csv --out runs/batch_01

    # resume after a crash / Ctrl-C: run the exact same command again
    # re-run only retriable failures (timeouts, 5xx/429 exhausted, download errors):
    python3 liveness_runner.py --input batch_01.csv --out runs/batch_01 --retry-failed
    # re-run rows with regenerated URLs: edit the `image` column of
    # runs/batch_01/failures.csv (keep row_uid) and feed it back in:
    python3 liveness_runner.py --input fixed_failures.csv --out runs/batch_01

OUTPUTS (in --out)
    results.csv         append-only log of every processed row (this is the checkpoint)
    results_latest.csv  one row per row_uid (latest attempt wins), rebuilt at exit
    failures.csv        rows whose latest status is a failure, with image URL, rebuilt at exit
    failures_log.csv    append-only log of every failure (with image URL)
    run.log             full log
    manifest.json       input file fingerprint, used to refuse unsafe resumes

ALERTS (emailed to ALERT_EMAIL_TO, CC ALERT_EMAIL_CC — defaults below)
    - batch finished: summary of status counts, duration, output folder
    - writer failure: results can't be written (disk full etc.) — the run stops immediately
    - stopped unexpectedly: SIGTERM, SIGHUP (e.g. SSH dropped outside tmux), or an unhandled
      error.
    - stopped manually: Ctrl-C (SIGINT)
    Mail is sent via SMTP if SMTP_HOST is set, otherwise via the local `sendmail` binary.
        export SMTP_HOST=smtp.gmail.com SMTP_PORT=587
        export SMTP_USER=you@gmail.com SMTP_PASS='<gmail app password>'
        # optional: SMTP_FROM, ALERT_EMAIL_TO, ALERT_EMAIL_CC (comma-separated)
    Check delivery before a batch with:  python3 liveness_runner.py --test-alert

STATUSES
    SUCCESS            HTTP 200 and status == "success"
    API_REJECTED       API processed the request and rejected it (HTTP 4xx other than 429,
                       e.g. face not detected) — a model/business outcome, not a script failure
    API_ERROR          unexpected API response (200 with non-success status, unparseable body,
                       other non-4xx/5xx codes)
    RETRIES_EXHAUSTED  429 / 5xx / timeout / connection error on every attempt
    URL_EMPTY          image column blank
    URL_EXPIRED        presigned URL past its X-Amz-Expires, or S3 said "Request has expired"
    DOWNLOAD_FAILED    S3 403/404/5xx/timeout
    UNSUPPORTED_FORMAT a valid file of a type the API isn't sent (HEIC/HEIF, AVIF, GIF, BMP,
                       TIFF, PDF) or larger than --max-image-mb
    IMAGE_CORRUPT      0 bytes, unrecognisable bytes (e.g. an XML/HTML error page), or a
                       JPEG/PNG/WEBP that fails to decode (truncated, damaged)

Exit codes: 0 done, 130 interrupted (resume with same command), 2 aborted at pre-flight,
3 writer failure (alert sent).

Requires: python3.8+, `pip install requests pillow` (pillow optional but recommended).
"""

import argparse
import csv
import hashlib
import io
import json
import logging
import os
import queue
import random
import shutil
import signal
import smtplib
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter, deque
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests

try:
    from PIL import Image

    HAVE_PIL = True
except ImportError:  # pragma: no cover
    HAVE_PIL = False

# --------------------------------------------------------------------------- config

DEFAULT_API_URL = "https://global.rl.hyperverge.co/v1/checkLiveness"
ALERT_EMAIL_TO = os.environ.get("ALERT_EMAIL_TO", "s.aravindaan@gmail.com")
ALERT_EMAIL_CC = os.environ.get("ALERT_EMAIL_CC", "aravindaan.s@hyperverge.co, eshaan.j@hyperverge.co")
ALERT_RECIPIENTS = ", ".join(x for x in (ALERT_EMAIL_TO, ALERT_EMAIL_CC) if x)
MIN_FREE_DISK_MB = 500
FORM_FIELDS = {
    "returnCroppedImageURL": "no",
    "qualityChecks.eyesClosed": "yes",
}
# Fixed namespace so row_uid is stable across runs/restarts. Do not change mid-project.
UID_NAMESPACE = uuid.UUID("6f1c2b7e-3d4a-4e8b-9c1d-2a5b7e9f0c13")

QUALITY_CHECKS = [
    "multipleFaces", "eyewear", "readingGlasses", "sunglasses", "faceOccluded",
    "headTurned", "blur", "eyesClosed", "hat",
]
QC_FIELDS = ["value", "score", "confidence"]

BASE_COLS = [
    "run_id", "row_uid", "row_number", "riderId", "transactionId", "status",
    "http_status", "attempts", "api_latency_ms", "image_bytes", "error_message",
    "started_at", "finished_at",
]
RESPONSE_COLS = (
    ["api_status", "api_statusCode", "requestId", "response_transactionId",
     "liveFace_value", "liveFace_confidence", "liveFace_score"]
    + [f"qc_{c}_{f}" for c in QUALITY_CHECKS for f in QC_FIELDS]
    + ["summary_action", "summary_details", "raw_response"]
)
RESULT_COLS = BASE_COLS + RESPONSE_COLS
FAILURE_COLS = ["row_uid", "row_number", "riderId", "image", "status", "error_message", "finished_at"]

SUCCESS = "SUCCESS"
API_REJECTED = "API_REJECTED"
API_ERROR = "API_ERROR"
RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED"
URL_EMPTY = "URL_EMPTY"
URL_EXPIRED = "URL_EXPIRED"
DOWNLOAD_FAILED = "DOWNLOAD_FAILED"
IMAGE_CORRUPT = "IMAGE_CORRUPT"
UNSUPPORTED_FORMAT = "UNSUPPORTED_FORMAT"
RETRIABLE = {RETRIES_EXHAUSTED, DOWNLOAD_FAILED, URL_EXPIRED}

log = logging.getLogger("liveness")
RUN_INFO = {}  # filled in by main(); used by the crash alert


def now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def fmt_ts(epoch):
    return datetime.fromtimestamp(epoch).astimezone().strftime("%Y-%m-%d %H:%M:%S %z")


def fmt_dur(seconds):
    seconds = int(max(0, seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s"


# --------------------------------------------------------------------------- rate limiter

class RampLimiter:
    """Evenly spaced request slots, ramping RPM in fixed steps up to a peak.

    Levels: start, start+step, ... , peak. Each level is held for `interval_s`.
    Slots are spaced 60/rpm seconds apart (no bursts). Idle time never accumulates
    credit, so a quiet period can't turn into a burst later.
    Optionally steps down one level on HTTP 429 and holds there for one interval.
    """

    def __init__(self, start_rpm, step_rpm, interval_s, peak_rpm, step_down_on_429, stop_event):
        levels, r = [], start_rpm
        while r < peak_rpm:
            levels.append(r)
            r += step_rpm
        levels.append(peak_rpm)
        self.levels = levels
        self.interval = interval_s
        self.step_down_on_429 = step_down_on_429
        self.stop = stop_event
        self.lock = threading.Lock()
        self.idx = 0
        self.level_since = time.monotonic()
        self.next_slot = time.monotonic()
        self.last_stepdown = -1e9

    def _advance(self):
        now = time.monotonic()
        while self.idx < len(self.levels) - 1 and now - self.level_since >= self.interval:
            self.idx += 1
            self.level_since += self.interval
            log.info("[ramp] stepping up to %s RPM", self.levels[self.idx])

    @property
    def rpm(self):
        with self.lock:
            self._advance()
            return self.levels[self.idx]

    def acquire(self):
        """Block until this caller's slot. Returns False if shutting down."""
        with self.lock:
            self._advance()
            gap = 60.0 / self.levels[self.idx]
            slot = max(time.monotonic(), self.next_slot)
            self.next_slot = slot + gap
        delay = slot - time.monotonic()
        if delay > 0 and self.stop.wait(delay):
            return False
        return not self.stop.is_set()

    def on_throttle(self):
        if not self.step_down_on_429:
            return
        with self.lock:
            now = time.monotonic()
            if now - self.last_stepdown < self.interval:
                return
            self.idx = max(0, self.idx - 1)
            self.level_since = now
            self.last_stepdown = now
            self.next_slot = max(self.next_slot, now) + 60.0 / self.levels[self.idx]
            rpm = self.levels[self.idx]
        log.warning("[ramp] 429 received — stepping down to %s RPM and holding", rpm)


def projected_seconds(n, levels, interval_s):
    t, remaining = 0.0, float(n)
    for i, rpm in enumerate(levels):
        if i < len(levels) - 1:
            cap = rpm * interval_s / 60.0
            if remaining <= cap:
                return t + remaining * 60.0 / rpm
            remaining -= cap
            t += interval_s
        else:
            return t + remaining * 60.0 / rpm
    return t


# --------------------------------------------------------------------------- stats

class Stats:
    def __init__(self, total):
        self.total = total
        self.lock = threading.Lock()
        self.done = 0
        self.by_status = Counter()
        self.api_calls = deque()  # monotonic timestamps of API calls, last 60s
        self.http_codes = Counter()

    def record_call(self, code):
        with self.lock:
            now = time.monotonic()
            self.api_calls.append(now)
            while self.api_calls and now - self.api_calls[0] > 60:
                self.api_calls.popleft()
            self.http_codes[str(code)] += 1

    def record_result(self, status):
        with self.lock:
            self.done += 1
            self.by_status[status] += 1

    def snapshot(self):
        with self.lock:
            now = time.monotonic()
            while self.api_calls and now - self.api_calls[0] > 60:
                self.api_calls.popleft()
            return self.done, dict(self.by_status), len(self.api_calls), dict(self.http_codes)


# --------------------------------------------------------------------------- input / state

def url_expiry(url):
    """Epoch seconds when a presigned URL expires, or None if not determinable."""
    try:
        q = parse_qs(urlparse(url).query)
        q = {k.lower(): v for k, v in q.items()}
        if "x-amz-date" in q and "x-amz-expires" in q:
            signed = datetime.strptime(q["x-amz-date"][0], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
            return signed.timestamp() + int(q["x-amz-expires"][0])
        if "expires" in q:  # SigV2
            return float(q["expires"][0])
    except Exception:
        pass
    return None


def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_input(path, batch_name):
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fields = [c.strip() for c in (reader.fieldnames or [])]
        reader.fieldnames = fields
        missing = {"riderId", "image"} - set(fields)
        if missing:
            raise SystemExit(f"Input CSV is missing column(s): {', '.join(sorted(missing))}. Found: {fields}")
        explicit = "row_uid" in fields
        rows = []
        for i, row in enumerate(reader, start=1):
            rider = (row.get("riderId") or "").strip()
            img = (row.get("image") or "").strip()
            if explicit and (row.get("row_uid") or "").strip():
                uid = row["row_uid"].strip()
                rn = int(row.get("row_number") or i)
            else:
                uid = str(uuid.uuid5(UID_NAMESPACE, f"{batch_name}:{i}:{rider}"))
                rn = i
            rows.append({"row_uid": uid, "row_number": rn, "riderId": rider, "image": img})
    return rows, explicit


def repair_partial_line(path):
    """If a crash left a half-written last line, cut it off so appends stay valid."""
    if not path.exists() or path.stat().st_size == 0:
        return
    with open(path, "rb+") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        f.seek(size - 1)
        if f.read(1) == b"\n":
            return
        pos = size - 1
        while pos > 0:
            f.seek(pos - 1)
            if f.read(1) == b"\n":
                break
            pos -= 1
        f.truncate(pos)
        log.warning("Removed a partially written last line from %s (crash recovery)", path.name)


def load_latest(results_path):
    latest = {}
    if results_path.exists():
        with open(results_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("row_uid") and row.get("status"):
                    latest[row["row_uid"]] = row["status"]
    return latest


# --------------------------------------------------------------------------- worker logic

class Aborted(Exception):
    pass


class DownloadError(Exception):
    def __init__(self, status, msg):
        super().__init__(msg)
        self.status = status


def backoff_delay(attempt, base, cap):
    d = min(cap, base * (2 ** (attempt - 1)))
    return d / 2 + random.uniform(0, d / 2)  # equal jitter


def download(session, url, ctx):
    last = ""
    for attempt in range(1, ctx.args.download_retries + 1):
        if ctx.stop.is_set():
            raise Aborted()
        try:
            r = session.get(url, timeout=(10, ctx.args.download_timeout))
        except (requests.Timeout, requests.ConnectionError) as e:
            last = f"{type(e).__name__}: {e}"
        else:
            if r.status_code == 200:
                return r.content
            snippet = r.text[:300].replace("\n", " ")
            if r.status_code == 403 and "expired" in snippet.lower():
                raise DownloadError(URL_EXPIRED, f"S3 403 Request has expired: {snippet}")
            if r.status_code in (400, 403, 404):
                raise DownloadError(DOWNLOAD_FAILED, f"S3 HTTP {r.status_code}: {snippet}")
            last = f"S3 HTTP {r.status_code}: {snippet}"
        if attempt < ctx.args.download_retries and ctx.stop.wait(backoff_delay(attempt, 1, 15)):
            raise Aborted()
    raise DownloadError(DOWNLOAD_FAILED, f"after {ctx.args.download_retries} attempts: {last}")


HEIF_BRANDS = {b"heic", b"heix", b"hevc", b"hevx", b"heim", b"heis", b"hevm", b"hevs", b"mif1", b"msf1"}


def sniff_format(data):
    """(name, ext, mime, supported) from magic bytes, or None if unrecognised."""
    if data[:3] == b"\xff\xd8\xff":
        return "JPEG", "jpg", "image/jpeg", True
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "PNG", "png", "image/png", True
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "WEBP", "webp", "image/webp", True
    if data[4:8] == b"ftyp":
        brand = data[8:12]
        if brand in HEIF_BRANDS:
            return "HEIC/HEIF", None, None, False
        if brand in (b"avif", b"avis"):
            return "AVIF", None, None, False
    if data[:4] in (b"GIF8",):
        return "GIF", None, None, False
    if data[:2] == b"BM":
        return "BMP", None, None, False
    if data[:4] in (b"II*\x00", b"MM\x00*"):
        return "TIFF", None, None, False
    if data[:4] == b"%PDF":
        return "PDF", None, None, False
    return None


def validate_image(data, max_bytes):
    """Returns (status, message, (ext, mime)). status is None when the image is usable."""
    if not data:
        return IMAGE_CORRUPT, "empty file (0 bytes)", None
    fmt = sniff_format(data)
    if fmt is None:
        head = data[:16]
        hint = " (looks like XML/HTML, not an image)" if head.lstrip()[:1] == b"<" else ""
        return IMAGE_CORRUPT, f"unrecognised file content, first bytes {head!r}{hint}", None
    name, ext, mime, supported = fmt
    if not supported:
        return UNSUPPORTED_FORMAT, f"{name} image ({len(data)} bytes) — format not sent to the API", None
    if len(data) > max_bytes:
        return UNSUPPORTED_FORMAT, f"{name} image too large ({len(data) / 1048576:.1f} MB > limit)", None
    if HAVE_PIL:
        try:
            with Image.open(io.BytesIO(data)) as im:
                im.load()  # full decode — catches truncated files
        except Exception as e:
            return IMAGE_CORRUPT, f"{name} failed to decode: {type(e).__name__}: {e}", None
    return None, "", (ext, mime)


_warned_unknown = set()
_warned_lock = threading.Lock()


def flatten(body):
    out = {}
    if not isinstance(body, dict):
        return out
    md = body.get("metadata") or {}
    res = body.get("result") or {}
    det = res.get("details") or {}
    lf = det.get("liveFace") or {}
    qc = det.get("qualityChecks") or {}
    summ = res.get("summary") or {}
    out.update({
        "api_status": body.get("status"),
        "api_statusCode": body.get("statusCode"),
        "requestId": md.get("requestId") or body.get("requestId"),
        "response_transactionId": md.get("transactionId"),
        "liveFace_value": lf.get("value"),
        "liveFace_confidence": lf.get("confidence"),
        "liveFace_score": lf.get("score"),
        "summary_action": summ.get("action"),
        "summary_details": json.dumps(summ["details"], ensure_ascii=False) if "details" in summ else "",
    })
    for c in QUALITY_CHECKS:
        node = qc.get(c) or {}
        for f in QC_FIELDS:
            out[f"qc_{c}_{f}"] = node.get(f)
    unknown = set(qc) - set(QUALITY_CHECKS)
    if unknown:
        with _warned_lock:
            new = unknown - _warned_unknown
            _warned_unknown.update(new)
        if new:
            log.warning("Response has quality checks not mapped to columns (kept in raw_response): %s", sorted(new))
    return out


def call_api(session, item, data, ext, mime, ctx):
    """Returns (status, partial_row). Raises Aborted on shutdown."""
    a = ctx.args
    txn = item["riderId"] or item["row_uid"]
    headers = {"appId": ctx.app_id, "appKey": ctx.app_key, "transactionId": txn}
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in txn)[:80] or "image"
    files = {"image": (f"{safe}.{ext}", data, mime)}
    last_err, last_code, latency = "", "", ""
    for attempt in range(1, a.max_attempts + 1):
        if not ctx.limiter.acquire():
            raise Aborted()
        t0 = time.monotonic()
        try:
            r = session.post(a.api_url, headers=headers, data=FORM_FIELDS, files=files,
                             timeout=(10, a.api_timeout))
        except (requests.Timeout, requests.ConnectionError) as e:
            latency = int((time.monotonic() - t0) * 1000)
            ctx.stats.record_call(type(e).__name__)
            last_err, last_code = f"{type(e).__name__}: {e}", ""
            log.debug("row %s attempt %d: %s", item["row_number"], attempt, last_err)
        else:
            latency = int((time.monotonic() - t0) * 1000)
            code = r.status_code
            ctx.stats.record_call(code)
            try:
                body = r.json()
            except ValueError:
                body = None
            raw = json.dumps(body, ensure_ascii=False, separators=(",", ":")) if body is not None else r.text[:5000]
            if code == 429 or code >= 500:
                if code == 429:
                    ctx.limiter.on_throttle()
                last_err, last_code = f"HTTP {code}: {raw[:300]}", code
                ra = r.headers.get("Retry-After")
                wait = None
                if ra:
                    try:
                        wait = min(float(ra), a.backoff_cap)
                    except ValueError:
                        wait = None
                if attempt < a.max_attempts:
                    if ctx.stop.wait(wait if wait is not None else backoff_delay(attempt, a.backoff_base, a.backoff_cap)):
                        raise Aborted()
                continue
            row = {"http_status": code, "attempts": attempt, "api_latency_ms": latency, "raw_response": raw}
            row.update(flatten(body))
            if code == 200 and isinstance(body, dict) and body.get("status") == "success":
                return SUCCESS, row
            err = ""
            if isinstance(body, dict):
                err = body.get("error") or body.get("message") or (body.get("result") or {}).get("error") or ""
                if isinstance(err, (dict, list)):
                    err = json.dumps(err, ensure_ascii=False)
            row["error_message"] = err or f"HTTP {code}"
            return (API_REJECTED if 400 <= code < 500 else API_ERROR), row
        if attempt < a.max_attempts and ctx.stop.wait(backoff_delay(attempt, a.backoff_base, a.backoff_cap)):
            raise Aborted()
    return RETRIES_EXHAUSTED, {"http_status": last_code, "attempts": a.max_attempts,
                               "api_latency_ms": latency, "error_message": last_err}


def process(item, session, ctx):
    base = {
        "run_id": ctx.run_id, "row_uid": item["row_uid"], "row_number": item["row_number"],
        "riderId": item["riderId"], "transactionId": item["riderId"] or item["row_uid"],
        "started_at": now_iso(), "_image": item["image"],
    }

    def done(status, extra):
        base.update(extra)
        base["status"] = status
        base["finished_at"] = now_iso()
        return base

    url = item["image"]
    if not url:
        return done(URL_EMPTY, {"error_message": "image URL is empty"})
    exp = url_expiry(url)
    if exp is not None and time.time() >= exp:
        return done(URL_EXPIRED, {"error_message": f"presigned URL expired at {fmt_ts(exp)}"})
    try:
        data = download(session, url, ctx)
    except DownloadError as e:
        return done(e.status, {"error_message": str(e)})
    bad, msg, kind = validate_image(data, ctx.args.max_image_mb * 1024 * 1024)
    if bad:
        return done(bad, {"error_message": msg, "image_bytes": len(data)})
    status, row = call_api(session, item, data, kind[0], kind[1], ctx)
    row["image_bytes"] = len(data)
    return done(status, row)


def worker_loop(ctx):
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=4)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    while True:
        item = ctx.work_q.get()
        if item is None:
            break
        if ctx.stop.is_set():
            continue
        try:
            res = process(item, session, ctx)
        except Aborted:
            continue  # not written -> picked up again on resume
        except Exception as e:  # never let one row kill a worker
            log.exception("Unexpected error on row %s", item["row_number"])
            res = {
                "run_id": ctx.run_id, "row_uid": item["row_uid"], "row_number": item["row_number"],
                "riderId": item["riderId"], "transactionId": item["riderId"] or item["row_uid"],
                "status": RETRIES_EXHAUSTED, "error_message": f"unexpected: {type(e).__name__}: {e}",
                "finished_at": now_iso(), "_image": item["image"],
            }
        ctx.result_q.put(res)


# --------------------------------------------------------------------------- alerts

def alert_channel():
    if os.environ.get("SMTP_HOST"):
        return f"SMTP {os.environ['SMTP_HOST']}:{os.environ.get('SMTP_PORT', '587')}"
    sm = shutil.which("sendmail") or (os.path.exists("/usr/sbin/sendmail") and "/usr/sbin/sendmail")
    return f"sendmail ({sm})" if sm else None


def send_alert(subject, body):
    """Best effort. Returns True if the mail was handed off successfully."""
    host = os.environ.get("SMTP_HOST")
    user = os.environ.get("SMTP_USER")
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = os.environ.get("SMTP_FROM") or user or f"liveness-runner@{socket.gethostname()}"
    msg["To"] = ALERT_EMAIL_TO
    if ALERT_EMAIL_CC:
        msg["Cc"] = ALERT_EMAIL_CC
    msg.set_content(body)
    try:
        if host:
            port = int(os.environ.get("SMTP_PORT", "587"))
            if port == 465:
                smtp = smtplib.SMTP_SSL(host, port, timeout=30)
            else:
                smtp = smtplib.SMTP(host, port, timeout=30)
                smtp.ehlo()
                if smtp.has_extn("starttls"):
                    smtp.starttls()
                    smtp.ehlo()
            with smtp:
                if user:
                    smtp.login(user, os.environ.get("SMTP_PASS", ""))
                smtp.send_message(msg)
        else:
            sm = shutil.which("sendmail") or "/usr/sbin/sendmail"
            if not os.path.exists(sm):
                log.error("ALERT NOT SENT: set SMTP_HOST (or install sendmail) to enable email alerts.")
                return False
            subprocess.run([sm, "-t", "-oi"], input=msg.as_bytes(), timeout=30, check=True)
        log.info("Alert email sent to %s", ALERT_RECIPIENTS)
        return True
    except Exception as e:
        log.error("ALERT NOT SENT to %s: %s: %s", ALERT_RECIPIENTS, type(e).__name__, e)
        return False


def run_header_lines():
    return [
        f"Host:       {socket.gethostname()}",
        f"Input:      {RUN_INFO.get('input', '?')}",
        f"Output dir: {RUN_INFO.get('out', '?')}",
        f"Run id:     {RUN_INFO.get('run_id', '?')}",
        f"Time:       {now_iso()}",
    ]


def summary_lines(counts, total_rows, run_done, run_by_status, codes, elapsed):
    lines = [f"Batch totals ({sum(counts.values())} of {total_rows} rows recorded, latest status per row):"]
    for k, v in sorted(counts.items(), key=lambda kv: -kv[1]):
        lines.append(f"  {k:<20} {v:>7}  ({100.0 * v / max(sum(counts.values()), 1):.1f}%)")
    lines += [
        "",
        f"This run: {run_done} rows in {fmt_dur(elapsed)}  {dict(run_by_status)}",
        f"API HTTP codes this run: {dict(codes)}",
    ]
    return lines


# --------------------------------------------------------------------------- writer / progress

def writer_loop(ctx):
    try:
        _writer_body(ctx)
    except BaseException as e:
        ctx.writer_error = f"{type(e).__name__}: {e}"
        ctx.stop.set()
        log.critical("WRITER FAILED — stopping the run: %s", ctx.writer_error)
        done, by_status, _, _ = ctx.stats.snapshot()
        send_alert(
            f"[liveness-runner] STOPPED: cannot write results ({ctx.args.input})",
            "\n".join([
                "The liveness batch run stopped because results could not be written.",
                "",
                f"Error:      {ctx.writer_error}",
                f"Host:       {socket.gethostname()}",
                f"Input:      {Path(ctx.args.input).resolve()}",
                f"Output dir: {ctx.out.resolve()}",
                f"Run id:     {ctx.run_id}",
                f"Time:       {now_iso()}",
                f"Rows written this run before failure: {done} ({by_status})",
                "",
                "No API calls are being made now. Fix the cause (e.g. free disk space), then",
                "resume with the same command; already-written rows will be skipped.",
            ]),
        )


def _writer_body(ctx):
    rp, fp = ctx.out / "results.csv", ctx.out / "failures_log.csv"
    r_new = not rp.exists() or rp.stat().st_size == 0
    f_new = not fp.exists() or fp.stat().st_size == 0
    with open(rp, "a", newline="", encoding="utf-8") as rf, open(fp, "a", newline="", encoding="utf-8") as ff:
        rw = csv.DictWriter(rf, fieldnames=RESULT_COLS, extrasaction="ignore")
        fw = csv.DictWriter(ff, fieldnames=FAILURE_COLS, extrasaction="ignore")
        if r_new:
            rw.writeheader()
        if f_new:
            fw.writeheader()
        n = 0
        while True:
            res = ctx.result_q.get()
            if res is None:
                break
            rw.writerow({k: ("" if res.get(k) is None else res.get(k)) for k in RESULT_COLS})
            rf.flush()
            if res["status"] != SUCCESS:
                fw.writerow({**res, "image": res.get("_image", "")})
                ff.flush()
                log.info("row %s rider=%s -> %s: %s", res["row_number"], res["riderId"],
                         res["status"], str(res.get("error_message", ""))[:200])
            ctx.stats.record_result(res["status"])
            n += 1
            if n % 50 == 0:
                os.fsync(rf.fileno())
                os.fsync(ff.fileno())
        os.fsync(rf.fileno())
        os.fsync(ff.fileno())


def progress_loop(ctx, every):
    t0 = time.monotonic()
    while not ctx.progress_stop.wait(every):
        done, by_status, calls_60s, _ = ctx.stats.snapshot()
        remaining = ctx.stats.total - done
        target = ctx.limiter.rpm
        eta = remaining * 60.0 / max(target, 1)
        fails = {k: v for k, v in by_status.items() if k != SUCCESS}
        log.info("[progress] %d/%d (%.1f%%) | API calls last 60s: %d (target %d RPM) | ok %d | fail %s | elapsed %s | ETA ~%s",
                 done, ctx.stats.total, 100.0 * done / max(ctx.stats.total, 1), calls_60s, target,
                 by_status.get(SUCCESS, 0), fails or 0, fmt_dur(time.monotonic() - t0), fmt_dur(eta))


def finalize(out):
    """Rebuild results_latest.csv and failures.csv from the append-only logs."""
    rp = out / "results.csv"
    if not rp.exists():
        return Counter()
    latest = {}
    with open(rp, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("row_uid"):
                latest[row["row_uid"]] = row
    rows = sorted(latest.values(), key=lambda r: int(r.get("row_number") or 0))
    with open(out / "results_latest.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RESULT_COLS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    images = {}
    fp = out / "failures_log.csv"
    if fp.exists():
        with open(fp, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                images[row["row_uid"]] = row.get("image", "")
    with open(out / "failures.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FAILURE_COLS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            if r["status"] != SUCCESS:
                w.writerow({**r, "image": images.get(r["row_uid"], "")})
    return Counter(r["status"] for r in rows)


# --------------------------------------------------------------------------- main

class Ctx:
    pass


def confirm(prompt, assume_yes):
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        log.error("%s — not running interactively; pass --yes to proceed anyway.", prompt)
        return False
    return input(f"{prompt} Continue anyway? [y/N] ").strip().lower() in ("y", "yes")


def parse_args():
    p = argparse.ArgumentParser(description="Batch runner for HyperVerge checkLiveness",
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--input", required="--test-alert" not in sys.argv, help="CSV with riderId,image (optionally row_uid,row_number)")
    p.add_argument("--out", required="--test-alert" not in sys.argv, help="output directory for this batch")
    p.add_argument("--batch-name", help="defaults to input file name; part of row_uid")
    p.add_argument("--api-url", default=DEFAULT_API_URL)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--start-rpm", type=int, default=20)
    p.add_argument("--step-rpm", type=int, default=15)
    p.add_argument("--step-interval", type=float, default=5.0, help="minutes per ramp step")
    p.add_argument("--peak-rpm", type=int, default=98)
    p.add_argument("--step-down-on-429", action="store_true", help="drop one ramp step on 429 and hold")
    p.add_argument("--max-attempts", type=int, default=6, help="API attempts per row")
    p.add_argument("--backoff-base", type=float, default=2.0, help="seconds")
    p.add_argument("--backoff-cap", type=float, default=60.0, help="seconds")
    p.add_argument("--api-timeout", type=float, default=60.0, help="read timeout, seconds")
    p.add_argument("--download-retries", type=int, default=3)
    p.add_argument("--download-timeout", type=float, default=30.0)
    p.add_argument("--max-image-mb", type=float, default=10.0)
    p.add_argument("--retry-failed", action="store_true", help="also re-run rows whose latest status is retriable")
    p.add_argument("--limit", type=int, help="process at most N pending rows (test runs)")
    p.add_argument("--progress-every", type=float, default=30.0, help="seconds")
    p.add_argument("--dry-run", action="store_true", help="pre-flight checks only, no API calls")
    p.add_argument("--skip-preflight", action="store_true")
    p.add_argument("--force", action="store_true", help="resume even if input file changed")
    p.add_argument("--yes", action="store_true", help="answer yes to warnings")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--test-alert", action="store_true", help="send a test alert email and exit")
    args, _ = p.parse_known_args()
    if args.test_alert:
        return args
    return p.parse_args()


def setup_logging(out, verbose):
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")
    log.setLevel(logging.DEBUG if verbose else logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    fh = logging.FileHandler(out / "run.log", encoding="utf-8")
    fh.setFormatter(fmt)
    log.addHandler(sh)
    log.addHandler(fh)


def main():
    a = parse_args()
    if a.test_alert:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
        log.info("Sending test alert to %s via %s", ALERT_RECIPIENTS, alert_channel() or "nothing configured")
        ok = send_alert("[liveness-runner] test alert",
                        f"Test alert from liveness_runner.py on {socket.gethostname()} at {now_iso()}.")
        return 0 if ok else 1
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    setup_logging(out, a.verbose)
    if a.peak_rpm < a.start_rpm or a.start_rpm <= 0 or a.step_rpm <= 0:
        raise SystemExit("Invalid ramp: need 0 < start-rpm <= peak-rpm and step-rpm > 0")

    app_id, app_key = os.environ.get("HV_APP_ID", ""), os.environ.get("HV_APP_KEY", "")
    if not (app_id and app_key) and not a.dry_run:
        raise SystemExit("Set HV_APP_ID and HV_APP_KEY environment variables.")

    free_mb = shutil.disk_usage(out).free / 1048576
    if free_mb < MIN_FREE_DISK_MB:
        raise SystemExit(f"Only {free_mb:.0f} MB free in {out} (need {MIN_FREE_DISK_MB} MB). Free some space first.")
    channel = alert_channel()
    if channel:
        log.info("alerts: %s -> %s (check with --test-alert)", channel, ALERT_RECIPIENTS)
    else:
        log.warning("alerts: no SMTP_HOST and no sendmail found — a writer failure will stop the run "
                    "but NO email will be sent.")

    inp = Path(a.input)
    batch_name = a.batch_name or inp.stem
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    RUN_INFO.update(input=str(inp.resolve()), out=str(out.resolve()), run_id=run_id,
                    batch=batch_name, cmd=" ".join(sys.argv))
    log.info("=== run %s | input %s | out %s | batch '%s' ===", run_id, inp, out, batch_name)
    if not HAVE_PIL:
        log.warning("Pillow not installed — only file-signature checks for corrupt images (pip install pillow).")

    rows, explicit = read_input(inp, batch_name)

    # input fingerprint guard (row_uid depends on row positions)
    mpath = out / "manifest.json"
    if not explicit:
        digest = file_sha256(inp)
        if mpath.exists():
            m = json.loads(mpath.read_text())
            if m.get("input_sha256") != digest or m.get("batch_name") != batch_name:
                msg = (f"Input file differs from the one this output folder was started with "
                       f"({m.get('input_path')}). Row positions may have shifted, so resume would be unsafe.")
                if not a.force:
                    raise SystemExit(msg + " Use a new --out folder, or --force if you are sure.")
                log.warning(msg + " Continuing because --force was given.")
        else:
            mpath.write_text(json.dumps({"input_path": str(inp.resolve()), "input_sha256": digest,
                                         "batch_name": batch_name, "rows": len(rows),
                                         "created": now_iso()}, indent=2))

    rpath = out / "results.csv"
    repair_partial_line(rpath)
    repair_partial_line(out / "failures_log.csv")
    latest = load_latest(rpath)
    retry_mode = a.retry_failed or explicit
    if explicit:
        pending = [r for r in rows if latest.get(r["row_uid"]) != SUCCESS]
    elif retry_mode:
        pending = [r for r in rows if r["row_uid"] not in latest or latest[r["row_uid"]] in RETRIABLE]
    else:
        pending = [r for r in rows if r["row_uid"] not in latest]
    if a.limit:
        pending = pending[: a.limit]

    dup = sum(c - 1 for c in Counter(r["riderId"] for r in rows).values() if c > 1)
    log.info("rows in input: %d | already recorded: %d | to process now: %d | duplicate riderId rows: %d%s",
             len(rows), len(latest), len(pending), dup, " | mode: retry" if retry_mode else "")
    if not pending:
        counts = finalize(out)
        log.info("Nothing to do. Latest status counts: %s", dict(counts))
        return 0

    stop = threading.Event()
    limiter = RampLimiter(a.start_rpm, a.step_rpm, a.step_interval * 60, a.peak_rpm, a.step_down_on_429, stop)
    proj = projected_seconds(len(pending), limiter.levels, a.step_interval * 60) * 1.05
    finish = time.time() + proj
    log.info("ramp: %s RPM, %.1f min per step | projected duration ~%s (finish ~%s)",
             " -> ".join(map(str, limiter.levels)), a.step_interval, fmt_dur(proj), fmt_ts(finish))

    # ---- pre-flight
    if not a.skip_preflight:
        exps = [(url_expiry(r["image"]), r) for r in pending if r["image"]]
        known = [e for e, _ in exps if e is not None]
        already = sum(1 for e in known if e <= time.time())
        if known:
            earliest = min(known)
            log.info("presigned URLs: earliest expiry %s (%s from now); %d already expired",
                     fmt_ts(earliest), fmt_dur(earliest - time.time()), already)
            if earliest < finish:
                at_risk = sum(1 for e in known if e < finish)
                if not confirm(f"WARNING: projected finish {fmt_ts(finish)} is after the earliest URL expiry; "
                               f"~{at_risk} rows may expire before they are processed.", a.yes):
                    return 2
        else:
            log.info("presigned URLs: expiry not readable from URL parameters — skipping expiry check")
        sample = [r for e, r in exps if e is None or e > time.time()][:3]
        if sample:
            s = requests.Session()
            c = Ctx()
            c.args, c.stop = a, stop
            ok = 0
            for r in sample:
                try:
                    data = download(s, r["image"], c)
                    bad, msg, _ = validate_image(data, a.max_image_mb * 1024 * 1024)
                    ok += bad is None
                    log.info("pre-flight sample row %d: %s", r["row_number"], f"{bad}: {msg}" if bad else "OK")
                except (DownloadError, Aborted) as e:
                    log.warning("pre-flight sample row %d: %s", r["row_number"], e)
            if ok == 0 and not confirm("WARNING: none of the sample images could be downloaded/decoded.", a.yes):
                return 2
    if a.dry_run:
        log.info("Dry run complete — no API calls made.")
        return 0

    ctx = Ctx()
    ctx.args, ctx.out, ctx.run_id, ctx.stop = a, out, run_id, stop
    ctx.app_id, ctx.app_key = app_id, app_key
    ctx.limiter = limiter
    ctx.stats = Stats(len(pending))
    ctx.work_q = queue.Queue(maxsize=a.workers * 2)
    ctx.result_q = queue.Queue()
    ctx.progress_stop = threading.Event()
    ctx.writer_error = None

    interrupts = {"n": 0, "sig": None}

    def on_signal(signum, _frame):
        interrupts["n"] += 1
        if interrupts["sig"] is None:
            interrupts["sig"] = signum
        if interrupts["n"] == 1:
            log.warning("Signal %s received — finishing in-flight calls and saving. Press Ctrl-C again to force quit.", signum)
            stop.set()
        else:
            log.error("Forced exit. Rows in flight will be re-run on resume.")
            os._exit(130)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, on_signal)

    writer = threading.Thread(target=writer_loop, args=(ctx,), name="writer", daemon=True)
    workers = [threading.Thread(target=worker_loop, args=(ctx,), name=f"w{i}", daemon=True) for i in range(a.workers)]
    prog = threading.Thread(target=progress_loop, args=(ctx, a.progress_every), name="progress", daemon=True)
    writer.start()
    for w in workers:
        w.start()
    prog.start()

    t0 = time.monotonic()
    for r in pending:
        while not stop.is_set():
            try:
                ctx.work_q.put(r, timeout=0.5)
                break
            except queue.Full:
                pass
        if stop.is_set():
            break
    for _ in workers:
        ctx.work_q.put(None)
    for w in workers:
        while w.is_alive():
            w.join(0.5)
    ctx.result_q.put(None)
    while writer.is_alive():
        writer.join(0.5)
    ctx.progress_stop.set()

    done, by_status, _, codes = ctx.stats.snapshot()
    try:
        counts = finalize(out)
    except Exception as e:
        log.error("Could not rebuild results_latest.csv / failures.csv: %s", e)
        counts = Counter()
    log.info("=== run %s %s in %s | processed this run: %d | this run: %s | API HTTP codes: %s ===",
             run_id, "INTERRUPTED" if stop.is_set() else "finished", fmt_dur(time.monotonic() - t0),
             done, by_status, codes)
    log.info("Batch totals (latest status per row, %d rows recorded%s): %s", sum(counts.values()),
             "" if explicit else f" of {len(rows)}", dict(counts))
    elapsed = time.monotonic() - t0
    total_rows = sum(counts.values()) if explicit else len(rows)
    summ = summary_lines(counts, total_rows, done, by_status, codes, elapsed)
    if ctx.writer_error:  # writer already sent its own alert
        log.critical("Run stopped due to writer failure: %s. Fix it, then resume with the same command.",
                     ctx.writer_error)
        return 3
    if stop.is_set():
        sig = interrupts["sig"]
        if sig is not None:
            name = signal.Signals(sig).name
            if sig == signal.SIGINT:
                subject = f"[liveness-runner] STOPPED MANUALLY (Ctrl-C): {batch_name}"
                first = "The batch run was stopped manually with Ctrl-C."
            else:
                why = ("the SSH session ended while the script was not inside tmux" if name == "SIGHUP"
                       else "the process was asked to terminate (shutdown, kill, OOM handling, etc.)")
                subject = f"[liveness-runner] STOPPED ({name}): {batch_name}"
                first = f"The batch run stopped after receiving {name} — likely {why}."
            send_alert(subject,
                       "\n".join([first,
                                   "In-flight rows were saved; nothing was lost.", ""]
                                  + run_header_lines() + [""] + summ
                                  + ["", "Resume with the same command:", f"  {RUN_INFO['cmd']}"]))
        log.info("Resume with the same command.")
        return 130
    pending_left = total_rows - sum(counts.values())
    retriable = sum(v for k, v in counts.items() if k in RETRIABLE)
    next_steps = []
    if retriable:
        next_steps.append(f"- {retriable} rows have retriable statuses; see failures.csv (error_message) "
                          f"and consider --retry-failed.")
    if pending_left > 0:
        next_steps.append(f"- {pending_left} rows not yet recorded (e.g. --limit was used).")
    send_alert(f"[liveness-runner] FINISHED: {batch_name} — "
               f"{counts.get(SUCCESS, 0)}/{total_rows} success",
               "\n".join(["The batch run finished.", ""] + run_header_lines() + [""] + summ
                          + ["", "Results: results_latest.csv (one row per input row), failures.csv"]
                          + (["", "Next steps:"] + next_steps if next_steps else [])))
    return 0


def run_with_crash_alert():
    try:
        return main()
    except SystemExit:
        raise
    except KeyboardInterrupt:  # Ctrl-C before processing started (e.g. during pre-flight)
        log.warning("Stopped with Ctrl-C before processing started.")
        if RUN_INFO.get("run_id"):
            send_alert(f"[liveness-runner] STOPPED MANUALLY (Ctrl-C): {RUN_INFO.get('batch', '')}",
                       "\n".join(["The batch run was stopped manually with Ctrl-C during startup/pre-flight,",
                                   "before any rows were processed in this run.", ""] + run_header_lines()
                                  + ["", "Start it with:", f"  {RUN_INFO.get('cmd', '')}"]))
        return 130
    except BaseException as e:
        import traceback
        tb = traceback.format_exc()
        log.critical("Unhandled error — run stopped:\n%s", tb)
        if RUN_INFO.get("run_id"):
            send_alert(f"[liveness-runner] CRASHED: {RUN_INFO.get('batch', '')} — {type(e).__name__}",
                       "\n".join(["The batch run stopped because of an unexpected error.",
                                   "Rows already written are safe; resume with the same command after fixing the cause.",
                                   ""] + run_header_lines()
                                  + ["", f"Command: {RUN_INFO.get('cmd', '')}", "", tb]))
        return 1


if __name__ == "__main__":
    sys.exit(run_with_crash_alert())
