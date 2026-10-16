# liveness_runner.py

Batch runner for HyperVerge's **checkLiveness** API. It reads a CSV of rider selfies (S3 presigned URLs), sends each image to the API at a controlled, ramped rate, and records every response field in a CSV. It is built to run unattended on a VM for hours: it survives crashes, retries transient failures, and emails you when a batch finishes or stops.

- [How it works](#how-it-works)
- [Setup (one time)](#setup-one-time)
- [Input format](#input-format)
- [Running a batch](#running-a-batch)
- [Rate limiting and ramp-up](#rate-limiting-and-ramp-up)
- [Output files](#output-files)
- [Row statuses](#row-statuses)
- [Resume and retries](#resume-and-retries)
- [Email alerts](#email-alerts)
- [CLI reference](#cli-reference)
- [Exit codes](#exit-codes)
- [Troubleshooting](#troubleshooting)
- [Operational notes](#operational-notes)

---

## How it works

```
input.csv ──► Reader ──bounded queue──► 8 worker threads ──► result queue ──► Writer thread
 (skips rows                              │                                    │
  already done)                           ├─ check URL expiry (from URL params) ├─ results.csv
                                          ├─ download image from S3 (in memory) ├─ failures_log.csv
                                          ├─ validate image (format + decode)   └─ run.log
                                          ├─ wait for a rate-limiter slot
                                          └─ POST checkLiveness (retry + backoff)
```

- **Images are never written to disk.** Each image is downloaded into memory and sent as the multipart `image` field.
- **`transactionId` = `riderId`** on every call. The API's `requestId` (unique per call) is recorded so any single call can be traced.
- **One writer thread** owns all output files, so concurrent workers can never corrupt a CSV row. Every row is flushed to disk as soon as it is written.
- **`results.csv` is the checkpoint.** On restart, rows already in it are skipped.
- **Each input row gets a stable `row_uid`** (UUIDv5 of batch name + row number + riderId). Duplicate riderIds are processed as separate rows, and the same row always gets the same `row_uid` across restarts.

Request form fields sent with every call (constants at the top of the script):

| Field | Value |
|---|---|
| `returnCroppedImageURL` | `no` |
| `qualityChecks.eyesClosed` | `yes` |

---

## Setup (one time)

### 1. Upload the script and CSVs to the VM

From your laptop:

```bash
ssh -J jump@13.207.11.237 ubuntu@15.206.89.28 'mkdir -p ~/liveness'
scp -J jump@13.207.11.237 liveness_runner.py batch_0*.csv ubuntu@15.206.89.28:~/liveness/
```

Optional: add an alias in `~/.ssh/config` on your laptop to shorten every command:

```
Host liveness-vm
    HostName 15.206.89.28
    User ubuntu
    ProxyJump jump@13.207.11.237
```

Then: `ssh liveness-vm`, `scp liveness_runner.py liveness-vm:~/liveness/`.

### 2. Install dependencies (on the VM)

Requires Python 3.8+.

```bash
cd ~/liveness
sudo apt update && sudo apt install -y python3-venv   # or python3.X-venv matching your Python
python3 -m venv venv && source venv/bin/activate
pip install requests pillow
```

No sudo? Use `pip3 install --user --break-system-packages requests pillow` instead.

`pillow` is optional but recommended. Without it, corrupt-image detection falls back to file-signature checks and won't catch truncated JPEGs.

### 3. Create a Gmail app password (for alerts)

1. Turn on 2-Step Verification: myaccount.google.com → Security → 2-Step Verification.
2. Go to **myaccount.google.com/apppasswords**, name it `liveness-runner`, click **Create**.
3. Copy the 16-character password. Google shows it only once.

Revoke it from the same page after all batches are done.

### 4. Create the environment file

Use an editor, not `echo` or a heredoc, so the credentials don't end up in shell history:

```bash
nano ~/.liveness_env
```

```bash
source ~/liveness/venv/bin/activate
export HV_APP_ID='your-app-id'
export HV_APP_KEY='your-app-key'
export SMTP_HOST=smtp.gmail.com
export SMTP_PORT=587
export SMTP_USER=s.aravindaan@gmail.com
export SMTP_PASS='abcd efgh ijkl mnop'
```

Save with `Ctrl+O`, `Enter`, `Ctrl+X`. Then:

```bash
chmod 600 ~/.liveness_env
source ~/.liveness_env
python3 ~/liveness/liveness_runner.py --test-alert
```

Confirm the test email arrived in **all three** inboxes (check spam on the HyperVerge addresses the first time and mark it "not spam").

---

## Input format

A CSV with a header row containing at least:

| Column | Description |
|---|---|
| `riderId` | String. Used as the API `transactionId`. Duplicates are allowed. |
| `image` | S3 presigned URL (SigV4 or SigV2). |

```csv
riderId,image
RDR12345,https://bucket.s3.ap-south-1.amazonaws.com/selfies/a.jpg?X-Amz-Algorithm=...&X-Amz-Date=20261016T100000Z&X-Amz-Expires=21600&...
RDR12346,https://...
```

- Extra columns are ignored.
- A UTF-8 BOM (as Excel writes) is handled.
- **Do not edit an input CSV once its batch has started.** `row_uid` depends on row position; the script refuses to resume if the file's hash has changed (see [`--force`](#cli-reference)).
- A CSV that also has `row_uid` (and `row_number`) columns, such as an edited `failures.csv`, is treated as a **retry file**: the given `row_uid`s are used as-is. See [Resume and retries](#resume-and-retries).

---

## Running a batch

### Smoke test (once, before batch 1)

```bash
tmux new -s smoke
source ~/.liveness_env && cd ~/liveness
python3 liveness_runner.py --input batch_01.csv --out runs/smoke --limit 50 --start-rpm 20 --peak-rpm 20
```

Check `runs/smoke/results_latest.csv` and that the FINISHED email reached all three inboxes. The smoke test uses its own output folder, so it doesn't count toward batch 1.

### Each batch

Change the batch number in both places (`01` → `02` … `05`):

```bash
tmux new -s batch_01
source ~/.liveness_env && cd ~/liveness
python3 liveness_runner.py --input batch_01.csv --out runs/batch_01 --step-down-on-429
```

Startup prints the ramp plan, projected finish time, earliest URL expiry and a pre-flight check of 3 sample images. If the projected finish is later than the earliest URL expiry, it asks for confirmation before starting.

### While it runs

| What | How |
|---|---|
| Detach and leave it running | `Ctrl+b` then `d` |
| Reattach | `tmux attach -t batch_01` |
| Follow the log from another session | `tail -f ~/liveness/runs/batch_01/run.log` |
| Stop cleanly | `Ctrl+C` **once**, then wait for it to save |
| Force quit (no save of in-flight rows, no email) | `Ctrl+C` twice |

A progress line is logged every 30 s:

```
[progress] 4210/20000 (21.1%) | API calls last 60s: 97 (target 98 RPM) | ok 4150 | fail {...} | elapsed 52m10s | ETA ~2h41m
```

**Always run inside tmux.** If SSH drops while the script runs outside tmux, it receives SIGHUP, saves, stops and emails you. Inside tmux it just keeps going.

### Pull results to your laptop

```bash
scp -J jump@13.207.11.237 -r ubuntu@15.206.89.28:~/liveness/runs/batch_01 .
```

---

## Rate limiting and ramp-up

The API limit for this appId is 100 RPM. The script ramps up in fixed steps and then holds the peak:

| Setting | Default |
|---|---|
| Start | 20 RPM |
| Step | +15 RPM every 5 minutes |
| Peak | 98 RPM, held until the batch ends |
| Ramp | 20 → 35 → 50 → 65 → 80 → 95 → 98 (peak after 30 min) |
| Time per 20k batch | ≈ 3 h 36 min |

- **Evenly spaced, no bursts.** At 98 RPM a slot opens every ~612 ms. Idle time never accumulates credit, so a pause can't turn into a burst afterwards.
- **Retries count toward the RPM.** A retried call waits for a slot like any other call, so total traffic stays under the peak even while the API is throttling.
- **The ramp restarts on every launch**, including resumes after a crash.
- **`--step-down-on-429`** (recommended): on an HTTP 429 the limiter drops one step and holds for one interval before climbing again. It steps down at most once per interval.
- **Workers vs RPM.** The 8 workers let image downloads happen ahead of API slots. Adding workers does not increase throughput; the RPM cap does.

---

## Output files

All in the `--out` folder (e.g. `runs/batch_01/`):

| File | Description |
|---|---|
| `results.csv` | Append-only log of every processed row. **This is the checkpoint.** A row retried later appears more than once; the latest wins. |
| `results_latest.csv` | One row per input row (latest attempt), sorted by row number. Rebuilt at the end of every run. **Use this for analysis.** |
| `failures.csv` | Rows whose latest status is not SUCCESS, with the image URL. Rebuilt at the end of every run. Usable as a retry input. |
| `failures_log.csv` | Append-only log of every failure, with image URL. |
| `run.log` | Full log (same as the console). |
| `manifest.json` | Input file path, SHA-256 and batch name, used to refuse unsafe resumes. |

### `results.csv` / `results_latest.csv` columns (50)

**Run and row bookkeeping**

| Column | Description |
|---|---|
| `run_id` | Timestamp ID of the run that produced the row |
| `row_uid` | Stable unique ID for the input row |
| `row_number` | 1-based position in the input CSV |
| `riderId` | From input |
| `transactionId` | Sent to the API (= riderId; falls back to row_uid if riderId is blank) |
| `status` | See [Row statuses](#row-statuses) |
| `http_status` | HTTP code of the final API attempt |
| `attempts` | Number of API attempts made |
| `api_latency_ms` | Latency of the final API attempt |
| `image_bytes` | Downloaded image size |
| `error_message` | Reason for any non-SUCCESS status |
| `started_at`, `finished_at` | ISO timestamps |

**API response fields**

| Column | JSON path |
|---|---|
| `api_status` | `status` |
| `api_statusCode` | `statusCode` |
| `requestId` | `metadata.requestId` |
| `response_transactionId` | `metadata.transactionId` |
| `liveFace_value`, `liveFace_confidence`, `liveFace_score` | `result.details.liveFace.*` |
| `qc_<check>_value`, `qc_<check>_score`, `qc_<check>_confidence` | `result.details.qualityChecks.<check>.*` for `multipleFaces`, `eyewear`, `readingGlasses`, `sunglasses`, `faceOccluded`, `headTurned`, `blur`, `eyesClosed`, `hat` |
| `summary_action` | `result.summary.action` |
| `summary_details` | `result.summary.details` as a JSON string (a variable-length list) |
| `raw_response` | Full response body. Any field not mapped above, such as a new quality check, is preserved here; the log warns once if one appears. |

---

## Row statuses

| Status | Meaning | Retried by `--retry-failed`? |
|---|---|---|
| `SUCCESS` | HTTP 200 and `status: "success"` | — |
| `API_REJECTED` | API processed the image and rejected it: HTTP 4xx other than 429 (e.g. "Face not detected"). **A valid model outcome, not a script failure.** | No |
| `API_ERROR` | Unexpected API response: 200 with non-success status, or an unexpected code | No |
| `RETRIES_EXHAUSTED` | 429 / 5xx / timeout / connection error on all attempts (default 6) | **Yes** |
| `URL_EMPTY` | `image` column blank | No |
| `URL_EXPIRED` | Presigned URL past its expiry (read from the URL, no download attempted), or S3 said "Request has expired" | Yes, but only helps with fresh URLs (see below) |
| `DOWNLOAD_FAILED` | S3 timeout / 5xx after 3 tries, **or** S3 404 / 403 / 400 | Yes, but only timeouts and 5xx will succeed |
| `IMAGE_CORRUPT` | Empty file, unrecognisable bytes (e.g. an XML/HTML error page), or a JPEG/PNG/WEBP that fails to decode (truncated, damaged) | No |
| `UNSUPPORTED_FORMAT` | A valid image of a type not sent to the API (HEIC/HEIF, AVIF, GIF, BMP, TIFF, PDF), or larger than `--max-image-mb` | No |

Accepted image formats: JPEG, PNG, WEBP. Retries happen inside a run, with exponential backoff and jitter (2 s base, 60 s cap, `Retry-After` honoured). Non-429 4xx responses are never retried, because retrying won't change them.

---

## Resume and retries

### After a crash or stop

Run the **exact same command** again. Rows already in `results.csv` are skipped, a half-written last line left by a crash is cleaned up automatically, and the ramp starts again from 20 RPM.

### Re-run transient failures

```bash
python3 liveness_runner.py --input batch_01.csv --out runs/batch_01 --retry-failed
```

Re-runs rows whose latest status is `RETRIES_EXHAUSTED`, `DOWNLOAD_FAILED` or `URL_EXPIRED`. Rows with missing files (404) or expired URLs will just fail again quickly, without using API calls. Check `error_message` in `failures.csv` first.

### Re-run rows with regenerated URLs

1. Copy `runs/batch_01/failures.csv`.
2. Replace the `image` column with fresh presigned URLs. **Keep `row_uid` and `row_number` unchanged.**
3. Feed it back into the **same** output folder:

```bash
python3 liveness_runner.py --input batch_01_fixed.csv --out runs/batch_01
```

Any row in the file that isn't already SUCCESS is processed, and `results_latest.csv` is updated with the new outcomes.

### URL expiry

Batch URLs expire 6 hours after they are generated, and a batch takes ~3.6 hours. **Start each batch soon after its URLs are generated.** At startup the script compares the projected finish time with the earliest expiry and warns before starting if they overlap.

---

## Email alerts

All alerts go **To** `s.aravindaan@gmail.com`, **CC** `aravindaan.s@hyperverge.co`, `eshaan.j@hyperverge.co`.

| Event | Subject | Contents |
|---|---|---|
| Batch finished | `FINISHED: batch_01 — 19850/20000 success` | Status breakdown with %, HTTP code counts, duration, output folder, next steps |
| Stopped with Ctrl-C | `STOPPED MANUALLY (Ctrl-C): batch_01` | Totals so far, resume command |
| SIGTERM (system or `kill`) | `STOPPED (SIGTERM): batch_01` | Totals so far, resume command |
| SSH dropped outside tmux | `STOPPED (SIGHUP): batch_01` | Totals so far, resume command |
| Writer failure (disk full etc.) | `STOPPED: cannot write results (...)` | Error, rows written; run stops immediately |
| Unexpected error | `CRASHED: batch_01 — <ErrorType>` | Full traceback, command |

Not covered: a double Ctrl-C (force quit), `kill -9`, or the VM losing power. The process can't send mail in those cases.

**Delivery.** SMTP is used when `SMTP_HOST` is set; otherwise the local `sendmail` binary is used if present. If neither is available, the startup log warns that alerts won't be delivered. The run itself still behaves the same.

**Changing recipients** without editing the script:

```bash
export ALERT_EMAIL_TO="someone@example.com"
export ALERT_EMAIL_CC="a@hyperverge.co, b@hyperverge.co"   # empty string = no CC
```

Optional `SMTP_FROM` overrides the sender address. Port 465 uses SSL; other ports use STARTTLS when the server offers it.

---

## CLI reference

| Flag | Default | Description |
|---|---|---|
| `--input` | required | Input CSV |
| `--out` | required | Output folder for this batch |
| `--batch-name` | input file name | Part of `row_uid`; keep it the same across resumes |
| `--api-url` | `https://global.rl.hyperverge.co/v1/checkLiveness` | API endpoint |
| `--workers` | 8 | Worker threads |
| `--start-rpm` | 20 | Ramp starting rate |
| `--step-rpm` | 15 | RPM added per step |
| `--step-interval` | 5 | Minutes per ramp step |
| `--peak-rpm` | 98 | Ramp ceiling, held until the end |
| `--step-down-on-429` | off | Drop one ramp step on 429 and hold |
| `--max-attempts` | 6 | API attempts per row |
| `--backoff-base` | 2 | Backoff base, seconds |
| `--backoff-cap` | 60 | Backoff cap, seconds (also caps `Retry-After`) |
| `--api-timeout` | 60 | API read timeout, seconds |
| `--download-retries` | 3 | S3 download attempts |
| `--download-timeout` | 30 | S3 read timeout, seconds |
| `--max-image-mb` | 10 | Larger images → `UNSUPPORTED_FORMAT` |
| `--retry-failed` | off | Also re-run rows whose latest status is retriable |
| `--limit N` | — | Process at most N pending rows (smoke tests) |
| `--progress-every` | 30 | Seconds between progress lines |
| `--dry-run` | off | Run startup and pre-flight checks only; no API calls |
| `--skip-preflight` | off | Skip the URL-expiry and sample-image checks |
| `--force` | off | Resume even if the input file changed (unsafe) |
| `--yes` | off | Auto-confirm warnings (required when not running interactively) |
| `--verbose` | off | Debug logging, including each failed attempt |
| `--test-alert` | — | Send a test email and exit |

Environment variables: `HV_APP_ID`, `HV_APP_KEY` (required), `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `SMTP_FROM`, `ALERT_EMAIL_TO`, `ALERT_EMAIL_CC`.

---

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Finished (or nothing left to do) |
| 1 | Crashed on an unexpected error (alert sent), or `--test-alert` failed |
| 2 | Aborted at pre-flight (warning not confirmed) |
| 3 | Writer failure; results could not be written (alert sent) |
| 130 | Stopped by Ctrl-C / SIGTERM / SIGHUP; resume with the same command |

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Set HV_APP_ID and HV_APP_KEY environment variables.` | Run `source ~/.liveness_env` in this shell. |
| `ModuleNotFoundError: requests` | venv not active: `source ~/liveness/venv/bin/activate`, or run `~/liveness/venv/bin/python`. |
| `ensurepip is not available` when creating the venv | `sudo apt install python3.X-venv`, then `rm -rf venv` and recreate. |
| `Input file differs from the one this output folder was started with` | The CSV changed after the batch started. Use the original file, or a new `--out` folder. |
| `Only N MB free` | Free at least 500 MB of disk. |
| `alerts: no SMTP_HOST and no sendmail found` | SMTP env vars not loaded; alerts won't be sent. |
| `ALERT NOT SENT ... SMTPAuthenticationError` | Wrong app password, or 2-Step Verification is off. |
| Many `URL_EXPIRED` near the end of a batch | URLs were generated too long before the run started. Regenerate them and use the retry-file flow. |
| `429` codes in the progress line | Expected occasionally. With `--step-down-on-429` the ramp backs off automatically. If 429s persist, lower `--peak-rpm`. |
| `Response has quality checks not mapped to columns` | The API added a new check. Its data is in `raw_response`; add it to `QUALITY_CHECKS` in the script for future batches. |

---

## Operational notes

- **One batch at a time.** The RPM limit is per process; two concurrent runs would share the 100 RPM.
- **Don't change the script mid-batch.** `results.csv` is append-only; a version with different columns would misalign rows. Upgrade between batches.
- **Duplicate API calls are possible but rare.** A call that times out after the server processed it, or a crash mid-call, is sent again on retry or resume. `requestId` identifies duplicates if billing reconciliation is needed.
- **The output folder contains live presigned URLs** (`failures.csv`, `failures_log.csv`) to rider selfies, valid until expiry. Keep `runs/` permission-restricted and delete those files once a batch is closed out.
- **Opening in Excel** can mangle long numeric riderIds (scientific notation, dropped leading zeros). Import with columns as text, or use pandas or Google Sheets.
- **Reading results in pandas:**

```python
import pandas as pd
df = pd.read_csv("runs/batch_01/results_latest.csv", dtype=str)
df["status"].value_counts()
df[df.status == "SUCCESS"]["liveFace_value"].value_counts()
```
