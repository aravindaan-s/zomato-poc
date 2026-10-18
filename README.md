# liveness_runner.py

Batch runner for HyperVerge's **checkLiveness** API. It reads a CSV of rider selfies (S3 presigned URLs), sends each image to the API at a controlled, ramped rate, records every response field in a CSV, and keeps a local copy of every image for review. It is built to run unattended on a VM for hours: it survives crashes, retries transient failures, and emails you when a batch finishes or stops.

- [How it works](#how-it-works)
- [Setup (one time)](#setup-one-time)
- [Input format](#input-format)
- [Running a batch](#running-a-batch)
- [Rate limiting and ramp-up](#rate-limiting-and-ramp-up)
- [Output files](#output-files)
- [Saved images](#saved-images)
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
                                          ├─ download image from S3             ├─ failures_log.csv
                                          ├─ save a copy to images/             └─ run.log
                                          ├─ validate image (format + decode)
                                          ├─ wait for a rate-limiter slot
                                          └─ POST checkLiveness (retry + backoff)
```

- **Every downloaded image is saved** to `images/` in the batch folder (see [Saved images](#saved-images)), then sent from memory as the multipart `image` field.
- **`transactionId` = `driver_id`** on every call. The API's `requestId` (unique per call) is recorded so any single call can be traced.
- **Every input column is copied as-is** into the results, next to the API response fields, so model predictions and liveness results sit side by side.
- **One writer thread** owns all output files, so concurrent workers can never corrupt a CSV row. Every row is flushed to disk as soon as it is written.
- **`results.csv` is the checkpoint.** On restart, rows already in it are skipped.
- **Each input row gets a stable `row_uid`** (UUIDv5 of batch name + row number + driver_id). Duplicate driver_ids are processed as separate rows, and the same row always gets the same `row_uid` across restarts.

Request form fields sent with every call (constants at the top of the script):

| Field | Value |
|---|---|
| `returnCroppedImageURL` | `no` |
| `qualityChecks.eyesClosed`, `.blur`, `.hat`, `.multipleFaces`, `.sunglasses`, `.readingGlasses`, `.headTurned`, `.eyewear`, `.occlusion` | `yes` |
| `preferences.returnScore`, `preferences.returnConfidence` | `yes` |

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

A CSV with a header row. Expected columns:

| Column | Used for |
|---|---|
| `driver_id` | **Required.** Sent as the API `transactionId`; also used in image filenames. Duplicates are allowed. |
| `presigned_url` | **Required.** S3 presigned URL (SigV4 or SigV2) the image is downloaded from. |
| `v7_prediction`, `v8_prediction`, `llm_prediction`, `hyperverge_prediction`, `selfie_url`, `created_at`, `selfies` | Not used by the script; copied as-is to the results. |

```csv
driver_id,v7_prediction,v8_prediction,llm_prediction,hyperverge_prediction,selfie_url,presigned_url,created_at,selfies
D10001,live,live,live,pass,s3://bucket/selfies/D10001.jpg,https://bucket.s3.ap-south-1.amazonaws.com/selfies/D10001.jpg?X-Amz-Algorithm=...&X-Amz-Date=20261017T100000Z&X-Amz-Expires=21600&...,2026-10-01 10:00:00,...
```

- **Every column in the input is copied unchanged** into the results CSV, in the same order, so any column (present or added later) flows through. Only `driver_id` and `presigned_url` are required.
- If the column names differ, point the script at them with `--id-col` and `--image-col`.
- If an input column has the same name as one of the script's own columns (e.g. `status`), it is written as `input_<name>`.
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
| `images/` | A copy of every downloaded image. See [Saved images](#saved-images). |

### `results.csv` / `results_latest.csv` columns (59 with the current input)

Order: bookkeeping → **all input columns as-is** → script fields → API response fields.

**Bookkeeping and input**

| Column | Description |
|---|---|
| `run_id` | Timestamp ID of the run that produced the row |
| `row_uid` | Stable unique ID for the input row |
| `row_number` | 1-based position in the input CSV |
| `driver_id` … `selfies` | Every input column, unchanged, in input order |

**Script fields**

| Column | Description |
|---|---|
| `transactionId` | Sent to the API (= driver_id; falls back to row_uid if driver_id is blank) |
| `status` | See [Row statuses](#row-statuses) |
| `http_status` | HTTP code of the final API attempt |
| `attempts` | Number of API attempts made |
| `api_latency_ms` | Latency of the final API attempt |
| `image_bytes` | Downloaded image size |
| `saved_image_path` | Path of the saved copy, relative to the batch folder (e.g. `images/001842_D10001.jpg`) |
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

## Saved images

Every image the script downloads is saved, whatever its outcome (SUCCESS, API_REJECTED, IMAGE_CORRUPT, …), in a flat folder:

```
runs/batch_01/images/
├── 000001_D10001.jpg
├── 000002_D10002.jpg
├── 000031_D10031.heic
└── 000034_D10034.bin
```

**Filename:** `<row_number>_<driver_id>.<ext>`

- `row_number` is zero-padded to 6 digits, so files sort in input order. It is unique within a batch, so duplicate driver_ids never overwrite each other, and it matches `row_number` in the CSV.
- Characters in `driver_id` that aren't filename-safe become `_`. A blank driver_id uses the first 8 characters of `row_uid` instead.
- `ext` comes from the file's actual content (`jpg`, `png`, `webp`, `heic`, `avif`, `gif`, `bmp`, `tif`, `pdf`), or `bin` if unrecognisable.
- Saved bytes are exactly what S3 returned; nothing is re-encoded.

**Rows with no image file:** `URL_EMPTY`, `URL_EXPIRED`, `DOWNLOAD_FAILED`, and empty (0-byte) downloads. Their `saved_image_path` is blank.

**Retries** overwrite the same file. Writes are atomic (written as `.part`, then renamed), so a crash never leaves a half-written image; stray `.part` files are removed on the next start.

**Finding images by outcome:** filter `results_latest.csv` on `status` (or `summary_action`, `liveFace_value`, …) and use `saved_image_path`:

```python
import pandas as pd, shutil, os
df = pd.read_csv("runs/batch_01/results_latest.csv", dtype=str)
os.makedirs("review/rejected", exist_ok=True)
for p in df[df.status == "API_REJECTED"].saved_image_path.dropna():
    shutil.copy(f"runs/batch_01/{p}", "review/rejected/")
```

**Disk space.** At ~300 KB per selfie, one 20k batch is about **6 GB**, and all five about **30 GB**. At startup the script estimates the space needed from the pre-flight sample images and asks for confirmation if there isn't enough. During the run it stops cleanly (and emails you) if free space drops below 200 MB, before the disk is actually full. The progress line shows free space. Check before batch 1 with `df -h ~`.

**Turning it off:** add `--no-save-images` to a run.

**These are rider selfies, and unlike presigned URLs they don't expire.** Keep `runs/` permission-restricted (`chmod -R go-rwx ~/liveness/runs`), copy off only what you need for review, and delete `images/` once a batch is closed out.

---

## Row statuses

| Status | Meaning | Retried by `--retry-failed`? |
|---|---|---|
| `SUCCESS` | HTTP 200 and `status: "success"` | — |
| `API_REJECTED` | API processed the image and rejected it: HTTP 4xx other than 429 (e.g. "Face not detected"). **A valid model outcome, not a script failure.** | No |
| `API_ERROR` | Unexpected API response: 200 with non-success status, or an unexpected code | No |
| `RETRIES_EXHAUSTED` | 429 / 5xx / timeout / connection error on all attempts (default 6) | **Yes** |
| `URL_EMPTY` | `presigned_url` blank | No |
| `URL_EXPIRED` | Presigned URL past its expiry (read from the URL, no download attempted), or S3 said "Request has expired" | Yes, but only helps with fresh URLs (see below) |
| `DOWNLOAD_FAILED` | S3 timeout / 5xx after 3 tries, **or** S3 404 / 403 / 400 | Yes, but only timeouts and 5xx will succeed |
| `IMAGE_CORRUPT` | Empty file, unrecognisable bytes (e.g. an XML/HTML error page), or a JPEG/PNG/WEBP that fails to decode (truncated, damaged) | No |
| `UNSUPPORTED_FORMAT` | A valid image of a type not sent to the API (HEIC/HEIF, AVIF, GIF, BMP, TIFF, PDF), or larger than `--max-image-mb` | No |

Accepted image formats: JPEG, PNG, WEBP. Retries happen inside a run, with exponential backoff and jitter (2 s base, 60 s cap, `Retry-After` honoured). Non-429 4xx responses are never retried, because retrying won't change them.

---

## Resume and retries

### After a crash or stop

Run the **exact same command** again. Rows already in `results.csv` are skipped, a half-written last line or image left by a crash is cleaned up automatically, and the ramp starts again from 20 RPM.

### Re-run transient failures

```bash
python3 liveness_runner.py --input batch_01.csv --out runs/batch_01 --retry-failed
```

Re-runs rows whose latest status is `RETRIES_EXHAUSTED`, `DOWNLOAD_FAILED` or `URL_EXPIRED`. Rows with missing files (404) or expired URLs will just fail again quickly, without using API calls. Check `error_message` in `failures.csv` first.

### Re-run rows with regenerated URLs

1. Copy `runs/batch_01/failures.csv`.
2. Replace the `presigned_url` column with fresh presigned URLs. **Keep `row_uid` and `row_number` unchanged**, and don't add or remove columns. `failures.csv` already contains every original input column, so the results stay complete.
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
| Disk failure: results or an image can't be written, or free space < 200 MB | `STOPPED: cannot write to disk: batch_01` | Error, free space, rows written; run stops immediately |
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
| `--id-col` | `driver_id` | Column sent as `transactionId` |
| `--image-col` | `presigned_url` | Column holding the presigned image URL |
| `--batch-name` | input file name | Part of `row_uid`; keep it the same across resumes |
| `--api-url` | `https://ind.idv.hyperverge.co/v1/checkLiveness` | API endpoint |
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
| `--max-image-mb` | 10 | Larger images → `UNSUPPORTED_FORMAT` (still saved) |
| `--no-save-images` | off | Don't save images to `images/` |
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
| 3 | Disk/write failure: results or images could not be written, or disk nearly full (alert sent) |
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
| `estimated space needed ~X GB but only Y GB free` | Free space, delete `images/` from finished batches, or run with `--no-save-images`. |
| `... was written by a different version of this script (columns differ)` | The output folder was created by an older version. Use a new `--out` folder. |
| `alerts: no SMTP_HOST and no sendmail found` | SMTP env vars not loaded; alerts won't be sent. |
| `ALERT NOT SENT ... SMTPAuthenticationError` | Wrong app password, or 2-Step Verification is off. |
| Many `URL_EXPIRED` near the end of a batch | URLs were generated too long before the run started. Regenerate them and use the retry-file flow. |
| `429` codes in the progress line | Expected occasionally. With `--step-down-on-429` the ramp backs off automatically. If 429s persist, lower `--peak-rpm`. |
| `Response has quality checks not mapped to columns` | The API added a new check. Its data is in `raw_response`; add it to `QUALITY_CHECKS` in the script for future batches. |

---

## Operational notes

- **One batch at a time.** The RPM limit is per process; two concurrent runs would share the 100 RPM.
- **Don't change the script mid-batch.** `results.csv` is append-only; the script refuses to append to a `results.csv` whose columns don't match its own. Upgrade between batches.
- **Duplicate API calls are possible but rare.** A call that times out after the server processed it, or a crash mid-call, is sent again on retry or resume. `requestId` identifies duplicates if billing reconciliation is needed.
- **The output folder contains rider selfies** (`images/`) and live presigned URLs (`failures.csv`, `failures_log.csv`). Keep `runs/` permission-restricted and delete both once a batch is closed out.
- **Opening in Excel** can mangle long numeric driver_ids (scientific notation, dropped leading zeros). Import with columns as text, or use pandas or Google Sheets.
- **Reading results in pandas:**

```python
import pandas as pd
df = pd.read_csv("runs/batch_01/results_latest.csv", dtype=str)
df["status"].value_counts()
df[df.status == "SUCCESS"]["liveFace_value"].value_counts()
# compare with the existing model predictions
pd.crosstab(df["v8_prediction"], df["liveFace_value"])
```
