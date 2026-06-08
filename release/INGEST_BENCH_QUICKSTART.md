# `eo-ingest-bench` Quickstart

Run an honest, client-side wall-clock comparison of all three EntropyOps
ingest paths (OTLP HTTP, OTLP gRPC, AgenticUDP) in under five minutes.

> **Read this first:** even with everything below set up correctly, this
> benchmark **cannot** today be read as a clean head-to-head ranking of the
> three transports. The HTTP traces handler runs the head/tail sampler and
> the k8s-attribute joiner inline; the gRPC and AgenticUDP handlers do not.
> The bench's footer reprints the full caveats — **read them before quoting
> any number**. The relevant section in `PROGRAM_SUMMARY.md` is "What this
> benchmark currently CANNOT tell you."

---

## 1. What you need on disk

> **Convention used by every command in this doc:**
> Windows = `C:\entropyops`. macOS / Linux = `~/entropyops-bench`.
> Every command in steps 2–8 starts with `Set-Location C:\entropyops`
> (Windows) or `cd ~/entropyops-bench` (mac/Linux). If you put the
> binaries somewhere else, you are on your own.

You need exactly two binaries in that directory: the **server** and the
**bench client**. They keep the platform suffix in the filename
(this catches almost everyone the first time — there is no plain
`entropyops-server.exe` or `eo-ingest-bench`):

| OS | Server binary | Bench binary |
|----|---------------|--------------|
| Windows x86_64       | `entropyops-windows-amd64.exe` | `eo-ingest-bench-windows-amd64.exe` |
| macOS Apple Silicon  | `entropyops-darwin-arm64`      | `eo-ingest-bench-darwin-arm64` |
| macOS Intel          | `entropyops-darwin-amd64`      | `eo-ingest-bench-darwin-amd64` |
| Linux x86_64         | `entropyops-linux-amd64`       | `eo-ingest-bench-linux-amd64` |
| Linux ARM64          | `entropyops-linux-arm64`       | `eo-ingest-bench-linux-arm64` |

### 1a. Already done this once? Skip to step 2.

If `Get-ChildItem C:\entropyops\entropyops-windows-amd64.exe,
C:\entropyops\eo-ingest-bench-windows-amd64.exe` shows two files, you
are set. Go to step 2. (mac/Linux: `ls -la
~/entropyops-bench/entropyops-* ~/entropyops-bench/eo-ingest-bench-*`.)

### 1b. First-time setup — Windows (three commands, no edits)

> **Use `tar -xf`, not `Expand-Archive`.** Windows PowerShell 5.1's
> `Expand-Archive` (which is what every default Windows host runs)
> silently truncates zips that came in with a Mark-of-the-Web stream
> (Downloads, RDP clipboard, network share copy) and is finicky about
> cross-platform zip metadata. `tar.exe` ships built-in on every Windows
> 10 build 17063+ (October 2018) and Just Works on these zips. The
> shipped `bench-vm\unzip.ps1` script prefers `tar.exe` automatically;
> use it if you'd rather not type these commands directly.

```powershell
# Make the working dir, drop the two zips into it from wherever the
# release is sitting (Downloads is just where browsers default — adjust
# if your release lives elsewhere).
New-Item -ItemType Directory -Force -Path C:\entropyops | Out-Null
Copy-Item "$env:USERPROFILE\Downloads\entropyops-windows-amd64.zip"      C:\entropyops\
Copy-Item "$env:USERPROFILE\Downloads\eo-ingest-bench-windows-amd64.zip" C:\entropyops\

# Strip Mark-of-the-Web on the local copies (no-op when not present).
Set-Location C:\entropyops
Get-ChildItem *.zip | Unblock-File

# Extract both in place using bsdtar (built-in tar.exe on Windows 10/11).
tar -xf .\entropyops-windows-amd64.zip      -C .
tar -xf .\eo-ingest-bench-windows-amd64.zip -C .

# Sanity — both files must be listed. If not, the zips weren't where
# you thought; see "Bootstrap when you have no idea where the zips are"
# at the bottom of this doc.
Get-ChildItem entropyops-windows-amd64.exe, eo-ingest-bench-windows-amd64.exe
```

If `tar` reports `command not found`, the host is older than Windows 10
1803. Fall back to `Expand-Archive`, but `Unblock-File` the zips first
or it will silently truncate:

```powershell
Get-ChildItem *.zip | Unblock-File
Expand-Archive -Path .\entropyops-windows-amd64.zip      -DestinationPath . -Force
Expand-Archive -Path .\eo-ingest-bench-windows-amd64.zip -DestinationPath . -Force
```

### 1c. First-time setup — macOS / Linux

```bash
# Auto-pick platform suffix
case "$(uname -sm)" in
    "Darwin arm64")  SUFFIX="darwin-arm64" ;;
    "Darwin x86_64") SUFFIX="darwin-amd64" ;;
    "Linux x86_64")  SUFFIX="linux-amd64"  ;;
    "Linux aarch64"|"Linux arm64") SUFFIX="linux-arm64" ;;
esac

mkdir -p ~/entropyops-bench && cd ~/entropyops-bench
cp ~/Downloads/entropyops-${SUFFIX}.zip      .
cp ~/Downloads/eo-ingest-bench-${SUFFIX}.zip .
unzip -o entropyops-${SUFFIX}.zip
unzip -o eo-ingest-bench-${SUFFIX}.zip
chmod +x entropyops-${SUFFIX} eo-ingest-bench-${SUFFIX}
ls -la entropyops-${SUFFIX} eo-ingest-bench-${SUFFIX}
```

On macOS the first launch may be blocked by Gatekeeper. If you see
"unidentified developer", strip the quarantine attribute once:

```bash
xattr -d com.apple.quarantine ./entropyops-${SUFFIX} ./eo-ingest-bench-${SUFFIX} 2>/dev/null || true
```

### 1d. Refresh after a new release (two commands)

When a new server build drops, the only thing that *usually* changes
is the server `.exe`. The bench client used to need refreshing
almost never; that has changed in the current release — see the
note below.

```powershell
# Windows — drop the new zip into C:\entropyops, extract over the old
# binary. Use tar.exe (bundled), NOT Expand-Archive — see the warning in
# §1b for why.
Copy-Item "$env:USERPROFILE\Downloads\entropyops-windows-amd64.zip" C:\entropyops\ -Force
Set-Location C:\entropyops
Unblock-File .\entropyops-windows-amd64.zip
tar -xf .\entropyops-windows-amd64.zip -C .
```

```bash
# mac/Linux equivalent
cp ~/Downloads/entropyops-${SUFFIX}.zip ~/entropyops-bench/
unzip -o ~/entropyops-bench/entropyops-${SUFFIX}.zip -d ~/entropyops-bench/
```

If the running server is open in another window, **Ctrl+C it first** —
Windows can't overwrite a running `.exe`. Both `tar -xf` and
`Expand-Archive` will fail with "in use" / "permission denied"
otherwise.

> **This release: the bench client also changed.** Earlier bench
> builds silently dropped failed HTTP/gRPC iterations from the
> per-path sample distribution and only stored the *last* error
> string in a free-form `note` field, while AgenticUDP exposed
> timeouts and protocol-level retransmit / drop counters
> separately. That asymmetry was an artifact of the original n=20
> cold-burst design space; under sustained load it systematically
> understated HTTP/gRPC failures relative to AgenticUDP failures
> in any side-by-side comparison. The current `eo-ingest-bench`
> binary fixes that — every path now reports `failures`,
> `timeouts`, `non_ok`, `other_errors`, and `failure_rate_pct`
> as first-class structured fields, and a new status
> `PASS_WITH_FAILURES` covers the partial-success case. See
> methodology constraint #3 in `AGENTICUDP_BENCH_WRITEUP.md` for
> the full explanation. **Refresh `eo-ingest-bench-<platform>.exe`
> (and `eo-bench-merge-<platform>.exe` if you use the chunk-and-
> merge recipe in Scenario B below) the same way you'd refresh
> the server:**

```powershell
# Windows — refresh the bench client(s).
Copy-Item "$env:USERPROFILE\Downloads\eo-ingest-bench-windows-amd64.zip" C:\entropyops\ -Force
Expand-Archive -Path C:\entropyops\eo-ingest-bench-windows-amd64.zip -DestinationPath C:\entropyops -Force
Copy-Item "$env:USERPROFILE\Downloads\eo-bench-merge-windows-amd64.zip"  C:\entropyops\ -Force
Expand-Archive -Path C:\entropyops\eo-bench-merge-windows-amd64.zip  -DestinationPath C:\entropyops -Force
```

```bash
# mac/Linux equivalent
cp ~/Downloads/eo-ingest-bench-${SUFFIX}.zip ~/entropyops-bench/
unzip -o ~/entropyops-bench/eo-ingest-bench-${SUFFIX}.zip -d ~/entropyops-bench/
cp ~/Downloads/eo-bench-merge-${SUFFIX}.zip ~/entropyops-bench/
unzip -o ~/entropyops-bench/eo-bench-merge-${SUFFIX}.zip -d ~/entropyops-bench/
chmod +x ~/entropyops-bench/eo-ingest-bench-${SUFFIX} ~/entropyops-bench/eo-bench-merge-${SUFFIX}
```

> JSON files written by old bench builds remain readable by the
> new tool and the new merger — the new fields are `omitempty` and
> decode as zero-valued for old chunks. But mixed old+new chunks
> in a single merge will produce a per-path failure-count total
> that excludes the old-format chunks' failures (since they
> weren't recorded), so re-run any chunked sweep that was already
> in progress under the new tool if you want the failure rates to
> be cited honestly.

---

## 2. Start the server with all three paths enabled

The bench requires the server's AgenticUDP receiver to be on. The standalone
release zip enables it by default; if you've overridden things, set these
explicitly before starting Core.

### Windows (PowerShell)

```powershell
Set-Location C:\entropyops

$env:ENTROPYOPS_DEPLOYMENT_MODE          = "appliance"
$env:ENTROPYOPS_HTTP_PORT                = "8000"
$env:ENTROPYOPS_GRPC_PORT                = "4317"
$env:ENTROPYOPS_ENABLE_AGENTIC_UDP       = "true"
$env:ENTROPYOPS_AGENTIC_UDP_PORT         = "4320"
$env:ENTROPYOPS_AGENTIC_UDP_RCVBUF_BYTES = "67108864"

# Tee output so SLOW FLUSH / flush_stats lines are recoverable after the run.
.\entropyops-windows-amd64.exe *>&1 | Tee-Object -FilePath server.log
```

If `.\entropyops-windows-amd64.exe` is "not recognized", you are not in
`C:\entropyops` or step 1 didn't complete — re-run step 1b/1d. **Don't
edit the path; refresh the dir.**

### macOS / Linux

```bash
cd ~/entropyops-bench

# Auto-pick platform suffix (same as step 1c)
case "$(uname -sm)" in
    "Darwin arm64")  SUFFIX="darwin-arm64" ;;
    "Darwin x86_64") SUFFIX="darwin-amd64" ;;
    "Linux x86_64")  SUFFIX="linux-amd64"  ;;
    "Linux aarch64"|"Linux arm64") SUFFIX="linux-arm64" ;;
esac

export ENTROPYOPS_DEPLOYMENT_MODE=appliance
export ENTROPYOPS_HTTP_PORT=8000
export ENTROPYOPS_GRPC_PORT=4317
export ENTROPYOPS_ENABLE_AGENTIC_UDP=true
export ENTROPYOPS_AGENTIC_UDP_PORT=4320
export ENTROPYOPS_AGENTIC_UDP_RCVBUF_BYTES=67108864

./entropyops-${SUFFIX} 2>&1 | tee server.log
```

**Confirm the boot log shows the server is in the right bench mode.**
The single most important line is the `BENCH MODE = ...` banner near
the top — it tells you whether the OTLP HTTP / gRPC paths will use
per-row INSERT (the realistic comparison) or the optimized bulk path
(platform-self-test only). For the headline AgenticUDP-vs-industry
numbers you want **`BENCH MODE = standard-baseline (DEFAULT)`**.

```
BENCH MODE = standard-baseline (DEFAULT). OTLP HTTP/gRPC use per-row INSERT; SQLite uses default PRAGMAs (FULL fsync, autocheckpoint=1000); out-of-band WAL checkpointer NOT started. AgenticUDP keeps its bulk-write fast path. ...
sqlite: tuning pragmas applied [mode=standard-baseline]: synchronous=2 cache_size=-2000 temp_store=0 mmap_size=0 wal_autocheckpoint=1000 journal_size_limit=67108864
sqlite: out-of-band checkpointer NOT started [mode=standard-baseline]; SQLite's inline auto-checkpoint applies
sqlite: bulk-insert chunk size = 100 rows/statement (per-row INSERT loop replaced); slow-write log threshold = 100ms
AgenticUDP receiver: ENABLED on udp://0.0.0.0:4320 (tls_mode=none)
agenticudp: storage coalescers started (max_items=2000 max_wait=50ms chan_cap=4096 slow_flush_threshold=200ms)
agenticudp: socket receive buffer set to 67108864 bytes
agenticudp: listening on :4320 (Path B, cleartext)
OTLP gRPC receiver listening on :4317
EntropyOps v2 (Go) listening on :8000 [mode=appliance profile=full]
```

What each line proves:

| Line | What it means | If missing |
|------|---------------|------------|
| `BENCH MODE = standard-baseline (DEFAULT). ...` | OTLP HTTP/gRPC are in their off-the-shelf-equivalent configuration. | Older build — pull a fresh `release/entropyops-<os>-<arch>.zip`. |
| `sqlite: tuning pragmas applied [mode=standard-baseline]: synchronous=2 ... wal_autocheckpoint=1000 ...` | SQLite is at default fsync + inline auto-checkpoint, like an untuned third-party OTLP backend. If you want the tuned/optimized configuration instead, set `ENTROPYOPS_STANDARD_BASELINE_MODE=false` and restart. | Older build — re-download. |
| `sqlite: out-of-band checkpointer NOT started [mode=standard-baseline]` | The platform's WAL checkpointer is intentionally off in baseline mode. | Older build — re-download. |
| `sqlite: bulk-insert chunk size = 100 rows/statement ...` | The bulk-INSERT helper is compiled in (used by AgenticUDP's coalescer; OTLP HTTP/gRPC bypass it in baseline mode). | Older build — re-download. |
| `AgenticUDP receiver: ENABLED ...` | UDP path is enabled in config | Re-set `ENTROPYOPS_ENABLE_AGENTIC_UDP=true` and restart |
| `agenticudp: storage coalescers started ... slow_flush_threshold=200ms` | Per-signal batch coalescers + flush-latency instrumentation are running | Older build — re-download |
| `agenticudp: socket receive buffer set to 67108864 bytes` | Kernel UDP receive buffer is the value you set | OS rejected the size. Try a smaller value (e.g. `33554432` = 32 MB) |
| `agenticudp: listening on :4320` | Kernel actually bound the UDP port | Port already in use. `Get-NetUDPEndpoint -LocalPort 4320` (Windows) / `lsof -nP -iUDP:4320` (mac/Linux) and free it |
| `OTLP gRPC receiver listening on :4317` | gRPC path is up | Port already in use, or another EntropyOps server is running |
| `EntropyOps v2 (Go) listening on :8000 ...` | HTTP/UI path is up; profile + mode confirmed | Port 8000 in use, or `ENTROPYOPS_HTTP_PORT` was overridden |

**Want the platform-self-test (tuned) configuration instead?** Set
`ENTROPYOPS_STANDARD_BASELINE_MODE=false` before launching and the
boot banner will read `BENCH MODE = tuned`. Don't forget to label
the captured JSON accordingly (`bench-<size>-tuned.json`) — see
`docs/operator/bench-results/README.md`.

If all nine lines are present, the server is healthy and you can move to
step 3. **Leave this terminal window open** — closing it kills the server.

While the bench runs, the server will print one or both of:

```
agenticudp: SLOW FLUSH signal=traces tenant=Trading-dev items=5000 duration=2871ms (threshold=200ms) — investigate storage write contention
agenticudp: flush_stats signal=traces flushes=22 items=100000 slow=4 max_ms=2871 avg_ms=486
```

Both are useful — they're how the bimodal-tail investigation gets
ground truth. Tune the threshold via
`ENTROPYOPS_AGENTIC_UDP_SLOW_FLUSH_MS` (default 200) or disable the
periodic stats line with
`ENTROPYOPS_AGENTIC_UDP_FLUSH_STATS_INTERVAL_S=0`.

---

## 3. Smoke test (200 spans, 50 iterations)

> **Open a SECOND terminal window for this step.** The server from step 2
> is running in the foreground of the first window — closing or `Ctrl+C`-ing
> that window will kill the server mid-bench. Leave the server window
> alone, open a new PowerShell / Terminal, and run the bench from there.

This is the size where things should always work. If it fails, the rest of
the suite will too — fix here first.

### Windows (PowerShell, in the SECOND window)

```powershell
Set-Location C:\entropyops

.\eo-ingest-bench-windows-amd64.exe `
  -server http://localhost:8000 `
  -grpc localhost:4317 `
  -udp 127.0.0.1:4320 `
  -tenant Trading-dev `
  -spans 200 -iters 50 -warmup 5 `
  -json bench-200-baseline.json
```

(The `-baseline` suffix on the JSON filename is convention only — it
labels the output as a standard-baseline-mode run. Use `-tuned` if
you ran the server with `ENTROPYOPS_STANDARD_BASELINE_MODE=false`.)
The bench tool also writes `bench_mode` into the JSON itself, so you
can verify after the fact.

### macOS / Linux (in the SECOND terminal)

```bash
cd ~/entropyops-bench   # new terminal forgets cwd
case "$(uname -sm)" in
    "Darwin arm64")  SUFFIX="darwin-arm64" ;;
    "Darwin x86_64") SUFFIX="darwin-amd64" ;;
    "Linux x86_64")  SUFFIX="linux-amd64"  ;;
    "Linux aarch64"|"Linux arm64") SUFFIX="linux-arm64" ;;
esac

./eo-ingest-bench-${SUFFIX} \
  -server http://localhost:8000 \
  -grpc localhost:4317 \
  -udp 127.0.0.1:4320 \
  -tenant Trading-dev \
  -spans 200 -iters 50 -warmup 5 \
  -json bench-200-baseline.json
```

Expected: all three rows show `PASS`, p50 latencies in the low hundreds of
ms, AgenticUDP `retx=0  drops=0`. The footer prints the three caveats —
this is the part you need to read before sharing results with anyone.

---

## 4. Stress test (1000 spans, then 5000 spans)

Same flags, larger payloads. These exercise the AgenticUDP chunking path
plus the new server-side coalescer. Tail latency at these sizes is the
honest measure of where the platform is today.

### Windows (PowerShell)

```powershell
.\eo-ingest-bench-windows-amd64.exe -server http://localhost:8000 `
  -grpc localhost:4317 -udp 127.0.0.1:4320 -tenant Trading-dev `
  -spans 1000 -iters 30 -warmup 3 -json bench-1000-baseline.json

.\eo-ingest-bench-windows-amd64.exe -server http://localhost:8000 `
  -grpc localhost:4317 -udp 127.0.0.1:4320 -tenant Trading-dev `
  -spans 5000 -iters 20 -warmup 2 `
  -udp-ack-timeout-ms 30000 -json bench-5000-baseline.json
```

### macOS / Linux

```bash
./eo-ingest-bench-${SUFFIX} -server http://localhost:8000 -grpc localhost:4317 \
  -udp 127.0.0.1:4320 -tenant Trading-dev \
  -spans 1000 -iters 30 -warmup 3 -json bench-1000-baseline.json

./eo-ingest-bench-${SUFFIX} -server http://localhost:8000 -grpc localhost:4317 \
  -udp 127.0.0.1:4320 -tenant Trading-dev \
  -spans 5000 -iters 20 -warmup 2 \
  -udp-ack-timeout-ms 30000 -json bench-5000-baseline.json
```

The `-udp-ack-timeout-ms 30000` raise on the 5000-span run is intentional:
under the current bulk-send tuning a single 5000-span payload can take
tens of seconds end-to-end. We want to **measure** that tail, not have the
client give up at 10 s and report `FAIL_TIMEOUT`.

---

## 4b. 5000-span tail characterization (n = 10,000)

This is the long run. Use it when you want to **publish a tail claim**
(p99 / p99.9 / avg / spans/sec) at 5000 spans, not just a median.

### Why 10,000 iterations

- `n ≥ 10,000` → publishable p99 (the 9,900th-ranked sample, with
  neighbours 9,899 and 9,901 to sanity-check stability) AND an
  emerging p99.9 estimator (the 9,990th-ranked sample). p99.9 is
  the percentile that actually matters for SLO budgets at
  production volume.
- `n ≥ 1,000` is enough for a defensible p99 by itself; bump to
  10,000 when the bench is cheap and the marginal precision matters.
- `n ≈ 100` is the bare minimum to say *anything* about p99.
- `n ≤ 20` is medians-only — never quote the tail.

### Run-time budget (be honest with yourself before you start)

The bench tool runs the three paths **sequentially** (HTTP → gRPC
→ UDP, no parallelism). Wall-clock per path = `(iters + warmup) ×
average wall-clock per iteration`. Using the actual standard-
baseline n=20 averages from `bench-5000-baseline.json`:

| Path | avg/iter (n=20) | × 10,050 iters | Wall-clock |
|------|-----------------|----------------|------------|
| OTLP HTTP   | 1,893 ms | 10,050 × 1.893 s | **~5h 17m** |
| OTLP gRPC   | 2,300 ms | 10,050 × 2.300 s | **~6h 25m** |
| AgenticUDP (worst: n=20 avg holds) | 2,176 ms | 10,050 × 2.176 s | **~6h 4m** |
| AgenticUDP (best: tail dissolves) |   186 ms | 10,050 × 0.186 s | **~31m**    |

**Per replica, end-to-end:**

- Worst case (AgenticUDP tail is real at the n=20 rate):
  **~17.8 hours**, three replicas = ~2.2 days of wall-clock.
- Best case (AgenticUDP tail dissolves at higher n):
  **~12.2 hours per replica**, three replicas = ~1.5 days.
- Realistic middle (AgenticUDP avg lands around 1 s):
  **~14–15 hours per replica**, three replicas = ~1.8 days.

This is not "overnight." Plan it as a **multi-day run** and
budget the host accordingly (no Windows updates, no antivirus
scans, no Slack desktop). If you can dedicate the host, you can
launch all three replicas back-to-back in one shell session and
walk away.

### Isolated-per-path protocol (publishable cross-path numbers)

> **Why this protocol exists.** The single-invocation form in §4
> runs HTTP, gRPC, and AgenticUDP **sequentially against the same
> backing `./data`**. By the time the second path's measurement
> window starts, the first path has already grown the SQLite
> database, and standard-baseline-mode contention is itself a
> function of DB size (B-tree depth grows, page-cache hit rate
> falls, inline auto-checkpoint stalls get bigger). So the three
> paths are not measuring against the same backend, and a cross-
> path comparison drawn from a single-invocation JSON has a
> "but the DB was warmer when path X ran" footnote attached to it
> at all times. See methodology constraint #4 in
> `AGENTICUDP_BENCH_WRITEUP.md` for the full explanation.
>
> The fix is to run each path in **its own bench invocation**
> against a **freshly-initialized server** (`./data` wiped and
> the server restarted between paths), then merge the per-path
> JSONs with `eo-bench-merge`. The merger detects this pattern
> ("cross-path isolated merge") automatically and suppresses the
> per-path "present in 1/3 chunks" warnings that would otherwise
> fire and (under the `-strict=true` default) escalate to fatal.

The protocol below is the canonical recipe for n=1,000 at
5,000 spans (the writeup's open n=1,000 follow-up). Bump
`-iters` from 1000 to 10000 for the n=10,000 variant; the rest
of the recipe is unchanged.

#### Windows (PowerShell)

```powershell
Set-Location C:\entropyops

# Helper: stop server, wipe data, relaunch with the standard env.
function Restart-EntropyOpsFresh {
    Get-Process entropyops-windows-amd64 -EA SilentlyContinue | Stop-Process -Force
    Start-Sleep -Seconds 2
    if (Test-Path C:\entropyops\data) {
        Remove-Item C:\entropyops\data -Recurse -Force
    }
    $serverEnv = @{
      ENTROPYOPS_DEPLOYMENT_MODE          = 'appliance'
      ENTROPYOPS_HTTP_PORT                = '8000'
      ENTROPYOPS_GRPC_PORT                = '4317'
      ENTROPYOPS_ENABLE_AGENTIC_UDP       = 'true'
      ENTROPYOPS_AGENTIC_UDP_PORT         = '4320'
      ENTROPYOPS_AGENTIC_UDP_RCVBUF_BYTES = '67108864'
    }
    foreach ($k in $serverEnv.Keys) {
        [Environment]::SetEnvironmentVariable($k, $serverEnv[$k], 'Process')
    }
    $srv = Start-Process -FilePath 'cmd.exe' `
      -ArgumentList '/c','C:\entropyops\entropyops-windows-amd64.exe > C:\entropyops\server.log 2>&1' `
      -WindowStyle Hidden -PassThru
    $srv.Id | Out-File 'C:\entropyops\server.pid'
    Start-Sleep -Seconds 5
    $realPid = (Get-Process entropyops-windows-amd64 -EA SilentlyContinue).Id
    if (-not $realPid) {
        Write-Host "Server failed to start — see C:\entropyops\server.log" -ForegroundColor Red
        throw "server-start"
    }
    $realPid | Out-File 'C:\entropyops\server.realpid'
    "Server started — wrapper $($srv.Id), real $realPid"
}

# Step 1: HTTP only against a fresh DB.
Restart-EntropyOpsFresh
.\eo-ingest-bench-windows-amd64.exe `
  -server http://localhost:8000 -grpc localhost:4317 -udp 127.0.0.1:4320 `
  -tenant Trading-dev -spans 5000 -iters 1000 -warmup 10 `
  -keep-samples -udp-ack-timeout-ms 60000 `
  -paths http -json bench-5000-symmetric-http.json

# Step 2: gRPC only against a fresh DB.
Restart-EntropyOpsFresh
.\eo-ingest-bench-windows-amd64.exe `
  -server http://localhost:8000 -grpc localhost:4317 -udp 127.0.0.1:4320 `
  -tenant Trading-dev -spans 5000 -iters 1000 -warmup 10 `
  -keep-samples -udp-ack-timeout-ms 60000 `
  -paths grpc -json bench-5000-symmetric-grpc.json

# Step 3: AgenticUDP only against a fresh DB.
Restart-EntropyOpsFresh
.\eo-ingest-bench-windows-amd64.exe `
  -server http://localhost:8000 -grpc localhost:4317 -udp 127.0.0.1:4320 `
  -tenant Trading-dev -spans 5000 -iters 1000 -warmup 10 `
  -keep-samples -udp-ack-timeout-ms 60000 `
  -paths udp -json bench-5000-symmetric-udp.json

# Step 4: merge the three single-path JSONs into one publishable JSON.
.\eo-bench-merge-windows-amd64.exe `
  bench-5000-symmetric-http.json `
  bench-5000-symmetric-grpc.json `
  bench-5000-symmetric-udp.json `
  -out bench-5000-symmetric-merged.json -summary
```

The merger will print `INFO: cross-path isolated merge detected
— 3 chunks each contribute a disjoint set of paths …` to stderr,
then write `bench-5000-symmetric-merged.json` with the same shape
as a single-invocation run plus a `merged_from` array recording
the per-chunk SHA-256s so the merged result is reproducible from
the same chunks.

#### macOS / Linux

```bash
restart_entropyops_fresh() {
    pkill -f "entropyops-${SUFFIX}" 2>/dev/null
    sleep 2
    rm -rf ./data
    export ENTROPYOPS_DEPLOYMENT_MODE=appliance
    export ENTROPYOPS_HTTP_PORT=8000
    export ENTROPYOPS_GRPC_PORT=4317
    export ENTROPYOPS_ENABLE_AGENTIC_UDP=true
    export ENTROPYOPS_AGENTIC_UDP_PORT=4320
    export ENTROPYOPS_AGENTIC_UDP_RCVBUF_BYTES=67108864
    ./entropyops-${SUFFIX} > server.log 2>&1 &
    SERVER=$!
    sleep 5
    if ! kill -0 "$SERVER" 2>/dev/null; then
        echo "Server failed to start — see server.log" >&2
        return 1
    fi
    echo "Server started PID $SERVER"
}

# Step 1: HTTP only.
restart_entropyops_fresh
./eo-ingest-bench-${SUFFIX} \
  -server http://localhost:8000 -grpc localhost:4317 -udp 127.0.0.1:4320 \
  -tenant Trading-dev -spans 5000 -iters 1000 -warmup 10 \
  -keep-samples -udp-ack-timeout-ms 60000 \
  -paths http -json bench-5000-symmetric-http.json

# Step 2: gRPC only.
restart_entropyops_fresh
./eo-ingest-bench-${SUFFIX} \
  -server http://localhost:8000 -grpc localhost:4317 -udp 127.0.0.1:4320 \
  -tenant Trading-dev -spans 5000 -iters 1000 -warmup 10 \
  -keep-samples -udp-ack-timeout-ms 60000 \
  -paths grpc -json bench-5000-symmetric-grpc.json

# Step 3: AgenticUDP only.
restart_entropyops_fresh
./eo-ingest-bench-${SUFFIX} \
  -server http://localhost:8000 -grpc localhost:4317 -udp 127.0.0.1:4320 \
  -tenant Trading-dev -spans 5000 -iters 1000 -warmup 10 \
  -keep-samples -udp-ack-timeout-ms 60000 \
  -paths udp -json bench-5000-symmetric-udp.json

# Step 4: merge.
./eo-bench-merge-${SUFFIX} \
  bench-5000-symmetric-http.json \
  bench-5000-symmetric-grpc.json \
  bench-5000-symmetric-udp.json \
  -out bench-5000-symmetric-merged.json -summary
```

#### When the cheaper split (HTTP+gRPC at n=1k, UDP at n=10k) is still OK

The full per-path n=10,000 protocol above is `~14–18 hours` on
the standard-baseline reference VM. If you only need a publishable
*AgenticUDP* tail and HTTP/gRPC are happy at n=1,000, you can
shorten it: run steps 1 and 2 above with `-iters 1000` and step
3 with `-iters 10000`. The merger handles the unequal n
correctly. **Don't** combine HTTP+gRPC into a single
`-paths http,grpc` invocation against one server — that
re-introduces the order effect this whole protocol exists to
eliminate.

---

## 4c. Running through VM idle timeouts

Two scenarios, two different fixes — and they need different
recipes.

- **Scenario A** — RDP/SSH session times out, but the **VM itself
  stays up**. The bench process survives if you launch it
  detached from your console and disable host-side sleep.
- **Scenario B** — the **VM itself shuts down or suspends** on
  idle (cloud auto-stop policy, hibernate-on-AC-power, etc.).
  Detaching does nothing because the host literally goes away.
  You need to either disable the policy or chunk the bench into
  shorter runs that each fit before the timeout.

Quick check (PowerShell on the Windows VM) tells you which:

```powershell
# Sleep / hibernate / monitor-off (timeouts in seconds; 0 = never)
powercfg /query SCHEME_CURRENT SUB_SLEEP STANDBYIDLE
powercfg /query SCHEME_CURRENT SUB_SLEEP HIBERNATEIDLE
powercfg /query SCHEME_CURRENT SUB_VIDEO VIDEOIDLE

# RDP "log off on disconnect" (default off; processes survive)
(Get-ItemProperty 'HKLM:\SYSTEM\CurrentControlSet\Control\Terminal Server\WinStations\RDP-Tcp' -EA SilentlyContinue).MaxDisconnectionTime

# Cloud VM auto-stop is provider-side — check the Azure / AWS / GCP
# console for the host's idle / auto-shutdown policy.
```

### Scenario A: detach + keep VM awake

```powershell
# Step 1: keep the VM awake for the full duration
powercfg /change standby-timeout-ac 0
powercfg /change hibernate-timeout-ac 0
powercfg /change monitor-timeout-ac 0

# Step 2: launch detached, log to file, save PID for later checks
$argList = @(
  '-server','http://localhost:8000',
  '-grpc','localhost:4317',
  '-udp','127.0.0.1:4320',
  '-tenant','Trading-dev',
  '-paths','udp',
  '-spans','5000',
  '-iters','10000',
  '-warmup','50',
  '-udp-ack-timeout-ms','30000',
  '-json','C:\entropyops\bench-5000-n10000-udp-run1.json'
)
$proc = Start-Process -FilePath 'C:\entropyops\eo-ingest-bench-windows-amd64.exe' `
  -ArgumentList $argList `
  -RedirectStandardOutput 'C:\entropyops\bench-udp-run1.out' `
  -RedirectStandardError  'C:\entropyops\bench-udp-run1.err' `
  -WindowStyle Hidden -PassThru
$proc.Id | Out-File 'C:\entropyops\bench-udp-run1.pid'
"Started PID $($proc.Id). Safe to disconnect RDP."
```

Disconnect the RDP window (the X — **do not log off**). On
reconnect:

```powershell
$pid = Get-Content C:\entropyops\bench-udp-run1.pid
Get-Process -Id $pid -ErrorAction SilentlyContinue   # empty = finished
Get-Content C:\entropyops\bench-udp-run1.out -Tail 20
Get-Item    C:\entropyops\bench-5000-n10000-udp-run1.json -EA SilentlyContinue
```

### Scenario B: chunk the run + merge

If the VM auto-stops on idle and you can't disable the policy,
run **many small benches back-to-back** that each fit in the
timeout window, then merge them into a single artifact with
`eo-bench-merge`. Statistically equivalent to one big run; you
also get replication baked in (which is what you'd need anyway
to claim the tail is reproducible).

#### Pick a chunk size that fits your timeout

| Idle timeout | Recommended `-iters` | Chunks for n=10,000 | Worst-case per chunk |
|---|---|---|---|
| 15 min  | 50    | 200 chunks | ~2.6 min  |
| 30 min  | 100   | 100 chunks | ~5.2 min  |
| 60 min  | 200   |  50 chunks | ~10.4 min |
| 4 h     | 1000  |  10 chunks | ~52 min   |

(Worst-case assumes the AgenticUDP n=20 avg of 2.176 s holds at
higher n; if the tail dissolves the chunk runs in `~0.186 s ×
iters`, ~30× faster.)

#### Run the chunks (n=200 × 50 example)

The chunks **must be written with `-keep-samples`** so the
merger can recompute percentiles from the raw per-iteration
samples. Without that flag the merger aborts with a clear error.

```powershell
# 50 back-to-back AgenticUDP-only chunks, each n=200, ~10 min worst case
1..50 | ForEach-Object {
  $i = "{0:D3}" -f $_
  & 'C:\entropyops\eo-ingest-bench-windows-amd64.exe' `
    -server http://localhost:8000 -grpc localhost:4317 -udp 127.0.0.1:4320 `
    -tenant Trading-dev -paths udp -spans 5000 -iters 200 -warmup 5 `
    -udp-ack-timeout-ms 30000 -keep-samples `
    -json "C:\entropyops\bench-5000-udp-chunk$i.json"
}
```

(macOS/Linux equivalent: a `for i in $(seq -f "%03g" 1 50); do
./eo-ingest-bench-${SUFFIX} ... -keep-samples -json
"bench-5000-udp-chunk$i.json"; done` loop.)

#### Merge into the publishable artifact

`eo-bench-merge` concatenates per-iteration samples from all
chunks and recomputes the same statistics the bench tool would
have produced for one big n=10,000 run. The merged JSON is
**bit-identical** to a single live run because it imports the
exact same percentile/mean/rounding code the bench tool uses.

```powershell
# Glob expands inside the tool; PowerShell does not need to expand it
.\eo-bench-merge-windows-amd64.exe `
  -in 'C:\entropyops\bench-5000-udp-chunk*.json' `
  -out C:\entropyops\bench-5000-n10000-udp-run1.json
```

```bash
./eo-bench-merge-${SUFFIX} \
  -in 'bench-5000-udp-chunk*.json' \
  -out bench-5000-n10000-udp-run1.json
```

The merger:

- Aborts if `spans_per_call` differs across chunks (different
  workloads cannot be merged).
- Aborts if `bench_mode` differs across chunks (mixing
  standard-baseline with tuned chunks would be a methodology
  error).
- Aborts if any chunk reported PASS without samples (someone
  forgot `-keep-samples`).
- Warns (and `-strict=true` aborts) on tenant mismatch or paths
  missing from some chunks.
- Records sha256 of every input chunk in `merged_from`, so the
  publishable artifact is fully reproducible from the chunk
  files.

Repeat the chunk-and-merge for replica 2 and replica 3 (different
chunk-file prefix per replica). The three merged JSONs are what
get committed under `docs/operator/bench-results/` and cited in
the writeup — exactly the same outputs as the full-sweep recipe,
generated through any number of short uninterrupted sessions.

#### Full sweep (n=10,000 across all three paths in one shot, ~1.5–2.5 days)

Use this only if you genuinely have a multi-day uninterrupted
host (no idle disconnect, no auto-stop, no patch reboots):

```powershell
.\eo-ingest-bench-windows-amd64.exe -server http://localhost:8000 `
  -grpc localhost:4317 -udp 127.0.0.1:4320 -tenant Trading-dev `
  -spans 5000 -iters 10000 -warmup 50 `
  -udp-ack-timeout-ms 30000 -json bench-5000-n10000-run1.json
```

Otherwise prefer the split + chunk + merge recipe above; it
produces a publishable artifact of identical shape and statistics
without requiring a multi-day host.

### What the result resolves to

The follow-up will land in one of three publishable buckets:

1. **p99 clusters tightly across all three n=10,000 replicas.**
   Tail is real and characterized; the storage-side stall
   hypotheses in [`AGENTICUDP_BENCH_WRITEUP.md`](../writeups/AGENTICUDP_BENCH_WRITEUP.md)
   become things to fix.
2. **p99 jumps around across replicas.** Tail is environmental
   noise (Windows fsync, GC, antivirus, host contention) and the
   original n=20 result was misleading.
3. **p99 collapses to near-p50.** No bimodal tail; the
   coalescer-stall hypothesis dissolves. Headline: "AgenticUDP
   stays sub-300 ms across 10,000 iterations at 5000 spans."

Whichever lands gets folded into the writeup. The architectural
p50 win does not depend on which one it is.

---

## 5. What to look at in the output

For each path the bench prints:

| Column | Meaning |
|--------|---------|
| `p50/p90/p99 ms` | Client wall-clock per request |
| `ms/span` | Same number divided by `spans` — the cross-batch-size comparable |
| `spans/sec` | Throughput at this batch size |
| `retx` (UDP only) | AgenticUDP retransmits. Should be `0`. Non-zero means receive buffer pressure or coalescer pressure. |
| `drops` (UDP only) | Server kernel drops. Should be `0`. Non-zero means raise `ENTROPYOPS_AGENTIC_UDP_RCVBUF_BYTES`. |

The footer reprints the **WHAT THESE NUMBERS DO NOT TELL YOU** caveats.
Screenshot the footer along with the table; the numbers without the
caveats are misleading.

---

## 6. Auth (only if your server requires it)

If you've started Core with `requireAPIKey=true`, pass the key explicitly:

```powershell
.\eo-ingest-bench-windows-amd64.exe -api-key "YOUR_KEY" `
  -server http://localhost:8000 -grpc localhost:4317 -udp 127.0.0.1:4320 `
  -tenant Trading-dev -spans 200 -iters 50
```

```bash
./eo-ingest-bench-${SUFFIX} -api-key "YOUR_KEY" -server http://localhost:8000 \
  -grpc localhost:4317 -udp 127.0.0.1:4320 -tenant Trading-dev \
  -spans 200 -iters 50
```

For Bearer-token auth use `-bearer "..."` instead. AgenticUDP carries the
tenant in the envelope and does not need either header.

---

## 7. Running only one or two paths

The `-paths` flag is comma-separated; allowed values are `http`,
`grpc`, `udp` (case-insensitive, whitespace-tolerant). Invalid
tokens fail at flag-parse time so you don't accidentally run zero
paths and exit clean.

Three legitimate use cases:

1. **Triage:** one path is broken (e.g. AgenticUDP receiver is
   down) and you don't want it to dominate the output table. Use
   `-paths` to skip the broken path while you investigate.
2. **Path-specific exploratory work:** you're optimising the
   coalescer and only care about AgenticUDP for this iteration.
   `-paths udp` skips the HTTP/gRPC measurement so the smoke is
   faster.
3. **Publishable cross-path numbers under the isolated-per-path
   protocol** — see §4b. This is the case that needs the
   server restart between paths; do not use the §7 form for it.

```powershell
# Triage: AgenticUDP receiver is down, run HTTP and gRPC only.
.\eo-ingest-bench-windows-amd64.exe -paths http,grpc -spans 200 -iters 50 ...

# Exploratory: AgenticUDP only against the live server.
.\eo-ingest-bench-windows-amd64.exe -paths udp -spans 1000 -iters 30 ...
```

```bash
./eo-ingest-bench-${SUFFIX} -paths http,grpc -spans 200 -iters 50 ...
./eo-ingest-bench-${SUFFIX} -paths udp        -spans 1000 -iters 30 ...
```

> **Do NOT use `-paths http,grpc,udp` against one server for
> publishable cross-path numbers** — that's the single-invocation
> default, and it has the cross-path order effect documented in
> methodology constraint #4 of `AGENTICUDP_BENCH_WRITEUP.md`.
> Publishable cross-path numbers go through the isolated-per-path
> protocol in §4b (one server restart with fresh `./data` between
> each path's invocation, then `eo-bench-merge` stitches the
> per-path JSONs).

---

## 8. Sending results back

The `-json bench-<size>.json` file is the canonical artifact — it contains
the full table plus metadata (tenant, iteration count, timestamp). Attach
those JSON files plus a screenshot of the **footer** (so the caveats
travel with the numbers) and we'll fold them into `PROGRAM_SUMMARY.md`.

After all three sizes complete you should have three files in your
working directory:

```bash
ls -la bench-200.json bench-1000.json bench-5000.json   # macOS / Linux
```

```powershell
Get-ChildItem bench-200.json, bench-1000.json, bench-5000.json   # Windows
```

**Stop-and-share rules** (do this instead of grinding through if a step
fails — the failure mode tells us more than retrying with the same setup
ever will):

1. If the **smoke test (200 spans)** fails, stop. Paste the table and
   footer here and we'll fix the setup before going further.
2. If 200 passes but **1000 spans** shows AgenticUDP `retx > 0`, capture
   the table and footer; the retransmit count under load is exactly the
   signal we need to tune the inflight window or coalescer settings.
3. If 1000 passes but **5000 spans** times out (`FAIL_TIMEOUT`), share
   the partial run anyway. A clean 1000-span run plus a 5000-span timeout
   is a useful data point — it tells us where the current ceiling is.

---

## 9. Common failures and what they mean

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `'.\entropyops-windows-amd64.exe' is not recognized` (or `entropyops-server.exe`) | You are not in `C:\entropyops`, **or** step 1b never completed (the zip wasn't where the script expected). There is no `entropyops-server.exe` — the only Windows binary is `entropyops-windows-amd64.exe`. | `Set-Location C:\entropyops; Get-ChildItem entropyops-windows-amd64.exe`. If the file isn't there, re-run step 1b. If your release zip lives somewhere other than `$env:USERPROFILE\Downloads\`, edit the `Copy-Item` source path in step 1b — that's the one place a path is allowed to vary. |
| `Expand-Archive : The path '...' either does not exist` | The release zip isn't where step 1b expects it (`$env:USERPROFILE\Downloads\`) | `dir $env:USERPROFILE\Downloads\*.zip` to confirm where the zip actually is, then update the `Copy-Item` source path in step 1b. If you genuinely don't know where it is, see appendix 11. |
| `.\entropyops-windows-amd64.exe : ... not recognized` despite `Get-ChildItem` showing the file | PowerShell ExecutionPolicy or AV quarantine | `Get-MpThreat`; if the file is quarantined, restore it. Otherwise try `Start-Process .\entropyops-windows-amd64.exe -NoNewWindow -Wait`. |
| `Expand-Archive` fails with "file in use" | Old server process still has the `.exe` open | `Get-Process entropyops-windows-amd64 \| Stop-Process` (or Ctrl+C its window) and re-run the Expand-Archive |
| Permission denied on macOS / Linux | Extracted file is not executable | `chmod +x entropyops-${SUFFIX} eo-ingest-bench-${SUFFIX}` |
| "unidentified developer" pop-up on macOS | Gatekeeper quarantine on the downloaded zip | `xattr -d com.apple.quarantine ./entropyops-${SUFFIX} ./eo-ingest-bench-${SUFFIX}` |
| `FAIL_NO_DATA  message too long` | Server's UDP receive buffer too small for the payload | Set `ENTROPYOPS_AGENTIC_UDP_RCVBUF_BYTES=67108864` and restart the server |
| `FAIL_HANDSHAKE` | AgenticUDP receiver not listening, or wrong port | Confirm boot log shows `AgenticUDP receiver: ENABLED on udp://...:4320` |
| AgenticUDP `retx > 0` but `drops = 0` | Client outpacing the server's coalescer | Lower `ENTROPYOPS_AGENT_UDP_MAX_INFLIGHT` (default 64; try 32), or shorten the coalescer flush window via `ENTROPYOPS_AGENTIC_UDP_BATCH_MAX_WAIT_MS` (default 50; try 20), or raise the coalescer channel via `ENTROPYOPS_AGENTIC_UDP_BATCH_CHAN_CAP` (default 4096) |
| AgenticUDP `drops > 0` | Kernel UDP buffer overflow | Raise `ENTROPYOPS_AGENTIC_UDP_RCVBUF_BYTES` |
| `agenticudp: SLOW FLUSH` lines appear in the server log | A storage write took longer than `ENTROPYOPS_AGENTIC_UDP_SLOW_FLUSH_MS` (default 200) — this is **expected** at high load and is the diagnostic signal we want | Note the `tenant`, `items`, and `duration` from each line. If `items=5000` and `duration` is multi-second, the stall is `WriteTraces` itself. If `items` is small and `duration` is multi-second, contention from another writer (pipeline / self-obs). Share the lines back to triage. |
| HTTP returns 401/403 | Server requires auth | Pass `-api-key` or `-bearer` |
| gRPC returns `Unauthenticated` | Same | Same |
| All three `FAIL_NO_DATA` | Server not running, or wrong host/port | Re-check the boot log and the `-server`, `-grpc`, `-udp` flags |

---

## 10. The numbers are not the story — the caveats are

The single most important sentence in this whole document:

> Until the three ingest handlers run the **same** enrichment chain
> (sampler + k8s join), the bench is a lower bound on HTTP and an upper
> bound on gRPC + AgenticUDP. It is **not** a clean cross-protocol ranking.

This is tracked as the "handler parity" work item. When that lands, this
quickstart will be updated and the asymmetry caveat will go away.

---

## 11. Appendix — bootstrap when you have no idea where the zips are

Use this only when step 1b's `Copy-Item ... -Path $env:USERPROFILE\Downloads\...`
fails because the zip really isn't in `Downloads`. The script searches
the whole `C:\` drive for either the zip or the already-extracted exe,
picks the most recent, and stages it into `C:\entropyops`.

```powershell
$WorkDir = "C:\entropyops"
New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null
Set-Location $WorkDir

function Stage-Artifact {
    param([string]$ExeName, [string]$ZipName)
    if (Test-Path (Join-Path $WorkDir $ExeName)) {
        Write-Host "already staged: $ExeName" -ForegroundColor Green
        return
    }
    Write-Host "searching C:\ for $ExeName or $ZipName ..." -ForegroundColor Cyan
    $hits = Get-ChildItem -Path C:\ -Recurse -ErrorAction SilentlyContinue `
        -Include $ExeName, $ZipName | Sort-Object LastWriteTime -Descending
    if ($hits.Count -eq 0) {
        Write-Host "MISSING: neither $ExeName nor $ZipName found anywhere on C:\" -ForegroundColor Red
        Write-Host "         copy the release zip from your dev box first, then re-run." -ForegroundColor Red
        return
    }
    $best = $hits[0]
    if ($best.Name -like "*.exe") {
        Copy-Item $best.FullName -Destination $WorkDir -Force
        Write-Host "copied $($best.FullName) -> $WorkDir" -ForegroundColor Green
    } else {
        Expand-Archive -Path $best.FullName -DestinationPath $WorkDir -Force
        Write-Host "extracted $($best.FullName) -> $WorkDir" -ForegroundColor Green
    }
}

Stage-Artifact -ExeName "entropyops-windows-amd64.exe"      -ZipName "entropyops-windows-amd64.zip"
Stage-Artifact -ExeName "eo-ingest-bench-windows-amd64.exe" -ZipName "eo-ingest-bench-windows-amd64.zip"

Get-ChildItem entropyops-windows-amd64.exe, eo-ingest-bench-windows-amd64.exe
```

If the final `Get-ChildItem` is missing either binary, the script
printed `MISSING:` and the artifact is nowhere on the box — get the
release zip onto disk by any means (USB, network share, scp from your
dev box) and re-run. Once both files are listed, **never** re-run this
script — go straight to step 2 from now on.
