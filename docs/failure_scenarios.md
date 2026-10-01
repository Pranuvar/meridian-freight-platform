# Failure scenarios

"Exercised" means I ran it and saw the result. "Reasoned" means the design handles it but I did not run it.

| Scenario | What failed | Detected by | Recovery | Prevention | Status |
|---|---|---|---|---|---|
| Reference API down | `check_api` failed after 6 attempts (2, 4, 8, 16, 32 s backoff) | Non-zero exit with a readable error; five downstream tasks skipped; monitor lists tasks with no result | Restart the API, rerun; finished tasks are idempotent | Retries absorb short outages; alert on a failed run; publish gate blocks partial runs | Exercised |
| Crash between Bronze write and watermark | Bronze held rows twice | Silver accounting line showed 4,749 duplicates | Rerun: Silver kept the newest version | Watermark advances last, from rows actually landed | Exercised |
| Duplicate records | 39,398 scan and 12,097 invoice duplicates | Independent recount from Bronze in reconciliation | Deduplicated in Silver on a standardised key | Idempotent merges | Exercised |
| Late-arriving scans | 1,148 scans arrived days after the event | `arrival_lag_days`; Gold affected-shipment logic | Gold recomputes affected shipments; a replay leaves the content fingerprint unchanged | Layer watermark uses arrival time | Exercised |
| Schema drift in scan files | Early files lack `scan_source` | Null column in Bronze | Explicit schema reads it as null | Explicit schemas | Exercised |
| Missing dimension member | Unknown location codes (9,640 events) | Unknown-member count in Gold | Mapped to the Unknown member; fix reference data | Reference check rule | Exercised |
| Wrong data | Silent logic error | Reconciliation against an independent answer key | Fix and backfill the affected window | Reconciliation after every run | Exercised |
| Malformed or reordered carrier CSV | Column positions shift | Unparseable dates or amounts become quarantine reasons | Fix the parser, replay quarantined lines | Layout checks | Reasoned |
| Failed reporting refresh | Export or model refresh fails | Task failure | Reports keep the last exported version | Publish gate and versioned export | Reasoned |
