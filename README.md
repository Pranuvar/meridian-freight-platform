# Meridian Freight Data Platform

An end-to-end data platform for a (fictional) Indian road-freight forwarder: messy multi-source data goes in, a governed semantic layer and a Power BI report come out. Built to show the whole path: **raw data, PySpark engineering, data quality, dimensional modelling, semantic layer, DAX, Power BI.**

## The business problem
Meridian books freight for customers and subcontracts each shipment to a carrier. Ops, Finance and Sales each calculated "on-time delivery" differently, carrier invoices were checked by hand in Excel, tracking scans arrived late and out of order, and nobody trusted margin per lane. This platform gives one definition of every KPI, reconciles every layer, and refreshes daily.

## Architecture

    Postgres (TMS) -+
    Scan Parquet ---+--> Ingestion --> BRONZE --> Quality --> SILVER --> Reconcile --> GOLD --> Semantic model --> DAX --> Power BI
    Invoice CSVs ---+     (PySpark)    (Delta)   +quarantine   (Delta)                 (star)   (Import, RLS)
    Reference API --+
                                Airflow DAG orchestrates; pipeline_runs log + monitor report; publish gate

| Layer | What it holds |
|---|---|
| Bronze | Source data as received, plus lineage columns (`_ingested_at`, `_batch_id`, `_source_file`). Append-only |
| Silver | Typed, standardised, deduplicated, validated. Rejected rows go to a quarantine table with the reason |
| Gold | Star schema: 3 facts, 6 dimensions, one SCD Type 2 (customer) |
| Semantic | Power BI model on surrogate keys, 16 documented KPIs, dynamic row-level security |

## Sources (all with deliberate mess)
- **PostgreSQL TMS**: shipments, charges, claims, customers, carriers, locations. Watermark on `updated_at`.
- **Tracking scans**: 523 daily Parquet files named by *arrival* date. Late arrivals, duplicates, three timestamp formats, two reference formats, orphans, schema drift.
- **Carrier invoices**: 3 CSV layouts (delimiters, decimal commas, date formats, GST split), duplicate lines, ghost lines, overcharges.
- **Reference API**: FX (weekdays only) and diesel index, with simulated 5xx/429 failures. Retries with exponential backoff.

## Results
Numbers below are from the final run; the generator also writes an answer key, and the pipeline reproduces it.

| Check | Result |
|---|---|
| Revenue control total | Rs 3,451,524,930.32 identical in Silver, Gold, the export and Power BI |
| On-time % vs the generator's truth | 0 timestamp or flag mismatches; carrier on-time within 0.02 pp |
| Carrier cost | 0 per-shipment mismatches against truth |
| Reconciliation | 27 PASS, 7 EXPLAINED, 11 INFO, **0 FAIL** |
| USD revenue vs ledger | -Rs 392,653.63, explained by the FX-rate effect to within Rs 0.49 |
| Headline finding | Carrier drives lateness: BlueLine 88.1%, SwiftHaul 81.8%, TruckNet 70.1%. Region, segment and lane do not |

## Key engineering ideas
- **Quarantine, not deletion.** Every source row ends up in Silver, quarantine, or a counted duplicate. Each Silver job proves `in = out + rejected + duplicates`.
- **Incremental and idempotent.** Source and layer watermarks, advanced only after a job's books balance. A simulated crash between write and watermark recovered with no manual repair (4,749 duplicate rows collapsed in Silver).
- **SCD Type 2 customers.** Facts store the customer version valid on the booking date, which stops a reassignment from rewriting history. Joining on the natural key instead inflated revenue by 7.7% in a test.
- **Late data fixes the past.** A late DELIVERED scan updates the shipment fact; replay and backfill leave a content fingerprint of Gold unchanged.
- **Reconciliation with explained gaps.** Duplicate counts are recomputed independently from Bronze. The unexplained residual in "delivered but no delivery scan" matches the generator's 1% scanner-loss rate.
- **Publish gate.** Power BI reads only the Gold versions that passed reconciliation (Delta time travel on a published marker).

## Performance lab (measured, see docs)
Broadcast join vs sort-merge: 0 vs 20.1 MB shuffled. Column pruning: 1.4 vs 39.9 MB read. Partition pruning on one month: 0.2 MB vs 4.1 MB. Python UDF vs built-in: 5.1 s vs 1.0 s. `collect()` vs aggregation: 7.2 s vs 0.4 s. Key skew: largest/median partition 21.9x, 2.9x with a salt. Caching did **not** help at this size, and the lab says so.

## Semantic layer, DAX, security
KPIs are defined once in the model. Percentages are measures, not stored columns; date roles use `USERELATIONSHIP`; Gross Margin % divides by the revenue of *invoiced* shipments, with a coverage measure beside it. Dynamic RLS maps users to regions through a `Security` table filtering `Origin`, which flows to all three facts. Tested with "View as": North 17.5% of revenue, a two-region user sees the union.

## Repository layout
`src/` pipeline code (generator, bronze, silver, gold, quality, reconciliation, ops, serve, perf) | `api/` mock reference API | `orchestration/` Airflow DAGs | `tests/` | `docs/` | `docker-compose.yml`

## Run it
    docker compose up -d
    python -m src.generator.generate        # synthetic data + answer key
    python -m src.ops.run_local daily --asof 2026-06-07 --fast
    python -m src.serve.export              # Parquet for Power BI
    python -m pytest tests -q

## Limitations (honest list)
- Single machine, local Spark and standalone Airflow. Not a production deployment.
- Reconciliation runs after Gold, not before it; a true pre-Gold gate needs the checks split in two.
- Airflow's failure behaviour was exercised through the local runner that reads the same task spec. A manually triggered Airflow run stalled once, most likely because the DAG was paused, but I did not confirm that.
- The 5% overbilling tolerance is a placeholder pending business sign-off.
- Quarantined shipments (630) and their 1,252 invoice lines (Rs 8.87M) are excluded from the model and exposed in a Data Status table.
- The two most recent days of scan-based KPIs are provisional (late scans); margin is provisional for about 30 days.
- No customer-caused-delay code exists in the data, so Sales' variant of on-time delivery cannot be built.
- Hard deletes at the source and restated FX rates are not detected automatically.
- The ledger used in reconciliation is the generator's answer key; a real deployment would use the finance system.
