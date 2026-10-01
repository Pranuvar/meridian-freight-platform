# Technology decision record (Problem, Options, Decision, Reason)

| # | Problem | Options | Decision | Reason |
|---|---|---|---|---|
| 1 | Transform ~2M scan rows and join them to shipments and invoices | pandas, SQL only, PySpark | PySpark | Scan, invoice and shipment joins and windows scale past one process, and Spark's execution model was a stated learning goal. Honest caveat: at this size pandas would also work, and measured Spark advantages are small |
| 2 | Storage with upserts and safe replays | CSV, Parquet, Delta | Delta Lake | ACID MERGE for incremental loads and SCD2, time travel for the publish marker and for before/after checks |
| 3 | Where Spark runs | Databricks, cluster, local | Local Spark | Free and reproducible; the Spark UI is still available |
| 4 | Surrogate keys | Sequence numbers, hashes | Deterministic 64-bit hash | Reruns never change keys already used by facts; no shared counter. Collision is checked on every write |
| 5 | Customer history | Overwrite, SCD2 | SCD2 on segment, account manager, region | Reassignments must not rewrite old revenue. Which columns are Type 2 is a business choice |
| 6 | Bad records | Drop, fix silently, reject | Reject to quarantine with reason; warn and standardise where recoverable | Every row stays accountable, so reconciliation can balance |
| 7 | What is new? | Full reload, source watermark, file listing | Watermark on `updated_at` for Postgres; file names derived from Bronze for files; layer watermark on `_ingested_at` | A late record has an old event time but a new arrival time |
| 8 | Orchestration | cron, Airflow | Airflow reading one task spec | Real dependencies, retries and a gate; the same spec feeds the local runner and the tests |
| 9 | Streaming | Kafka and Spark streaming, batch | Batch | Scans arrive in daily files and reports refresh daily; streaming would add cost with no benefit |
| 10 | How Power BI reads Gold | Direct Lake, DirectQuery, Import of Parquet | Import of Parquet exported from the published Delta version | Works with local Delta; the publish gate stays enforced |
| 11 | Percentages | Stored columns, measures | Measures; row-level flags stay in Gold | A stored percentage cannot be re-aggregated correctly |
| 12 | Security | Static roles, dynamic | Dynamic RLS from a Security table | One role instead of one per region; supports users with several regions |
