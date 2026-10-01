# Lineage for three KPIs

## On-Time Delivery %
| Step | Where | Logic |
|---|---|---|
| Source | `scans_YYYYMMDD.parquet`; `tms.shipments.promised_delivery_at` | DELIVERED scan time lives only in the scan files; the promise lives in the TMS |
| Bronze | `bronze/scans`, `bronze/tms_shipments` | `event_ts` kept as a string in 3 formats; `_arrival_date` taken from the file name |
| Silver | `silver/scans`, `silver/shipments` | `parse_event_ts` (day-first dd/MM), refs normalised to `MFL-yyyy-nnnnnnn`, duplicates removed, orphans quarantined |
| Gold | `fact_shipment` | `delivered_at` = earliest DELIVERED scan; `is_on_time` = `delivered_at <= promised_delivery_at`, NULL until delivered |
| Semantic | `Shipments[Is On Time]` | `On-Time Delivery %` = On-Time Shipments / Delivered Shipments, cancelled excluded |
| Visual | KPI cards, carrier chart, matrix | |
| Proof | Generator answer key | 0 mismatches on delivered timestamps and flags; carrier percentages within 0.02 pp |

## Revenue (Rs)
| Step | Where | Logic |
|---|---|---|
| Source | `tms.shipment_charges`; FX API | Freight, fuel surcharge, detention; export shipments billed in USD |
| Bronze | `bronze/tms_shipment_charges`, `bronze/api_fx_rates` | As received |
| Silver | `silver/shipment_charges` (decimal), `silver/fx_rates` | Charges of a rejected shipment quarantined as `parent_shipment_missing` |
| Gold | `fact_shipment.revenue_total_inr` | Sum of charges x FX rate at the booking date (weekends carry the last rate forward) |
| Semantic | `Revenue Net (Rs)` | `SUM(revenue_total_inr)`, cancelled excluded |
| Proof | Reconciliation | INR exact to the paisa; USD gap to the flat-rate ledger = FX effect within Rs 0.49 |

## Gross Margin %
| Step | Where | Logic |
|---|---|---|
| Source | Carrier invoice CSVs (3 layouts) | Freight, fuel, detention lines per carrier |
| Bronze | `bronze/invoices_blz`, `_swf`, `_trk` | All strings; each layout has its own table |
| Silver | `silver/carrier_invoice_lines` | Three parsers into one schema; decimal commas and thousands separators handled; duplicates and ghost lines removed or quarantined |
| Gold | `fact_carrier_invoice_line`, then `fact_shipment.carrier_cost_invoiced_inr` and `margin_inr` | Cost is NULL (not zero) until an invoice arrives; margin = revenue - invoiced cost |
| Semantic | `Gross Margin %` | Gross margin / revenue of **invoiced** shipments; `Margin Coverage %` shown beside it |
| Proof | Generator answer key | 0 per-shipment cost mismatches; invoice cost with no shipment (Rs 8,873,944.98) visible in Data Status |
