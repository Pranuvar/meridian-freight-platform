# Metric dictionary

Rules for every KPI: cancelled shipments and the 630 shipments quarantined in Silver are excluded. Customer attributes are as of the booking date. Currency is INR, with USD converted at the booking-date daily rate. Scan-based KPIs are provisional for 5 days, claim KPIs for 10, invoice-based KPIs for about 30.

| # | KPI | Definition | Source column | Grain | Specific rule or caveat | Owner |
|---|---|---|---|---|---|---|
| 1 | Shipments | Count of shipments booked | fact_shipment | Shipment | Cancelled excluded | Head of Operations |
| 2 | Delivered Shipments | Shipments with a DELIVERED scan | is_delivered | Shipment | Confirmed by scan, not by TMS status | Head of Operations |
| 3 | On-Time Delivery % | Delivered on or before the promise / delivered | is_on_time | Shipment | No grace period (to confirm). Sales' variant excluding customer-caused delay cannot be built: no delay-reason code | Head of Operations |
| 4 | Average Delay (late, hours) | Average delay over late shipments | delay_hours | Shipment | Late shipments only | Head of Operations |
| 5 | Average Transit Hours | Pickup scan to delivery scan | transit_hours | Shipment | Needs both scans | Network Operations |
| 6 | Revenue (Rs) | Freight + fuel surcharge + detention billed | revenue_total_inr | Shipment | Attributed to the booking date | Finance Controller |
| 7 | Carrier Cost (Rs) | Invoiced carrier cost, GST excluded | carrier_cost_invoiced_inr | Shipment | NULL until invoiced (not zero) | Finance Controller |
| 8 | Gross Margin (Rs) | Revenue - carrier cost | margin_inr | Shipment | Invoiced shipments only | Finance Controller |
| 9 | Gross Margin % | Margin / revenue of invoiced shipments | margin_inr, revenue_total_inr | Shipment | Denominator matches the numerator's population; Margin Coverage % shown beside it | Finance Controller |
| 10 | Quote Variance (Rs) | Invoiced cost - quoted cost | quote_variance_inr | Shipment | Positive = paid more than quoted | Carrier Procurement |
| 11 | Overbilled Shipments % | Invoiced over quote by more than 5% / shipments with both | is_overbilled | Shipment | 5% tolerance is a placeholder pending Finance sign-off | Carrier Procurement |
| 12 | Claim Rate % / Claim Amount | Shipments with a claim / delivered; sum of claim value | claim_count, claim_amount_inr | Shipment | Provisional for 10 days | Customer Service Manager |
| 13 | Delivery Confirmation Gap % | TMS-delivered with no delivery scan / TMS-delivered | status_delivered_no_scan | Shipment | Data-quality KPI, about 1.5% | Data Platform Owner |
| 14 | Late-Arriving Scan % | Scans that arrived after their event date / all scans | is_late_arrival | Scan event | About 10% in this data by construction | Data Platform Owner |
| 15 | Hours Since Previous Scan | Average gap to the previous scan of the shipment | hours_since_prev_scan | Scan event | Time leading up to the scan, not dwell time | Network Operations |
| 16 | Data-Quality Exclusions | Quarantined shipments; carrier cost with no shipment in the model | Data Status | Run | 630 shipments; Rs 8,873,944.98 | Data Platform Owner |
