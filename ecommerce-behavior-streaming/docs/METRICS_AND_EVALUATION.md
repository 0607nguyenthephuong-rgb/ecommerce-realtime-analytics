# Metrics and evaluation protocol

These are proposed project definitions, not measured findings.

## Behavioral metrics

| Metric | Definition | Qualification |
| --- | --- | --- |
| Event count | Accepted events grouped by action and event-time window | Include all four actions |
| Active users | Distinct `user_id` in a stated window | If approximated, report the algorithm and error setting |
| Product popularity | Top products ranked by view count | State window and tie-breaking rule |
| Purchase events | Count of accepted `purchase` records | Does not directly count orders |
| Purchase value proxy | Sum of nonnegative prices on accepted purchase records | Not audited revenue; no order ID, quantity, refund, or currency assumptions |
| Cart removal count | Count of accepted `remove_from_cart` records | Does not directly measure abandoned sessions |
| Session purchase rate | Distinct observed sessions with a purchase / distinct observed sessions | Use `(user_id, user_session)`; exclude missing sessions and report them |
| Ordered funnel conversion | Sessions with view → cart → purchase for the same product within the agreed horizon / eligible viewed sessions | Group by user, session, and product; define ties and observation boundaries |

For an initial funnel experiment, propose a 30-minute horizon and strict event-time ordering. Report simultaneous-timestamp sequences as unresolved rather than inventing a true order. Only include denominator events with a complete 30-minute follow-up window; distinguish right-censored sessions. Define how cart removal affects the funnel before implementing it.

## System measurements

- **Input rate:** successfully acknowledged Kafka events divided by producer elapsed time.
- **Processing throughput:** accepted output records per second over the measured interval.
- **Transport backlog:** Kafka end offsets minus the offsets actually processed by Spark, per partition. Spark progress is authoritative for this job; it need not commit a conventional consumer-group offset.
- **Operational latency:** processing/display time minus producer `emitted_at`, never minus a 2019/2020 source timestamp. Synchronize clocks when services run on different machines.
- **Window publication delay:** track provisional and final aggregate publication separately. Watermark delay and dashboard refresh affect final visibility.
- **Resources:** CPU, RAM, disk/network use, file count, and relevant Spark state size.

Record at least three achievable load levels and repeat each scenario at least three times after warm-up. Report measured rates, p50/p95 latency, duration, hardware, service versions, and input subset. Do not claim a target rate was achieved merely because it was configured.

## Correctness and recovery scenarios

| Scenario | Expected evidence |
| --- | --- |
| Small deterministic input | Event counts and value sums match a hand-checked reference |
| Replay of the same records | Raw counts increase; clean behavior matches the declared deduplication scope |
| Late records | Within-policy and beyond-policy outcomes are counted explicitly |
| Invalid payloads | Rejection count and reason reconcile with raw input |
| Restart after interruption | No unexplained gaps or duplicate analytical effects in the tested interval |
| Increasing load | Rate, backlog, latency, resources, and failure points are reported together |

The included synthetic fixture has eight rows: three views, three cart additions, one cart removal, and one purchase. Its nonnegative purchase-value proxy is 12.50. It is a fixture expectation, not a result from the Kaggle data.

