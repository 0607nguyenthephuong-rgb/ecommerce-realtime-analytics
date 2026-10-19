# Batch analysis module

Owner: Quyết. Implementation is pending.

Read accepted clean events from HDFS and reproduce the streaming metric definitions over the same event population. Compare event-type counts, distinct users, product rankings, and purchase-value proxies. Record how missing sessions, negative prices, duplicates, and late exclusions are handled.

The batch reference must use the same cutoff and finalization policy as streaming. A full-history count is not automatically comparable to a partially finalized streaming window.

