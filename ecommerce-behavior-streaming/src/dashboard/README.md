# Dashboard module

Owner: Phương. Implementation is pending.

Build a Streamlit dashboard against agreed serving tables. Show event counts by action, active users, top products, purchase-value proxies, freshness, and provisional/final window status. Include session conversion only after the metric contract is agreed.

Avoid repeatedly scanning all raw HDFS files for interactive refreshes. Coordinate serving-table keys and retry-safe updates with Quyền. Label simulated replay clearly and do not present source-event age as processing latency.

