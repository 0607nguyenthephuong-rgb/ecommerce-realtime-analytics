# Infrastructure implementation contract

Owner: Anh. HDFS co-owner: Quyết.

Add the deployment here after the team chooses its runtime environment. No runnable Docker Compose file is included yet.

Deliverables:

- Exact Kafka, Spark, Hadoop, Java, and Python versions.
- Broker listeners reachable from both host clients and containers.
- Explicit topic creation, partition count, and retention configuration.
- Persistent Kafka/HDFS volumes and unique Spark checkpoint paths.
- Startup, health-check, shutdown, and recovery commands.
- Machine resource limits and the distinction between a local demo and multi-node deployment.

Do not advertise replication or distributed fault tolerance unless the actual deployment and tests support those claims. Raw data, credentials, and volumes stay outside version control.

