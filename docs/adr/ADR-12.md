# ADR-12: Cluster version matrix

**Status:** PROPOSED (not stakeholder-approved).

**Pilot rule:** Spark/PySpark 3.5.9 Java17 (+ spark-sql-kafka-0-10_2.12:3.5.9, kafka-clients 3.4.1, commons-pool2 2.11.1 baked into the image), Hadoop 3.2.1 Docker demo (bde2020), Kafka 3.9.0 (apache/kafka). HDFS and Kafka smoke run in the isolated stockbatch-verify project (see docs/BUILD_AUDIT.md).

**Decision gate:** Approve after matching a real source payload, exchange calendar or measured acceptance test as relevant.

**Risk:** Changing this rule affects result reproducibility, data contracts and regression fixtures. Increment formula/mapping/schema versions if semantics change.
