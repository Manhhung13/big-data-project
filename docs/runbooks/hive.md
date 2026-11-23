# Hive Metastore integration (FR-13 / AC-16, P1)

`register-hive` creates, for the newest catalog generation `g`, one EXTERNAL Parquet table per Silver/Gold dataset
named `<dataset>_g<g>` whose partitions point (`ALTER TABLE ... ADD PARTITION ... LOCATION`) at the **immutable**
partition paths of that generation, plus a view `current_<dataset>` on it. Parquet is never rewritten and `DROP
TABLE` never deletes lake data. Bronze and quarantine are not registered.

## Embedded Derby metastore (pilot / tests)

```powershell
docker compose --profile tools run --rm spark-job --env dev register-hive --database stock_batch --embedded-metastore /tmp/hive-meta
```

Single-process only (Derby). `tests/integration/test_hive_embedded.py` registers the tables in one subprocess and
queries them in another, asserting the table type is EXTERNAL and that
`SELECT * FROM stock_batch.current_stock_features WHERE trade_date = DATE'2026-10-07'` reads only the files of that
partition (`inputFiles`) with a `PartitionFilters` entry in the physical plan.

## Shared metastore (team / production)

Provision a compatible Hive Metastore (thrift) with its DB, set `HIVE_METASTORE_URIS=thrift://host:9083` for the
driver and run `register-hive --database stock_batch`. Re-run it after each publication you want visible in SQL;
the `current_*` views are switched per dataset (not atomically across datasets — readers that need a consistent
multi-table snapshot should use `LakeReader` or the versioned `*_g<g>` tables).

Not verified here: a shared metastore service, concurrent SQL clients, Hive/Trino engines other than Spark.
