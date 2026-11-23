"""Kafka -> Bronze: incremental, exactly-once at the catalog level (FR-01).

* The offset range ``[committed, latest)`` of every topic partition is resolved ONCE on the driver
  (KafkaConsumer via py4j) and passed to Spark as explicit JSON start/end offsets, and the batch
  is staged once to Parquet under the run id, so every action sees exactly the same records.
* Records already in Bronze are removed by an anti-join on ``(topic, partition, offset)``.
* Only the touched ``ingest_date`` partitions are rewritten.
* The next offsets are stored in the catalog commit itself (``kafka_offsets``), i.e. atomically
  with the data. Rollback restores them together with Bronze.
* Kafka headers are kept as ``map<string,string>``; undecodable payloads also go to bad_records.

``ingest_frames`` is the broker-free core used by tests: it takes Kafka-shaped DataFrames
(topic, partition, offset, timestamp, key, value[, headers]).
"""
from __future__ import annotations

import json

from pyspark.sql import functions as F

from stock_batch.storage.catalog import Transaction, partition_key
from stock_batch.transforms.stock import is_json_object

HEADERS_EXPR = ("map_from_entries(transform(filter(headers, (h, i) -> NOT exists(slice(headers, 1, i), "
                "p -> p.key = h.key)), h -> struct(h.key AS key, cast(h.value AS string) AS value)))")


def bronze_rows(kafka_df, cfg):
    """Kafka-shaped DataFrame -> (bronze envelope rows, bad records). Pure; no I/O."""
    js = F.col('value').cast('string')
    sv = F.get_json_object(js, '$.schema_version')
    status = (F.when(~is_json_object(js), 'INVALID_JSON')
              .when(sv.isNull(), 'MISSING_SCHEMA_VERSION').otherwise('OK'))
    headers = (F.expr(HEADERS_EXPR) if 'headers' in kafka_df.columns
               else F.lit(None).cast('map<string,string>'))
    ingested = F.current_timestamp()
    data = kafka_df.select(
        'topic', F.col('partition').cast('int').alias('partition'), F.col('offset').cast('long').alias('offset'),
        F.col('timestamp').alias('kafka_timestamp'), ingested.alias('ingested_at'),
        F.to_date(F.from_utc_timestamp(ingested, 'Asia/Ho_Chi_Minh')).alias('ingest_date'),
        F.col('key').cast('string').alias('message_key'), headers.alias('headers'),
        F.col('value').alias('raw_payload'),
        F.when(is_json_object(js), F.get_json_object(js, '$.raw_html_ref')).alias('raw_html_ref'),
        F.coalesce(sv, F.lit('UNKNOWN')).alias('schema_version'), status.alias('decode_status'))
    return data, bad_records(data)


def bad_records(data):
    return (data.filter(F.col('decode_status') != 'OK')
            .select('topic', 'partition', 'offset', 'ingest_date', 'raw_payload',
                    F.col('decode_status').alias('reason_code')))


def _append_new(tx, name, rows):
    """Anti-join on (topic, partition, offset); rewrite only touched partitions. Returns new rows.

    ``rows`` must be deterministic on re-evaluation (it is read from the staged Parquet), and the
    existing Bronze it is compared with comes from immutable catalog paths.
    """
    keys = ['topic', 'partition', 'offset']
    lows = rows.groupBy('topic', 'partition').agg(F.min('offset').alias('_lo'))
    existing = (tx.read(name).select(*keys).join(F.broadcast(lows), ['topic', 'partition'])
                .filter(F.col('offset') >= F.col('_lo')).select(*keys))
    new = rows.join(existing, keys, 'left_anti').dropDuplicates(keys)
    touched = sorted({partition_key(r[0]) for r in new.select('ingest_date').distinct().collect()})
    count = new.count()
    if touched:
        tx.write(name, tx.read(name, partitions=touched).unionByName(new), only_partitions=touched)
    return count


def staging_root(tx):
    return f'{tx.root}/_system/ingest_staging/run_id={tx.run_id}'


def ingest_frames(spark, cfg, tx, frames, next_offsets=None):
    """Ingest Kafka-shaped DataFrames ``{'bronze_stock'|'bronze_news': df}`` into ``tx``.

    Each batch is first written once to Parquet staging under the run id (BRD 8.1 step 3), so the
    Kafka source is read exactly once and every later action re-reads identical staged rows.
    (Stages that mix driver-local and HDFS partitions hang in local mode unless
    ``spark.locality.wait=0s``; ``check_local_scheduling`` enforces that.)

    ``next_offsets`` ({topic: {partition: next_offset}}) is what the reader resolved; when omitted
    (tests) it is derived as max(offset)+1 of the batch. Offsets are merged into the catalog.
    """
    offsets = tx.catalog.setdefault('kafka_offsets', {})
    for name, kdf in frames.items():
        staged = f'{staging_root(tx)}/{name}'
        bronze_rows(kdf, cfg)[0].write.mode('errorifexists').parquet(staged)   # the only Kafka read
        data = spark.read.parquet(staged)
        ranges = [r.asDict() for r in data.groupBy('topic', 'partition').agg(
            F.min('offset').alias('first'), F.max('offset').alias('last'), F.count('*').alias('records')).collect()]
        new_rows = _append_new(tx, name, data)
        bad_rows = _append_new(tx, 'bronze_bad_records', bad_records(data))
        read = sum(r['records'] for r in ranges)
        tx.quality[name] = {'kafka_offset_ranges': ranges, 'records_read': read, 'new_records': new_rows,
                            'duplicates_skipped': read - new_rows, 'bad_records_new': bad_rows}
        for r in ranges:
            topic_offsets = offsets.setdefault(r['topic'], {})
            nxt = int(r['last']) + 1
            topic_offsets[str(r['partition'])] = max(int(topic_offsets.get(str(r['partition']), 0)), nxt)
    for topic, parts in (next_offsets or {}).items():
        topic_offsets = offsets.setdefault(topic, {})
        for p, nxt in parts.items():
            topic_offsets[str(p)] = max(int(topic_offsets.get(str(p), 0)), int(nxt))
    return tx.quality


def resolve_offsets(spark, brokers, topic, committed, max_records=None):
    """{partition: (start, end)} for ``topic`` using a KafkaConsumer on the driver JVM."""
    jvm = spark._jvm
    props = jvm.java.util.Properties()
    props.put('bootstrap.servers', brokers)
    props.put('key.deserializer', 'org.apache.kafka.common.serialization.ByteArrayDeserializer')
    props.put('value.deserializer', 'org.apache.kafka.common.serialization.ByteArrayDeserializer')
    props.put('request.timeout.ms', '30000')
    props.put('default.api.timeout.ms', '60000')
    consumer = jvm.org.apache.kafka.clients.consumer.KafkaConsumer(props)
    try:
        infos = consumer.partitionsFor(topic)
        tps = jvm.java.util.ArrayList()
        for info in (infos or []):
            tps.add(jvm.org.apache.kafka.common.TopicPartition(topic, info.partition()))
        if tps.isEmpty():
            return {}, []
        begin, end = consumer.beginningOffsets(tps), consumer.endOffsets(tps)
        out, gaps = {}, []
        for tp in tps:
            p = int(tp.partition())
            lo, hi = int(begin.get(tp)), int(end.get(tp))
            start = int(committed.get(str(p), lo))
            if start < lo:                     # retention removed records we never ingested
                gaps.append({'partition': p, 'committed': start, 'earliest_available': lo})
                start = lo
            stop = hi if max_records is None else min(hi, start + int(max_records))
            out[p] = (start, max(stop, start))
        return out, gaps
    finally:
        consumer.close()


def ingest_kafka(spark, cfg, brokers='kafka:9092', max_records_per_partition=None):
    """Read [committed, latest) of both topics once and commit Bronze + offsets atomically."""
    from stock_batch.jobs.pipeline import check_session
    check_session(spark)
    tx = Transaction(spark, cfg, operation='ingest-kafka')
    tx.begin(operation='ingest-kafka', brokers=brokers)
    try:
        frames, resolved, gaps = {}, {}, {}
        committed = tx.catalog.get('kafka_offsets', {})
        for name, topic in (('bronze_stock', cfg['kafka']['stock_topic']), ('bronze_news', cfg['kafka']['news_topic'])):
            ranges, topic_gaps = resolve_offsets(spark, brokers, topic, committed.get(topic, {}),
                                                 max_records_per_partition)
            if topic_gaps:
                gaps[topic] = topic_gaps
            if not ranges:
                continue
            resolved[topic] = {p: e for p, (_, e) in ranges.items()}
            if all(s == e for s, e in ranges.values()):
                continue
            start = json.dumps({topic: {str(p): s for p, (s, _) in ranges.items()}})
            end = json.dumps({topic: {str(p): e for p, (_, e) in ranges.items()}})
            frames[name] = (spark.read.format('kafka').option('kafka.bootstrap.servers', brokers)
                            .option('subscribe', topic).option('startingOffsets', start)
                            .option('endingOffsets', end).option('includeHeaders', 'true')
                            .option('failOnDataLoss', 'true').load())
        ingest_frames(spark, cfg, tx, frames, next_offsets=resolved)
        if gaps:
            tx.quality['offset_gaps'] = gaps
            tx.warnings.append(f'Kafka retention removed un-ingested offsets: {gaps}')
        tx.commit()
        tx.fs.delete(staging_root(tx), recursive=True)   # failed runs keep staging for diagnosis
        return tx.run_id
    except BaseException as exc:
        tx.fail(exc)
        raise
