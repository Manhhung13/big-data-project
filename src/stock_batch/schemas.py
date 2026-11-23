"""Explicit Spark schemas, loaded lazily to let pure unit tests run without PySpark."""

def get(name):
    from pyspark.sql.types import (StructType, StructField as F, StringType as S,
        IntegerType as I, LongType as L, DoubleType as D, BooleanType as B,
        TimestampType as T, DateType as DT, DecimalType as DEC, BinaryType as BIN,
        ArrayType as A, MapType as M)
    d = DEC(20,4)
    types = {'s':S(),'i':I(),'l':L(),'d':D(),'b':B(),'t':T(),'date':DT(),'money':d,
             'bin':BIN(), 'arr':A(S()), 'map':M(S(),S())}
    fields = {
      'bronze_stock': [('topic','s'),('partition','i'),('offset','l'),('kafka_timestamp','t'),('ingested_at','t'),('ingest_date','date'),('message_key','s'),('headers','map'),('raw_payload','bin'),('raw_html_ref','s'),('schema_version','s'),('decode_status','s')],
      'bronze_news': [('topic','s'),('partition','i'),('offset','l'),('kafka_timestamp','t'),('ingested_at','t'),('ingest_date','date'),('message_key','s'),('headers','map'),('raw_payload','bin'),('raw_html_ref','s'),('schema_version','s'),('decode_status','s')],
      'stock_price': [('symbol','s'),('exchange','s'),('bar_start','t'),('bar_end','t'),('trade_date','date'),('session_id','s'),('open','money'),('high','money'),('low','money'),('close','money'),('volume','l'),('source','s'),('source_unit','s'),('quality_flag','s'),('source_record_key','s'),('schema_version','s'),('processed_at','t')],
      'news': [('article_id','s'),('canonical_url','s'),('content_hash','s'),('title','s'),('content_text','s'),('source','s'),('published_at','t'),('crawled_at','t'),('time_quality','s'),('publish_date','date'),('duplicate_group','s'),('source_record_key','s'),('schema_version','s'),('rights_policy','s'),('raw_html_ref','s')],
      'news_stock_mapping': [('article_id','s'),('symbol','s'),('rule_id','s'),('matched_alias','s'),('relevance','d'),('mapping_version','s'),('publish_date','date')],
      'stock_features': [('symbol','s'),('exchange','s'),('window_start','t'),('window_end','t'),('trade_date','date'),('session_id','s'),('time_bucket','s'),('interval','s'),('open','money'),('high','money'),('low','money'),('close','money'),('volume_sum','l'),('bar_count','i'),('expected_bar_count','i'),('completeness','d'),('return_simple','d'),('return_log','d'),('return_missing_reason','s'),('feature_version','s'),('source_snapshot','s'),('run_id','s'),('code_version','s'),('config_hash','s'),('processing_timestamp','t')],
      'news_density': [('symbol','s'),('window_start','t'),('window_end','t'),('window_size','s'),('session_id','s'),('time_bucket','s'),('event_date','date'),('news_count','l'),('eligible_article_count','l'),('mapping_version','s'),('source_snapshot','s'),('run_id','s'),('code_version','s'),('config_hash','s'),('processing_timestamp','t')],
      'stock_baseline': [('baseline_version','s'),('baseline_date','date'),('valid_for_trading_date','date'),('symbol','s'),('exchange','s'),('time_bucket','s'),('session_id','s'),('metric','s'),('window_size','s'),('train_start','date'),('train_end','date'),('sample_count','l'),('mean','d'),('std','d'),('median','d'),('p95','d'),('p99','d'),('baseline_status','s'),('fallback_level','s'),('formula_version','s'),('source_snapshot','s'),('run_id','s'),('code_version','s'),('config_hash','s'),('processing_timestamp','t')],
      'historical_events': [('event_id','s'),('symbol','s'),('event_time','t'),('event_end','t'),('event_date','date'),('event_type','s'),('status','s'),('price_flag','b'),('volume_flag','b'),('news_flag','b'),('z_return','d'),('z_volume','d'),('z_news','d'),('baseline_version','s'),('rule_version','s'),('related_article_ids','arr'),('source_snapshot','s'),('run_id','s'),('code_version','s'),('config_hash','s'),('processing_timestamp','t'),('price_state','s'),('volume_state','s'),('news_state','s'),('baseline_sample_count','l')],
      'event_analysis': [('event_id','s'),('event_date','date'),('horizon','s'),('return_before','d'),('return_after','d'),('volume_z_at_event','d'),('news_count_before','l'),('news_count_after','l'),('cooccurrence_type','s'),('is_complete','b'),('missing_reason','s'),('analysis_version','s'),('source_snapshot','s'),('run_id','s'),('code_version','s'),('config_hash','s'),('processing_timestamp','t')],
      'bronze_bad_records': [('topic','s'),('partition','i'),('offset','l'),('ingest_date','date'),('raw_payload','bin'),('reason_code','s')],
      'quarantine': [('dataset','s'),('reason_code','s'),('source_record_key','s'),('raw_payload','bin'),('record_date','date')],
    }
    return StructType([F(k, types[t], True) for k,t in fields[name]])


def names():
    return ['bronze_stock','bronze_news','stock_price','news','news_stock_mapping','stock_features',
            'news_density','stock_baseline','historical_events','event_analysis','bronze_bad_records','quarantine']


def empty(spark, name):
    return spark.createDataFrame([], get(name))


def conform(df, name):
    from pyspark.sql import functions as F
    schema = get(name)
    for field in schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    return df.select(*[F.col(field.name).cast(field.dataType).alias(field.name) for field in schema.fields])
