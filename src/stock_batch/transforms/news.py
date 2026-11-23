"""B2 — canonical article dedup, HTML cleanup and multi-symbol dictionary mapping (FR-03, FR-04)."""
from pyspark.sql import Window, functions as F, types as T

from stock_batch.reference import effective_on, symbols
from stock_batch.rules import canonical_url, clean_text
from stock_batch.schemas import conform
from stock_batch.transforms.stock import is_json_object

NEWS_JSON = T.StructType([T.StructField(x, T.StringType()) for x in
    ('schema_version', 'article_id', 'url', 'title', 'content_text', 'content_html', 'raw_html_ref',
     'published_at', 'crawled_at', 'source', 'rights_policy')])


def _udfs():
    # Python UDFs only where Spark 3.5 has no native equivalent: URL query normalisation
    # (parse/sort/re-encode params) and HTML-entity decoding + Unicode NFC normalisation.
    return F.udf(canonical_url, T.StringType()), F.udf(clean_text, T.StringType())


def short_hash(col, length=24):
    """Native equivalent of rules.stable_id."""
    return F.substring(F.sha2(col, 256), 1, length)


def alias_pattern(alias_col):
    r"""Case-insensitive (Unicode) whole-word regex; alias quoted with \Q..\E, never interpreted."""
    quoted = F.regexp_replace(alias_col, r'\\E', r'\\E\\\\E\\Q')
    return F.concat(F.lit(r'(?iu)(?<![\p{L}\p{N}])\Q'), quoted, F.lit(r'\E(?![\p{L}\p{N}])'))


def clean_news(bronze, spark, cfg, *, cache_classified=False):
    """Return ``(news, mapping, rejects[, classified])`` for the full Bronze news history."""
    canon, clean = _udfs()
    rows = (bronze.withColumn('json', F.col('raw_payload').cast('string'))
        .withColumn('_valid_json', is_json_object(F.col('json')))
        .withColumn('x', F.when(F.col('_valid_json'), F.from_json('json', NEWS_JSON)))
        .withColumn('canonical_url', canon('x.url'))
        .withColumn('title', clean('x.title'))
        .withColumn('content_text', clean(F.coalesce('x.content_text', 'x.content_html')))
        .withColumn('published_at', F.to_timestamp('x.published_at'))
        .withColumn('crawled_at', F.to_timestamp('x.crawled_at'))
        .withColumn('source', F.col('x.source'))
        .withColumn('rights_policy', F.col('x.rights_policy'))
        .withColumn('raw_html_ref', F.coalesce('x.raw_html_ref', 'raw_html_ref'))
        .withColumn('source_record_key', F.concat_ws(':', 'topic', 'partition', 'offset'))
        .withColumn('content_hash', F.sha2(F.lower(F.concat_ws(' ', F.coalesce('title', F.lit('')),
                                                                F.coalesce('content_text', F.lit('')))), 256))
        .withColumn('article_id', short_hash(F.coalesce('canonical_url', 'content_hash')))
        .withColumn('time_quality', F.when(F.col('published_at').isNotNull(), 'TRUSTED').otherwise('MISSING'))
        .withColumn('publish_date', F.to_date(F.from_utc_timestamp('published_at', 'Asia/Ho_Chi_Minh')))
        .withColumn('schema_version', F.lit(cfg['project']['schema_version'])))
    # Syndication (same normalised title + body under different URLs) shares one duplicate_group;
    # density counts groups, not copies (P5, ADR-05). Policy 'article' restores 1 article = 1 group.
    policy = cfg['news'].get('duplicate_group_policy', 'content_hash')
    rows = rows.withColumn('duplicate_group', F.col('article_id') if policy == 'article'
                           else F.concat(F.lit('dg_'), F.substring('content_hash', 1, 24)))
    reason = (F.when(~F.col('_valid_json'), 'INVALID_JSON')
        .when(F.col('canonical_url').isNull(), 'INVALID_URL')
        .when(F.col('title').isNull(), 'EMPTY_TITLE')
        .when(F.col('source').isNull(), 'MISSING_SOURCE'))
    rows = rows.withColumn('reason_code', reason)
    # The latest received version of an article wins (a correction re-crawls / re-sends it);
    # a valid record always beats malformed candidates sharing its ID.
    w = Window.partitionBy('article_id').orderBy(
        F.when(F.col('reason_code').isNull(), F.lit(0)).otherwise(F.lit(1)),
        F.col('crawled_at').desc_nulls_last(),
        F.col('kafka_timestamp').desc_nulls_last(),
        F.col('offset').desc(),
        F.col('partition').desc(),
        F.col('topic').asc())
    classified = rows.withColumn('_candidate_rank', F.row_number().over(w))
    classified = classified.withColumn(
        'reason_code',
        F.when(F.col('reason_code').isNull() & (F.col('_candidate_rank') > 1), F.lit('DUPLICATE_LOSER'))
         .otherwise(F.col('reason_code')))
    if cache_classified:
        from pyspark import StorageLevel
        classified = classified.persist(StorageLevel.MEMORY_AND_DISK)  # caller unpersists
    news = classified.filter(F.col('reason_code').isNull())

    # Versioned alias dictionary; aliases are cleaned/NFC-normalised exactly like the text.
    aliases = (symbols(spark, cfg).withColumn('_alias', F.explode('aliases_array'))
               .withColumn('matched_alias', clean('_alias'))
               .filter(F.col('matched_alias').isNotNull())
               .withColumn('_pattern', alias_pattern(F.col('matched_alias')))
               .select('symbol', 'matched_alias', '_pattern', 'effective_from', 'effective_to'))
    text = F.concat_ws(' ', 'title', 'content_text')
    mapping = (news.select('article_id', 'title', 'content_text', 'publish_date')
               .crossJoin(F.broadcast(aliases))
               .where(F.col('publish_date').isNotNull() & effective_on(F.col('publish_date')))
               .withColumn('_text', text)
               .where(F.expr('_text RLIKE _pattern')))
    w2 = Window.partitionBy('article_id', 'symbol').orderBy(F.length('matched_alias').desc(),
                                                            F.col('matched_alias').asc())
    mapping = (mapping.withColumn('_n', F.row_number().over(w2)).filter('_n = 1')
        .withColumn('rule_id', F.lit('alias_case_insensitive_word'))
        .withColumn('relevance', F.lit(1.0))
        .withColumn('mapping_version', F.lit(cfg['news']['mapping_version'])))
    rejects = (classified.filter(F.col('reason_code').isNotNull())
               .withColumn('dataset', F.lit('news'))
               .withColumn('record_date', F.col('ingest_date')))
    result = (conform(news, 'news'), conform(mapping, 'news_stock_mapping'), conform(rejects, 'quarantine'))
    return (*result, classified) if cache_classified else result
