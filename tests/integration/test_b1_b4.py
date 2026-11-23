import json
from datetime import datetime,timezone
from decimal import Decimal
from stock_batch.schemas import get


def envelope(topic,offset,obj):
    d=datetime(2026,10,1,tzinfo=timezone.utc)
    return (topic,0,offset,d,d,d.date(),'FPT',{},json.dumps(obj).encode(),None,'1.0','RAW')


def test_ac01_stock_dedup_invalid_and_source_priority(spark,cfg):
    from stock_batch.transforms.stock import clean_stock
    base={'schema_version':'1.0','symbol':'FPT','source':'provider_a',
          'source_price_unit':'VND','bar_start':'2026-10-01T03:30:00Z',
          'open':100000,'high':100200,'low':99900,'close':100100,'volume':100}
    bad=dict(base,high=90000)
    other=dict(base,source='provider_b',volume=999)
    rows=[envelope('stock-price-topic',0,base),envelope('stock-price-topic',1,bad),
          envelope('stock-price-topic',2,other)]
    df=spark.createDataFrame(rows,get('bronze_stock'))
    clean,reject=clean_stock(df,spark,cfg)
    assert clean.count()==1
    assert clean.first()['volume']==100
    assert reject.count()==2
    assert {r['reason_code'] for r in reject.collect()}=={'INVALID_OHLC','DUPLICATE_LOSER'}


def test_ac02_ac03_canonical_url_two_symbols(spark,cfg):
    from stock_batch.transforms.news import clean_news
    base={'url':'https://example.org/news/123?utm_source=a','title':'FPT and HPG agree',
          'content_text':'FPT HPG','source':'press','published_at':'2026-10-01T03:25:00Z',
          'crawled_at':'2026-10-01T03:27:00Z'}
    alt=dict(base,url='http://www.example.org/news/123?utm_source=b')
    data=spark.createDataFrame([envelope('news-topic',0,base),envelope('news-topic',1,alt)],get('bronze_news'))
    n,m,reject=clean_news(data,spark,cfg)
    assert n.count()==1
    assert {x['symbol'] for x in m.collect()}=={'FPT','HPG'}
    assert reject.count()==1


def test_ac04_ac05_features_lunch_break(spark,cfg):
    from stock_batch.transforms.features import stock_features
    from stock_batch.schemas import conform
    from pyspark.sql import functions as F
    starts=['2026-10-01T04:%02d:00+00:00'%x for x in range(25,30)]
    starts+=['2026-10-01T06:%02d:00+00:00'%x for x in range(0,5)]
    # first 5 bars at 11:25 VN and second at 13:00 VN
    data=[]
    for i,t in enumerate(starts):
        d=datetime.fromisoformat(t)
        data.append({'symbol':'FPT','exchange':'HOSE','bar_start':d,'trade_date':d.astimezone(timezone.utc).date(),
          'session_id':'morning' if i<5 else 'afternoon',
          'open':Decimal(100000+i*10),'high':Decimal(100100+i*10),'low':Decimal(99900+i*10),
          'close':Decimal(100000+i*10),'volume':100})
    # Explicit trade date is the VN trading date, equal to UTC for these hours.
    df=conform(spark.createDataFrame(data), 'stock_price')
    out=stock_features(df,spark,cfg).orderBy('window_start').collect()
    assert len(out)==2
    assert out[0]['completeness']==1.0 and out[0]['volume_sum']==500
    assert out[1]['return_simple'] is None


def test_ac07_news_zero_materialized(spark,cfg):
    from stock_batch.transforms.density import news_density
    from stock_batch.schemas import empty
    df=news_density(empty(spark,'news'),empty(spark,'news_stock_mapping'),spark,cfg,['2026-10-01'])
    assert df.count()>0
    assert df.filter('news_count != 0').count()==0
    assert {r['window_size'] for r in df.select('window_size').distinct().collect()}=={'5m','30m','1h'}


def test_ac06_incomplete_bucket_is_not_filled(spark,cfg):
    from stock_batch.transforms.features import stock_features
    from stock_batch.schemas import conform
    from decimal import Decimal
    from datetime import date
    ts=['2026-10-01T03:%02d:00+00:00'%x for x in (30,31,32,34)]
    data=[]
    for x in ts:
        data.append({'symbol':'FPT','exchange':'HOSE','bar_start':datetime.fromisoformat(x),
           'trade_date':date(2026,10,1),'session_id':'morning','open':Decimal(100000),
           'high':Decimal(100500),'low':Decimal(99900),'close':Decimal(100100),'volume':100})
    out=stock_features(conform(spark.createDataFrame(data),'stock_price'),spark,cfg).collect()
    assert len(out)==1
    assert out[0]['completeness']==0.8
    assert out[0]['return_simple'] is None


def test_ac08_missing_publish_time_never_counts(spark,cfg):
    from stock_batch.transforms.news import clean_news
    from stock_batch.transforms.density import news_density
    raw={'url':'https://example.org/missing/1','title':'FPT missing publication time',
         'content_text':'FPT','source':'provider','crawled_at':'2026-10-01T03:30:00Z'}
    n,m,_=clean_news(spark.createDataFrame([envelope('news-topic',1,raw)],get('bronze_news')),spark,cfg)
    assert n.first()['time_quality']=='MISSING'
    d=news_density(n,m,spark,cfg,['2026-10-01'])
    assert d.filter('news_count != 0').count()==0
