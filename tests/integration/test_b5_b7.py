from datetime import date,datetime,timezone,timedelta
from pyspark.sql import functions as F
from stock_batch.schemas import conform,empty


def test_ac09_ac10_baseline_asof(spark,cfg):
    from stock_batch.transforms.baseline import build_baselines
    base=datetime(2026,10,1,3,30,tzinfo=timezone.utc)
    data=[]
    for d in (1,2,5):
        t=base.replace(day=d)
        data.append({'symbol':'FPT','exchange':'HOSE','trade_date':date(2026,10,d),
             'session_id':'morning','time_bucket':'10:30','window_start':t,
             'window_end':t+timedelta(minutes=5),'interval':'5m','volume_sum':100,
             'return_simple':0.01,'completeness':1.0})
    f=conform(spark.createDataFrame(data),'stock_features')
    d=empty(spark,'news_density')
    b=build_baselines(f,d,spark,cfg,['2026-10-05']).filter("metric='volume_sum'").collect()
    assert len(b)==1
    assert b[0]['sample_count']==2
    assert b[0]['train_end']<b[0]['valid_for_trading_date']
    assert b[0]['baseline_status']=='ZERO_STD'


def test_ac11_composite_event(spark,cfg):
    from stock_batch.transforms.events import historical_events
    t=datetime(2026,10,7,3,30,tzinfo=timezone.utc)
    f=conform(spark.createDataFrame([{'symbol':'FPT','exchange':'HOSE','trade_date':date(2026,10,7),
      'session_id':'morning','time_bucket':'10:30','window_start':t,
      'window_end':t+timedelta(minutes=5),'interval':'5m','return_simple':0.2,
      'volume_sum':8000,'completeness':1.0}]),'stock_features')
    d=conform(spark.createDataFrame([{'symbol':'FPT','event_date':date(2026,10,7),
      'session_id':'morning','time_bucket':'10:30','window_start':t,
      'window_end':t+timedelta(minutes=5),'window_size':'30m','news_count':10}]),'news_density')
    bs=[]
    for metric,win,mean,std,p99 in [('return_simple','5m',0.01,0.01,0.09),
        ('volume_sum','5m',100.0,100.0,800.0),('news_count','30m',1.0,1.0,4.0)]:
        bs.append({'symbol':'FPT','session_id':'morning','time_bucket':'10:30',
          'metric':metric,'window_size':win,'mean':mean,'std':std,'p99':p99,
          'baseline_status':'READY','baseline_version':'bl_test',
          'valid_for_trading_date':date(2026,10,7),'train_end':date(2026,10,6)})
    b=conform(spark.createDataFrame(bs),'stock_baseline')
    ev=historical_events(f,d,b,empty(spark,'news'),empty(spark,'news_stock_mapping'),cfg).collect()
    assert len(ev)==1
    assert ev[0]['event_type']=='COMPOSITE'
    assert ev[0]['price_flag'] and ev[0]['volume_flag'] and ev[0]['news_flag']


def test_ac13_study_unknown_future(spark,cfg):
    from stock_batch.transforms.study import event_study
    t=datetime(2026,10,7,3,35,tzinfo=timezone.utc)
    events=conform(spark.createDataFrame([{'event_id':'demo_event','symbol':'FPT',
      'event_time':t,'event_date':date(2026,10,7),'event_type':'PRICE_ONLY'}]),'historical_events')
    result=event_study(events,empty(spark,'stock_features'),empty(spark,'news_density'),spark,cfg)
    out={r['horizon']:r for r in result.collect()}
    assert len(out)==3
    assert all(r['is_complete'] is False for r in out.values())
    assert all(r['return_after'] is None for r in out.values())
    assert all(r['missing_reason'] for r in out.values())


def test_ac12_merge_two_adjacent_event_buckets(spark,cfg):
    from stock_batch.transforms.events import historical_events
    start=datetime(2026,10,7,3,30,tzinfo=timezone.utc)
    frows=[]; brows=[];drows=[]
    for n in (0,1):
        t=start+timedelta(minutes=5*n)
        local='10:'+str(30+5*n)
        frows.append({'symbol':'FPT','exchange':'HOSE','trade_date':date(2026,10,7),
          'session_id':'morning','time_bucket':local,'window_start':t,
          'window_end':t+timedelta(minutes=5),'interval':'5m','return_simple':0.2,
          'volume_sum':8000,'completeness':1.0})
        drows.append({'symbol':'FPT','event_date':date(2026,10,7),'session_id':'morning',
          'time_bucket':local,'window_start':t-timedelta(minutes=25),
          'window_end':t+timedelta(minutes=5),'window_size':'30m','news_count':10})
        for metric,win in [('return_simple','5m'),('volume_sum','5m'),('news_count','30m')]:
            brows.append({'symbol':'FPT','session_id':'morning','time_bucket':local,
              'metric':metric,'window_size':win,'mean':1.0 if metric!='return_simple' else 0.0,
              'std':1.0,'p99':1.0 if metric!='return_simple' else 0.01,
              'baseline_status':'READY','baseline_version':'bl_test',
              'valid_for_trading_date':date(2026,10,7),'train_end':date(2026,10,6)})
    events=historical_events(conform(spark.createDataFrame(frows),'stock_features'),
         conform(spark.createDataFrame(drows),'news_density'),
         conform(spark.createDataFrame(brows),'stock_baseline'),
         empty(spark,'news'),empty(spark,'news_stock_mapping'),cfg)
    actual_end = events.first()["event_end"]
    expected_end = start + timedelta(minutes=10)

    # So sánh cùng thời điểm Unix timestamp
    assert actual_end.timestamp() == expected_end.timestamp()
