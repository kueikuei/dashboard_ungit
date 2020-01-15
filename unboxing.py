#!/usr/bin/env python
# coding: utf-8

# In[32]:


# 執行 - 也可切換內核至2再切回來
# !pip install -r requirements.txt


# In[1]:


# 套件載入
from google.cloud import bigquery
from google.cloud import firestore
import pandas
from datetime import datetime
import pytz


# In[2]:


# Query Box
qryStrAll = """
-- 註解: 
-- gsce = goodscode
-- nbxot = noboxout
-- s = split

-- DECLARE start_traceback_daynum INT64 DEFAULT -30;
-- DECLARE end_traceback_daynum INT64 DEFAULT -6;
DECLARE start_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @start_traceback_daynum HOUR), "Asia/Taipei");
DECLARE end_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @end_traceback_daynum HOUR), "Asia/Taipei");

-- 原始表格
with t_origin as 
(
SELECT *,case when dgroupname='贈品' or setGoodflag='贈品 'then 1 else 0 end as isgift
FROM `momo-develop.boxSaver.slipInfo`  
-- where date(orderdate)>='2020-01-13' and date(orderdate)<'2020-01-14' and delytype='乙配'
WHERE DATETIME(orderDate) BETWEEN start_datetime AND end_datetime and delytype='乙配'  
),
-- Q台表格
t_origin_Q as 
(
SELECT *,case when dgroupname='贈品' or setGoodflag='贈品 'then 1 else 0 end as isgift
FROM `momo-develop.boxSaver.regularQC_slipInfo`   
-- where date(orderdate)>='2020-01-13' and date(orderdate)<'2020-01-14' and delytype='乙配'
WHERE DATETIME(orderDate) BETWEEN start_datetime AND end_datetime and delytype='乙配'
),
-- table origin AB
t_origin_AB as 
(
    -- A & B
    -- 總訂單數、總出貨數
    -- 三種表格 AB都一樣
    select 
        count(distinct orderno) as order_count,
        count(distinct slipno) as slip_count
    from t_origin 
),
-- table origin AB
t_origin_AsubC_BsubD as 
( 
    --(A-C & B-D)
    select 
        count(*) as s_order,
        sum(slipcount) as s_slip
    from 
    (
    -- 訂單中出貨數 訂單中箱出數 訂單中非箱出數
    select orderno, count(distinct slipno) as slipcount
    from t_origin 
    group by orderno
    )
    -- 出貨數-箱出數>1代表為有拆箱
    where slipcount>1
),
-- table origin AB 排出箱出 
t_origin_nbxot_AsubC_BsubD as 
(
--排除箱出統計
--(A-C & B-D)
  select 
        count(*) as s_nbxot_order,
        sum(slipcount)-sum(isboxcount) as s_nbxot_slip
  from
  (
    select orderno,slipcount,isboxcount
    from 
    (
    -- 訂單中出貨數 訂單中箱出數 訂單中非箱出數
    select orderno, count(distinct slipno) as slipcount, countif(isboxout=1) as isboxcount
    from t_origin 
    group by orderno
    )
    -- 出貨數-箱出數>1代表為有拆箱
    where slipcount-isboxcount>1
  )
),
-- table origin AB 排出箱出 & 排除Q台
t_origin_nbxot_Q_AsubC_BsubD as 
(
--排除箱出統計 & 排除 Q台加箱
--(A-C & B-D)
  select 
    count(*) as s_nbxot_Q_order,
    sum(slipcount)-sum(isboxcount) as s_nbxot_Q_slip
  from
  (
    select orderno,slipcount,isboxcount
    from 
    (
    -- 訂單中出貨數 訂單中箱出數 訂單中非箱出數
    select orderno, count(distinct slipno) as slipcount,countif(isboxout=1) as isboxcount
    from t_origin_Q 
    group by orderno
    )
    -- 出貨數-箱出數>1代表為有拆箱
    where slipcount-isboxcount>1
  )
),
t_origin_ED as 
(
    -- 一個訂單同一品號拆箱出貨訂單數 用這個
    -- E & F
    -- 解釋: gdsd = goodscode
    select 
        COUNT(DISTINCT IF(slipc > 1, orderNo, NULL)) as one_order_gsce_s_order,
        SUM(IF(slipc>1, slipc, NULL)) as one_order_gsce_s_slip   
    from
    (
        select orderno,goodscode, count(distinct slipno) as slipc
        from t_origin
        group by orderno,goodscode
    )
),
-- table origin ED 排除箱出
t_originn_nbxot_ED as 
(
    -- 一個訂單同一品號拆箱出貨訂單數 用這個
    -- E & F
    select 
        COUNT(DISTINCT IF(slipc > 1, orderNo, NULL)) as one_order_gsce_nbxot_s_order,
        SUM(IF(slipc>1, slipc, NULL)) as one_order_gsce_nbxot_s_slip   
    from
    (
        select orderno, goodscode, count(distinct slipno) as slipc
        from t_origin
        where isboxout<>1
        group by orderno,goodscode
    )
),
-- table origin ED 排除箱出 & Q台
t_originn_nbxot_Q_ED as
(
    -- 一個訂單同一品號拆箱出貨訂單數 用這個
    -- E & F
    select 
        COUNT(DISTINCT IF(slipc > 1, orderNo, NULL)) as one_order_gsce_nbxot_Q_s_order,
        SUM(IF(slipc>1, slipc, NULL)) as one_order_gsce_nbxot_Q_s_slip   
    from
    (
        select orderno, goodscode, count(distinct slipno) as slipc
        from t_origin_Q
        where isboxout<>1
        group by orderno,goodscode
    )
),
-- 結果表
t_result as
(
    select
        STRING(TIMESTAMP(start_datetime)) as date, -- 日期 ex 2020/01/01 00:00:00 - 2020/01/02 00:00:00紀錄 2020/01/01
        COUNT(DISTINCT orderNo) as order_count, -- A 總訂單數
        COUNT(DISTINCT slipNo) as slip_count, -- B 總出貨數
        (COUNT(DISTINCT orderNo) - (select s_order from t_origin_AsubC_BsubD)) as no_s_order, -- 原始 C 無拆箱訂單數
        (COUNT(DISTINCT slipNo) - (select s_slip from t_origin_AsubC_BsubD)) as no_s_slip, -- 原始 D 無拆箱出貨數
        (select s_order from t_origin_AsubC_BsubD) as s_order, -- 原始(A-C) 拆箱訂單數
        (select s_slip from t_origin_AsubC_BsubD) as s_slip, -- 原始(B-D) 拆箱出貨數
        ROUND((select s_order from t_origin_AsubC_BsubD)/(COUNT(DISTINCT orderNo))*100,2) as s_order_rate, -- 原始((A-C)/A) 拆箱訂單佔總訂單比例
        ROUND((select s_slip from t_origin_AsubC_BsubD)/(COUNT(DISTINCT slipNo))*100,2) as s_slip_rate, -- 原始((B-D)/B) 拆箱訂單佔總訂單比例
        (select one_order_gsce_s_order from t_origin_ED) as one_order_gsce_s_order, -- 原始E 一訂單同一品號拆箱訂單數
        (select one_order_gsce_s_slip from t_origin_ED) as one_order_gsce_s_slip, -- 原始F 一訂單同一品號拆箱出貨數
        ROUND((select one_order_gsce_s_order from t_origin_ED)/COUNT(DISTINCT orderNo)*100,2) as one_order_gsce_s_order_rate,-- (E/A) 一訂單同一品號拆箱訂單數佔總訂單比例
        ROUND((select one_order_gsce_s_slip from t_origin_ED)/COUNT(DISTINCT orderNo)*100,2) as one_order_gsce_s_slip_rate,-- (F/B) 一訂單同一品號拆箱出貨數佔總出貨比例  

        (COUNT(DISTINCT orderNo) - (select s_nbxot_order from t_origin_nbxot_AsubC_BsubD)) as no_s_nbxot_order, -- 排除箱出C 無拆箱訂單數
        (COUNT(DISTINCT slipNo) - (select s_nbxot_slip from t_origin_nbxot_AsubC_BsubD)) as no_s_nbxot_slip, -- 排除箱出D 無拆箱出貨數
        (select s_nbxot_order from t_origin_nbxot_AsubC_BsubD) as s_nbxot_order, -- 排除箱出(A-C)  
        (select s_nbxot_slip from t_origin_nbxot_AsubC_BsubD) as s_nbxot_slip, -- 排除箱出(B-D)
        ROUND((select s_nbxot_order from t_origin_nbxot_AsubC_BsubD)/(COUNT(DISTINCT orderNo))*100,2) as s_order_nbxot_rate, -- 排除箱出((A-C)/A) 拆箱訂單佔總訂單比例
        ROUND((select s_nbxot_slip from t_origin_nbxot_AsubC_BsubD)/(COUNT(DISTINCT slipNo))*100,2) as s_slip_nbxot_rate, -- 排除箱出((B-D)/B) 拆箱訂單佔總訂單比例
        (select one_order_gsce_nbxot_s_order from t_originn_nbxot_ED) as one_order_gsce_nbxot_s_order, -- 排除箱出E
        (select one_order_gsce_nbxot_s_slip from t_originn_nbxot_ED) as one_order_gsce_nbxot_s_slip, -- 排除箱出F
        ROUND((select one_order_gsce_nbxot_s_order from t_originn_nbxot_ED)/COUNT(DISTINCT orderNo)*100,2) as one_order_gsce_s_nbxot_order_rate,-- 排除箱出(E/A) 一訂單同一品號拆箱訂單數佔總訂單比例
        ROUND((select one_order_gsce_nbxot_s_slip from t_originn_nbxot_ED)/COUNT(DISTINCT orderNo)*100,2) as one_order_gsce_s_nbxot_slip_rate,-- 排除箱出(F/B) 一訂單同一品號拆箱出貨數佔總出貨比例  

        (COUNT(DISTINCT orderNo) - (select s_nbxot_Q_order from t_origin_nbxot_Q_AsubC_BsubD)) as no_s_nbxot_Q_order, -- 排除箱出和Q台加箱C 無拆箱訂單數
        (COUNT(DISTINCT slipNo) - (select s_nbxot_Q_slip from t_origin_nbxot_Q_AsubC_BsubD)) as no_s_nbxot_Q_slip, -- 排除箱出和Q台加箱D 無拆箱出貨數
        (select s_nbxot_Q_order from t_origin_nbxot_Q_AsubC_BsubD) as s_nbxot_Q_order, -- 排除箱出和Q台加箱(A-C)
        (select s_nbxot_Q_slip from t_origin_nbxot_Q_AsubC_BsubD) as s_nbxot_Q_slip, -- 排除箱出和Q台加箱(B-D)
        ROUND((select s_nbxot_Q_order from t_origin_nbxot_Q_AsubC_BsubD)/(COUNT(DISTINCT orderNo))*100,2) as s_order_nbxot_Q_rate, -- 排除箱出和Q台加箱((A-C)/A) 拆箱訂單佔總訂單比例
        ROUND((select s_nbxot_Q_slip from t_origin_nbxot_Q_AsubC_BsubD)/(COUNT(DISTINCT slipNo))*100,2) as s_slip_nbxot_Q_rate, -- 排除箱出和Q台加箱((B-D)/B) 拆箱訂單佔總訂單比例
        (select one_order_gsce_nbxot_Q_s_order from t_originn_nbxot_Q_ED) as one_order_gsce_nbxot_Q_s_order, -- 排除箱出和Q台加箱 E
        (select one_order_gsce_nbxot_Q_s_slip from t_originn_nbxot_Q_ED) as one_order_gsce_nbxot_Q_s_slip, -- 排除箱出和Q台加箱 F
        ROUND((select one_order_gsce_nbxot_Q_s_order from t_originn_nbxot_Q_ED)/COUNT(DISTINCT orderNo)*100,2) as one_order_gsce_s_nbxot_Q_order_rate,-- 排除箱出和Q台加箱(E/A) 一訂單同一品號拆箱訂單數佔總訂單比例
        ROUND((select one_order_gsce_nbxot_Q_s_slip from t_originn_nbxot_Q_ED)/COUNT(DISTINCT orderNo)*100,2) as one_order_gsce_s_nbxot_Q_slip_rate-- 排除箱出和Q台加箱(F/B) 一訂單同一品號拆箱出貨數佔總出貨比例  
    from t_origin 
)
 
select * from  t_result
"""


# In[19]:


# 測試與查看 Qry 資料
def test_getSplitBox(s_t_h,e_t_h):
    # 定義參數 
    query_params = [
      bigquery.ScalarQueryParameter("start_traceback_daynum", "INT64", s_t_h),
      bigquery.ScalarQueryParameter("end_traceback_daynum", "INT64", e_t_h),
    ]

    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params

    # 開始 Qry
    bq_client = bigquery.Client()
    query_job = bq_client.query(qryStrAll, job_config=job_config) # API request

    rows_df = query_job.result().to_dataframe() # Waits for query to finish
    postdata = rows_df.to_dict('index')
    print(postdata)

    


# In[3]:


# 寫入 Qry 資料
def getSplitBox(s_t_h,e_t_h):
    # 定義參數 
    query_params = [
      bigquery.ScalarQueryParameter("start_traceback_daynum", "INT64", s_t_h),
      bigquery.ScalarQueryParameter("end_traceback_daynum", "INT64", e_t_h),
    ]

    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    
    # 定義等待容器
    # box = {}
    # 定義 document
    doc = datetime.now(pytz.timezone('Asia/Taipei')).strftime("%Y-%m-%d-%H-%M-%S")
    # 定義 DB
    db = firestore.Client()
    doc_ref = db.collection(u'unboxing').document(doc)
        
    bq_client = bigquery.Client()
    query_job = bq_client.query(qryStrAll, job_config=job_config) # API request

    rows_df = query_job.result().to_dataframe() # Waits for query to finish
    postdata = rows_df.to_dict('index')

    # 寫入 DB    
    doc_ref.set(postdata[0])

    


# In[4]:


# 測試用
# st = -64
# et = st + 24
# for i in range(10):
# #   test_getSplitBox(st,et)
#   getSplitBox(st,et)
#   et = st 
#   st = st - 24
  


# In[ ]:


# 正式執行
getSplitBox(-30,-6)
