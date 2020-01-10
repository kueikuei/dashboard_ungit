#!/usr/bin/env python
# coding: utf-8

# In[3]:


# 執行 - 也可切換內核至2再切回來
# !pip install -r requirements.txt


# In[1]:


# 套件載入
from google.cloud import bigquery
from google.cloud import firestore
import pandas
from datetime import datetime
import pytz


# In[33]:


# Query Box
qryStrAll = """
-- 總訂單數 order_count A
-- 總出貨數 slip_count B
-- 無拆箱訂單數（會包含拆箱）nosplit_order C
-- 無拆箱出貨數（會包含拆箱）nosplit_slip D
-- 拆箱訂單數(排除箱出) split_order A-C
-- 拆箱出貨數（排除箱出）split_slipnobox B-D
-- # 拆箱訂單佔總訂單比例（排除箱出）split_order_rate (A-C)/A
-- # 拆箱訂單出貨箱數佔總出貨箱數比例（排除箱出）split_slip_rate (B-D)/B
-- 一個訂單一個商品拆箱訂單數 one_order_goodscode_splitorder E 
-- 一個訂單一個商品拆箱出貨數 one_order_goodscode_splitslip F
-- # 一個訂單一個商品拆箱訂單數佔總出貨箱數比例（排除箱出）one_order_goodscode_splitorder_rate E/A
-- # 一個訂單一個商品拆箱出貨數佔總出貨箱數比例（排除箱出）one_order_goodscode_splitslip_rate F/B

DECLARE start_dtraceback_daynum INT64 DEFAULT -1;
DECLARE end_traceback_daynum INT64 DEFAULT 0;
DECLARE start_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL start_dtraceback_daynum DAY), "Asia/Taipei");
DECLARE end_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL end_traceback_daynum DAY), "Asia/Taipei");

with t as 
(
  select * 
  FROM `momo-develop.boxSaver.regularQC_slipInfo`
  WHERE DATETIME(orderDate) BETWEEN start_datetime AND end_datetime 
--   where date(orderdate)>='2019-09-01' and date(orderdate)<'2019-10-01' and delytype='乙配'
--   and orderno in ('20190911020687','20190926432398','20190925793553','20190923800761','20190927892249','20190912768263')
),
t1 as 
(
        select orderno,
        count(distinct slipno) as slip_count,
        countif(isBoxOut = 1) AS isBoxOut_slipcount
        from t 
        group by orderno
),
t2 as 
(
  --一個訂單一個商品 用這個

    select orderno,goodscode,
    count(distinct slipno) as slipc
    from t
    where isboxout=0
    group by orderno,goodscode

),
result as
(
  SELECT 
      -- 總訂單數 A
      COUNT(DISTINCT orderNo) AS order_count,
      -- 總出貨箱數 B
      COUNT(DISTINCT slipNo) AS slip_count,
      --完全箱出訂單
     (
      select countif(slip_count=isBoxOut_slipcount) from t1
     ) as pure_boxout_order,
      --拆箱出貨數（訂單中有箱出 但扣掉箱出 還是拆箱的出貨數） 
     (
      select  
      SUM(IF(slip_count=isBoxOut_slipcount, slip_count, NULL))
      from t1

     ) as pure_boxout_slip
     --拆箱訂單 A-C（訂單中有箱出 但扣掉箱出 還是拆箱的訂單數）
     ,(
      select countif((slip_count-isBoxOut_slipcount)>1) from t1
     ) as split_order
     ,  
      --拆箱出貨數 B-D（訂單中有箱出 但扣掉箱出 還是拆箱的出貨數）
     (
      select  
      SUM(IF((slip_count-isBoxOut_slipcount)>1, slip_count, NULL)) - SUM(IF((slip_count-isBoxOut_slipcount)>1, isBoxOut_slipcount, NULL)) 

      from t1

     ) as split_slipnobox
     ,  --拆箱出中箱出數
     (
      select  
      SUM(IF((slip_count-isBoxOut_slipcount)>1, isBoxOut_slipcount, NULL)) 

      from t1

     ) as split_slipbox
     ,  
     --沒拆箱訂單 C
     (
      select countif((slip_count-isBoxOut_slipcount)=1) from t1
     ) as nosplit_order
     ,
     --沒拆箱出貨數 D
     (
     select
     SUM(IF((slip_count-isBoxOut_slipcount)=1, slip_count, NULL))  
     from t1
     ) as nosplit_slip
     ,
     --箱出數比出貨數多的訂單數
     (
      select countif((slip_count-isBoxOut_slipcount)<0) from t1
     ) as error_order
     ,
     --箱出數比出貨數多的出貨數
     (
     select
     SUM(IF((slip_count-isBoxOut_slipcount)<0, slip_count, NULL))  
     from t1
     ) as error_split
     ,
     --一個訂單一個商品 拆箱出貨訂單 E      
     (
      select COUNT(DISTINCT IF(slipc > 1, orderNo, NULL)) from t2
     ) as one_order_goodscode_splitorder
     ,     
     --一個訂單一個商品 拆箱出貨數  F 
     (
      select 
     SUM(IF(slipc>1, slipc, NULL))  

      from t2
     ) as one_order_goodscode_splitslip
   from t 
),
res_rate as
(
   SELECT 
    STRING(TIMESTAMP(start_datetime)) as start_datetime,
    STRING(TIMESTAMP(end_datetime)) as end_datetime,
    order_count,
    slip_count,
    nosplit_order,
    nosplit_slip,
    split_order,
    split_slipnobox,
    ROUND((split_order/order_count)*100,2) as split_order_rate,
    ROUND((split_slipnobox/slip_count)*100,2) as split_slip_rate,
    one_order_goodscode_splitorder,
    one_order_goodscode_splitslip,
    ROUND((one_order_goodscode_splitorder/order_count)*100,2) as one_order_goodscode_splitorder_rate,
    ROUND((one_order_goodscode_splitslip/slip_count)*100,2) as one_order_goodscode_splitslip_rate
   FROM result 
)
 
select * from res_rate
"""


# In[12]:


def getSplitBox(self):
    # 定義等待容器
    # box = {}
    # 定義 document
    doc = datetime.now(pytz.timezone('Asia/Taipei')).strftime("%Y-%m-%d-%H-%M-%S")
    # 定義 DB
    db = firestore.Client()
    doc_ref = db.collection(u'unboxing').document(doc)
        
    bq_client = bigquery.Client()
    query_job = bq_client.query(qryStrAll) # API request

    print(query_job.result())
    return query_job.result()
#     rows_df = query_job.result().to_dataframe() # Waits for query to finish
#     postdata = rows_df.to_dict('index')

    # 寫入 DB    
#     doc_ref.set(postdata[0])

    


# In[7]:


def getSplitBox(self):
    # 定義等待容器
    # box = {}
    # 定義 document
    doc = datetime.now(pytz.timezone('Asia/Taipei')).strftime("%Y-%m-%d-%H-%M-%S")
    # 定義 DB
    db = firestore.Client()
    doc_ref = db.collection(u'unboxing').document(doc)
        
    bq_client = bigquery.Client()
    query_job = bq_client.query(qryStrAll) # API request

    rows_df = query_job.result().to_dataframe() # Waits for query to finish
    postdata = rows_df.to_dict('index')

    # 寫入 DB    
    doc_ref.set(postdata[0])

    


# In[34]:


# 執行所有 qru
# getSplitBox('123')

# db = firestore.Client()
# doc_ref = db.collection(u'unboxing').document(doc)
# 寫入資料
# doc_ref.set(box)


# In[66]:


#  測試用
# getSplitBox(qryStr1)

# Then query for documents
# users_ref = db.collection(u'users')

# for doc in users_ref.stream():
#     print(u'{} => {}'.format(doc.id, doc.to_dict()))
