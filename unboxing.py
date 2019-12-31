#!/usr/bin/env python
# coding: utf-8

# In[17]:


# 套件載入
from google.cloud import bigquery
from google.cloud import firestore
import pandas
from datetime import datetime
import pytz


# In[18]:


# 定義等待容器
# box = {}
# 定義 document
doc = datetime.now(pytz.timezone('Asia/Taipei')).strftime("%Y-%m-%d-%H-%M-%S")
# 定義 DB
db = firestore.Client()
doc_ref = db.collection(u'unboxing').document(doc)


# In[14]:


# Query Box
qryStrAll = """
DECLARE start_dtraceback_daynum INT64 DEFAULT -3;
DECLARE end_traceback_daynum INT64 DEFAULT -2;
DECLARE start_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL start_dtraceback_daynum DAY), "Asia/Taipei");
DECLARE end_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL end_traceback_daynum DAY), "Asia/Taipei");


with t as 
(
select * 
FROM `momo-develop.boxSaver.regularQC_slipInfo`
WHERE DATETIME(orderDate)
BETWEEN start_datetime AND end_datetime 

)

SELECT 
    STRING(TIMESTAMP(start_datetime)) as start_datetime,
    STRING(TIMESTAMP(end_datetime)) as end_datetime,
    COUNT(DISTINCT orderNo) AS order_count,
    COUNT(DISTINCT slipNo) AS slip_count,
    COUNT(DISTINCT IF(isBoxOut = 1, orderNo, NULL)) AS isBoxOut_count,
(
  select count(orderno)
  from 
  (
  select orderno,count(*) as slip_count 
  FROM t
  WHERE isBoxOut <> 1 GROUP BY orderno 
  HAVING slip_count > 1
  ) 
) as split_order_count
,
(
  select sum(slip_count) 
  from 
  (
  select orderno,count(*) as slip_count FROM t
  WHERE isBoxOut <> 1 GROUP BY orderno 
  HAVING slip_count > 1
  ) 
) as split_slip_count
FROM t
"""

qryStr0 = """
-- 取得取得資料的時間區段
DECLARE start_dtraceback_daynum INT64 DEFAULT -3;
DECLARE end_traceback_daynum INT64 DEFAULT -2;
DECLARE start_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL start_dtraceback_daynum DAY), "Asia/Taipei");
DECLARE end_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL end_traceback_daynum DAY), "Asia/Taipei");

SELECT STRING(TIMESTAMP(start_datetime)) as start_datetime,STRING(TIMESTAMP(end_datetime)) as end_datetime
"""


# -- 訂單數量
qryStr1 = """
-- 訂單數量
DECLARE start_dtraceback_daynum INT64 DEFAULT -3;
DECLARE end_traceback_daynum INT64 DEFAULT -2;
DECLARE start_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL start_dtraceback_daynum DAY), "Asia/Taipei");
DECLARE end_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL end_traceback_daynum DAY), "Asia/Taipei");

SELECT COUNT(DISTINCT orderNo) AS order_count
FROM `momo-develop.boxSaver.regularQC_slipInfo`
WHERE DATETIME(orderDate)
BETWEEN start_datetime AND end_datetime
"""

# -- 出貨數量
qryStr2 = """
-- 出貨數量 
DECLARE start_dtraceback_daynum INT64 DEFAULT -3;
DECLARE end_traceback_daynum INT64 DEFAULT -2;
DECLARE start_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL start_dtraceback_daynum DAY), "Asia/Taipei");
DECLARE end_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL end_traceback_daynum DAY), "Asia/Taipei");

SELECT COUNT(DISTINCT slipNo) AS slip_count
FROM `momo-develop.boxSaver.regularQC_slipInfo`
WHERE DATETIME(orderDate)
BETWEEN start_datetime AND end_datetime 
"""

# -- 箱出訂單數
qryStr3 = """
DECLARE start_dtraceback_daynum INT64 DEFAULT -3;
DECLARE end_traceback_daynum INT64 DEFAULT -2;
DECLARE start_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL start_dtraceback_daynum DAY), "Asia/Taipei");
DECLARE end_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL end_traceback_daynum DAY), "Asia/Taipei");

SELECT COUNT(DISTINCT orderNo) AS isBoxOut_count
FROM `momo-develop.boxSaver.regularQC_slipInfo`
WHERE isBoxOut = 1
AND DATETIME(orderDate)
BETWEEN start_datetime AND end_datetime 
"""

qryStr0123 = """
DECLARE start_dtraceback_daynum INT64 DEFAULT -3;
DECLARE end_traceback_daynum INT64 DEFAULT -2;
DECLARE start_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL start_dtraceback_daynum DAY), "Asia/Taipei");
DECLARE end_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL end_traceback_daynum DAY), "Asia/Taipei");

SELECT 
    STRING(TIMESTAMP(start_datetime)) as start_datetime,
    STRING(TIMESTAMP(end_datetime)) as end_datetime,
    COUNT(DISTINCT orderNo) AS order_count,
    COUNT(DISTINCT slipNo) AS slip_count,
    COUNT(DISTINCT IF(isBoxOut = 1, orderNo, NULL)) AS isBoxOut_count
FROM `momo-develop.boxSaver.regularQC_slipInfo`
WHERE DATETIME(orderDate)
BETWEEN start_datetime AND end_datetime
"""
# -- 拆箱訂單數(排除箱出) 
qryStr4 = """
DECLARE start_dtraceback_daynum INT64 DEFAULT -3;
DECLARE end_traceback_daynum INT64 DEFAULT -2;
DECLARE start_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL start_dtraceback_daynum DAY), "Asia/Taipei");
DECLARE end_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL end_traceback_daynum DAY), "Asia/Taipei");

with a as
(
SELECT *
FROM `momo-develop.boxSaver.regularQC_slipInfo`
WHERE isboxout<>1
AND DATETIME(orderDate)
BETWEEN start_datetime AND end_datetime 
),
-- 將slipno>1找出來就是拆箱
b as
(
SELECT orderno,count(distinct slipno) as slip_count
FROM a
GROUP BY orderno 
HAVING slip_count > 1
)

-- 印出拆箱訂單數(排除箱出)
SELECT count(orderno) as split_order_count
FROM b
"""

# 拆箱訂單總出貨數
qryStr5 = """
DECLARE start_dtraceback_daynum INT64 DEFAULT -3;
DECLARE end_traceback_daynum INT64 DEFAULT -2;
DECLARE start_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL start_dtraceback_daynum DAY), "Asia/Taipei");
DECLARE end_datetime DATETIME DEFAULT DATETIME(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL end_traceback_daynum DAY), "Asia/Taipei");

with a as
(
SELECT *
FROM `momo-develop.boxSaver.regularQC_slipInfo`
WHERE isboxout<>1
AND DATETIME(orderDate)
BETWEEN start_datetime AND end_datetime 
),
-- 將slipno>1找出來就是拆箱
b as
(
SELECT orderno,count(distinct slipno) as slip_count
FROM a
GROUP BY orderno 
HAVING slip_count > 1
)

-- 拆箱訂單總出貨數
SELECT sum(slip_count) as split_slip_count
FROM b
"""


# In[19]:


def getSplitBox(qryStr):
    
    # ref = db.reference('/'+col)
    
    bq_client = bigquery.Client()
    query_job = bq_client.query(qryStr) # API request

    rows_df = query_job.result().to_dataframe() # Waits for query to finish
    postdata = rows_df.to_dict('index')
    
    # 寫入 DB    
    doc_ref.set(postdata[0])
    


# In[20]:


# 執行所有 qru
getSplitBox(qryStrAll)

# db = firestore.Client()
# doc_ref = db.collection(u'unboxing').document(doc)
# 寫入資料
# doc_ref.set(box)


# In[13]:


#  測試用
# getSplitBox(qryStr1)

# Then query for documents
# users_ref = db.collection(u'users')

# for doc in users_ref.stream():
#     print(u'{} => {}'.format(doc.id, doc.to_dict()))
