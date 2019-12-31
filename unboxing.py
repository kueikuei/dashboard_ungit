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


# In[21]:


get_ipython().system('touch unboxing.py')


# In[19]:


def getSplitBox(qryStr):
    
    # ref = db.reference('/'+col)
    
    bq_client = bigquaery.Client()
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
