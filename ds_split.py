#!/usr/bin/env python
# coding: utf-8

# 套件載入
from google.cloud import bigquery
from google.cloud import firestore
import pandas
from datetime import datetime
import pytz


# Query Box
qryStrAll = """
SELECT * FROM `momo-develop.scheduledReport.ds_split_firestore`
"""

# In[64]:


def get_ds_split(self):
    # 定義等待容器
    # box = {}
    # 定義 document
    doc = datetime.now(pytz.timezone('Asia/Taipei')).strftime("%Y-%m-%d-%H-%M-%S")
    # 定義 DB
    db = firestore.Client()
    doc_ref = db.collection(u'ds_split').document(doc)
        
    bq_client = bigquery.Client()
    query_job = bq_client.query(qryStrAll) # API request

    rows_df = query_job.result().to_dataframe() # Waits for query to finish
    postdata = rows_df.to_dict('index')

    # 寫入 DB    
    doc_ref.set(postdata[0])

    



