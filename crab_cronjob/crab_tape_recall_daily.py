# import pickle
from datetime import datetime, timedelta

# import click
import os
import pandas as pd
# import pprint
import time
# from dateutil.relativedelta import relativedelta
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, collect_list, concat_ws, greatest, lit, lower, when,
    avg as _avg,
    count as _count,
    hex as _hex,
    max as _max,
    min as _min,
    round as _round,
    sum as _sum,
)

from pyspark.sql.types import (
    LongType,
)

import numpy as np
# import math
import osearch
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("crab_tape_recall")\
        .getOrCreate()

TODAY = str(datetime.now())[:10]
# TODAY = '2023-07-11'
TOYEAR = TODAY[:4]
YESTERDAY = str(datetime.now()-timedelta(days=1))[:10]
# YESTERDAY = '2023-07-10'

wa_date = TODAY
HDFS_RUCIO_RULES_HISTORY = f'/project/awg/cms/rucio/{wa_date}/rules_history/'
print("===============================================", "File Directory:", HDFS_RUCIO_RULES_HISTORY, "Work Directory:", os.getcwd(), "===============================================", sep='\n')

rucio_rules_history = spark.read.format('avro').load(HDFS_RUCIO_RULES_HISTORY).withColumn('ID', lower(_hex(col('ID'))))
rucio_rules_history.createOrReplaceTempView("rules_history")

query = query = f"""\
WITH filter_t AS (
SELECT ID, NAME, STATE, EXPIRES_AT, UPDATED_AT, CREATED_AT
FROM rules_history 
WHERE 1=1
AND ACCOUNT = "crab_tape_recall"
AND CREATED_AT >= unix_timestamp("{TOYEAR}-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss")*1000
),
rn_t AS (
SELECT ID, NAME, STATE, EXPIRES_AT, UPDATED_AT, CREATED_AT,
row_number() over(partition by ID order by UPDATED_AT desc) as rn
FROM filter_t
),
calc_days_t AS (
SELECT ID, NAME, STATE, EXPIRES_AT, UPDATED_AT, CREATED_AT,
   CASE 
      WHEN STATE = 'O' THEN ceil((UPDATED_AT-CREATED_AT)/86400000)  
      WHEN STATE != 'O' AND EXPIRES_AT < unix_timestamp("{wa_date} 00:00:00", "yyyy-MM-dd HH:mm:ss")*1000 THEN ceil((EXPIRES_AT-CREATED_AT)/86400000)
      ELSE 0
   END AS DAYS
FROM rn_t
WHERE rn = 1
)
SELECT * 
FROM calc_days_t
WHERE 1=1
AND EXPIRES_AT >= unix_timestamp("{YESTERDAY} 00:00:00", "yyyy-MM-dd HH:mm:ss")*1000
AND EXPIRES_AT < unix_timestamp("{TODAY} 00:00:00", "yyyy-MM-dd HH:mm:ss")*1000 
"""

tmpdf = spark.sql(query)
tmpdf.show()

docs = tmpdf.toPandas().to_dict('records')

def get_index_schema():
    return {
        "settings": {"index": {"number_of_shards": "1", "number_of_replicas": "1"}},
        "mappings": {
            "properties": {
                "timestamp": {"format": "epoch_second", "type": "date"},
                "ID": {"ignore_above": 1024, "type": "keyword"},
                "NAME": {"ignore_above": 2048, "type": "keyword"},
                "STATE": {"ignore_above": 1024, "type": "keyword"},
                "EXPIRES_AT": {"format": "epoch_millis", "type": "date"},
                "UPDATED_AT": {"format": "epoch_millis", "type": "date"},
                "CREATED_AT": {"format": "epoch_millis", "type": "date"},
                "DAYS": {"type": "long"},
            }
        }
    }
    
_index_template = 'test-crab-tape-recall-daily'
client = osearch.get_es_client("es-cms1.cern.ch/es", 'secret_opensearch.txt', get_index_schema())
# index_mod="": 'test-foo', index_mod="Y": 'test-foo-YYYY', index_mod="M": 'test-foo-YYYY-MM', index_mod="D": 'test-foo-YYYY-MM-DD',
idx = client.get_or_create_index(timestamp=time.time(), index_template=_index_template, index_mod="M")
client.send(idx, docs, metadata=None, batch_size=10000, drop_nulls=False)

print("========================================================================", "FINISHED", "========================================================================", sep='\n')