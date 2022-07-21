#This script helps to collect the list of table counts when we have large number of tables
#Takes input list as a csv and gives counts with table name as output in csv

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType
import boto3
import datetime

from pyspark.sql.functions import *


dtm = datetime.datetime.now().strftime("%Y%m%d_%H%M")
input_file = sys.argv[1]
#input_file = "s3://bucket/sathya/datbasename.csv"
print("Input file is : ", input_file)

dbname=input_file.rsplit("/",1)[1].split(".")[0]
tgt_path=input_file.rsplit("/",1)[0]

output_file=tgt_path+"/"+dbname+"_counts_"+dtm+".csv"
print("Output file is : ", output_file)

s3 = boto3.resource('s3')
spark = SparkSession.builder.appName('Table counts').config("spark.sql.catalogImplementation", "hive").config("hive.metastore.connect.retries", 15).config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").config("spark.hadoop.fs.s3.consistent.retryPolicyType","exponential").config("spark.hadoop.fs.s3.maxRetries",30).config("hive.exec.dynamic.partition.mode","nonstrict").enableHiveSupport().getOrCreate()

input_table_list =spark.read.text(input_file).rdd.flatMap(lambda x: x).collect()
table_catalog_list=spark.sql("show tables from "+dbname).select("tableName").rdd.flatMap(lambda x: x).collect()

result=[] 
for table in input_table_list:
    if table.lower() in table_catalog_list:
        t_count = spark.table(dbname+"."+table).count()
        result.append((table,t_count))
    else:
        result.append((table,"Table not Found"))

df = spark.createDataFrame(result, ["TableName","Count"]).coalesce(1)
df.coalesce(1).write.mode('overwrite').option("header","true").csv(output_file)


#Script Name:           counts_check.py
#Input file format:    <dbname>.csv 
#Output file:          <dbname>_counts_<dateTime>.csv

#Run commands
#spark-submit  --driver-memory 4G --executor-memory 5G --conf "spark.driver.maxResultSize=5GB" --conf "spark.dynamicAllocation.enabled=true" --conf "spark.executor.cores=3" --conf "spark.sql.broadcastTimeout=12000" --conf "spark.sql.autoBroadcastJoinThreshold=-1" --conf "spark.sql.hive.caseSensitiveInferenceMode=NEVER_INFER" --conf "spark.streaming.stopGracefullyOnShutdown=true" --conf "spark.task.maxFailures=5"  --conf spark.rpc.askTimeout=100s --conf spark.rpc.numRetries=5 "s3://bucket/sathya/counts_check.py" "s3://bucket/sathya/datbasename.csv"

