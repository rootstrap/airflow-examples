#!/usr/bin/python3


import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from random import random
from operator import add

output=sys.argv[2]
input_file = sys.argv[1]

print("Starting data transformation...")
spark = SparkSession\
        .builder\
        .appName("XmlToParquet")\
        .getOrCreate()

df = spark.read.format("com.databricks.spark.xml") \
        .options(rowTag="PatientMatching") \
        .load(input_file)

print("Dataframe read")
df.repartition(1).write.mode('overwrite').parquet(output)
print("Completed data transformation!")