#!/usr/local/bin/python


import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from random import random
from operator import add
import os
import shutil

output=sys.argv[2]
input_file = sys.argv[1]
temporary_directory="/tmp/output/"

if (os.path.exists(temporary_directory)):
	shutil.rmtree(temporary_directory, ignore_errors=True)

shutil.rmtree(temporary_directory, ignore_errors=True)

print("Starting data transformation...")
spark = SparkSession\
        .builder\
        .appName("XmlToParquet")\
        .getOrCreate()

df = spark.read.format("com.databricks.spark.xml") \
        .options(rowTag="PatientMatching") \
        .load(input_file)

print("Saving dataframe")
df.repartition(1).write.mode('overwrite').parquet(temporary_directory)

if (os.path.exists(temporary_directory + "_SUCCESS")):
	print("Removing file " + temporary_directory + "_SUCCESS")
	os.remove(temporary_directory + "_SUCCESS")	
	files = [f for f in os.listdir(temporary_directory)]
	for f in files:
		print("Renaming file " + f)
		os.rename(temporary_directory + f, output)

os.rmdir(temporary_directory)
