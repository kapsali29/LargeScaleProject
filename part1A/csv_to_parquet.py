from pyspark.sql.types import DoubleType
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
from helpers import load_csv_data, init_spark, haversine, elapsed_time, find_max
from settings import TRIP_DATA,TRIP_VENDORS, HDFS_TRIP_DATA_PATH, PARQUET_TRIP_DATA,PARQUET_TRIP_VENDORS, DATE_FORMAT

from pyspark.sql import SparkSession


spark, sc = init_spark()

trip_data = spark.read.format("csv").option("header", "false").load(TRIP_DATA)
trip_data.write.parquet(PARQUET_TRIP_DATA, mode='overwrite')

df_trip_vendors = spark.read.format("csv").option("header", "false").load(TRIP_VENDORS)
df_trip_vendors.write.parquet(PARQUET_TRIP_VENDORS, mode='overwrite')
