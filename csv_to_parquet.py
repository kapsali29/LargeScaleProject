import time

from helpers import init_spark
from settings import TRIP_DATA,TRIP_VENDORS, PARQUET_TRIP_DATA,PARQUET_TRIP_VENDORS


spark, sc = init_spark()

start_time1 = time.time()

trip_data = spark.read.format("csv").option("header", "false").load(TRIP_DATA)
trip_data.write.parquet(PARQUET_TRIP_DATA, mode='overwrite')
end_time1 = time.time()
trip_data_time = end_time1 - start_time1
print("Time needed for transforming TRIP DATA file to PARQUET: {}".format(trip_data_time))

start_time2 = time.time()
df_trip_vendors = spark.read.format("csv").option("header", "false").load(TRIP_VENDORS)
df_trip_vendors.write.parquet(PARQUET_TRIP_VENDORS, mode='overwrite')
end_time2 = time.time()
trip_vendors_time = end_time2 - start_time2

print("Time needed for transforming TRIP VENDORS file to PARQUET: {}".format(trip_vendors_time))
