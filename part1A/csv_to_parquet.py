import time

from helpers import init_spark
from settings import TRIP_DATA,TRIP_VENDORS, PARQUET_TRIP_DATA,PARQUET_TRIP_VENDORS


spark, sc = init_spark()

start_time = time.time()

trip_data = spark.read.format("csv").option("header", "false").load(TRIP_DATA)
trip_data.write.parquet(PARQUET_TRIP_DATA, mode='overwrite')

df_trip_vendors = spark.read.format("csv").option("header", "false").load(TRIP_VENDORS)
df_trip_vendors.write.parquet(PARQUET_TRIP_VENDORS, mode='overwrite')

end_time = time.time()
parquet_transformation_time = end_time - start_time
print("Time needed for transforming CSV files to PARQUET: {}".format(parquet_transformation_time))
