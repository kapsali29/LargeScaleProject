

from helpers import load_csv_data, init_spark, haversine
from settings import TRIP_DATA, HDFS_TRIP_DATA_PATH, PARQUET_TRIP_DATA

spark, sc = init_spark()

# q1 = Q1()
trip_data = load_csv_data(spark, TRIP_DATA)
# trip_rdd = trip_data.rdd
# q1.mr_api(trip_rdd)
# q1.sql_api(trip_data)

# df_to_parquet(trip_data, PARQUET_TRIP_DATA)

# write_df_to_hdfs(trip_data, HDFS_TRIP_DATA_PATH)

trip_data = trip_data.withColumn("_c3", trip_data["_c3"].cast("double"))
trip_data = trip_data.withColumn("_c4", trip_data["_c4"].cast("double"))
trip_data = trip_data.withColumn("_c5", trip_data["_c5"].cast("double"))
trip_data = trip_data.withColumn("_c6", trip_data["_c6"].cast("double"))

result = trip_data.rdd.map(haversine).toDF()
result.show()
