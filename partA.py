from helpers import init_spark, load_csv_data, write_df_to_hdfs, df_to_parquet
from settings import TRIP_DATA, HDFS_TRIP_DATA_PATH, PARQUET_TRIP_DATA

spark, sc = init_spark()
trip_data = load_csv_data(spark, TRIP_DATA)
df_to_parquet(trip_data, PARQUET_TRIP_DATA)

# write_df_to_hdfs(trip_data, HDFS_TRIP_DATA_PATH)


# cast to double
# df = df.withColumn("_c3", df["_c3"].cast("double"))