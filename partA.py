from pyspark.sql.functions import col, udf

from helpers import init_spark, load_csv_data, write_df_to_hdfs, df_to_parquet
from settings import TRIP_DATA, HDFS_TRIP_DATA_PATH, PARQUET_TRIP_DATA

spark, sc = init_spark()
trip_data = load_csv_data(spark, TRIP_DATA)
# df_to_parquet(trip_data, PARQUET_TRIP_DATA)

# write_df_to_hdfs(trip_data, HDFS_TRIP_DATA_PATH)

# trip_data = trip_data.dropDuplicates()
# trip_data = trip_data.na.drop()
# Remove dirty
filtered_df = trip_data.filter(trip_data['_c3'] != '0').filter(trip_data['_c4'] != '0').filter(
    trip_data['_c5'] != '0').filter(trip_data['_c6'] != '0')

# Transform Hour
get_hour_from_date = udf(lambda x: x.split()[1][:2])
trips_df = filtered_df.withColumn('_c1', get_hour_from_date(col('_c1')))
trips_partition = trips_df.select('_c1', '_c3', '_c4')
trips_partition.show()

# cast to Coordinates to double
# trip_data = trip_data.withColumn("_c3", trip_data["_c3"].cast("double"))
# trip_data = trip_data.withColumn("_c4", trip_data["_c4"].cast("double"))
# trip_data = trip_data.withColumn("_c5", trip_data["_c5"].cast("double"))
# trip_data = trip_data.withColumn("_c6", trip_data["_c6"].cast("double"))
