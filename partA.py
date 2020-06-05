from pyspark.sql.functions import col, udf

from helpers import init_spark, load_csv_data, write_df_to_hdfs, df_to_parquet, date_to_hour
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
trips_df = filtered_df.rdd.map(date_to_hour).toDF(['Hour', 'Latitude', 'Longitude'])
trips_df.show()

# cast to Coordinates to double
trips_df = trips_df.withColumn("Latitude", trips_df["Latitude"].cast("double"))
trips_df = trips_df.withColumn("Longitude", trips_df["Longitude"].cast("double"))

# Execute Q1 query
trips_df.groupBy('Hour').avg().orderBy('Hour', ascending=True).show(24)
