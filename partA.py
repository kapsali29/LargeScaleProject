from helpers import init_spark, load_csv_data, write_df_to_hdfs
from settings import TRIP_DATA, HDFS_TRIP_DATA_PATH

spark, sc = init_spark()
trip_data = load_csv_data(spark, TRIP_DATA)

write_df_to_hdfs(trip_data, HDFS_TRIP_DATA_PATH)