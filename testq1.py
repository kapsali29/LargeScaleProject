from part1A.Q1 import Q1
from settings import HDFS_TRIP_DATA_PATH

q1 = Q1()

trip_data = q1.spark.read.csv(HDFS_TRIP_DATA_PATH)
q1.sql_api()