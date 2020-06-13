from pyspark.sql.types import DoubleType
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
from helpers import load_csv_data, init_spark, haversine, elapsed_time, find_max
from settings import TRIP_DATA, HDFS_TRIP_DATA_PATH, PARQUET_TRIP_DATA, DATE_FORMAT

from pyspark.sql import SparkSession


spark, sc = init_spark()

# q1 = Q1()
#trip_data = load_csv_data(spark, TRIP_DATA)

#('240518298477', ((4, 64.93966808512545), '2'))

trip_data = sc.textFile("file:////home/user/project-data/yellow_trip_data/yellow_tripdata_1m.csv")
vendors = sc.textFile("file:///home/user/project-data/yellow_trip_data/yellow_tripvendors_1m.csv")

trip_data = trip_data.map(lambda x : (x.split(",")[0],x.split(",")[1],x.split(",")[2],x.split(",")[3],x.split(",")[4],x.split(",")[5],x.split(",")[6]))

filtered_trip_data=trip_data.filter(lambda x : x[3]!='0' and x[4]!='0' and x[5]!='0' and x[6]!='0'). \
                             map(lambda x : (x[0],(elapsed_time(x[1],x[2]),haversine(float(x[3]), float(x[4]), float(x[5]), float(x[6])))))

vendors = vendors.map(lambda x : (x.split(",")[0], x.split(",")[1]))

result = filtered_trip_data.join(vendors). \
                            map(lambda x : (x[1][1], [x[1][0][0],x[1][0][1]])). \
                            reduceByKey(lambda x,y: find_max(x,y)). \
                            map(lambda x : (x[0],x[1][0],x[1][1]))

for i in result.collect():
    print(i)
