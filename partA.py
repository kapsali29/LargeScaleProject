from datetime import datetime
from math import radians, sin, cos, asin, sqrt

from pyspark.sql.types import DoubleType

from helpers import load_csv_data, init_spark
from settings import TRIP_DATA, HDFS_TRIP_DATA_PATH, PARQUET_TRIP_DATA

spark, sc = init_spark()

# q1 = Q1()
trip_data = load_csv_data(spark, TRIP_DATA)


# trip_rdd = trip_data.rdd
# q1.mr_api(trip_rdd)
# q1.sql_api(trip_data)

# df_to_parquet(trip_data, PARQUET_TRIP_DATA)

# write_df_to_hdfs(trip_data, HDFS_TRIP_DATA_PATH)

# trip_data = trip_data.withColumn("_c3", trip_data["_c3"].cast(DoubleType()))
# trip_data = trip_data.withColumn("_c4", trip_data["_c4"].cast(DoubleType()))
# trip_data = trip_data.withColumn("_c5", trip_data["_c5"].cast(DoubleType()))
# trip_data = trip_data.withColumn("_c6", trip_data["_c6"].cast(DoubleType()))
#
#
# result = trip_data.rdd.map(haversine).map(elapsed_time).toDF(['Vendor', 'Time', 'Distance'])
# group = result.groupBy('Vendor').agg({'Distance': 'max'})
#
# group.show()

def elapsed_time(start, end):
    """This function is used to find trip duration"""

    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')

    duration = end_date - start_date
    days, seconds = duration.days, duration.seconds
    minutes = (seconds % 3600) // 60
    return minutes


def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """

    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles

    distance = c * r

    return distance


spark.udf.register('elapsed_time', elapsed_time)
spark.udf.register('haversine', haversine)

trip_data.registerTempTable("trips")

res = spark.sql(
    "SELECT _c0 as vendor, elapsed_time(_c1, _c2) as duration,haversine(cast(_c3 as float), cast(_c4 as float), cast(_c5 as float), cast(_c6 as float)) as distance from trips")

res.registerTempTable("results")
groups = spark.sql("SELECT vendor, MAX(distance) FROM results GROUP BY vendor")

