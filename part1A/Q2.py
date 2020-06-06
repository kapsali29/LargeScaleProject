from pyspark.sql.types import DoubleType

from helpers import init_spark, haversine, elapsed_time


class Q2(object):
    def __init__(self):
        self.spark, self.sc = init_spark()

    @staticmethod
    def sql_api(trip_data):
        """This function is used to execute Q1 using SparkSQL"""

        trip_data = trip_data.withColumn("_c3", trip_data["_c3"].cast(DoubleType()))
        trip_data = trip_data.withColumn("_c4", trip_data["_c4"].cast(DoubleType()))
        trip_data = trip_data.withColumn("_c5", trip_data["_c5"].cast(DoubleType()))
        trip_data = trip_data.withColumn("_c6", trip_data["_c6"].cast(DoubleType()))

        result = trip_data.rdd.map(haversine).map(elapsed_time).toDF(['Vendor', 'Time', 'Distance'])
        group = result.groupBy('Vendor').agg({'Distance': 'max'})

        group.show()
