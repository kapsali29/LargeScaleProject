from pyspark.sql.types import DoubleType

from helpers import init_spark, haversine, elapsed_time


class Q2(object):
    def __init__(self):
        self.spark, self.sc = init_spark()

    def sql_api(self, trip_data):
        """This function is used to execute Q1 using SparkSQL"""

        self.spark.udf.register('elapsed_time', elapsed_time)
        self.spark.udf.register('haversine', haversine)

        trip_data.registerTempTable("trips")
        res = self.spark.sql(
            "SELECT _c0 as vendor, elapsed_time(_c1, _c2) as duration,haversine(cast(_c3 as float), cast(_c4 as float), cast(_c5 as float), cast(_c6 as float)) as distance from trips")

        res.registerTempTable("results")
        groups = self.spark.sql("SELECT vendor, MAX(distance), MAX(duration) FROM results GROUP BY vendor")
        groups.show()
