from helpers import load_csv_data, init_spark, haversine, elapsed_time
from settings import PARQUET_TRIP_DATA,PARQUET_TRIP_VENDORS
from pyspark.sql import SparkSession


def part1b(trip_data, vendors, spark):
    trip_data.createOrReplaceTempView("trip_data")
    vendors.createOrReplaceTempView("vendors")
    spark.udf.register('elapsed_time', elapsed_time)
    spark.udf.register('haversine', haversine)

    vendors_50  = spark.sql("SELECT * from vendors LIMIT 50")
    vendors_50.registerTempTable("vendors_50")

    all_data = spark.sql("SELECT * FROM trip_data as trips INNER JOIN vendors_50  ON trips._c0 = vendors_50._c0")
    all_data.show()
    print(all_data.explain())
    #broadcast hash join was selected

    configuration = spark.sql("SET spark.sql.autoBroadcastJoinThreshold = -1") #disable broadcast join
    all_data_without_broadcast = spark.sql("SELECT * FROM trip_data as trips INNER JOIN vendors_50  ON trips._c0 = vendors_50._c0")
    all_data_without_broadcast.show()
    print(all_data_without_broadcast.explain())
    #ShortMergeJoin was selected


if __name__ == "__main__":

    spark, sc = init_spark()

    trip_data = spark.read.parquet(PARQUET_TRIP_DATA)
    vendors = spark.read.parquet(PARQUET_TRIP_VENDORS)

    part1b(trip_data, vendors, spark)
