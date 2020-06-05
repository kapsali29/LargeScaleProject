from pyspark import Row

from helpers import init_spark, date_to_hour


class Q1(object):
    """This is Q1 object"""

    def __init__(self):
        self.spark, self.sc = init_spark()

    @staticmethod
    def sql_api(trip_data):
        """This function is used to execute Q1 using SparkSQL"""

        # Remove dirty rows
        filtered_df = trip_data.filter(trip_data['_c3'] != '0').filter(trip_data['_c4'] != '0').filter(
            trip_data['_c5'] != '0').filter(trip_data['_c6'] != '0')

        # Transform date to Hour
        trips_df = filtered_df.rdd.map(date_to_hour).toDF(['Hour', 'Latitude', 'Longitude'])

        # cast to Coordinates to double
        trips_df = trips_df.withColumn("Latitude", trips_df["Latitude"].cast("double"))
        trips_df = trips_df.withColumn("Longitude", trips_df["Longitude"].cast("double"))

        # Find avg Lat Lon for each Hour
        q1 = trips_df.groupBy('Hour').avg().orderBy('Hour', ascending=True)
        q1.show(24)

    @staticmethod
    def mr_api(trip_rdd):
        """This function is used to execute Q1 using RDD"""

        trip_rdd = trip_rdd.map(lambda row: (Row(Hour=row._c1.split()[1][:2], Latitude=row._c3, Longitude=row._c4)))
        filtered_rdd = trip_rdd.filter(lambda row: (row.Latitude != '0') & (row.Longitude != '0'))
        convert_to_float = trip_rdd.map(
            lambda row: Row(Hour=row.Hour, Latitude=float(row.Latitude), Longitude=float(row.Longitude)))

        convert_to_float.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))
        groups = convert_to_float.map(lambda row: (row[0], row[1][0] / row[1][2], row[1][1] / row[1][2]))



