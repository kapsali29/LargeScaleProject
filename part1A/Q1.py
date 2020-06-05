from helpers import init_spark, date_to_hour


class Q1(object):
    """This is Q1 object"""

    def __init__(self):
        self.spark, self.sc = init_spark()

    @staticmethod
    def sql_api(trip_data):
        """
        This function is used to execute Q1 query using SparkSQL
        """
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
