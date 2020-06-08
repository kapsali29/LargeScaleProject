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

	@staticmethod
	def mr_api(trip_data,vendors):

		trip_data = sc.textFile(HDFS_TRIP_DATA_PATH). \
            		filter(lambda x : (float(x.split(",")[3])!=0 and float(x.split(",")[4]!=0
                        and float(x.split(",")[5]!=0 and float(x.split(",")[6]!=0))))). \
            		map(lambda x : (x.split(",")[0],(elapsed_time(x.split(",")[1],
                        x.split(",")[2]),haversine(float(x.split(",")[3]),float(x.split(",")[4]),
                        float(x.split(",")[5]),float(x.split(",")[6])))))
	
		vendors = sc.textFile(HDFS_TRIP_VENDORS_PATH). \
            		map(lambda x : (x.split(",")[0], x.split(",")[1]))  


		results = trip_data.join(vendors). \
    				map(lambda x : (x[1][1], [x[1][0][0],x[1][0][1]])). \
    				reduceByKey(lambda x,y: find_max(x,y)). \
    				map(lambda x : (x[0],x[1][0],x[1][1]))
    

		for result in results.collect():
   			 print(result)

