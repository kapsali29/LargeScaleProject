# from part1A.Q2 import Q2
# from settings import HDFS_TRIP_DATA_PATH, HDFS_TRIP_VENDORS_PATH
#
# q2 = Q2()
#
# trip_data = q2.spark.read.csv(HDFS_TRIP_DATA_PATH)
# trip_vendors = q2.spark.read.csv(HDFS_TRIP_VENDORS_PATH)
#
# q2.sql_api(trip_data=trip_data, vendors=trip_vendors)


# formatted_data = spark.sql(
#     "SELECT _c0, _c1, _c2, cast(_c3 as float), cast(_c4 as float), cast(_c5 as float), cast(_c6 as float) from trips")
# formatted_data.registerTempTable("formatted_trips")
# filtered_data = spark.sql("SELECT * FROM formatted_trips WHERE _c3!=0.0 AND _c4!=0.0 AND _c5!=0.0 AND _c6!=0.0")
# final_data = spark.sql("SELECT _c0, elapsed_time(_c1,_c2) as duration, haversine(_c3, _c4, _c5,_c6) as distance from fdata")
#
# vendor_trips = spark.sql("SELECT vendors._c1 as vendor_id, final_data.duration as duration, final_data.distance as distance FROM final_data INNER JOIN vendors ON final_data._c0=vendors._c0")
# vendor_trips.registerTempTable("vendor_trips")
#
# groups = self.spark.sql("SELECT vendor_id, MAX(distance) as max_distance FROM vendor_trips GROUP BY vendor_id")


trip_data.map(lambda x : (x[0],x[1],x[2],float(x[3]),float(x[4]),float(x[5]),float(x[6])))
