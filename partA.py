from helpers import init_spark, elapsed_time, haversine

spark, sc = init_spark()


# # groups = spark.sql("SELECT vendor, MAX(distance) FROM results GROUP BY vendor")
trip_df = spark.read.csv('file:////home/user/project-data/yellow_trip_data/yellow_tripdata_1m.csv')
vendors = spark.read.csv('file:///home/user/project-data/yellow_trip_data/yellow_tripvendors_1m.csv')
# spark.udf.register('elapsed_time', elapsed_time)
# spark.udf.register('haversine', haversine)
# trip_df.registerTempTable("trips")
# vendors.registerTempTable("vendors")
#
# vendor_trips = spark.sql("SELECT vendors._c1 as vendor_id, elapsed_time(trips._c1,trips._c2) as duration, haversine(cast(_c3 as float), cast(_c4 as float), cast(_c5 as float), cast(_c6 as float)) as distance FROM trips INNER JOIN vendors ON trips._c0=vendors._c0 WHERE trips._c3!='0' AND trips._c4!='0' AND trips._c5!='0' AND trips._c6!='0'")
#
# vendor_trips.registerTempTable("vendor_trips")
# groups = spark.sql("SELECT vendor_id, MAX(distance) as max_distance FROM vendor_trips GROUP BY vendor_id")
# grp_rdd = groups.rdd.collect()
#
# result = spark.sql("SELECT * FROM vendor_trips WHERE vendor_id={} AND distance={} OR vendor_id={} AND distance={}".
#                    format(grp_rdd[0]['vendor_id'], grp_rdd[0]['max(distance)'], grp_rdd[1]['vendor_id'], grp_rdd[1]['max(distance)'])
#                    )
# result.show()
from part1A.Q2 import Q2

q2 = Q2()
q2.sql_api(trip_df, vendors)




