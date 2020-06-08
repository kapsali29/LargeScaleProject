from pyspark.sql.types import DoubleType
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
from helpers import load_csv_data, init_spark, haversine, elapsed_time
from settings import TRIP_DATA, HDFS_TRIP_DATA_PATH, PARQUET_TRIP_DATA, DATE_FORMAT

from pyspark.sql import SparkSession


spark, sc = init_spark()

# q1 = Q1()
#trip_data = load_csv_data(spark, TRIP_DATA)

#('240518298477', ((4, 64.93966808512545), '2'))
'''
def time_travel_in_seconds(start,end):
	"""This function calculates the trip duration in seconds """
	start_date = datetime.strptime(start, DATE_FORMAT)
	end_date = datetime.strptime(end, DATE_FORMAT)

	duration = end_date - start_date
	return duration.seconds


def distance(lat1, lon1, lat2, lon2):
	"""
	This function calculates the trip 
	distance using haversine formula 
	"""
	# haversine formula
	dlon = lon2 - lon1
	dlat = lat2 - lat1
	a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
	c = 2 * asin(sqrt(a))
	r = 6371  # Radius of earth in kilometers. Use 3956 for miles

	return c * r


def find_max(x,y):
	"""
	This function receives two lists 
	and return the list, which has the greatest
	element in second position
	"""
	if x[1] > y[1]: return x
	return y

'''
trip_data = sc.textFile(HDFS_TRIP_DATA_PATH). \
			filter(lambda x : (float(x.split(",")[3])!=0 and float(x.split(",")[4]!=0 
						and float(x.split(",")[5]!=0 and float(x.split(",")[6]!=0))))). \
			map(lambda x : (x.split(",")[0],(elapsed_time(x.split(",")[1],
						x.split(",")[2]),haversine(float(x.split(",")[3]),float(x.split(",")[4]),
						float(x.split(",")[5]),float(x.split(",")[6]))))). \
			take(20)
'''
vendors = sc.textFile(HDFS_TRIP_VENDORS_PATH). \
			map(lambda x : (x.split(",")[0], x.split(",")[1]))	



result = trip_data.join(vendors). \
	map(lambda x : (x[1][1], [x[1][0][0],x[1][0][1]])). \
	reduceByKey(lambda x,y: find_max(x,y)). \
	map(lambda x : (x[0],x[1][0],x[1][1]))
	
'''
for i in trip_data:
	print(i)

