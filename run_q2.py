import time

import numpy as np

import matplotlib

from part1A.Q2 import Q2
from settings import HDFS_TRIP_DATA_PATH, HDFS_TRIP_VENDORS_PATH, PARQUET_TRIP_DATA, PARQUET_TRIP_VENDORS

matplotlib.use("Agg")

import matplotlib.pyplot as plt

q2_instance = Q2()

# Execute Q2 using SPARK-SQL and CSV
sql_start_time = time.time()

trip_data = q2_instance.spark.read.csv(HDFS_TRIP_DATA_PATH)
trip_vendors = q2_instance.spark.read.csv(HDFS_TRIP_VENDORS_PATH)

q2_instance.sql_api(trip_data=trip_data, vendors=trip_vendors)

sql_end_time = time.time()
sql_q2_time = sql_end_time - sql_start_time
print(" Time needed to execute Q2 using SparkSQL and CSV: {}".format(sql_q2_time))

# Execute Q2 using MR and CSV
mr_start_time = time.time()

trip_data_rdd = q2_instance.sc.textFile(HDFS_TRIP_DATA_PATH)
vendors_rdd = q2_instance.sc.textFile(HDFS_TRIP_VENDORS_PATH)

q2_instance.mr_api(trip_data=trip_data_rdd, vendors=vendors_rdd)

mr_end_time = time.time()
mr_q2_time = mr_end_time - mr_start_time
print(" Time needed to execute Q2 using Map Reduce and CSV: {}".format(mr_q2_time))

# Execute Q2 using SPARK-SQL and PARQUET
parquet_start_time = time.time()

trip_data_parquet = q2_instance.spark.read.parquet(PARQUET_TRIP_DATA)
vendors_parquet = q2_instance.spark.read.parquet(PARQUET_TRIP_VENDORS)

q2_instance.sql_api(trip_data=trip_data_parquet, vendors=vendors_parquet)

parquet_end_time = time.time()
parquet_q2_time = parquet_end_time - parquet_start_time
print(" Time needed to execute Q2 using SparkSQL and PARQUET: {}".format(parquet_q2_time))

objects = ("RDD", "SQL/csv", "SQL/parquet")
y_pos = np.arange(len(objects))

performance = [mr_q2_time, sql_q2_time, parquet_q2_time]

plt.barh(y_pos, performance, align="center", alpha=0.5)
plt.yticks(y_pos, objects)
plt.xlabel("Time(s)")
plt.title("Time needed for Query 2")
plt.show()
plt.savefig("Q2.png")
