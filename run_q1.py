import time

import numpy as np

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

from part1A.Q1 import Q1
from settings import HDFS_TRIP_DATA_PATH, PARQUET_TRIP_DATA

q1_instance = Q1()

trip_data = q1_instance.spark.read.csv(HDFS_TRIP_DATA_PATH)

# Execute Q1 using SPARK-SQL and CSV
sql_start_time = time.time()

q1_instance.sql_api(trip_data=trip_data)

sql_end_time = time.time()
sql_q1_time = sql_end_time - sql_start_time
print(" Time needed to execute Q1 using SparkSQL and CSV: {}".format(sql_q1_time))

trip_data_rdd = trip_data.rdd

# Execute Q1 using MR and CSV
mr_start_time = time.time()

q1_instance.sql_api(trip_data=trip_data)

mr_end_time = time.time()
mr_q1_time = mr_end_time - mr_start_time
print(" Time needed to execute Q1 using Map Reduce and CSV: {}".format(mr_q1_time))

# Execute Q1 using SPARK-SQL and PARQUET
trip_data_parquet = q1_instance.spark.read.parquet(PARQUET_TRIP_DATA)

parquet_start_time = time.time()

q1_instance.sql_api(trip_data_parquet)

parquet_end_time = time.time()
parquet_q1_time = parquet_end_time - parquet_start_time
print(" Time needed to execute Q1 using SparkSQL and PARQUET: {}".format(parquet_q1_time))

objects = ("RDD", "SQL/csv", "SQL/parquet")
y_pos = np.arange(len(objects))

performance = [mr_q1_time, sql_q1_time, parquet_q1_time]

plt.barh(y_pos, performance, align="center", alpha=0.5)
plt.yticks(y_pos, objects)
plt.xlabel("Time(s)")
plt.title("Time needed for Query 1")
plt.show()
plt.savefig("Q1.png")
