import time

import numpy as np

import matplotlib

from part1A.Q2 import Q2
from settings import HDFS_TRIP_DATA_PATH, HDFS_TRIP_VENDORS_PATH, PARQUET_TRIP_DATA, PARQUET_TRIP_VENDORS

q2_instance = Q2()

mr_start_time = time.time()

trip_data_rdd = q2_instance.sc.textFile(HDFS_TRIP_DATA_PATH)
vendors_rdd = q2_instance.sc.textFile(HDFS_TRIP_VENDORS_PATH)

q2_instance.mr_api(trip_data=trip_data_rdd, vendors=vendors_rdd)

mr_end_time = time.time()
mr_q2_time = mr_end_time - mr_start_time
print(" Time needed to execute Q2 using Map Reduce and CSV: {}".format(mr_q2_time))

