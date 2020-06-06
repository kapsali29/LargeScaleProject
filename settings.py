import os

# HADOOP SETTINGS
MASTER_HOST = 'master'
MASTER_PORT = 9000

# APPLICATION SETTINGS

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

YELLOW_TRIP_DATA = '/home/user/project-data/yellow_trip_data'
TRIP_DATA = "file:///{}".format(os.path.join(YELLOW_TRIP_DATA, 'yellow_tripdata_1m.csv'))
TRIP_VENDORS = "file:///{}".format(os.path.join(YELLOW_TRIP_DATA, 'yellow_tripvendors_1m.csv'))

HDFS_TRIP_DATA_PATH = "hdfs://{}:{}/yellow_trip_data/yellow_tripdata_1m.csv".format(MASTER_HOST, MASTER_PORT)
HDFS_TRIP_VENDORS_PATH = "hdfs://{}:{}/yellow_trip_data/yellow_tripvendors_1m.csv".format(MASTER_HOST, MASTER_PORT)

PARQUET_TRIP_DATA = "hdfs://{}:{}/yellow_trip_data/yellow_tripdata_1m.parquet".format(MASTER_HOST, MASTER_PORT)
PARQUET_TRIP_VENDORS = "hdfs://{}:{}/yellow_trip_data/yellow_tripvendors_1m.parquet".format(MASTER_HOST, MASTER_PORT)
