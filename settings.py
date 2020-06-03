import os

# APPLICATION SETTINGS

YELLOW_TRIP_DATA = '/home/user/project-data/yellow_trip_data'
TRIP_DATA = os.path.join(YELLOW_TRIP_DATA, 'yellow_tripdata_1m.csv')
TRIP_VENDORS = os.path.join(YELLOW_TRIP_DATA, 'yellow_tripvendors_1m.csv')

HDFS_TRIP_DATA_PATH = "hdfs://yellow_trip_data/yellow_tripdata_1m.csv"
HDFS_TRIP_VENDORS_PATH = "hdfs://yellow_trip_data/yellow_tripvendors_1m.csv"
