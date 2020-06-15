import os

# HADOOP SETTINGS
MASTER_HOST = 'master'
MASTER_PORT = 9000

# ######################
# APPLICATION SETTINGS
########################

# PART1 SETTINGS
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

YELLOW_TRIP_DATA = '/home/user/project-data/yellow_trip_data'
TRIP_DATA = "file:///{}".format(os.path.join(YELLOW_TRIP_DATA, 'yellow_tripdata_1m.csv'))
TRIP_VENDORS = "file:///{}".format(os.path.join(YELLOW_TRIP_DATA, 'yellow_tripvendors_1m.csv'))

HDFS_TRIP_DATA_PATH = "hdfs://{}:{}/yellow_trip_data/yellow_tripdata_1m.csv".format(MASTER_HOST, MASTER_PORT)
HDFS_TRIP_VENDORS_PATH = "hdfs://{}:{}/yellow_trip_data/yellow_tripvendors_1m.csv".format(MASTER_HOST, MASTER_PORT)

PARQUET_TRIP_DATA = "hdfs://{}:{}/yellow_trip_data/yellow_tripdata_1m.parquet".format(MASTER_HOST, MASTER_PORT)
PARQUET_TRIP_VENDORS = "hdfs://{}:{}/yellow_trip_data/yellow_tripvendors_1m.parquet".format(MASTER_HOST, MASTER_PORT)

# PART2 SETTINGS
CUSTOMER_COMPLAINTS_HDFS = "hdfs://{}:{}/customer_complaints/customer_complaints.csv".format(MASTER_HOST, MASTER_PORT)

STOP_WORDS = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",
              "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself",
              "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these",
              "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do",
              "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
              "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
              "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
              "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
              "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
              "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]

