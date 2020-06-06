from datetime import datetime

from pyspark.sql import SparkSession
from math import radians, cos, sin, asin, sqrt

from settings import DATE_FORMAT


def init_spark():
    """This function is used to initialize Spark Session and Context"""
    spark = SparkSession.builder.appName("PROJECT").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")  # Set logLevel to ERROR
    return spark, sc


def load_csv_data(spark, file_path):
    """
    This function is used to load csv files

    :param spark: spark session
    :return: csv file
    """
    data = spark.read.csv(file_path)
    return data


def write_df_to_hdfs(df, hdfs_path):
    """This function is used to write a DataFrame to HDFS"""
    df.write.csv(hdfs_path)


def df_to_parquet(df, path):
    """This function is used to store a DataFrame as Parquet type"""
    df.write.parquet(path)


def load_parquet_file(spark, file_path):
    """This function is used to load a parquet file as DataFrame"""
    parquet_file = spark.read.parquet(file_path)
    return parquet_file


def date_to_hour(row):
    """This function received a row and transform date to Hour"""
    hour = row[1].split()[1][:2]
    latitude = row[3]
    longitude = row[4]
    return hour, latitude, longitude


def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """

    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles

    distance = c * r

    return distance


def elapsed_time(start, end):
    """This function is used to find trip duration"""

    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')

    duration = end_date - start_date
    days, seconds = duration.days, duration.seconds
    minutes = (seconds % 3600) // 60
    return minutes
