from pyspark.sql import SparkSession


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
