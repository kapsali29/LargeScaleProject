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
    local_file_path = "file:///{}".format(file_path)
    data = spark.read.csv(local_file_path, header=True, inferSchema=True)
    return data


def write_df_to_hdfs(df, hdfs_path):
    """This function is used to write a DataFrame to HDFS"""
    df.write.csv(hdfs_path)
