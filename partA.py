from helpers import init_spark, load_csv_data
from settings import TRIP_DATA

spark, sc = init_spark()
trip_data = load_csv_data(spark, TRIP_DATA)