#!/usr/bin/env bash

hadoop fs -put ../../project-data/yellow_trip_data/yellow_tripdata_1m.csv hdfs://master:9000/yellow_trip_data/.
hadoop fs -put ../../project-data/yellow_trip_data/yellow_tripvendors_1m.csv hdfs://master:9000/yellow_trip_data/.

hadoop fs -put ../../project-data/customer_complaints/customer_complaints.csv hdfs://master:9000/customer_complaints/.