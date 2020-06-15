#!/usr/bin/env bash
echo "Move yellow trip data to HDFS"
hadoop fs -put $HOME/project-data/yellow_trip_data/yellow_tripdata_1m.csv hdfs://master:9000/yellow_trip_data/.
hadoop fs -put $HOME/project-data/yellow_trip_data/yellow_tripvendors_1m.csv hdfs://master:9000/yellow_trip_data/.

echo "Move customer complaints data to HDFS"
hadoop fs -put $HOME/project-data/customer_complaints/customer_complaints.csv hdfs://master:9000/customer_complaints/.