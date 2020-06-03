# Install unzip
sudo apt-get install -y unzip

# Get taxi data
wget http://www.cslab.ntua.gr/courses/atds/yellow_trip_data.zip

# Unzip data
unzip yellow_trip_data.zip -d yellow_trip_data
