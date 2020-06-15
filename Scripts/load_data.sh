# Create Data Directory
mkdir $HOME/project-data

# Install unzip
sudo apt-get install -y unzip

# Go to project-data directory
cd $HOME/project-data

# Get taxi data
wget http://www.cslab.ntua.gr/courses/atds/yellow_trip_data.zip

# Unzip data
unzip yellow_trip_data.zip -d yellow_trip_data

# Get customer complaints data
wget http://www.cslab.ntua.gr/courses/atds/customer_complains.tar.gz
mkdir customer_complaints

# Extract customer complaints
tar xvzf  customer_complains.tar.gz -C customer_complaints/

# Remove compressed files from directory
rm *.gz *.zip
