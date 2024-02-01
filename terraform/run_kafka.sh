#!/bin/bash

echo "Installing git"
sudo apt update && sudo apt install -y git

# Specify the destination folder
destination_folder="/opt/stream_project"

# Check if the folder exists or not
if [ ! -d "$destination_folder" ]; then
    echo "Creating folder: $destination_folder"
    mkdir -p "$destination_folder"
else
    echo "Folder $destination_folder already exists."
fi

echo "Cloning repository"
# Clone the Git repository into the specified folder
git clone https://github.com/deepanshu-yadav/stream_processing_project_s23.git  "$destination_folder"

echo "Running script"
cd "$destination_folder/scripts" && ./setup_kafka.sh
