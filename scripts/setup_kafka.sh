#!/bin/bash

# Environment variables
# Source the .env file to load the variables
if [ -f ../.env ]; then
  source ../.env
else
  echo "Error: .env file not found."
  exit 1
fi

# Export the variables
export KAFKA_BROKER_ADDRESS
export KAFKA_BROKER_PORT
export KAFKA_EVENTS_TOPIC

# Update the package lists
sudo apt update

# Install Java (required for Kafka)
sudo apt install -y default-jdk

echo "installed java"

# Download and extract Kafka
wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz
tar -xzf kafka_2.13-3.6.1.tgz
sudo mv kafka_2.13-3.6.1 /opt/kafka
rm kafka_2.13-3.6.1.tgz

echo "Copied kafka."


# Start Zookeeper (required for Kafka)
/opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties

echo "Started Zoo keeper."

# Wait for Zookeeper to start
sleep 30

# Start Kafka
/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties

echo "Started kafka broker."

# Wait for Kafka to start
sleep 30

# Create Kafka topic 
/opt/kafka/bin/kafka-topics.sh --create --topic $KAFKA_EVENTS_TOPIC --bootstrap-server "$KAFKA_BROKER_ADDRESS:$KAFKA_BROKER_PORT" --partitions 1 --replication-factor 1

echo "Created topic $KAFKA_EVENTS_TOPIC on $KAFKA_BROKER_ADDRESS:$KAFKA_BROKER_PORT "

echo "Kafka and Zookeeper installation completed."

sudo apt install pip

pip install -r ../requirements.txt

echo "Installed python requirments."

echo "Running python script."

cd kafka/ && python3 producer.py




