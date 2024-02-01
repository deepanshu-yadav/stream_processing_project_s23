# Use the Kafka base image
FROM wurstmeister/kafka:2.13-2.7.0

# Install Python and other dependencies
RUN apt-get update && \
    apt-get install -y python3 python3-pip

# Set the working directory
WORKDIR /app

# Expose the port the app runs on
EXPOSE 9092

# Copy the Python code from GitHub to the container
RUN git clone https://github.com/deepanshu-yadav/stream_processing_project_s23.git .

# Install Python dependencies
RUN pip3 install -r requirements.txt

RUN cd kafka
# Command to run your Python code
CMD ["python3", "producer.py"]