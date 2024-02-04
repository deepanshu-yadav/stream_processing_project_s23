# Processing Real Time data from spotify using kafka and data bricks.

This project gets data from spotify API ingest into a kafka broker. Spark streaming can be connected to this broker
and process it. 
This project can be setup on your local computer or even deployed to azure.

### Directory Structure
```

    ├── README.md
    ├── kafka
    │   ├── cities.csv
    │   ├── config.yml
    │   ├── kafka_utils.py
    │   ├── producer.py
    │   ├── spotify_utils.py
    │   └── user_utils.py
    ├── requirements.txt
    ├── scripts
    │   ├── install_docker.sh
    │   └── setup_kafka.sh
    ├── spark_streaming
    │   ├── README.md
    │   ├── images
    │   │   ├── databricks.png
    │   │   └── notebook.png
    │   ├── process_stream.py
    │   ├── schema.py
    │   └── spark streaming notebook.ipynb
    └── terraform
        ├── README.md
        ├── data.tf
        ├── images
        │   └── resources.png
        ├── main.tf
        ├── modules
        │   └── general_vm
        │       ├── main.tf
        │       ├── outputs.tf
        │       ├── providers.tf
        │       ├── run_kafka.sh
        │       └── variables.tf
        ├── outputs.tf
        ├── providers.tf
        ├── terraform.tfstate
        ├── terraform.tfstate.backup
        ├── terraform.tfvars
        ├── variables.tf
        ├── vnet.tf
        └── workspace.tf
```

### Spotify 
You need to provide your credentials in the  .env file. 

Go to https://developer.spotify.com/. 
Sign in with your account.


Now create a new app as follows

![alt text](images/app.png?raw=true)

After filling in all the options click on submit
![alt text](images/submit.png?raw=true)

Now go to settings click on app settings 
Note down your client id and client secret as follows:
![alt text](images/cred.png?raw=true)

Note No need to fill in the .env file with your secret. I have already done. When the kafka broker VM gets deployed it uses these credentials. 
Future scope would be to use credentails given by user when they are used in azure VM.

### Kafka 
The Realtime data from spotify gets ingested into Kakfa. Look at the kafka directory to know more about code.

#### Local
For running it locally use
1. Install kafka.
2. python -m pip install -r requirements.txt
3. cd kafka
4. Create a topic mentioned in the .env file.
3. Now run `python producer.py`

#### Azure
For running it on azure follow this [README](terraform/README.md)

### Spark Streaming

We can connect to kafka cluster and process the data. One such example is

Find out which song is most popular among Indian males?

We can either run locally or on azure. 

#### Locally
1. For that you must ensure kafka is running. Follow the kafka section for that.
2. Install pyspark.
3. cd spark_streaming
4. Run `python process_stream.py`

#### Azure 

Follow this [README](spark_streaming/README.md) to know more.

## Future Work.

1. Include CI / CD with GitHub actions.
2. Handle spotify credentials better.
3. Try more complex queries.
4. Try delta tables.
5. Add vizualization like showing popular songs in every city of India and display it on a map.
6. Store query results in Azure blob storage.
