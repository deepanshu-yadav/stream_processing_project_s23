## Spark streaming on Azure.

If you had deployed this solution on Azure using terraform. 
(If not please do so in terraform section)
If yes then you will notice a resource group named stream_project_kafka_databricks(some random string at end). 
In this open azure data bricks resource group. 
It will open like this. 

![alt text](images/databricks.png?raw=true)

Launch the workspace. 

Once the workspace is launch is launched click on Notebook and upload the notebook given in the directory to the workspace.
It will be like this.
![alt text](images/notebook.png?raw=true)

Now you can execute the cells of notebook and wait for sometime till it fetches the most popular song among indian males.
