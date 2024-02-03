## ADB - Kafka Single VM Demo environment

This template provisions a single VM and Azure Databricks workspace. Major components to deploy include:
- 1 Vnet with 3 subnets (2 for Databricks, 1 for Kafka VM)
- 1 Azure VM (to host Kafka and Zookeeper services), with port 9092 exposed to other devices in same VNet (allowed by default NSG rules).
- 1 VNet injected Azure Databricks Workspace
- NSGs for Databricks and Kafka subnets

## Folder Structure
.
├── README.md
├── data.tf
├── images
│   ├── resources.png
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

`terraform.tfvars` is provided as reference variable values, you should change it based on your need.

## Getting Started

> Step 1: Preparation

Clone this repo to your local, and run `az login` to interactively login thus get authenticated with `azurerm` provider.

> Step 2: Deploy resources

Change the `terraform.tfvars` to your need (you can also leave as default values as a random string will be generated in prefix), then run:
```bash
terraform init
terraform apply
```
This will deploy all resources wrapped in a new resource group to your the default subscription of your `az login` profile; you will see the public ip address of the VM after the deployment is done. After deployment, you will get below resources:

![alt text](images/resources.png?raw=true)


> Step 3: Integration with Azure Databricks

No need to install anything in the server. Just open your data bricks workspace (it is in the same resource group as the vm) and then open a new notebook. 

Run the following piece of code in the notebook.

```
df = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "10.179.0.4:9092")
  .option("subscribe", "spotify")
  .option("startingOffsets", "latest")
  .load()
)
```
Notice `10.179.0.4` is from the configuration of subnet. Also `9092` is for port. Also `spotify` is for topic. A future work could be using the .env file presnt in parent directory. 
