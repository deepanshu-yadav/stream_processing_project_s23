#=====================================================================================
#                   Reource Names
#=====================================================================================
rg_name                 = "stream_project"
rg_location             = "francecentral"
databricks_name         = "stream-databricks"
adls_name               = "streamadls"

#=====================================================================================
#                   Linux Virtual Machine CONFIGURATION
#=====================================================================================
pip_allocation = "Dynamic"
vm_size        = "Standard_B2ms"
vm_username    = "stream" 
vm_password    = "stream-123" 

vm_image_publisher = "Canonical"
vm_image_offer     = "0001-com-ubuntu-minimal-jammy"
vm_image_sku       = "minimal-22_04-lts-gen2"
vm_image_version   = "latest"

vm_os_disk_strg_type = "Standard_LRS"
vm_os_disk_caching   = "ReadWrite"


#=====================================================================================
#               KAFKA - VM - CONFIGURATION
#=====================================================================================
## VNET - SUBNET
kafka_vm_nic_name        = "kafka-vm-nic"
kafka_ip_configuration   = "kafka_ip_config"
kafka_nsg_name           = "kafka-vm-nsg"
kafka_vm_name            = "kafka-vm"
kafka_vnet_name          = "kafka-vm-vnet"
kafka_vnet_address       = "178.29.192.0/20"
kafka_subnet_nameList    = ["kafka-vm-snet"]
kafka_subnet_addressList = ["178.29.192.0/26"]
kafka_pip_name           = "kafka-vm-pip"

