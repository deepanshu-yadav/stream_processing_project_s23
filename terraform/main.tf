terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "3.79.0"
    }
    databricks = {
      source = "databricks/databricks"
      version = "1.3.1"
    }
  }
}

provider "azurerm" {
  features {}

  subscription_id   = ""
  tenant_id         = ""
  client_id         = ""
  client_secret     = ""
}

# resource "azurerm_resource_group" "app_grp" {
#   name     = var.rg_name
#   location = var.rg_location

# }



#############################################################################
resource "azurerm_databricks_workspace" "azure_db" {
  name                        = var.databricks_name
  resource_group_name         = var.rg_name
  location                    = var.rg_location
  sku                         = "standard"
  managed_resource_group_name = "${var.databricks_name}-workspace-rg"
}


#############################################################################

######### Azure Linux Virtual Machine deployment #########

module "kafka_vm" {
  source               = "./terraform-modules/virtual_machine"
  rg_name              = var.rg_name
  location             = var.rg_location
  vnet_name            = var.kafka_vnet_name
  vnet_adddress        = var.kafka_vnet_address
  subnet_nameList      = var.kafka_subnet_nameList
  subnet_addressList   = var.kafka_subnet_addressList
  pip_name             = var.kafka_pip_name
  pip_allocation       = var.pip_allocation
  vm_nic_name          = var.kafka_vm_nic_name
  ip_configuration     = var.kafka_ip_configuration
  nsg_name             = var.kafka_nsg_name
  vm_name              = var.kafka_vm_name
  vm_size              = var.vm_size
  vm_username          = var.vm_username
  vm_password          = var.vm_password
  vm_image_publisher   = var.vm_image_publisher
  vm_image_offer       = var.vm_image_offer
  vm_image_sku         = var.vm_image_sku
  vm_image_version     = var.vm_image_version
  vm_os_disk_strg_type = var.vm_os_disk_strg_type
  vm_os_disk_caching   = var.vm_os_disk_caching
}


### Extensions


# resource "azurerm_virtual_machine_extension" "vmext" {
#     resource_group_name     = "${var.rg_name}"
#     location                = "${var.rg_location}"
#     name                    = "${var.kafka_vm_name}-vmext"

#     virtual_machine_name = "${var.kafka_vm_name}"
#     publisher            = "Microsoft.Azure.Extensions"
#     type                 = "CustomScript"
#     type_handler_version = "2.0"

#     protected_settings = <<PROT
#     {
#         "script": "${base64encode(file(var.scfile))}"
#     }
#     PROT
# }


# resource "azurerm_virtual_machine_extension" "kafka_vm_ext" {
#   name                 = "customScript"
#   virtual_machine_id   =  module.kafka_vm.id
#   publisher            = "Microsoft.Compute"
#   type                 = "CustomScriptExtension"
#   type_handler_version = "1.10"

#   settings = <<SETTINGS
#     {
#         "script": "${var.scfile}"
#     }
# SETTINGS
# }

# resource "azurerm_virtual_machine_extension" "kafka_extension" {
#   name                 = "customScript"
#   virtual_machine_id   = azurerm_windows_virtual_machine.kafka_vm.id
#   publisher            = "Microsoft.Compute"
#   type                 = "CustomScriptExtension"
#   type_handler_version = "1.10"

#   settings = <<SETTINGS
#     {
#         "script": "${var.kafka_extension_script}"
#     }
# SETTINGS

# }