# Azure Resource Group
resource "azurerm_resource_group" "az_rg" {
  name     = var.resource_group_name
  location = var.location
}

# Azure Storage Account
resource "azurerm_storage_account" "az_storage_account" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.az_rg.name
  location                 = azurerm_resource_group.az_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = "true"
  account_kind             = "StorageV2"

  tags = {
    environment = "dev"
  }
}

# Data Lake Gen2 Filesystem (Container)
resource "azurerm_storage_data_lake_gen2_filesystem" "data" {
  name               = "data"
  storage_account_id = azurerm_storage_account.az_storage_account.id
  depends_on = [
    azurerm_storage_account.az_storage_account
  ]
}

# Bronze layer
resource "azurerm_storage_data_lake_gen2_path" "bronze" {
  path               = "bronze"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.data.name
  storage_account_id = azurerm_storage_account.az_storage_account.id
  resource           = "directory"

  depends_on = [
    azurerm_storage_data_lake_gen2_filesystem.data
  ]
}

# Silver layer
resource "azurerm_storage_data_lake_gen2_path" "silver" {
  path               = "silver"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.data.name
  storage_account_id = azurerm_storage_account.az_storage_account.id
  resource           = "directory"

  depends_on = [
    azurerm_storage_data_lake_gen2_filesystem.data
  ]
}

# Gold layer
resource "azurerm_storage_data_lake_gen2_path" "gold" {
  path               = "gold"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.data.name
  storage_account_id = azurerm_storage_account.az_storage_account.id
  resource           = "directory"

  depends_on = [
    azurerm_storage_data_lake_gen2_filesystem.data
  ]
}

# Azure Key Vault
resource "azurerm_key_vault" "az_kv" {
  name                        = var.key_vault_name
  location                    = azurerm_resource_group.az_rg.location
  resource_group_name         = azurerm_resource_group.az_rg.name
  sku_name                    = "standard"
  tenant_id                   = var.tenant_id
  purge_protection_enabled    = true
}

# Azure Databricks Workspace
resource "azurerm_databricks_workspace" "az_db" {
  name                = var.databricks_workspace_name
  resource_group_name = azurerm_resource_group.az_rg.name
  location            = azurerm_resource_group.az_rg.location
  sku                 = "standard"
}

# Azure Data Factory
resource "azurerm_data_factory" "datafactory" {
  name                = var.datafactory_name
  location            = azurerm_resource_group.az_rg.location
  resource_group_name = azurerm_resource_group.az_rg.name
}