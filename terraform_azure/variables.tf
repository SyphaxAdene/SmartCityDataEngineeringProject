variable "resource_group_name" {
  description = "The name of the Resource Group"
  type        = string
}

variable "location" {
  description = "The location where the resources will be created"
  type        = string
}

variable "storage_account_name" {
  description = "The azure storage account name"
  type        = string
}

variable "datafactory_name" {
  description = "The name of the Azure Data Factory instance"
  type        = string
}

variable "key_vault_name" {
  description = "The name of the Azure Key Vault"
  type        = string
}

variable "databricks_workspace_name" {
  description = "The name of the Azure Databricks workspace"
  type        = string
}

variable "default_tags" {
  description = "Default tags to be applied to all resources"
  type        = map(string)
  default     = {
    environment = "dev"
    managed_by  = "terraform"
  }
}

# These variables are for documentation purposes.
# Values should be provided via environment variables or Azure CLI authentication.
variable "subscription_id" {
  description = "The Subscription ID where the resources will be created"
  type        = string
}

variable "tenant_id" {
  description = "The Tenant ID of the Azure AD"
  type        = string
}