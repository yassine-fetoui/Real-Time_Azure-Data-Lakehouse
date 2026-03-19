terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm    = { source = "hashicorp/azurerm", version = "~> 3.80" }
    databricks = { source = "databricks/databricks", version = "~> 1.30" }
    snowflake  = { source = "Snowflake-Labs/snowflake", version = "~> 0.76" }
  }

  backend "azurerm" {
    resource_group_name  = "rg-tfstate"
    storage_account_name = var.tf_state_storage_account
    container_name       = "tfstate"
    # key is set per environment: dev/terraform.tfstate | prod/terraform.tfstate
  }
}

provider "azurerm" {
  features {}
}

# ─── Resource Group ────────────────────────────────────────────────────────────

resource "azurerm_resource_group" "lakehouse" {
  name     = "rg-lakehouse-${var.environment}"
  location = var.location
  tags     = local.common_tags
}

# ─── Modules ───────────────────────────────────────────────────────────────────

module "adls" {
  source              = "../../modules/adls"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = var.location
  environment         = var.environment
  tags                = local.common_tags
}

module "databricks" {
  source              = "../../modules/databricks"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = var.location
  environment         = var.environment
  adls_id             = module.adls.storage_account_id
  tags                = local.common_tags
}

module "kafka" {
  source              = "../../modules/kafka"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = var.location
  environment         = var.environment
  tags                = local.common_tags
}

module "aks" {
  source              = "../../modules/aks"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = var.location
  environment         = var.environment
  node_count          = var.aks_node_count
  node_vm_size        = var.aks_node_vm_size
  tags                = local.common_tags
}

module "snowflake" {
  source      = "../../modules/snowflake"
  environment = var.environment
}

# ─── Locals ────────────────────────────────────────────────────────────────────

locals {
  common_tags = {
    environment = var.environment
    project     = "azure-lakehouse"
    managed_by  = "terraform"
    owner       = "data-engineering"
  }
}
