# Azure Data Engineering Infrastructure

This Terraform project sets up a comprehensive data engineering infrastructure on Microsoft Azure. It includes resources for data storage, processing, and analytics, tailored for a modern data engineering workflow.

## Resources Created

- Azure Resource Group
- Azure Storage Account (with Data Lake Gen2 capabilities)
- Data Lake Gen2 Filesystem with Bronze, Silver, and Gold layers
- Azure Key Vault
- Azure Databricks Workspace
- Azure Data Factory

## Prerequisites

- [Terraform](https://www.terraform.io/downloads.html) (latest version recommended)
- [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
- An Azure subscription
- Appropriate permissions to create resources in Azure

## Configuration

1. Clone this repository to your local machine.
2. Navigate to the project directory.
3. Copy `terraform.tfvars.template` to `terraform.tfvars` and fill in the required variables:

   ```
   cp terraform.tfvars.template terraform.tfvars
   ```

4. Edit `terraform.tfvars` with your specific values.

## Usage

1. Initialize Terraform:
   ```
   terraform init
   ```

2. Preview the changes:
   ```
   terraform plan
   ```

3. Apply the changes:
   ```
   terraform apply
   ```

4. When prompted, review the changes and enter `yes` to proceed.

## File Structure

- `main.tf`: Defines the main Azure resources.
- `variables.tf`: Declares input variables for the Terraform configuration.
- `terraform.tfvars`: (Not in version control) Assigns values to the declared variables.
- `provider.tf`: Configures the Azure provider.
- `backend.tf`: Configures the backend for storing Terraform state.
- `azure_pipeline.yaml`: Azure DevOps pipeline for CI/CD.
- `destroy_pipeline.yml`: Azure DevOps pipeline for destroying the infrastructure.
- `.gitignore`: Specifies intentionally untracked files to ignore.

## Security Notes

- Sensitive information like `subscription_id`, `client_id`, `client_secret`, and `tenant_id` should not be stored in `terraform.tfvars`. Use environment variables or Azure Key Vault to manage these securely.
- The `terraform.tfvars` file is intentionally ignored by git to prevent accidental commit of sensitive data.
- The `.gitignore` file is set up to exclude sensitive and generated files from version control.

## CI/CD

This project includes two Azure DevOps pipeline configurations:

1. `azure_pipeline.yaml`: For continuous integration and deployment. The pipeline:
   - Validates the Terraform configuration
   - Initializes Terraform
   - Plans the infrastructure changes
   - Applies the changes (if approved)

2. `destroy_pipeline.yml`: For destroying the infrastructure. Use with caution. The pipeline:
   - Initializes Terraform
   - Destroys all resources managed by Terraform

Ensure that you've set up the necessary variable group (`my_terraform_vg`) in Azure DevOps with the required variables.

## State Management

This project uses Azure Storage as a backend for storing Terraform state. The configuration is in `backend.tf`. Ensure you have the appropriate Azure Storage account set up before initializing Terraform.

## Best Practices

- Regularly update the Azure provider and Terraform version.
- Use consistent naming conventions for Azure resources.
- Implement proper access controls and network security for your Azure resources.
- Regularly review and rotate any secrets used in your infrastructure.
- Use the destroy pipeline with caution, preferably only in non-production environments.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request
