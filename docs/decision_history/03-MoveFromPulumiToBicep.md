# Move from Pulumi to Bicep

Pulumi proved to have a couple of downfalls that were not conducive to this project and introduced unnecessary complexity in both development, deployment, and target users consuming this project.

## Pulumi Issues

- Pulumi required an external account to be created and extra tooling to be installed in order to deploy the project. In light of this, the project set up and deployment was significantly easier to simply cut Pulumi out of the equation and use a Azure Native solution.
- Pulumi tightly coupled infrastructure deployment and code deployment, making Pulumi the primary deployment mechanism. Although doable, this is less then ideal because it limits the options to create a DevOps pipeline and increasing the complexity of the IoC
- Pulumi Role Base Access Control (RBAC) access policy creation is not supported in the azure-native package requiring azure-native and azure Classic (based on Terraform) to run side-by-side. which then to even more tooling and problems to get Azure Class to deploy.
- Pulumi documentation for Azure Native is lacking in both examples and how to's on relatively common scenarios (RBAC to a Azure key vault after the creation of a function app for an example) making development significantly longer since a lot of things have to be figured out be trial and error

## Why Bicep?

[Bicep](https://learn.microsoft.com/en-us/azure/azure-resource-manager/bicep/overview?view=azure-devops) is a Domain-Specific-Language (DSL) based on Azure Resource Manager (ARM) templates. Because it is base on ARM it comes with all/most the capabilities of ARM but makes a greatly simplified syntax. Because of this, it is significantly easier to use and has a robust ecosystem of tools (syntax highlighting, resource group visualization, deployment, etc...) combined with the wealth of samples the development experience is greatly improved and the feedback loop is shortened.