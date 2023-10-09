# Use Pulumi for Infrastructure as Code (IoC)

[Pulumi](https://www.pulumi.com/) is a IoC tool that allows for infrastructure to be put into code instead of the manually building in the cloud portal. The benefit is that this project can then be easily replicated in the cloud by anyone who has a Azure Subscription, even if they have minimal knowledge in cloud computing. Pulumi is the chosen tool because it allows for a much more expressive IoC because we able to write in the language of the repository (python, in this case). In addition, it has resource providers for a vast array of resources, including Azure and DataBricks.

Decisions: Use Pulumi for IoC