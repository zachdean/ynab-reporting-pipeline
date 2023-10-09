# Move from DataBricks to Azure Functions

The initial approach for this project was to use DataBricks to ingest and transform the data. The reasoning for this was because it is a common tool in this space and has the ability to handle datasets much, much larger then what is used in this project. One of the key aspects of this project is the ability to deploy the entire project across many different Azure accounts. This however, is where the problems started to come in.

First, because of how DataBricks works, I was not able to build the IoC in a single step. The user would first have to deploy the Azure resources, then log into DataBricks to get there user token, then take that token and set some environment variables so that the Pulumi DataBricks provider will work.

Secondly, There was some pretty significant limitations on the Pulumi DataBricks provider. It could be that I was not building the infrastructure correctly, but the documentation just simply gives descriptions of the resource parameters, and was less helpful in actually building a resource. I also ran across things that are likely bugs (like adding a time trigger was not working for some reason, it kept wanting a file trigger as well). Although these limitations are solvable, they would significantly more time then it is worth or a custom connector talking directly to the DataBricks API.

The Original code is available [here](https://github.com/dfroslie-ndsu-org/f23-project-zachdean/tree/40c833b75451f532925d2a97bba2e65d4c02ad8f)

**Decision:** Drop DataBricks as the compute resource and move to Azure Functions