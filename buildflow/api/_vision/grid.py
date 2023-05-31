# Users can use Grids to deploy multiple nodes together.

from buildflow import ComputeNode, DeploymentGrid

# In practice, this would be imported from a separate file.
app1 = ComputeNode()
...

app2 = ComputeNode()
...

grid = DeploymentGrid([app1, app2])

grid.deploy(...)
