from buildflow import Grid

from .step_count import node as sc_node
from .gait import node as gait_node

grid = Grid()

# All nodes get deployed to one cluster.
#    So the step count node will run both activity classification and step count
#    with PubSub inbetween
#
#   Gait will run in it's own cluster
grid.add_node(sc_node, name="step_count")
grid.add_node(gait_node, name="gait")
