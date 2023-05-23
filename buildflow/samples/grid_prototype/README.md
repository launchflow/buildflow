# Examples of running

## Running individual nodes

We can optionally add the `--local` flag that will spin up the entire grid/node on a local cluster and use local resources instead of remote resources (e.g. gcp pubsub).

This will run the step count node by itself.

```
buildflow run step_count:node
```

This will run the gait node by itself

```
buildflow run gait:node
```

TODO: how does it work running a node that sends something to a downstream node?

## Running entire grid

```
buildflow run main:grid
```

# Other potential useful / interesting commands

Prints out the graph of your grids and nodes, and asks if you would like to create the required resources.

```
buildflow plan main:grid
```

So for this example it would look something like:

The parenthesis indicate which node a processor is running in.

```
Grid Graph:

    activity_classification(step_count_node)
                 ||
                \  /
                 \/
        step_count(step_count_node)
                 ||
                \  /
                 \/
            gait(gait_node)

Resources:
    node:
        - name: step_count_node
        - activity_classification:
            - topic: /projects/gcp-project/topics/ac_pubsub
            - subscription: /projects/gcp-project/subscriptions/ac_pubsub
            - bigquery_table: gcp-project.dataset.activity_classification
        - step_count:
            - topic: /projects/gcp-project/topics/sc_pubsub
            - subscription: /projects/gcp-project/subscriptions/sc_pubsub
            - bigquery_table: gcp-project.dataset.step_count

    node:
        - name: gait_node
        - gait:
            - topic: /projects/gcp-project/topics/gait_pubsub
            - subscription: /projects/gcp-project/subscriptions/gait_pubsub
            - bigquery_table: gcp-project.dataset.gait

Would you like to create the desired resources (Y/n)?
```

Could also add an `as-user` flag to ensure the user has the correct permissions to read the resources.

```
buildflow plan main:grid --as-user=a-service-account
```



