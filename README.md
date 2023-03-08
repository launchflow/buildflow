# buildflow

![CI](https://github.com/launchflow/buildflow/actions/workflows/python_ci.yaml/badge.svg)

**buildflow** is a unified **batch** and **streaming** framework that turns
any python function into a scalable data pipeline that can read from our
supported IO resources.

Key Features:

- Production Ready - Ready made IO connectors let users focus on processing
  data instead of reading and writing data
- Fast - Scalable multiprocessing powered by [Ray](https://ray.io)
- Easy to learn- Get started with 2 lines of code

## Quick Start

Install the framework

```
pip install buildflow
```

Import the framework and create a flow.

```python
from buildflow import Flow
import buildflow

flow = Flow()
```

Add the `flow.processor` decorator to your function to attach IO.

```python
QUERY = 'SELECT * FROM `table`'
@flow.processor(source=buildflow.BigQuery(query=QUERY))
def process(bigquery_row):
    ...
```

Use `flow.run()` to kick off your pipeline.

```python
flow.run()
```

## Examples

All samples can be found [here](https://github.com/launchflow/buildflow/tree/main/samples).

Streaming pipeline reading from Google Pub/Sub and writing to BigQuery.

```python
import buildflow
from buildflow import Flow

flow = Flow()

# Turn your function into a stream processor
@flow.processor(
   source=buildflow.PubSub(subscription='my_subscription'),
   sink=buildflow.BigQuery(table_id='project.dataset.table'),
)
def stream_process(pubsub_message):
   ...

flow.run()

```

Streaming pipeline reading from / writing to Google Pub/Sub.

```python
import buildflow
from buildflow import Flow

flow = Flow()

# Turn your function into a stream processor
@flow.processor(
   source=buildflow.PubSub(subscription='my_subscription'),
   sink=buildflow.PubSub(topic='my_topic'),
)
def stream_process(pubsub_message):
   ...

flow.run()

```

Batch pipeline reading and writing to BigQuery.

```python
import buildflow
from buildflow import Flow

flow = Flow()

QUERY = 'SELECT * FROM `project.dataset.input_table`'
@flow.processor(
    source=buildflow.BigQuery(query=QUERY),
    sink=buildflow.BigQuery(table_id='project.dataset.output_table'),
)
def process(bigquery_row):
    ...

flow.run()
```

Batch pipeline reading from BigQuery and returning output locally.

```python
import buildflow
from buildflow import Flow

flow = Flow()

QUERY = 'SELECT * FROM `table`'
@flow.processor(source=buildflow.BigQuery(query=QUERY))
def process(bigquery_row):
    ...

output = flow.run()
process_rows = output['process']['local']
```
