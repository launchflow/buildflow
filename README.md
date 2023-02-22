# buildflow

![CI](https://github.com/launchflow/buildflow/actions/workflows/python_ci.yaml/badge.svg)

**buildflow** is a unified **batch** and **streaming** framework that turns
any python function into a scalable data pipeline.

Key Features:

- Fast - Scalable multiprocessing powered by [Ray](https://ray.io)
- Easy to learn- Get started with 2 lines of code
- Production Ready - Ready made IO connectors let users focus on processing
  data instead of reading and writing data

## Quick Start

Install the framework

```
pip install buildflow
```

Import the framework.

```python
import buildflow as flow
```

Add the `flow.processor` decorator to your function to attach IO.

```python
QUERY = 'SELECT * FROM `table`'
@flow.processor(input_ref=flow.BigQuery(query=QUERY))
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
# Turn your function into a stream processor
@flow.processor(
   input_ref=flow.PubSub(subscription_id='my_subscription'),
   output_ref=flow.BigQuery(table_id='project.dataset.table'),
)
def stream_process(pubsub_message):
   ...

flow.run()

```

Batch pipeline reading and writing to BigQuery.

```python
import buildflow as flow

QUERY = 'SELECT * FROM `project.dataset.input_table`'
@flow.processor(
    input_ref=flow.BigQuery(query=QUERY),
    output_ref=flow.BigQuery(table_id='project.dataset.output_table'),
)
def process(bigquery_row):
    ...

flow.run()
```

Batch pipeline reading from BigQuery and returning output locally.

```python
import buildflow as flow

QUERY = 'SELECT * FROM `table`'
@flow.processor(input_ref=flow.BigQuery(query=QUERY))
def process(bigquery_row):
    ...

processed_rows = flow.run()
```
