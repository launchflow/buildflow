# Users can use Nodes to deploy multiple processors together.

from typing import Any

from ray.data import Dataset

from buildflow import ComputeNode
from buildflow.io import BigQuery, PubSub

app = ComputeNode()


@app.processor(
    source=BigQuery(table_id='project.dataset.table1'),
    sink=BigQuery(table_id='project.dataset.table2'),
)
def batch_process(dataset: Dataset) -> Dataset:
    return dataset


@app.processor(
    source=PubSub(topic='projects/my-project/topics/my-topic1'),
    sink=BigQuery(table_id='project.dataset.table2'),
)
def process_dead_letter_queue(message: Any) -> Any:
    return message


app.run()
