# Users can use Depends to bring IO / Processor dependencies into their fn.

from typing import Any

from sqlalchemy.orm import Session

from buildflow import ComputeNode, Depends
from buildflow.io import BigQuery, PostGres, PubSub

app = ComputeNode()


@app.processor(
    source=PubSub(topic="projects/my-project/topics/my-topic1"),
    sink=BigQuery(table_id="project.dataset.table2"),
)
def dead_letter_queue(
        message: Any,
        db: Session = Depends(PostGres(...)),
) -> Any:
    if message["status"] == "special_case":
        db.insert(message)
    return message


@app.processor(
    source=PubSub(topic="projects/my-project/topics/my-topic1"),
    sink=BigQuery(table_id="project.dataset.table2"),
)
def stream_processor(
        message: Any,
        dlq: PubSub = Depends(dead_letter_queue),
) -> Any:
    if message["status"] == "failed":
        dlq.publish(message)
    return message


app.run()
