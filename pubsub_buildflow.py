from typing import Any, Dict

import buildflow
from buildflow import Flow

input_sub = buildflow.PubSub(
    subscription='projects/daring-runway-374503/subscriptions/new-sub',
    topic='projects/pubsub-public-data/topics/taxirides-realtime')
output_table = buildflow.BigQuery(
    table_id='daring-runway-374503.taxi_ride_benchmark.taxi_ride_data')

flow = Flow()


@flow.processor(source=input_sub, sink=output_table)
def process(element: Dict[str, Any]) -> Dict[str, Any]:
    return element


flow.run()
