# Users can connect their processor to a batch source.

from dataclasses import dataclass

import pandas as pd
from ray.data import Dataset

from buildflow import Flow
from buildflow.io import BigQuery

flow = Flow()


@dataclass
class input_schema:
    key: str
    value: int


@dataclass
class output_schema:
    key: str
    aggregated_value: int


# Compare to the Cron example in launchflow_provider.py.
@flow.processor(
    source=BigQuery(table_id='project.dataset.table1', schema=input_schema),
    sink=BigQuery(table_id='project.dataset.table2', schema=output_schema),
)
def batch_process_bigquery(dataset: Dataset) -> Dataset:
    return dataset.groupby('key').map_groups(process_group,
                                             batch_format='pandas')


def process_group(df: pd.DataFrame) -> pd.DataFrame:
    # TODO: Add logic to process the group.
    return df
