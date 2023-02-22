"""Sample usage of buildflow reading and writing to BigQuery.

steps to run:
    1. pip install .
    2. gcloud auth application-default login
    3. python bigquery_sample.py
"""

import logging
import buildflow as flow

# TODO(developer: Fill in the table.
_QUERY = """\
SELECT
  *
FROM
  `TODO`
"""


@flow.processor(
    input_ref=flow.BigQuery(query=_QUERY),
    # TODO(developer(): Fill in the table. This should be in the format:
    #   project.dataset.table
    output_ref=flow.BigQuery(table_id='TODO'),
)
def process(bq_row):
    return bq_row


logging.basicConfig(level=logging.INFO)
output = flow.run()
