"""Sample for reading and writing to BigQuery."""

import logging
import buildflow as flow

# TODO: fill in query in example.
_QUERY = """\
SELECT
  *
FROM
  `TABLE_ID`
LIMIT 10
"""


@flow.processor(
    input_ref=flow.BigQuery(query=_QUERY),
    # TODO: fill in out put table ID. Should be of format:
    #   project.dataset.table
    output_ref=flow.BigQuery(table_id='TABLE_ID'),
)
def process(bq_row):
    return bq_row


logging.basicConfig(level=logging.INFO)
output = flow.run()
