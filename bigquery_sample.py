"""Sample for reading and writing to BigQuery."""

import logging
import time
import buildflow as flow

_QUERY = """\
SELECT
  *
FROM
  `daring-runway-374503.covid_19_search_trends_symptoms_dataset.county_28d_historical`
LIMIT 10
"""


@flow.processor(
    input_ref=flow.BigQuery(query=_QUERY),
    output_ref=flow.BigQuery(
        table_id='daring-runway-374503.ray_example.public'),
)
def process(bq_row):
    return bq_row


logging.basicConfig(level=logging.INFO)
start = time.time()
output = flow.run()
total_time = time.time() - start
print('OUTPUT: ', output)
print('TOTAL SECONDS = ', total_time)
