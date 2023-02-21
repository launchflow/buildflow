"""Sample for reading and writing to BigQuery."""

import logging
import time
import launchflow as flow

_QUERY = """\
SELECT
  *
FROM
  `daring-runway-374503.covid_19_search_trends_symptoms_dataset.county_28d_historical`
LIMIT 1
"""


@flow.processor(
    input_ref=flow.BigQuery(query=flow.BigQuery.Query(
        _QUERY,
        temporary_dataset='daring-runway-374503.temporary',
    )),
)
def process(bq_row):
    return bq_row


logging.basicConfig(level=logging.INFO)
start = time.time()
output = flow.run()
print('TOTAL SECONDS = ', time.time() - start)
# TODO something funky going on here with nested data.
print('OUTPUT: ', output)
