"""Sample for reading and writing to BigQuery."""

import logging
import time
import flowstate as flow

_QUERY = """\
SELECT
  *
FROM
  `daring-runway-374503.covid_19_search_trends_symptoms_dataset.county_28d_historical`
LIMIT 1000000
"""


@flow.processor(
    input_ref=flow.BigQuery(query=flow.BigQuery.Query(
        _QUERY,
        temporary_dataset='daring-runway-374503.temporary',
    )),
    output_ref=flow.BigQuery(project='daring-runway-374503',
                             dataset='ray_example',
                             table='public'),
)
def process(bq_row):
    return bq_row


logging.basicConfig(level=logging.INFO)
start = time.time()
output = flow.run()
print('TOTAL SECONDS = ', time.time() - start)
# TODO something funky going on here with nested data.
# print('OUTPUT: ', output)
# print('OUTPUT SIZE: ', len(output[0]))
