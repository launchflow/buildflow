"""Sample for reading and writing to BigQuery."""

import flowstate as flow

_QUERY = """\
SELECT
  *
FROM
  `daring-runway-374503.covid_19_search_trends_symptoms_dataset.county_28d_historical`
"""


@flow.processor(input_ref=flow.BigQuery(query=flow.BigQuery.Query(
    _QUERY,
    temporary_dataset='daring-runway-374503.temporary',
)))
def process(bq_row):
    return bq_row


flow.run()
