import flow_io
import ray

_QUERY = """\
SELECT
  *
FROM
  `daring-runway-374503.covid_19_search_trends_symptoms_dataset.county_28d_historical`
"""
flow_io.init(
    config={
        'input':
        flow_io.BigQuery(query=_QUERY),
        'outputs': [
            flow_io.Empty()
        ]
    })


@ray.remote
def process(bq_row):
    return bq_row


print(flow_io.ray_io.run(process.remote))
