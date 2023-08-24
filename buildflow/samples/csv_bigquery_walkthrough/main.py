import csv
import dataclasses
import datetime
import io
import os
from typing import List

import buildflow
from buildflow.io.gcp import (
    BigQueryDataset,
    BigQueryTable,
    GCSBucket,
    GCSFileChangeStream,
)
from buildflow.types.gcp import GCSFileChangeEvent

gcp_project = os.environ["GCP_PROJECT"]
bucket_name = os.environ["BUCKET_NAME"]
bigquery_table = os.environ.get("BIGQUERY_TABLE", "wiki-page-views")
dataset = os.environ.get("DATASET", "buildflow_walkthrough")

# Set up a subscriber for the source.
# The source will setup a Pub/Sub topic and subscription to listen to new files
# uploaded to the GCS bucket.
source = GCSFileChangeStream(
    gcs_bucket=GCSBucket(
        project_id=gcp_project,
        bucket_name=bucket_name,
    ).options(
        managed=True,
        force_destroy=True,
        bucket_region="US",
    ),
)
# Set up a BigQuery table for the sink.
# If this table does not exist yet BuildFlow will create it.
sink = BigQueryTable(
    BigQueryDataset(project_id=gcp_project, dataset_name=dataset).options(managed=True),
    table_name=bigquery_table,
).options(managed=True, destroy_protection=False)


# Nested dataclasses can be used inside of your schemas.
@dataclasses.dataclass
class HourAggregate:
    hour: datetime.datetime
    stat: int


# Define an output type for our pipeline.
# By using a dataclass we can ensure our python type hints are validated
# against the BigQuery table's schema.
@dataclasses.dataclass
class AggregateWikiPageViews:
    date: datetime.date
    wiki: str
    title: str
    daily_page_views: int
    max_page_views_per_hour: HourAggregate
    min_page_views_per_hour: HourAggregate


app = buildflow.Flow(
    flow_options=buildflow.FlowOptions(
        require_confirmation=False, runtime_log_level="DEBUG"
    )
)


# Define our processor.
@app.pipeline(source=source, sink=sink)
def process(gcs_file_event: GCSFileChangeEvent) -> List[AggregateWikiPageViews]:
    csv_string = gcs_file_event.blob.decode()
    csv_reader = csv.DictReader(io.StringIO(csv_string))
    aggregate_stats = {}
    for row in csv_reader:
        timestamp = datetime.datetime.strptime(
            row["datehour"], "%Y-%m-%d %H:%M:%S.%f %Z"
        )
        wiki = row["wiki"]
        title = row["title"]
        views = row["views"]

        key = (wiki, title)
        if key in aggregate_stats:
            stats = aggregate_stats[key]
            stats.daily_page_views += views
            if views > stats.max_page_views_per_hour.stat:
                stats.max_page_views_per_hour = HourAggregate(timestamp, views)
            if views < stats.min_page_views_per_hour.stat:
                stats.min_page_views_per_hour = HourAggregate(timestamp, views)
        else:
            aggregate_stats[key] = AggregateWikiPageViews(
                date=timestamp.date(),
                wiki=wiki,
                title=title,
                daily_page_views=views,
                max_page_views_per_hour=HourAggregate(timestamp, views),
                min_page_views_per_hour=HourAggregate(timestamp, views),
            )

    return list(aggregate_stats.values())
