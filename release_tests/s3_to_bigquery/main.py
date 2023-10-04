import csv
import dataclasses
import datetime
import io
import os
from typing import List

import buildflow
from buildflow.io.aws import S3Bucket, S3FileChangeStream
from buildflow.io.gcp import (
    BigQueryDataset,
    BigQueryTable,
)
from buildflow.types.aws import S3ChangeStreamEventType, S3FileChangeEvent

gcp_project = os.environ["GCP_PROJECT"]
bucket_name = os.environ["BUCKET_NAME"]
bigquery_table = os.environ.get("BIGQUERY_TABLE", "wiki-page-views")
dataset = os.environ.get("DATASET", "buildflow_walkthrough")


# Nested dataclasses can be used inside of your schemas.
@dataclasses.dataclass
class HourAggregate:
    hour: datetime.datetime
    stat: int


# Define an output type for our consumer.
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


# Set up a subscriber for the source.
# The source will setup a Pub/Sub topic and subscription to listen to new files
# uploaded to the GCS bucket.
bucket = S3Bucket(bucket_name=bucket_name, aws_region="us-east-1").options(
    force_destroy=True,
)
source = S3FileChangeStream(s3_bucket=bucket)
# Set up a BigQuery table for the sink.
# If this table does not exist yet BuildFlow will create it.
dataset = BigQueryDataset(project_id=gcp_project, dataset_name=dataset)
sink = BigQueryTable(
    dataset=dataset,
    table_name=bigquery_table,
).options(destroy_protection=False, schema=AggregateWikiPageViews)


app = buildflow.Flow(
    flow_options=buildflow.FlowOptions(
        require_confirmation=False, runtime_log_level="DEBUG"
    )
)
app.manage(source, sink, dataset, bucket)


# Define our processor.
@app.consumer(source=source, sink=sink)
def process(s3_file_event: S3FileChangeEvent) -> List[AggregateWikiPageViews]:
    if s3_file_event.event_type not in S3ChangeStreamEventType.create_event_types():
        # skip non-created events
        return
    csv_string = s3_file_event.blob.decode()
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
