# flake8: noqa
import argparse
import csv
import dataclasses
import datetime
import io
import sys
from typing import List

import buildflow
from buildflow import Flow

# Parser to allow run time configuration of arguments
parser = argparse.ArgumentParser()
parser.add_argument('--gcp_project', type=str, required=True)
parser.add_argument('--bucket_name', type=str, required=True)
parser.add_argument('--table_name', type=str, default='csv_bigquery')
args, _ = parser.parse_known_args(sys.argv)

# Set up a subscriber for the source.
# The source will setup a Pub/Sub topic and subscription to listen to new files
# uploaded to the GCS bucket.
source = buildflow.GCSFileNotifications(project_id=args.gcp_project,
                                        bucket_name=args.bucket_name)
# Set up a BigQuery table for the sink.
# If this table does not exist yet BuildFlow will create it.
sink = buildflow.BigQuerySink(
    table_id=f'{args.gcp_project}.buildflow_walkthrough.{args.table_name}')


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


flow = Flow()


# Define our processor.
@flow.processor(source=source, sink=sink)
def process(
        gcs_file_event: buildflow.GCSFileEvent
) -> List[AggregateWikiPageViews]:
    csv_string = gcs_file_event.blob.decode()
    csv_reader = csv.DictReader(io.StringIO(csv_string))
    aggregate_stats = {}
    for row in csv_reader:
        timestamp = datetime.datetime.strptime(row['datehour'],
                                               '%Y-%m-%d %H:%M:%S.%f %Z')
        wiki = row['wiki']
        title = row['title']
        views = row['views']

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


# Run your flow.
flow.run().output()