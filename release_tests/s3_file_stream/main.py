import csv
import dataclasses
import datetime
import io
import os
from typing import List

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

import buildflow
from buildflow.io.aws import S3Bucket, S3FileChangeStream
from buildflow.io.snowflake import SnowflakeTable
from buildflow.types.aws import S3ChangeStreamEventType, S3FileChangeEvent
from buildflow.types.portable import PortableFileChangeEventType

app = buildflow.Flow(flow_options=buildflow.FlowOptions(require_confirmation=False))


# TODO: remove this!
with open("/home/caleb/code/backend/snowflake_keys/rsa_key.p8", "rb") as pem_in:
    p_key = serialization.load_pem_private_key(
        pem_in.read(),
        password=None,
        backend=default_backend(),
    )


pk_text = p_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
).decode("utf-8")

bucket_name = os.getenv("BUCKET_NAME", "caleb-test-s3-input-bucket")
snowflake_bucket = os.getenv("SNOWFLAKE_BUCKET", "caleb-s3-snowflake-bucket")

source = S3FileChangeStream(
    s3_bucket=S3Bucket(bucket_name=bucket_name, aws_region="us-east-1").options(
        force_destroy=True,
    ),
    event_types=[
        S3ChangeStreamEventType.OBJECT_CREATED_ALL,
        S3ChangeStreamEventType.OBJECT_REMOVED_ALL,
    ],
)
sink = SnowflakeTable(
    table="snowflake-table",
    database="snowflake-database",
    schema="snowflake-schema",
    bucket=S3Bucket(bucket_name=snowflake_bucket, aws_region="us-east-1").options(
        force_destroy=True
    ),
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    private_key=pk_text,
).options(database_managed=True, schema_managed=True)


app.manage(source, sink)


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


app = buildflow.Flow(
    flow_options=buildflow.FlowOptions(
        require_confirmation=False,
        runtime_log_level="DEBUG",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
)


# Define our processor.
@app.consumer(source=source, sink=sink)
def process(s3_file_event: S3FileChangeEvent) -> List[AggregateWikiPageViews]:
    if s3_file_event.event_type != PortableFileChangeEventType.CREATED:
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
