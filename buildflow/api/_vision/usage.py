# Goal: An entire backend system can be defined in a single file

import buildflow as flow
import psycopg2
import ray
import sqlalchemy
from fastapi import FastAPI


class FooSchema(flow.Schema):
    # The primary key is used to identify the object in the Storage API.
    foo_name: str
    foo_id: flow.PrimaryKey(str) = flow.uuid()


class BarSchema(flow.Schema):
    bar_name: str


# NOTE: this would be used in a separate file, most likely an ipynb notebook
@flow.processor(input_ref=flow.BigQuery(f'project_{flow.env()}.dataset.table'))
def my_notebook_processor(bar_dataset: ray.data.Dataset[BarSchema]):
    pass


# NOTE: local runs will call the python function directly, rather than sending
# to pubsub
@flow.processor(input_ref=flow.PubSub(...), output_ref=flow.BigQuery(...))
def my_async_processor(foo: FooSchema) -> BarSchema:
    # process logic goes here
    return foo


# The last arg is the sqlalchemy orm returned by the storage decorator
@flow.storage(storage_ref=flow.Postgres(...), use_orm=True)
def write_to_storage(foo_name: str,
                     session: sqlalchemy.Session = flow.GetSession()):
    foo = FooSchema(foo_name=foo_name)
    session.add(foo).commit()
    my_async_processor(foo)
    return foo.foo_id


# The last arg is the postgres client returned by the storage decorator
@flow.storage(storage_ref=flow.Postgres(...))
def get_from_storage(foo_id: int,
                     conn: psycopg2.connection = flow.GetConnection()):
    return conn.execute(f'select * from foo where foo_id={foo_id}')


# Example usage in a FastAPI app
app = FastAPI()


@app.put('/foo')
def put_foo(foo_name: str) -> FooSchema:
    return write_to_storage(foo_name)


@app.get('/foo/{foo_id}')
def get_foo(foo_id: int) -> FooSchema:
    return get_from_storage(foo_id)


# Local run is used for testing the user's logic locally.
flow.run(local=True)
