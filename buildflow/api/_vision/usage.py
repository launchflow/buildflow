# Goal: An entire backend system can be defined in a single file

import dataclasses

import buildflow as flow
import ray
import sqlalchemy
from fastapi import FastAPI


@dataclasses.dataclass
class FooSchema:
    foo_name: str
    # The ID type is an autoincrementing primary key used to identify the
    # object in the Storage API.
    foo_id: flow.ID()


@dataclasses.dataclass
class BarSchema:
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


# The last arg is the sqlalchemy orm injected by the storage decorator
@flow.storage(provider=flow.Postgres(...), schema=FooSchema)
def write_to_storage(foo_name: str, session: sqlalchemy.Session) -> int:
    foo = FooSchema(foo_name=foo_name)
    session.add(foo)
    session.commit()
    my_async_processor(foo)
    return foo.foo_id


# The last arg is the sqlalchemy orm injected by the storage decorator
@flow.storage(provider=flow.Postgres(...))
def get_from_storage(foo_id: int, session: sqlalchemy.Session) -> FooSchema:
    return session.get(FooSchema, foo_id)


# Example usage in a FastAPI app
app = FastAPI()


@app.put('/foo')
def put_foo(foo_name: str) -> int:
    return write_to_storage(foo_name)


@app.get('/foo/{foo_id}')
def get_foo(foo_id: int) -> FooSchema:
    return get_from_storage(foo_id)


# Local run is used for testing the user's logic locally.
flow.run(local=True)
