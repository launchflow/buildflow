# Goal: An entire backend system can be defined in a single file

import buildflow as flow
from fastapi import FastAPI


class FooSchema(flow.Schema):
    # The primary key is used to identify the object in the Storage API.
    foo_name: str
    foo_id: flow.PrimaryKey(int) = flow.uuid()


@flow.processor(input_ref=flow.PubSub(...), output_ref=flow.BigQuery(...))
def my_async_processor(foo: FooSchema):
    # process logic goes here
    return foo


# The last arg is the sqlalchemy orm return by the storage decorator
@flow.storage(storage_ref=flow.Postgres(...))
def write_to_storage(foo_name: str, session: sqlalchemy.Session = None):
    foo = FooSchema(foo_name=foo_name)
    session.add(foo).commit()
    my_async_processor(foo)
    return foo.foo_id


# The last arg is the sqlalchemy orm return by the storage decorator
@flow.storage(storage_ref=flow.Postgres(...))
def get_from_storage(foo_id: int, session: sqlalchemy.Session = None):
    return session.read(foo_id)


# Example usage in a FastAPI app
app = FastAPI()


@app.put('/foo')
def put_foo(foo_name: str) -> FooSchema:
    return write_to_storage(foo_name)


@app.get('/foo/{foo_id}')
def get_foo(foo_id: int) -> FooSchema:
    return get_from_storage(foo_id)


# Local mode will just run the async functions directly, rather than sending to pubsub
flow.run(local=True)