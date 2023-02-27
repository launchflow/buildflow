# Goal: An entire backend system can be defined in a single file

import buildflow as flow
from fastapi import FastAPI


class FooSchema(flow.Schema):
    # The primary key is used to identify the object in the Storage API.
    foo_id: flow.PrimaryKey(int)
    foo_name: str


@flow.processor(input_ref=flow.PubSub(...), output_ref=flow.BigQuery(...))
def my_async_processor(foo: FooSchema):
    # process logic goes here
    return foo


# The Storage API operates at the Table level. 1 class per table.
class MyStorage(flow.Storage):

    @staticmethod
    def _storage_provider():
        return flow.Postgres(schema=FooSchema)

    # Class methods can be used to add logic before / after interacting with
    # the Storage provider.
    def put_foo(self, foo_name: str) -> FooSchema:
        # The foo object is created and validated.
        foo = FooSchema(foo_id=12345, foo_name=foo_name)
        # the foo object is stored in Postgres.
        self.insert(foo)
        # the foo object is sent to PubSub for async processing
        my_async_processor(foo)
        return foo


# Example usage in a FastAPI app
app = FastAPI()


@app.put('/foo')
def put_foo(foo_name: str, db=MyStorage()) -> FooSchema:
    return db.put_foo(foo_name)


@app.get('/foo/{foo_id}')
def get_foo(foo_id: int, db=MyStorage()) -> FooSchema:
    return db.read(foo_id=foo_id)
