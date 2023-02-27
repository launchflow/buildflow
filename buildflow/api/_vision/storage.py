import buildflow as flow
from buildflow.api.resources import IO


# This is the base class for all Storage APIs. 1 class per table.
class StorageAPI:

    # This static method defines the storage provider reference (i.e. Postgres)
    @staticmethod
    def _provider() -> IO:
        raise NotImplementedError('_input not implemented')

    # This static method defines the schema for the table
    @staticmethod
    def _schema() -> flow.Schema:
        raise NotImplementedError('_schema not implemented')

    # Insert the object. The keys are contained in the object.
    def insert(self, _object: flow.Schema):
        raise NotImplementedError('insert not implemented')

    # Read the object(s) for the given keys
    def read(self, **keys):
        raise NotImplementedError('read not implemented')

    # Delete the object(s) for the given keys
    def delete(self, **keys):
        raise NotImplementedError('read not implemented')
