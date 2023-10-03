from schemas import InputRequest, OuptutResponse

from buildflow import Flow
from buildflow.dependencies import Scope, dependency
from buildflow.io.gcp import BigQueryDataset, BigQueryTable


@dependency(scope=Scope.NO_SCOPE)
class NoScope:
    def __init__(self):
        self.val = 1


@dependency(scope=Scope.GLOBAL)
class GlobalScope:
    def __init__(self, no: NoScope):
        self.val = 2
        self.no = no


@dependency(scope=Scope.REPLICA)
class ReplicaScope:
    def __init__(self, global_: GlobalScope):
        self.val = 3
        self.global_ = global_


@dependency(scope=Scope.PROCESS)
class ProcessScope:
    def __init__(self, replica: ReplicaScope):
        self.val = 4
        self.replica = replica


dataset = BigQueryDataset(
    project_id="caleb-launchflow-sandbox", dataset_name="collector_testing2"
)
sink1 = BigQueryTable(dataset=dataset, table_name="my_table").options(
    schema=OuptutResponse
)
sink2 = BigQueryTable(dataset=dataset, table_name="diff_table").options(
    schema=OuptutResponse
)


app = Flow()
app.manage(dataset, sink1, sink2)


@app.collector(route="/", method="POST", sink=sink1)
def process(
    input: InputRequest,
    no: NoScope,
    global_: GlobalScope,
    replica: ReplicaScope,
    process: ProcessScope,
) -> OuptutResponse:
    if id(process.replica) != id(replica):
        raise Exception("Replica scope not the same")
    if id(replica.global_) != id(global_):
        raise Exception("Global scope not the same")
    if id(global_.no) == id(no):
        raise Exception("No scope was the same")
    return [OuptutResponse(val=input.val + 1), OuptutResponse(val=input.val + 1)]


@app.collector(route="/diff", method="POST", sink=sink2)
def diff_process2(input: InputRequest) -> OuptutResponse:
    return OuptutResponse(val=input.val + 1)
