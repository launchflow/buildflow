from buildflow.api_v2.node import NodeAPI


DeploymentID = str


class DeploymentAPI:
    deployment_id: DeploymentID
    node: NodeAPI
