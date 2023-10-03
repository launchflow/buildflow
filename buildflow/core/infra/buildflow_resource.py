import pulumi

from buildflow.core.credentials import CredentialType
from buildflow.io.primitive import Primitive


class BuildFlowResource(pulumi.ComponentResource):
    def __init__(
        self,
        primitive: Primitive,
        credentials: CredentialType,
        opts: pulumi.ResourceOptions,
    ):
        super().__init__(
            f"buildflow:{type(primitive).__name__}",
            primitive.primitive_id(),
            None,
            opts,
        )

        opts = pulumi.ResourceOptions.merge(opts, pulumi.ResourceOptions(parent=self))

        primitive.pulumi_resources(credentials, opts=opts)

        self.register_outputs(
            {
                "primitive_id": primitive.primitive_id(),
            }
        )
