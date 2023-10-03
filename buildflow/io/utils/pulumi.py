import pulumi

from buildflow.core.credentials import CredentialType
from buildflow.io.primitive import Primitive


class PrimitiveComponentResource(pulumi.ComponentResource):
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

        outputs = primitive.pulumi_resources(credentials)

        self.register_outputs(outputs)
