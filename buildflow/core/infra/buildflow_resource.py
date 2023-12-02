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

        self.primitive = primitive
        self.child_resources = primitive.pulumi_resources(credentials, opts=opts)

        outputs = {
            "primitive_id": self.primitive.primitive_id(),
            "primitive_type": type(self.primitive).__name__,
        }
        cloud_console_url = self.primitive.cloud_console_url()
        if cloud_console_url is not None:
            outputs["cloud_console_url"] = cloud_console_url
        self.register_outputs(outputs)
