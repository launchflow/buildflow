from buildflow.core.options.infra_options import InfraOptions


class Infra:
    infra_options: InfraOptions

    async def refresh(self):
        """Refreshes the infrastructure state."""
        raise NotImplementedError("refresh not implemented")

    async def preview(self):
        """Returns a preview for the infrastructure."""
        raise NotImplementedError("preview not implemented")

    async def apply(self):
        """Applies the plan to the infrastructure."""
        raise NotImplementedError("apply not implemented")

    async def destroy(self):
        """Destroys the infrastructure."""
        raise NotImplementedError("destroy not implemented")
