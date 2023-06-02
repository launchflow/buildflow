class InfraAPI:
    async def plan(self):
        """Returns a plan for the infrastructure."""
        raise NotImplementedError("plan not implemented")

    async def apply(self):
        """Applies the plan to the infrastructure."""
        raise NotImplementedError("apply not implemented")

    async def destroy(self):
        """Destroys the infrastructure."""
        raise NotImplementedError("destroy not implemented")
