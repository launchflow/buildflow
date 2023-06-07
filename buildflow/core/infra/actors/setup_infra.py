from typing import Iterable

import ray

from buildflow.api import InfraAPI
from buildflow.core.infra.config import InfraConfig
from buildflow.core.processor.base import Processor
from buildflow.io.providers import SetupProvider
import logging


# TODO: Explore the idea of an infra actor that implements the RuntimeAPI so we
# can run a bunch of infra tasks in parallel and reuse the Runtime methods for
# managing the lifecycle of the tasks.
# TODO: Explore the idea of having AdHocInfrastructure that tears down the
# infrastructure when it's done
# TODO: Add support for 'bring your own terraform'
# TODO: Add a config with constructors like config.DEBUG() (see runtime)
@ray.remote
class SetupInfraActor(InfraAPI):
    def __init__(self, config: InfraConfig) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(config.log_level)

        # configuration
        self.config = config
        # initial runtime state

    async def plan(self, *, processors: Iterable[Processor]):
        raise NotImplementedError("Infrastructure.plan() is not implemented")

    async def apply(self, *, processors: Iterable[Processor]):
        for processor in processors:
            source_provider = processor.source().provider()
            if isinstance(source_provider, SetupProvider):
                try:
                    await source_provider.setup()
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to setup source provider: {source_provider}"
                    ) from e
            # TODO: Need to support .sinks() also (runtime also needs this)
            # This is assuming we use sinks() for containing any Depends()
            sink_provider = processor.sink().provider()
            if isinstance(sink_provider, SetupProvider):
                try:
                    await sink_provider.setup()
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to setup sink provider: {sink_provider}"
                    ) from e

    async def destroy(self, *, processors: Iterable[Processor]):
        raise NotImplementedError("Infrastructure.destroy() is not implemented")
