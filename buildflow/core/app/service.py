import dataclasses
from typing import List, Type

from buildflow.core.app.endpoint import Endpoint
from buildflow.core.options.runtime_options import AutoscalerOptions
from buildflow.core.utils import uuid
from buildflow.io.endpoint import Method, Route


@dataclasses.dataclass
class Service:
    base_route: str = "/"
    num_cpus: float = 1.0
    autoscale_options: AutoscalerOptions = AutoscalerOptions.default()
    log_level: str = "INFO"
    service_id: str = dataclasses.field(default_factory=uuid)
    endpoints: List[Endpoint] = dataclasses.field(default_factory=list, init=False)
    middleware: List = dataclasses.field(init=False, default_factory=list)

    def endpoint(self, route: Route, method: Method) -> None:
        def decorator_function(original_fn_or_class):
            endpoint = Endpoint(
                route=route,
                method=method,
                original_process_fn_or_class=original_fn_or_class,
            )
            self.endpoints.append(endpoint)
            return original_fn_or_class

        return decorator_function

    def add_endpoint(self, endpoint: Endpoint) -> None:
        self.endpoints.append(endpoint)

    def add_middleware(self, middleware_type: Type, **options) -> None:
        self.middleware.append((middleware_type, options))
