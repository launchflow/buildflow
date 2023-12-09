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
    max_concurrent_queries: int = 100
    enable_autoscaler: bool = True
    num_replicas: int = 1
    min_replics: int = 1
    max_replicas: int = 1000
    target_num_ongoing_requests_per_replica: int = 1
    log_level: str = "INFO"
    service_id: str = dataclasses.field(default_factory=uuid)
    endpoints: List[Endpoint] = dataclasses.field(default_factory=list, init=False)
    middleware: List = dataclasses.field(init=False, default_factory=list)
    autoscale_options: AutoscalerOptions = dataclasses.field(default=None, init=False)

    def __post_init__(self):
        self.autoscale_options = AutoscalerOptions(
            enable_autoscaler=self.enable_autoscaler,
            min_replicas=self.min_replics,
            num_replicas=self.num_replicas,
            max_replicas=self.max_replicas,
            target_num_ongoing_requests_per_replica=self.target_num_ongoing_requests_per_replica,
            max_concurrent_queries=self.max_concurrent_queries,
        )

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
