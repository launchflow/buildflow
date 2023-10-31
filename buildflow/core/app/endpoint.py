import dataclasses
from typing import Any, Callable

from buildflow.io.endpoint import Method, Route


@dataclasses.dataclass
class Endpoint:
    route: Route
    method: Method
    original_process_fn_or_class: Callable

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if isinstance(self.method, str):
            self.method = Method(self.method.upper())
        return self.original_process_fn_or_class(*args, **kwargs)


def endpoint(
    route: Route,
    method: Method,
):
    def decorator_function(original_fn_or_class):
        return Endpoint(
            route=route,
            method=method,
            original_process_fn_or_class=original_fn_or_class,
        )

    return decorator_function
