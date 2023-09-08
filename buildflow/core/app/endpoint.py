import dataclasses
from buildflow.io.endpoint import Method, Route

from typing import Callable, Any


@dataclasses.dataclass
class Endpoint:
    route: Route
    method: Method
    original_process_fn_or_class: Callable

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
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
