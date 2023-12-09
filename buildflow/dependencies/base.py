import asyncio
import inspect
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import ray
from starlette.requests import Request
from starlette.websockets import WebSocket

from buildflow.core.utils import uuid
from buildflow.exceptions.exceptions import InvalidDependencyHierarchyOrder


class Scope(Enum):
    PROCESS = 1
    REPLICA = 2
    GLOBAL = 3
    # No scope can be used anywhere but will not be cached.
    NO_SCOPE = 4

    @classmethod
    def all(cls):
        return [cls.PROCESS, cls.REPLICA, cls.GLOBAL]


class DependencyWrapper:
    def __init__(self, arg_name: str, dependency: "Dependency") -> None:
        self.arg_name = arg_name
        self.dependency = dependency


async def _create_dependency(
    dependency_fn: Callable,
    *args,
    **kwargs,
) -> Any:
    to_ret = dependency_fn(*args, **kwargs)
    if inspect.iscoroutine(to_ret):
        return await to_ret
    return to_ret


def dependency_wrappers(
    fn: Callable,
) -> Tuple[Iterable[DependencyWrapper], Optional[str]]:
    """Returns the dependencies of the processor."""
    full_arg_spec = inspect.getfullargspec(fn)
    dependencies = []
    for arg in full_arg_spec.args:
        if arg in full_arg_spec.annotations:
            if isinstance(full_arg_spec.annotations[arg], Dependency):
                dependencies.append(
                    DependencyWrapper(arg, full_arg_spec.annotations[arg])
                )
    request_arg = None
    for key, annotation in full_arg_spec.annotations.items():
        if annotation == Request or annotation == WebSocket:
            request_arg = key
    return dependencies, request_arg


async def resolve_dependencies(
    dependencies: List[DependencyWrapper],
    flow_dependencies: Dict[Type, Any],
    request: Optional[Union[Request, WebSocket]] = None,
) -> Dict[str, Any]:
    visited_dependencies: Dict[Callable, Any] = {}
    coros = {}
    for wrapper in dependencies:
        coros[wrapper.arg_name] = wrapper.dependency.resolve(
            flow_dependencies,
            visited_dependencies,
            request,
        )
    dependency_args = {}
    results = await asyncio.gather(*coros.values())
    for arg_name, result in zip(coros.keys(), results):
        dependency_args[arg_name] = result
    return dependency_args


async def initialize_dependencies(
    dependencies: List[DependencyWrapper],
    flow_dependencies: Dict[Type, Any],
    scopes: Iterable[Scope] = Scope.all(),
):
    visited_dependencies: Dict[Callable, Any] = {}
    dependency_coros = []
    for wrapper in dependencies:
        dependency_coros.append(
            wrapper.dependency.initialize(
                flow_dependencies, visited_dependencies, scopes
            )
        )
    await asyncio.gather(*dependency_coros)


class Dependency:
    _instance: Any

    def __init__(self, dependency_fn, scope: Scope):
        self.dependency_fn = dependency_fn
        self.scope = scope
        for attr in dependency_fn.__dict__:
            if callable(getattr(dependency_fn, attr)):
                setattr(self, attr, getattr(dependency_fn, attr))
        self.sub_dependencies: List[DependencyWrapper] = []
        full_arg_spec = inspect.getfullargspec(dependency_fn.__init__)
        for arg in full_arg_spec.args:
            if arg in full_arg_spec.annotations:
                if isinstance(full_arg_spec.annotations[arg], Dependency):
                    self.sub_dependencies.append(
                        DependencyWrapper(arg, full_arg_spec.annotations[arg])
                    )
        self.request_arg = None
        for key, annotation in full_arg_spec.annotations.items():
            if annotation == Request or annotation == WebSocket:
                self.request_arg = key

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self.dependency_fn(*args, **kwds)

    async def initialize(
        self,
        flow_dependencies: Dict[Type, Any],
        visited_dependencies: Dict[Callable, Any],
        scopes: Iterable[Scope] = Scope.all(),
    ):
        await self._initialize_dependencies(
            flow_dependencies, visited_dependencies, scopes
        )

    async def resolve(
        self,
        flow_dependencies: Dict[Type, Any],
        visited_dependencies: Dict[Callable, Any],
        request: Optional[Optional[Union[Request, WebSocket]]] = None,
    ):
        raise NotImplementedError()

    async def _resolve_dependencies(
        self,
        flow_dependencies: Dict[Type, Any],
        visited_dependencies: Dict[Callable, Any],
        request: Optional[Union[Request, WebSocket]] = None,
    ) -> Dict[str, Any]:
        deps = {}
        for dep in self.sub_dependencies:
            deps[dep.arg_name] = await dep.dependency.resolve(
                flow_dependencies, visited_dependencies, request
            )
        if self.request_arg is not None:
            if request is None:
                raise ValueError(
                    f"Unable to provide Request / WebSocket to dependency `{self.request_arg}`"  # noqa
                )
            deps[self.request_arg] = request
        full_arg_spec = inspect.getfullargspec(self.dependency_fn.__init__)
        for arg in full_arg_spec.args:
            if arg in full_arg_spec.annotations:
                if full_arg_spec.annotations[arg] in flow_dependencies:
                    deps[arg] = flow_dependencies[full_arg_spec.annotations[arg]]
        return deps

    async def _initialize_dependencies(
        self,
        flow_dependencies: Dict[Type, Any],
        visited_dependencies: Dict[Callable, Any],
        scopes: List[Scope],
    ):
        for dep in self.sub_dependencies:
            if dep.dependency.scope.value < self.scope.value:
                raise InvalidDependencyHierarchyOrder(
                    dep.arg_name, dep.dependency.scope.name, self.scope.name
                )
            await dep.dependency.initialize(
                flow_dependencies, visited_dependencies, scopes
            )


class ProcessScoped(Dependency):
    def __init__(self, dependency_fn):
        super().__init__(dependency_fn, Scope.PROCESS)

    async def resolve(
        self,
        flow_dependencies: Dict[Type, Any],
        visited_dependencies: Dict[Callable, Any],
        request: Optional[Union[Request, WebSocket]] = None,
    ):
        args = await self._resolve_dependencies(
            flow_dependencies, visited_dependencies, request
        )
        if self.dependency_fn in visited_dependencies:
            return visited_dependencies[self.dependency_fn]
        else:
            to_return = await _create_dependency(self.dependency_fn, **args)
            visited_dependencies[self.dependency_fn] = to_return
            return to_return


class ReplicaScoped(Dependency):
    _instance: Any = None

    def __init__(self, dependency_fn):
        super().__init__(dependency_fn, Scope.REPLICA)

    async def initialize(
        self,
        flow_dependencies: List[Any],
        visited_dependencies: Dict[Callable, Any],
        scopes: Iterable[Scope] = [Scope.REPLICA, Scope.GLOBAL],
    ):
        await super().initialize(flow_dependencies, visited_dependencies, scopes)
        if self._instance is not None or self.scope not in scopes:
            return
        args = await self._resolve_dependencies(flow_dependencies, visited_dependencies)
        if self.dependency_fn in visited_dependencies:
            self._instance = visited_dependencies[self.dependency_fn]
        else:
            self._instance = await _create_dependency(self.dependency_fn, **args)
            visited_dependencies[self.dependency_fn] = self._instance

    async def resolve(
        self,
        flow_dependencies: Dict[Type, Any],
        visited_dependencies: Dict[Callable, Any],
        request: Optional[Union[Request, WebSocket]] = None,
    ):
        if self._instance is None:
            raise ValueError("Replica scoped dependency not initialized")
        return self._instance


class GlobalScoped(Dependency):
    def __init__(self, dependency_fn):
        super().__init__(dependency_fn, Scope.GLOBAL)
        self._object_ref = None
        self.global_scoped_id = uuid()

    async def initialize(
        self,
        flow_dependencies: List[Any],
        visited_dependencies: Dict[Callable, Any],
        scopes: Iterable[Scope] = [Scope.GLOBAL],
    ):
        await super().initialize(flow_dependencies, visited_dependencies, scopes)
        if self._object_ref is not None or self.scope not in scopes:
            return
        args = await self._resolve_dependencies(flow_dependencies, visited_dependencies)
        if self.dependency_fn in visited_dependencies:
            self._instance = visited_dependencies[self.dependency_fn]
        else:
            self._instance = await _create_dependency(self.dependency_fn, **args)
            visited_dependencies[self.dependency_fn] = self._instance
        self._object_ref = ray.put(self._instance)

    async def resolve(
        self,
        flow_dependencies: Dict[Type, Any],
        visited_dependencies: Dict[Callable, Any],
        request: Optional[Union[Request, WebSocket]] = None,
    ):
        if self._instance is not None:
            return self._instance
        if self._object_ref is None:
            raise ValueError("Global scoped dependency not initialized")
        return ray.get(self._object_ref)


class NoScoped(Dependency):
    def __init__(self, dependency_fn):
        super().__init__(dependency_fn, Scope.NO_SCOPE)

    async def resolve(
        self,
        flow_dependencies: Dict[Type, Any],
        visited_dependencies: Dict[Callable, Any],
        request: Optional[Union[Request, WebSocket]] = None,
    ):
        args = await self._resolve_dependencies(
            flow_dependencies, visited_dependencies, request
        )
        return await _create_dependency(self.dependency_fn, **args)


T = TypeVar("T")
R = TypeVar("R")


def dependency(scope: Union[Scope, str]) -> Callable[[T], T]:
    if isinstance(scope, str):
        try:
            scope = Scope[scope.upper()]
        except KeyError:
            raise ValueError(f"Invalid scope `{scope}`. Valid scopes are {Scope.all()}")

    def decorator(fn: Callable[[T], T]) -> T:
        if inspect.iscoroutinefunction(fn.__init__):

            async def __new__(cls, *args, **kwargs):
                instance = super(cls, cls).__new__(cls)
                await instance.__init__(*args, **kwargs)
                return instance

            fn.__new__ = __new__

        if scope == Scope.PROCESS:
            return ProcessScoped(fn)
        elif scope == Scope.REPLICA:
            return ReplicaScoped(fn)
        elif scope == Scope.GLOBAL:
            return GlobalScoped(fn)
        elif scope == Scope.NO_SCOPE:
            return NoScoped(fn)
        else:
            raise ValueError(f"Invalid scope `{scope}`. Valid scopes are {Scope.all()}")

    return decorator
