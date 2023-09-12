import inspect
from enum import Enum
from typing import Any, Callable, Dict, Iterable

import ray


class Scope(Enum):
    PROCESS = 1
    REPLICA = 2
    GLOBAL = 3

    @classmethod
    def all(cls):
        return [cls.PROCESS, cls.REPLICA, cls.GLOBAL]


class DependencyWrapper:
    def __init__(self, arg_name: str, dependency: "Dependency") -> None:
        self.arg_name = arg_name
        self.dependency = dependency


def dependency_wrappers(fn: Callable) -> Iterable[DependencyWrapper]:
    """Returns the dependencies of the processor."""
    full_arg_spec = inspect.getfullargspec(fn)
    dependencies = []
    for arg in full_arg_spec.args:
        if arg in full_arg_spec.annotations:
            if isinstance(full_arg_spec.annotations[arg], Dependency):
                dependencies.append(
                    DependencyWrapper(arg, full_arg_spec.annotations[arg])
                )
    return dependencies


class Dependency:
    _instance: Any

    def __init__(self, dependency_fn, scope: Scope):
        self.dependency_fn = dependency_fn
        self.scope = scope

    def initialize(self, scopes: Iterable[Scope] = Scope.all()):
        self._initialize_dependencies(scopes)

    def resolve(self):
        raise NotImplementedError()

    def _resolve_dependencies(self) -> Dict[str, Any]:
        input_dependencies = dependency_wrappers(self.dependency_fn)
        deps = {}
        for dep in input_dependencies:
            deps[dep.arg_name] = dep.dependency.resolve()
        return deps

    def _initialize_dependencies(self, scopes):
        input_dependencies = dependency_wrappers(self.dependency_fn)
        for dep in input_dependencies:
            if dep.dependency.scope.value < self.scope.value:
                raise ValueError(
                    f"Dependency `{dep.arg_name}` has a scope of {dep.dependency.scope}"
                    f" and cannot be used in a dependency with a scope of {self.scope}"
                )
            dep.dependency.initialize(scopes)


class ProcessScoped(Dependency):
    def __init__(self, dependency_fn):
        super().__init__(dependency_fn, Scope.PROCESS)

    def resolve(self):
        args = self._resolve_dependencies()
        return self.dependency_fn(**args)


class ReplicaScoped(Dependency):
    _instance: Any = None

    def __init__(self, dependency_fn):
        super().__init__(dependency_fn, Scope.REPLICA)

    def initialize(self, scopes: Iterable[Scope] = [Scope.REPLICA, Scope.GLOBAL]):
        super().initialize(scopes)
        self._initialize_dependencies(scopes)
        if self._instance is not None or self.scope not in scopes:
            return
        args = self._resolve_dependencies()
        self._instance = self.dependency_fn(**args)

    def resolve(self):
        if self._instance is None:
            raise ValueError("Replica scoped dependency not initialized")
        return self._instance


class GlobalScoped(Dependency):
    _object_ref: Any = None

    def __init__(self, dependency_fn):
        super().__init__(dependency_fn, Scope.GLOBAL)

    def initialize(self, scopes: Iterable[Scope] = [Scope.GLOBAL]):
        super().initialize(scopes)
        if self._object_ref is not None or self.scope not in scopes:
            return
        args = self._resolve_dependencies()
        self._object_ref = ray.put(self.dependency_fn(**args))

    def resolve(self):
        if self._object_ref is None:
            raise ValueError("Global scoped dependency not initialized")
        return ray.get(self._object_ref)


def process_scoped(fn):
    return ProcessScoped(fn)


def replica_scoped(fn):
    return ReplicaScoped(fn)


def global_scoped(fn):
    return GlobalScoped(fn)
