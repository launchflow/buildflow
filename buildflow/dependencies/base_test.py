import unittest
from unittest import mock

from starlette.requests import Request

from buildflow.dependencies import base
from buildflow.exceptions.exceptions import InvalidDependencyHierarchyOrder


@base.dependency(scope=base.Scope.NO_SCOPE)
class NoScope:
    async def __init__(self):
        self.value = 0


@base.dependency(scope=base.Scope.GLOBAL)
class GlobalDep:
    async def __init__(self, no_scope: NoScope):
        self.value = 1
        self.no_scope = no_scope


@base.dependency(scope=base.Scope.REPLICA)
class ReplicaDep:
    def __init__(self, global_dep: GlobalDep):
        self.value = 2
        self.global_dep = global_dep


@base.dependency(scope=base.Scope.PROCESS)
class ProcessDep:
    def __init__(self, replica_dep: ReplicaDep, no_scope: NoScope):
        self.value = 3
        self.replia_dep = replica_dep
        self.no_scope = no_scope


@mock.patch("ray.put")
@mock.patch("ray.get")
class BaseDependenciesTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.NoScopeDep = NoScope
        self.GlobalDep = GlobalDep
        self.ReplicaDep = ReplicaDep
        self.ProcessDep = ProcessDep

    def setup_ray_mocks(self, mock_get: mock.MagicMock, mock_put: mock.MagicMock):
        put_value = None  # noqa: F841

        def set_put_value(val):
            global put_value
            put_value = val
            return True

        def get_put_value(ref):
            global put_value
            return put_value

        mock_put.side_effect = set_put_value
        mock_get.side_effect = get_put_value

    async def test_resolve_dependencies(
        self, mock_get: mock.MagicMock, mock_put: mock.MagicMock
    ):
        self.setup_ray_mocks(mock_get, mock_put)

        await self.GlobalDep.initialize({}, {}, scopes=[base.Scope.GLOBAL])
        global_dep = await self.GlobalDep.resolve({}, {})
        self.assertEqual(global_dep.value, 1)
        self.assertEqual(global_dep.no_scope.value, 0)

        await self.ReplicaDep.initialize({}, {}, scopes=[base.Scope.REPLICA])
        replica_dep = await self.ReplicaDep.resolve({}, {})
        self.assertEqual(replica_dep.value, 2)
        self.assertEqual(id(global_dep), id(replica_dep.global_dep))

        await self.ProcessDep.initialize({}, {}, scopes=[base.Scope.PROCESS])
        process_dep = await self.ProcessDep.resolve({}, {})
        self.assertEqual(process_dep.value, 3)
        self.assertEqual(id(replica_dep), id(process_dep.replia_dep))
        self.assertEqual(process_dep.no_scope.value, 0)
        # We expect these not to be equal because each the no scope dependency
        # will create a new instance for all of it's uses.
        self.assertNotEqual(id(process_dep.no_scope), id(global_dep.no_scope))

    async def test_invalid_dep_order(
        self, mock_get: mock.MagicMock, mock_put: mock.MagicMock
    ):
        self.setup_ray_mocks(mock_get, mock_put)

        @base.dependency(scope=base.Scope.PROCESS)
        class ProcessDep2:
            def __init__(self) -> None:
                pass

        @base.dependency(scope=base.Scope.REPLICA)
        class ReplicaDep2:
            def __init__(self, invalid_process_dep: ProcessDep2) -> None:
                pass

        with self.assertRaises(InvalidDependencyHierarchyOrder):
            await ReplicaDep2.initialize({}, {}, scopes=[base.Scope.REPLICA])

    async def test_inject_request(
        self, mock_get: mock.MagicMock, mock_put: mock.MagicMock
    ):
        self.setup_ray_mocks(mock_get, mock_put)

        @base.dependency(scope=base.Scope.PROCESS)
        class ProcessDep3:
            def __init__(self, request: Request) -> None:
                self.request = request

        # This is technically a type hint hack, but we really just want
        # to ensure the request is injected in the dependency we expect.
        request = "request"
        await ProcessDep3.initialize({}, {}, scopes=[base.Scope.PROCESS])
        process_dep = await ProcessDep3.resolve({}, {}, request=request)
        self.assertEqual(id(process_dep.request), id(request))

    async def test_inject_flow_dependencies(
        self, mock_get: mock.MagicMock, mock_put: mock.MagicMock
    ):
        self.setup_ray_mocks(mock_get, mock_put)

        class FlowDep:
            def __init__(self) -> None:
                pass

        flow_dep = FlowDep()

        @base.dependency(scope=base.Scope.PROCESS)
        class ProcessDep3:
            async def __init__(self, request: Request, f: FlowDep) -> None:
                self.request = request
                self.f = f

        # This is technically a type hint hack, but we really just want
        # to ensure the request is injected in the dependency we expect.
        request = "request"
        await ProcessDep3.initialize({}, {}, scopes=[base.Scope.PROCESS])
        process_dep = await ProcessDep3.resolve(
            {FlowDep: flow_dep}, {}, request=request
        )
        self.assertEqual(id(process_dep.request), id(request))
        self.assertEqual(id(process_dep.f), id(flow_dep))

    async def test_initialize_with_cache(
        self, mock_get: mock.MagicMock, mock_put: mock.MagicMock
    ):
        @base.dependency(scope=base.Scope.PROCESS)
        class ProcessDep2:
            class_val = 0

            def __init__(self):
                self.class_val += 1

        resolved = await base.resolve_dependencies(
            dependencies=[
                base.DependencyWrapper("a", ProcessDep2),
                base.DependencyWrapper("b", ProcessDep2),
            ],
            flow_dependencies={},
        )

        self.assertEqual(resolved["a"], resolved["b"])
        self.assertEqual(resolved["a"].class_val, 1)


if __name__ == "__main__":
    unittest.main()
