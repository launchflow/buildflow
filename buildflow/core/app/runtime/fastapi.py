import inspect
import time
from typing import Any, Callable, Dict, Type, Union

import fastapi
import ray
from fastapi.openapi.docs import (
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.openapi.utils import get_openapi
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.websockets import WebSocket

from buildflow.core.app.runtime._runtime import RunID
from buildflow.core.app.runtime.metrics.common import (
    num_events_processed,
    process_time_counter,
)
from buildflow.core.processor.patterns.collector import CollectorGroup
from buildflow.core.processor.patterns.endpoint import EndpointGroup
from buildflow.core.processor.utils import process_types
from buildflow.dependencies.base import (
    Scope,
    initialize_dependencies,
    resolve_dependencies,
)
from buildflow.dependencies.headers import security_dependencies
from buildflow.io.endpoint import Method


def create_app(
    processor_group: Union[EndpointGroup, CollectorGroup],
    flow_dependencies: Dict[Type, Any],
    run_id: RunID,
    process_fn: Callable,
    include_output_type: bool = True,
):
    app = fastapi.FastAPI(
        title=processor_group.group_id,
        version="0.0.1",
        docs_url=None,
        redoc_url=None,
    )
    for middleware in processor_group.middleware:
        app.add_middleware(middleware[0], **middleware[1])
    app.mount(
        "/static",
        StaticFiles(packages=[("buildflow", "static")]),
        name="static",
    )

    @app.get("/docs", include_in_schema=False)
    async def custom_swagger_ui_html(req: Request):
        root_path = req.scope.get("root_path", "").rstrip("/")
        openapi_url = root_path + app.openapi_url
        return get_swagger_ui_html(
            openapi_url=openapi_url,
            title=app.title,
            oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
            swagger_js_url="/static/swagger-ui-bundle.js",
            swagger_css_url="/static/swagger-ui.css",
        )

    @app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
    async def swagger_ui_redirect():
        return get_swagger_ui_oauth2_redirect_html()

    security_schemes = {}

    @app.on_event("startup")
    async def setup_processor_group():
        for processor in processor_group.processors:
            if hasattr(app.state, "processor_map"):
                app.state.processor_map[processor.processor_id] = processor
            else:
                app.state.processor_map = {processor.processor_id: processor}
            processor.setup()
            await initialize_dependencies(
                processor.dependencies(), flow_dependencies, [Scope.REPLICA]
            )

    for processor in processor_group.processors:
        input_types, output_type = process_types(processor)
        if not include_output_type:
            output_type = None

        class EndpointFastAPIWrapper:
            def __init__(self, processor_id, run_id, flow_dependencies):
                self.job_id = ray.get_runtime_context().get_job_id()
                self.run_id = run_id
                self.num_events_processed_counter = num_events_processed(
                    processor_id=processor_id,
                    job_id=self.job_id,
                    run_id=run_id,
                    status_code="200",
                )
                self.process_time_counter = process_time_counter(
                    processor_id=processor_id,
                    job_id=self.job_id,
                    run_id=run_id,
                    status_code="200",
                )
                self.processor_id = processor_id
                self.flow_dependencies = flow_dependencies
                self.request_arg = None
                self.websocket_arg = None
                argspec = inspect.getfullargspec(processor.process)
                for arg in argspec.args:
                    if arg in argspec.annotations and (
                        argspec.annotations[arg] == fastapi.Request
                    ):
                        self.request_arg = arg
                    if arg in argspec.annotations and (
                        argspec.annotations[arg] == fastapi.WebSocket
                    ):
                        self.websocket_arg = arg

            # NOTE: we have to import this seperately because it gets run
            # inside of the ray actor
            from buildflow.core.processor.utils import add_input_types

            async def process(
                self,
                internal_buildflow_request: Union[Request, WebSocket],
                *args,
                **kwargs,
            ):
                processor = app.state.processor_map[self.processor_id]
                start_time = time.monotonic()

                status_code = 200
                try:
                    dependency_args = await resolve_dependencies(
                        processor.dependencies(),
                        self.flow_dependencies,
                        internal_buildflow_request,
                    )
                    if self.request_arg is not None and self.request_arg not in kwargs:
                        kwargs[self.request_arg] = internal_buildflow_request
                    if (
                        self.websocket_arg is not None
                        and self.websocket_arg not in kwargs
                    ):
                        kwargs[self.websocket_arg] = internal_buildflow_request
                    output = await process_fn(
                        processor, *args, **kwargs, **dependency_args
                    )
                except Exception as e:
                    if isinstance(e, HTTPException):
                        status_code = e.status_code
                    else:
                        status_code = 500
                    raise e
                finally:
                    status_code = str(status_code)
                    self.num_events_processed_counter.inc(
                        tags={
                            "processor_id": processor.processor_id,
                            "JobId": self.job_id,
                            "RunId": self.run_id,
                            "StatusCode": status_code,
                        }
                    )
                    self.process_time_counter.inc(
                        (time.monotonic() - start_time) * 1000,
                        tags={
                            "processor_id": processor.processor_id,
                            "JobId": self.job_id,
                            "RunId": self.run_id,
                            "StatusCode": status_code,
                        },
                    )
                return output

            # TODO: there is a small issue here where this will fail if the user
            # includes an argument in their process with the name:
            #   buildflow_internal_websocket or buildflow_internal_request
            # we will fail.

            @add_input_types(input_types, output_type)
            async def handle_websocket(
                self, buildflow_internal_websocket: WebSocket = None, *args, **kwargs
            ):
                return await self.process(buildflow_internal_websocket, *args, **kwargs)

            @add_input_types(input_types, output_type)
            async def handle_request(
                self, buildflow_internal_request: Request = None, *args, **kwargs
            ) -> output_type:
                return await self.process(buildflow_internal_request, *args, **kwargs)

        endpoint_wrapper = EndpointFastAPIWrapper(
            processor.processor_id, run_id, flow_dependencies
        )
        security_deps = security_dependencies(processor.dependencies())
        security_openapi_extras = {}
        for security_dep in security_deps:
            security_openapi_extras.update(security_dep.endpoint_auth_info)
            security_schemes.update(security_dep.service_auth_info)
        openapi_extras = None
        if security_openapi_extras:
            openapi_extras = {"security": [security_openapi_extras]}
        if processor.route_info().method == Method.WEBSOCKET:
            app.add_api_websocket_route(
                path=processor.route_info().route,
                endpoint=endpoint_wrapper.handle_websocket,
            )
        else:
            app.add_api_route(
                processor.route_info().route,
                endpoint_wrapper.handle_request,
                methods=[processor.route_info().method.name],
                summary=processor.processor_id,
                openapi_extra=openapi_extras,
            )

    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema
        openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            routes=app.routes,
        )
        if "components" not in openapi_schema:
            openapi_schema["components"] = {}
        openapi_schema["components"]["securitySchemes"] = security_schemes
        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi

    return app
