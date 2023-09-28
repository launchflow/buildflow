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
from starlette.requests import Request

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


def create_app(
    processor_group: Union[EndpointGroup, CollectorGroup],
    flow_dependencies: Dict[Type, Any],
    run_id: RunID,
    process_fn: Callable,
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
    def setup_processor_group():
        for processor in processor_group.processors:
            if hasattr(app.state, "processor_map"):
                app.state.processor_map[processor.processor_id] = processor
            else:
                app.state.processor_map = {processor.processor_id: processor}
            processor.setup()
            initialize_dependencies(
                processor.dependencies(), flow_dependencies, [Scope.REPLICA]
            )

    for processor in processor_group.processors:
        input_types, output_type = process_types(processor)

        class EndpointFastAPIWrapper:
            def __init__(self, processor_id, run_id, flow_dependencies):
                self.job_id = ray.get_runtime_context().get_job_id()
                self.run_id = run_id
                self.num_events_processed_counter = num_events_processed(
                    processor_id=processor_id,
                    job_id=self.job_id,
                    run_id=run_id,
                )
                self.process_time_counter = process_time_counter(
                    processor_id=processor_id,
                    job_id=self.job_id,
                    run_id=run_id,
                )
                self.processor_id = processor_id
                self.flow_dependencies = flow_dependencies
                self.request_arg = None
                argspec = inspect.getfullargspec(processor.process)
                for arg in argspec.args:
                    if (
                        arg in argspec.annotations
                        and argspec.annotations[arg] == fastapi.Request
                    ):
                        self.request_arg = arg

            # NOTE: we have to import this seperately because it gets run
            # inside of the ray actor
            from buildflow.core.processor.utils import add_input_types

            @add_input_types(input_types, output_type)
            async def handle_request(
                self, raw_request: fastapi.Request, *args, **kwargs
            ) -> output_type:
                processor = app.state.processor_map[self.processor_id]
                self.num_events_processed_counter.inc(
                    tags={
                        "processor_id": processor.processor_id,
                        "JobId": self.job_id,
                        "RunId": self.run_id,
                    }
                )
                start_time = time.monotonic()
                dependency_args = resolve_dependencies(
                    processor.dependencies(), self.flow_dependencies, raw_request
                )
                if self.request_arg is not None:
                    kwargs[self.request_arg] = raw_request

                output = await process_fn(processor, *args, **kwargs, **dependency_args)
                self.process_time_counter.inc(
                    (time.monotonic() - start_time) * 1000,
                    tags={
                        "processor_id": processor.processor_id,
                        "JobId": self.job_id,
                        "RunId": self.run_id,
                    },
                )
                return output

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
