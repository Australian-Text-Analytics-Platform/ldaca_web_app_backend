from fastapi.routing import APIRoute
from ldaca_wordflow.main import app


def test_openapi_operation_ids_use_route_names() -> None:
    schema = app.openapi()
    route_by_path_method = {
        (route.path, next(iter(route.methods)).lower()): route.name
        for route in app.routes
        if isinstance(route, APIRoute) and route.include_in_schema and route.methods
    }

    assert (
        schema["paths"]["/api/files/"]["get"]["operationId"]
        == route_by_path_method[("/api/files/", "get")]
    )
    assert (
        schema["paths"]["/api/workspaces/nodes/{node_id}/data"]["get"]["operationId"]
        == route_by_path_method[("/api/workspaces/nodes/{node_id}/data", "get")]
    )


def test_openapi_route_names_are_unique() -> None:
    route_names = [
        route.name
        for route in app.routes
        if isinstance(route, APIRoute) and route.include_in_schema
    ]

    assert len(route_names) == len(set(route_names))
