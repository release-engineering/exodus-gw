import pytest
from fastapi.routing import APIRoute

from exodus_gw.main import app
from exodus_gw.retry import RetryRoute


def test_all_routes_use_retry_route():
    """Verify all API routes use RetryRoute class for retry functionality."""

    routes_not_using_retry = []

    for route in app.routes:
        if isinstance(route, APIRoute):
            if not isinstance(route, RetryRoute):
                route_info = f"{route.path} [{','.join(route.methods)}]"
                routes_not_using_retry.append(route_info)

    if routes_not_using_retry:
        pytest.fail(
            f"Found {len(routes_not_using_retry)} routes not using RetryRoute:\n"
            + "\n".join(
                f"  - {route}" for route in routes_not_using_retry[:10]
            )
            + (
                f"\n  ... and {len(routes_not_using_retry) - 10} more"
                if len(routes_not_using_retry) > 10
                else ""
            )
        )
