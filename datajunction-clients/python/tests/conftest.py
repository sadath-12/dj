"""
Fixtures for testing DJ client.
"""
# pylint: disable=redefined-outer-name, invalid-name, W0611

import os
from http.client import HTTPException
from pathlib import Path
from typing import AsyncGenerator, Dict, Iterator, List, Optional
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from cachelib import SimpleCache
from datajunction_server.api.access.authentication import basic
from datajunction_server.api.main import app
from datajunction_server.config import Settings
from datajunction_server.database.base import Base
from datajunction_server.database.column import Column
from datajunction_server.database.engine import Engine
from datajunction_server.models.materialization import MaterializationInfo
from datajunction_server.models.query import QueryCreate, QueryWithResults
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.typing import QueryState
from datajunction_server.utils import (
    get_query_service_client,
    get_session,
    get_settings,
)
from httpx import AsyncClient
from pytest_mock import MockerFixture
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool
from starlette.testclient import TestClient
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from datajunction import DJBuilder
from tests.examples import COLUMN_MAPPINGS, EXAMPLES, QUERY_DATA_MAPPINGS


@pytest.fixture
def settings(mocker: MockerFixture) -> Iterator[Settings]:
    """
    Custom settings for unit tests.
    """
    settings = Settings(
        index="sqlite://",
        repository="/path/to/repository",
        results_backend=SimpleCache(default_timeout=0),
        celery_broker=None,
        redis_cache=None,
        query_service=None,
        secret="a-fake-secretkey",
    )

    mocker.patch(
        "datajunction_server.utils.get_settings",
        return_value=settings,
    )

    yield settings


@pytest.fixture(scope="session")
def postgres_container() -> PostgresContainer:
    """
    Setup postgres container
    """
    postgres = PostgresContainer(
        image="postgres:latest",
        user="dj",
        password="dj",
        dbname="dj",
        port=5432,
        driver="psycopg",
    )
    with postgres:
        wait_for_logs(
            postgres,
            r"UTC \[1\] LOG:  database system is ready to accept connections",
            10,
        )
        yield postgres


@pytest_asyncio.fixture
async def session(
    postgres_container: PostgresContainer,
) -> AsyncGenerator[AsyncSession, None]:
    """
    Create a Postgres session to test models.
    """
    engine = create_async_engine(
        url=postgres_container.get_connection_url(),
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async_session_factory = async_sessionmaker(
        bind=engine,
        autocommit=False,
        expire_on_commit=False,
    )
    async with async_session_factory() as session:
        yield session

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    # for AsyncEngine created in function scope, close and
    # clean-up pooled connections
    await engine.dispose()


@pytest.fixture
def query_service_client(mocker: MockerFixture) -> Iterator[QueryServiceClient]:
    """
    Custom settings for unit tests.
    """
    qs_client = QueryServiceClient(uri="query_service:8001")
    qs_client.query_state = QueryState.RUNNING  # type: ignore

    def mock_get_columns_for_table(
        catalog: str,
        schema: str,
        table: str,
        engine: Optional[Engine] = None,  # pylint: disable=unused-argument
    ) -> List[Column]:
        return COLUMN_MAPPINGS[f"{catalog}.{schema}.{table}"]

    mocker.patch.object(
        qs_client,
        "get_columns_for_table",
        mock_get_columns_for_table,
    )

    def mock_submit_query(
        query_create: QueryCreate,
        headers: Optional[Dict[str, str]] = None,  # pylint: disable=unused-argument
    ) -> QueryWithResults:
        results = QUERY_DATA_MAPPINGS[
            query_create.submitted_query.strip()
            .replace('"', "")
            .replace("\n", "")
            .replace(" ", "")
        ]
        if isinstance(results, Exception):
            raise results

        if results.state not in (QueryState.FAILED,):
            results.state = qs_client.query_state  # type: ignore
            qs_client.query_state = QueryState.FINISHED  # type: ignore
        return results

    mocker.patch.object(
        qs_client,
        "submit_query",
        mock_submit_query,
    )

    mock_materialize = MagicMock()
    mock_materialize.return_value = MaterializationInfo(
        urls=["http://fake.url/job"],
        output_tables=["common.a", "common.b"],
    )
    mocker.patch.object(
        qs_client,
        "materialize",
        mock_materialize,
    )

    mock_deactivate_materialization = MagicMock()
    mock_deactivate_materialization.return_value = MaterializationInfo(
        urls=["http://fake.url/job"],
        output_tables=[],
    )
    mocker.patch.object(
        qs_client,
        "deactivate_materialization",
        mock_deactivate_materialization,
    )

    mock_get_materialization_info = MagicMock()
    mock_get_materialization_info.return_value = MaterializationInfo(
        urls=["http://fake.url/job"],
        output_tables=["common.a", "common.b"],
    )
    mocker.patch.object(
        qs_client,
        "get_materialization_info",
        mock_get_materialization_info,
    )
    yield qs_client


@pytest.fixture
def server(  # pylint: disable=too-many-statements
    session: AsyncSession,
    settings: Settings,
    query_service_client: QueryServiceClient,
) -> Iterator[TestClient]:
    """
    Create a mock server for testing APIs that contains a mock query service.
    """

    def get_query_service_client_override() -> QueryServiceClient:
        return query_service_client

    async def get_session_override() -> AsyncSession:
        return session

    def get_settings_override() -> Settings:
        return settings

    app.dependency_overrides[get_session] = get_session_override
    app.dependency_overrides[get_settings] = get_settings_override
    app.dependency_overrides[
        get_query_service_client
    ] = get_query_service_client_override

    app.include_router(basic.router)
    with TestClient(app) as test_client:

        test_client.post(
            "/basic/user/",
            data={
                "email": "dj@datajunction.io",
                "username": "datajunction",
                "password": "datajunction",
            },
        )
        test_client.post(
            "/basic/login/",
            data={
                "username": "datajunction",
                "password": "datajunction",
            },
        )
        yield test_client

    app.dependency_overrides.clear()


def post_and_raise_if_error(server: TestClient, endpoint: str, json: dict):
    """
    Post the payload to the client and raise if there's an error
    """
    response = server.post(endpoint, json=json)
    if not response.status_code < 400:
        raise HTTPException(response.text)


@pytest.fixture
def session_with_examples(server: TestClient) -> TestClient:
    """
    load examples
    """
    for endpoint, json in EXAMPLES:
        post_and_raise_if_error(server=server, endpoint=endpoint, json=json)  # type: ignore
    return server


@pytest.fixture
def builder_client(session_with_examples: TestClient):
    """
    Returns a DJ client instance
    """
    client = DJBuilder(requests_session=session_with_examples)  # type: ignore
    client.create_user(
        email="dj@datajunction.io",
        username="datajunction",
        password="datajunction",
    )
    client.basic_login(
        username="datajunction",
        password="datajunction",
    )
    return client


@pytest.fixture
def change_to_project_dir(request):
    """
    Returns a function that changes to a specified project directory
    only for a single test. At the end of the test, this will change back
    to the tests directory to prevent any side-effects.
    """

    def _change_to_project_dir(project: str):
        """
        Changes to the directory for a specific example project
        """
        os.chdir(os.path.join(request.fspath.dirname, "examples", project))

    try:
        yield _change_to_project_dir
    finally:
        os.chdir(request.config.invocation_params.dir)


@pytest.fixture
def change_to_package_root_dir(request):
    """
    Changes to the datajunction package root dir only for a single test
    At the end of the test, this will change back
    to the tests directory to prevent any side-effects.
    """
    try:
        os.chdir(Path(request.fspath.dirname).parent)
    finally:
        os.chdir(request.config.invocation_params.dir)


def pytest_addoption(parser):
    """
    Add flags
    """
    parser.addoption(
        "--integration",
        action="store_true",
        dest="integration",
        default=False,
        help="Run integration tests",
    )
