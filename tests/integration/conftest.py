from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from pydantic import PostgresDsn, SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from testcontainers.community.postgres import PostgresContainer

from payments.api.app import create_app
from payments.config import ApiSettings, DatabaseSettings
from payments.db import create_engine, create_session_factory

API_KEY = "test-api-key"
ROOT = Path(__file__).parents[2]


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    with PostgresContainer("postgres:18-alpine", driver="asyncpg") as postgres:
        url = postgres.get_connection_url()
        config = Config(ROOT / "alembic.ini")
        config.set_main_option("sqlalchemy.url", url)
        config.attributes["configure_logger"] = False
        command.upgrade(config, "head")
        yield url


@pytest.fixture(scope="session")
def db_settings(database_url: str) -> DatabaseSettings:
    return DatabaseSettings(database_url=PostgresDsn(database_url))


@pytest.fixture(scope="session")
async def engine(db_settings: DatabaseSettings) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(db_settings)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_session_factory(engine)


@pytest.fixture(autouse=True)
async def _clean_tables(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox"))


@pytest.fixture
async def app(db_settings: DatabaseSettings) -> AsyncIterator[FastAPI]:
    app = create_app(ApiSettings(api_key=SecretStr(API_KEY)), db_settings)
    async with app.router.lifespan_context(app):
        yield app


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"X-API-Key": API_KEY},
    ) as client:
        yield client
