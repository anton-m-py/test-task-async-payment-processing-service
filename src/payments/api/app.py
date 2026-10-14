from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from payments.api.routes import health_router, payments_router
from payments.config import ApiSettings, DatabaseSettings
from payments.db import create_engine, create_session_factory


def create_app(
    settings: ApiSettings | None = None,
    db_settings: DatabaseSettings | None = None,
) -> FastAPI:
    settings = settings or ApiSettings()
    db_settings = db_settings or DatabaseSettings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(db_settings)
        app.state.session_factory = create_session_factory(engine)
        yield
        await engine.dispose()

    docs = settings.docs_enabled
    app = FastAPI(
        title="Payments service",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs" if docs else None,
        redoc_url="/redoc" if docs else None,
        openapi_url="/openapi.json" if docs else None,
    )
    app.state.settings = settings
    app.include_router(payments_router)
    app.include_router(health_router)
    return app
