"""Testing utilities.

Utilities for testing _other_ apps using simple-kp.
"""
import aiosqlite
from fastapi import FastAPI

from asgiar import ASGIAR

from .build_db import add_data
from .server import kp_router

from ._contextlib import AsyncExitStack, asynccontextmanager


@asynccontextmanager
async def kp_app(**kwargs):
    """KP context manager."""
    app = FastAPI()

    async with aiosqlite.connect(":memory:") as connection:
        # add data to sqlite
        await add_data(connection, **kwargs)

        # add kp to app
        app.include_router(kp_router(connection, **kwargs))
        yield app


@asynccontextmanager
async def kp_overlay(host, **kwargs):
    """KP(s) server context manager."""
    async with AsyncExitStack() as stack:
        app = await stack.enter_async_context(
            kp_app(**kwargs)
        )
        await stack.enter_async_context(
            ASGIAR(app, host=host)
        )
        yield
