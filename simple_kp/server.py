"""Simple API server."""
import glob
import logging

from fastapi import FastAPI

from .router import kp_router

LOGGER = logging.getLogger(__name__)

app = FastAPI(
    title="Test KP",
    description="Simple dummy KP for testing",
    version="0.1.0",
)

database_files = glob.glob("./*.db")
if not database_files:
    raise RuntimeError("No database in sqlite/")
database_file = database_files[0]
if len(database_files) > 1:
    LOGGER.warning("More than one database file. Using %s", database_file)
app.include_router(kp_router(database_file))
