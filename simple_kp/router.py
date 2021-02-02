"""FastAPI router."""
import logging
from typing import List, Union

import aiosqlite
from fastapi import Depends, APIRouter
from reasoner_pydantic import Query, Response

from .engine import KnowledgeProvider

LOGGER = logging.getLogger(__name__)


def get_kp(database_file: Union[str, aiosqlite.Connection]):
    """Get KP dependable."""
    async def kp_dependable():
        """Get knowledge provider."""
        async with KnowledgeProvider(database_file) as kp:
            yield kp
    return kp_dependable


def kp_router(
        database_file: Union[str, aiosqlite.Connection],
):
    """Add KP to server."""
    router = APIRouter()

    @router.post("/query", response_model=Response)
    async def answer_question(
            query: Query,
            kp: KnowledgeProvider = Depends(get_kp(database_file))
    ) -> Response:
        """Get results for query graph."""
        query = query.dict()
        qgraph = query["message"]["query_graph"]

        kgraph, results = await kp.get_results(qgraph)

        response = {
            "message": {
                "knowledge_graph": kgraph,
                "results": results,
                "query_graph": qgraph,
            }
        }
        return response

    @router.get("/ops")
    async def get_operations(
            kp: KnowledgeProvider = Depends(get_kp(database_file)),
    ):
        """Get KP operations."""
        return await kp.get_operations()

    @router.get("/metadata")
    async def get_metadata(
            kp: KnowledgeProvider = Depends(get_kp(database_file)),
    ):
        """Get metadata."""
        return {
            "curie_prefixes": await kp.get_curie_prefixes(),
        }

    return router
