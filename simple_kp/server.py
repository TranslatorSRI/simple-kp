"""Simple API server."""
import glob
import os

from fastapi import Depends, FastAPI, APIRouter
from reasoner_pydantic import Message

from .engine import KnowledgeProvider


def get_kp(database_file):
    """Get KP dependable."""
    async def kp_dependable():
        """Get knowledge provider."""
        async with KnowledgeProvider(database_file) as kp:
            yield kp
    return kp_dependable


app = FastAPI(
    title='Test KP',
    description='Simple dummy KP for testing',
    version='0.1.0',
)


def add_kp(app, database_file, name=None):
    """Add KP to server."""
    if name is None:
        name = os.path.splitext(os.path.basename(database_file))[0]
    router = APIRouter()

    @router.post("/query")
    async def answer_question(
            message: Message,
            kp=Depends(get_kp(database_file))
    ):
        """Get results for query graph."""
        message = message.dict()
        qgraph = message['query_graph']

        kgraph, results = await kp.get_results(qgraph)

        message = {
            'knowledge_graph': kgraph,
            'results': results,
            'query_graph': qgraph,
        }
        return message

    @router.get("/ops")
    async def get_operations(
            kp=Depends(get_kp(database_file)),
    ):
        """Get KP operations."""
        return await kp.get_operations()

    app.include_router(router, prefix='/' + name)


database_files = glob.glob('data/*.db')
for database_file in database_files:
    add_kp(app, database_file)
