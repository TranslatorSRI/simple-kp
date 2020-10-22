"""Test /query endpoint."""
import json
import pytest

import aiosqlite

from data.build_db import add_data
from simple_kp.engine import KnowledgeProvider


@pytest.fixture
async def kp():
    """Return FastAPI app fixture."""
    async with aiosqlite.connect(":memory:") as connection:
        await add_data(connection, origin_prefix="FOTR")
        yield KnowledgeProvider(connection)


@pytest.mark.asyncio
async def test_main(kp: KnowledgeProvider):
    """Test simple KP."""
    message = {
        "query_graph": {
            "nodes": {
                "n0": {
                    "type": "Person",
                    "id": "TGATE:Aragorn",
                },
                "n1": {
                    "type": "Group",
                    # "curie": "TGATE:Fellowship",
                },
                "n2": {
                    "type": "Person",
                    "id": "TGATE:Boromir",
                },
            },
            "edges": {
                "e01": {
                    "id": "e01",
                    "subject": "n0",
                    "object": "n1",
                },
                "e21": {
                    "id": "e21",
                    "subject": "n2",
                    "object": "n1",
                },
                # {
                #     "id": "e02",
                #     "source_id": "n0",
                #     "target_id": "n2",
                # },
            },
        },
        "results": {},
        "knowledge_graph": {
            "nodes": {},
            "edges": {},
        },
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    print(json.dumps(results, indent=4))
