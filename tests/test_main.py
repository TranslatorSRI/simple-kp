"""Test /query endpoint."""
import json
import pytest

import aiosqlite

from data.build_db import add_data
from simple_kp.engine import KnowledgeProvider

from tests.logging_setup import setup_logger

setup_logger()


@pytest.fixture
async def kp():
    """Return FastAPI app fixture."""
    async with aiosqlite.connect(":memory:") as connection:
        await add_data(connection, origin="ctd")
        yield KnowledgeProvider(connection)


@pytest.mark.asyncio
async def test_main(kp: KnowledgeProvider):
    """Test simple KP."""
    message = {
        "query_graph": {
            "nodes": {
                "n0": {
                    "category": "biolink:Disease",
                    "id": "MONDO:0005148",
                },
                "n1": {
                    "category": "biolink:ChemicalSubstance",
                },
            },
            "edges": {
                "e01": {
                    "subject": "n1",
                    "object": "n0",
                    "predicate": "biolink:treats",
                },
            },
        },
        "results": {},
        "knowledge_graph": {
            "nodes": {},
            "edges": {},
        },
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    assert results
    print(json.dumps(results, indent=4))


@pytest.mark.asyncio
async def test_isittrue(kp: KnowledgeProvider):
    """Test is-it-true-that query."""
    message = {
        "query_graph": {
            "nodes": {
                "n0": {
                    "category": "biolink:Disease",
                    "id": "MONDO:0005148",
                },
                "n1": {
                    "category": "biolink:ChemicalSubstance",
                    "id": "CHEBI:6801",
                },
            },
            "edges": {
                "e01": {
                    "subject": "n1",
                    "object": "n0",
                    "predicate": "biolink:treats",
                },
            },
        },
        "results": {},
        "knowledge_graph": {
            "nodes": {},
            "edges": {},
        },
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    assert results
    print(json.dumps(results, indent=4))
