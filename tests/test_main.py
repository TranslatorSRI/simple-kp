"""Test /query endpoint."""
import json
import pytest

import aiosqlite

from simple_kp.build_db import add_data
from simple_kp.engine import KnowledgeProvider
from small_kg import mychem

from tests.logging_setup import setup_logger

setup_logger()


@pytest.fixture
async def connection():
    """Return FastAPI app fixture."""
    async with aiosqlite.connect(":memory:") as connection:
        await add_data(
            connection,
            nodes_file=mychem.nodes_file,
            edges_file=mychem.edges_file,
        )
        yield connection


@pytest.fixture
async def kp():
    """Return FastAPI app fixture."""
    async with aiosqlite.connect(":memory:") as connection:
        await add_data(
            connection,
            nodes_file=mychem.nodes_file,
            edges_file=mychem.edges_file,
        )
        yield KnowledgeProvider(connection)


@pytest.mark.asyncio
async def test_reverse(kp: KnowledgeProvider):
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
async def test_forward(kp: KnowledgeProvider):
    """Test subject->object lookup."""
    message = {
        "query_graph": {
            "nodes": {
                "n0": {
                    "category": "biolink:Disease",
                },
                "n1": {
                    "category": "biolink:ChemicalSubstance",
                    "id": "CHEBI:136043",
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
async def test_list_properties(kp: KnowledgeProvider):
    """Test that we correctly handle query graph where categories, ids, and predicates are lists."""
    message = {
        "query_graph": {
            "nodes": {
                "n0": {
                    "category": ["biolink:Disease"],
                },
                "n1": {
                    "category": ["biolink:ChemicalSubstance"],
                    "id": ["CHEBI:136043"],
                },
            },
            "edges": {
                "e01": {
                    "subject": "n1",
                    "object": "n0",
                    "predicate": ["biolink:treats"],
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
async def test_no_reverse(connection: aiosqlite.Connection):
    """Test prohibited object->subject lookup."""
    kp = KnowledgeProvider(
        connection,
        object_to_subject=False,
    )
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
    assert results == []


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
