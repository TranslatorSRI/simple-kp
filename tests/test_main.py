"""Test /query endpoint."""
import aiosqlite
import pytest

from reasoner_pydantic import KnowledgeGraph, Result

from simple_kp.build_db import add_data
from simple_kp.engine import KnowledgeProvider

from .logging_setup import setup_logger


setup_logger()


@pytest.fixture
async def connection():
    """Return FastAPI app fixture."""
    async with aiosqlite.connect(":memory:") as connection:
        yield connection


@pytest.mark.asyncio
async def test_reverse(connection: aiosqlite.Connection):
    """Test simple KP."""
    await add_data(
        connection,
        data="""
            MONDO:0005148(( category biolink:Disease ))
            MONDO:0005148<-- predicate biolink:treats --CHEBI:6801
            CHEBI:6801(( category biolink:ChemicalSubstance ))
        """,
    )
    kp = KnowledgeProvider(connection)
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
        }
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    assert results


@pytest.mark.asyncio
async def test_list_properties(connection: aiosqlite.Connection):
    """Test that we correctly handle query graph where categories, ids, and predicates are lists."""
    await add_data(
        connection,
        data="""
            CHEBI:136043(( category biolink:ChemicalSubstance ))
            CHEBI:136043-- predicate biolink:treats -->MONDO:0005148
            MONDO:0005148(( category biolink:Disease ))
        """,
    )
    kp = KnowledgeProvider(connection)
    message = {
        "query_graph": {
            "nodes": {
                "n0": {
                    "category": ["biolink:ChemicalSubstance"],
                    "id": ["CHEBI:136043"],
                },
                "n1": {
                    "category": ["biolink:Disease"],
                },
            },
            "edges": {
                "e01": {
                    "subject": "n0",
                    "object": "n1",
                    "predicate": ["biolink:treats"],
                },
            },
        }
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    assert results


@pytest.mark.asyncio
async def test_multiple_categories(connection: aiosqlite.Connection):
    """
    Test that when given multiple categories for a node
    we return all of them
    """
    await add_data(
        connection,
        data="""
            MONDO:0005148(( category biolink:Disease ))
            MONDO:0005148<-- predicate biolink:treats --CHEBI:6801
            CHEBI:6801(( category biolink:ChemicalSubstance ))
            CHEBI:6801(( category biolink:Drug ))
        """,
    )
    kp = KnowledgeProvider(connection)
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
        }
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    assert kgraph['nodes']['CHEBI:6801']['category'] == \
        ['biolink:ChemicalSubstance', 'biolink:Drug']
    assert results


@pytest.mark.asyncio
async def test_no_reverse(connection: aiosqlite.Connection):
    """Test prohibited object->subject lookup."""
    await add_data(
        connection,
        data="""
            MONDO:0005148(( category biolink:Disease ))
            MONDO:0005148<-- predicate biolink:treats --CHEBI:6801
            CHEBI:6801(( category biolink:ChemicalSubstance ))
        """,
    )
    kp = KnowledgeProvider(connection)
    message = {
        "query_graph": {
            "nodes": {
                "n0": {
                    "category": "biolink:ChemicalSubstance",
                    "id": "CHEBI:6801",
                },
                "n1": {
                    "category": "biolink:Disease",
                },
            },
            "edges": {
                "e01": {
                    "subject": "n0",
                    "object": "n1",
                    "predicate": "biolink:treats",
                },
            },
        }
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    assert results == []


@pytest.mark.asyncio
async def test_isittrue(connection: aiosqlite.Connection):
    """Test is-it-true-that query."""
    await add_data(
        connection,
        data="""
            MONDO:0005148(( category biolink:Disease ))
            MONDO:0005148<-- predicate biolink:treats --CHEBI:6801
            CHEBI:6801(( category biolink:ChemicalSubstance ))
        """,
    )
    kp = KnowledgeProvider(connection)
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
        }
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    assert results


@pytest.mark.asyncio
async def test_fail(connection: aiosqlite.Connection):
    """Test simple KP."""
    await add_data(
        connection,
        data="""
            MONDO:0005148(( category biolink:Disease ))
            MONDO:0005148<-- predicate biolink:treats --CHEBI:6801
            CHEBI:6801(( category biolink:ChemicalSubstance ))
        """,
    )
    kp = KnowledgeProvider(connection)
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
                    "predicate": "biolink:causes",
                },
            },
        }
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    assert results == []
