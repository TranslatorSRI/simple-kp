"""Test operations."""
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


@pytest.mark.asyncio
async def test_ops(connection: aiosqlite.Connection):
    """Test KP operations."""
    kp = KnowledgeProvider(connection)
    ops = await kp.get_operations()
    assert len(ops) == 2

    kp = KnowledgeProvider(
        connection,
        subject_to_object=False,
    )
    ops = await kp.get_operations()
    assert len(ops) == 1
    assert ops[0] == {
        "source_type": "biolink:Disease",
        "edge_type": "<-biolink:treats-",
        "target_type": "biolink:ChemicalSubstance",
    }

    kp = KnowledgeProvider(
        connection,
        object_to_subject=False,
    )
    ops = await kp.get_operations()
    assert len(ops) == 1
    assert ops[0] == {
        "source_type": "biolink:ChemicalSubstance",
        "edge_type": "-biolink:treats->",
        "target_type": "biolink:Disease",
    }

    kp = KnowledgeProvider(
        connection,
        object_to_subject=False,
        subject_to_object=False,
    )
    ops = await kp.get_operations()
    assert len(ops) == 0
