"""Test operations."""
import pytest

import aiosqlite

from simple_kp.build_db import add_data
from simple_kp.engine import KnowledgeProvider

from tests.logging_setup import setup_logger

setup_logger()


@pytest.fixture
async def connection():
    """Return FastAPI app fixture."""
    async with aiosqlite.connect(":memory:") as connection:
        yield connection


@pytest.mark.asyncio
async def test_ops(connection: aiosqlite.Connection):
    """Test KP operations."""
    await add_data(
        connection,
        data="""
        MONDO:0005148(( category biolink:Disease ))
        MONDO:0005148<-- predicate biolink:treats --CHEBI:6801
        CHEBI:6801(( category biolink:ChemicalSubstance ))
        """,
    )
    kp = KnowledgeProvider(connection)
    ops = await kp.get_operations()
    assert len(ops) == 1

    await add_data(
        connection,
        data="""
        CHEBI:6801(( category biolink:ChemicalSubstance ))
        CHEBI:6801-- predicate biolink:treats -->MONDO:0005148
        MONDO:0005148(( category biolink:Disease ))
        """,
    )
    kp = KnowledgeProvider(connection)
    ops = await kp.get_operations()
    assert len(ops) == 2


@pytest.mark.asyncio
async def test_prefixes(connection: aiosqlite.Connection):
    """Test CURIE prefixes."""
    await add_data(
        connection,
        data="""
        MONDO:0005148(( category biolink:Disease ))
        CHEBI:6801(( category biolink:ChemicalSubstance ))
        CHEBI:xxx(( category biolink:ChemicalSubstance ))
        """,
    )
    kp = KnowledgeProvider(connection)
    prefixes = await kp.get_curie_prefixes()
    assert prefixes == {
        "biolink:Disease": ["MONDO"],
        "biolink:ChemicalSubstance": ["CHEBI"],
    }
