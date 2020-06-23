"""Test /query endpoint."""
import json
import os
import pytest
import sqlite3

import aiosqlite
from fastapi import FastAPI
from fastapi.testclient import TestClient

from data.build_db import add_data
from simple_kp.engine import KnowledgeProvider


@pytest.fixture
async def kp():
    """Return FastAPI app fixture."""
    connection = await aiosqlite.connect(':memory:')
    await add_data(connection, origin_prefix='FOTR')
    kp = KnowledgeProvider(connection)
    yield kp
    await connection.close()


@pytest.mark.asyncio
async def test_main(kp):
    """Test simple KP."""
    message = {
        'query_graph': {
            'nodes': [
                {
                    'id': 'n0',
                    'type': 'Person',
                    'curie': 'TGATE:Aragorn',
                },
                {
                    'id': 'n1',
                    'type': 'Group',
                    # 'curie': 'TGATE:Fellowship',
                },
                {
                    'id': 'n2',
                    'type': 'Person',
                    'curie': 'TGATE:Boromir',
                },
            ],
            'edges': [
                {
                    'id': 'e01',
                    'source_id': 'n0',
                    'target_id': 'n1',
                },
                {
                    'id': 'e21',
                    'source_id': 'n2',
                    'target_id': 'n1',
                },
                # {
                #     'id': 'e02',
                #     'source_id': 'n0',
                #     'target_id': 'n2',
                # },
            ],
        },
        'results': [],
        'knowledge_graph': {
            'nodes': [],
            'edges': [],
        },
    }
    kgraph, results = await kp.get_results(message['query_graph'])
    print(json.dumps(results, indent=4))
