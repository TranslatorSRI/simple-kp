#!/usr/bin/env python
"""Data I/O."""
import argparse
import asyncio
import csv
import sqlite3

import aiosqlite


async def add_data(connection, origin_prefix=''):
    """Add data to SQLite database."""
    with open('data/nodes.csv', newline='') as csvfile:
        reader = csv.reader(
            csvfile,
            delimiter=',',
        )
        nodes = list(reader)
    with open('data/edges.csv', newline='') as csvfile:
        reader = csv.reader(
            csvfile,
            delimiter=',',
        )
        edges = list(reader)
        edges = [['id'] + edges[0]] + [
            [idx] + edge
            for idx, edge in enumerate(edges[1:])
            if edge[-1].startswith(origin_prefix)
        ]

    await connection.execute('CREATE TABLE nodes ({0})'.format(
        ', '.join([f'{val} text' for val in nodes[0]])
    ))
    await connection.executemany('INSERT INTO nodes VALUES ({0})'.format(
        ', '.join(['?' for _ in nodes[0]])
    ), nodes[1:])
    await connection.execute('CREATE TABLE edges ({0})'.format(
        ', '.join([f'{val} text' for val in edges[0]])
    ))
    await connection.executemany('INSERT INTO edges VALUES ({0})'.format(
        ', '.join(['?' for _ in edges[0]])
    ), edges[1:])
    await connection.commit()


async def main(filename, origin_prefix=''):
    """Load data from CSV files."""
    connection = await aiosqlite.connect(filename)
    await add_data(connection, origin_prefix=origin_prefix)
    await connection.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Build SQLite database from CSV files.',
    )

    parser.add_argument('filename', type=str, help='database file')
    parser.add_argument('--origin', type=str, default='', help='origin prefix')

    args = parser.parse_args()
    asyncio.run(main(args.filename, origin_prefix=args.origin))
