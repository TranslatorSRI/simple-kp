"""Build DB script."""
import argparse
import asyncio

import aiosqlite

from simple_kp.build_db import add_data


async def main(filename, **kwargs):
    """Load data from CSV files."""
    connection = await aiosqlite.connect(filename)
    await add_data(connection, **kwargs)
    await connection.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Build SQLite database from CSV files.',
    )

    parser.add_argument('filename', type=str, help='database file')
    parser.add_argument('--nodes', type=str, default='', help='nodes.csv')
    parser.add_argument('--edges', type=str, default='', help='edges.csv')

    args = parser.parse_args()
    asyncio.run(main(
        args.filename,
        nodes_file=args.nodes,
        edges_file=args.edges,
    ))
