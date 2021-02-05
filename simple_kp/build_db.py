#!/usr/bin/env python
"""Data I/O."""
import csv
import re

import aiosqlite

from small_kg import mychem, synonyms_file
from ._types import CURIEMap


async def get_data_from_string(data: str):
    """Get data from string.

    Each line should be of the form:
    <CURIE>(( category <category> ))
    or
    <CURIE>-- predicate <predicate> --><CURIE>
    """
    node_pattern = (
        r"(?P<id>[\w:]+)"
        r"\(\( category (?P<category>[\w:]+) \)\)"
    )
    edge_pattern = (
        r"(?P<source>[\w:]+)"
        r"(?P<o2s><?)-- predicate (?P<predicate>[\w:]+) --(?P<s2o>>?)"
        r"(?P<target>[\w:]+)"
    )
    nodes = {}
    edges = {}
    for line in data.split("\n"):
        line = line.strip()
        if not line:
            continue

        match = re.fullmatch(node_pattern, line)
        if match is not None:
            nid = match.group("id")
            if nid not in nodes:
                nodes[nid] = {
                    "id": nid,
                    "category": [],
                }

            nodes[nid]["category"].append(
                match.group("category")
            )
            continue

        match = re.fullmatch(edge_pattern, line)
        if match is not None:
            eid = f"{match.group('source')}-{match.group('target')}"
            if eid not in edges:
                edges[eid] = {
                    "id": eid,
                    "source": match.group("source"),
                    "predicate": [],
                    "target": match.group("target"),
                }
            predicate = match.group("predicate")
            if match.group("o2s"):
                predicate = f"<-{predicate}-"
            else:
                predicate = f"-{predicate}->"
            edges[eid]["predicate"].append(predicate)
            continue

        raise ValueError(f"Failed to parse '{line}'")
    return list(nodes.values()), list(edges.values())


async def get_data_from_files(
        curie_prefixes: CURIEMap = None,
        nodes_file=mychem.nodes_file,
        edges_file=mychem.edges_file,
):
    """Get data from files."""
    with open(nodes_file, newline="", encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(
            csvfile,
            delimiter=',',
        )
        nodes = list(reader)
    with open(edges_file, newline="", encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(
            csvfile,
            delimiter=',',
        )
        edges = list(reader)
    edges = [
        {
            **edge,
            "id": idx
        }
        for idx, edge in enumerate(edges)
    ]

    if curie_prefixes is not None:
        # map to synonyms with prefix
        with open(synonyms_file, newline="") as csvfile:
            reader = csv.reader(
                csvfile,
                delimiter=',',
            )
            synsets = list(reader)
        synset_map = {
            term: synset
            for synset in synsets
            for term in synset
        }
        node_map = dict()
        for node in nodes:
            # get preferred CURIE
            for curie_prefix in curie_prefixes[node["category"]]:
                # get CURIE with prefix, if one exists
                try:
                    node_map[node["id"]] = next(
                        curie
                        for curie in synset_map[node["id"]]
                        if curie.startswith(curie_prefix + ":")
                    )
                    break
                except StopIteration:
                    continue
        for node in nodes:
            node["id"] = node_map.get(node["id"], node["id"])
        for edge in edges:
            edge["subject"] = node_map.get(edge["subject"], edge["subject"])
            edge["object"] = node_map.get(edge["object"], edge["object"])
    return nodes, edges


async def add_data(
        connection: aiosqlite.Connection,
        data: str = None,
        **kwargs,
):
    """Add data to SQLite database."""
    if data is not None:
        nodes, edges = await get_data_from_string(data)
    else:
        nodes, edges = await get_data_from_files(**kwargs)

    if nodes:
        # Convert category list to our custom string format
        for node in nodes:
            if 'category' in node:
                node['category'] = "".join(f"|{c}|" for c in node['category'])

        await connection.execute('CREATE TABLE IF NOT EXISTS nodes ({0})'.format(
            ', '.join([f'{val} text' for val in nodes[0]])
        ))
        await connection.executemany('INSERT INTO nodes VALUES ({0})'.format(
            ', '.join(['?' for _ in nodes[0]])
        ), [list(node.values()) for node in nodes])
    if edges:
        # Convert predicate list to our custom string format
        for edge in edges:
            edge['predicate'] = "".join(f"|{c}|" for c in edge['predicate'])

        await connection.execute('CREATE TABLE IF NOT EXISTS edges ({0})'.format(
            ', '.join([f'{val} text' for val in edges[0]])
        ))
        await connection.executemany('INSERT INTO edges VALUES ({0})'.format(
            ', '.join(['?' for _ in edges[0]])
        ), [list(edge.values()) for edge in edges])
    await connection.commit()
