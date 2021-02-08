"""SQL query graph engine."""
from collections import defaultdict
import logging
import os
import re
import itertools
import sqlite3
from typing import Any, Dict, Tuple, Union

import aiosqlite

from .util import is_cyclic, to_list, validate_edge, validate_node, NoAnswersException

LOGGER = logging.getLogger(__name__)


def normalize_qgraph(qgraph):
    """Normalize query graph."""
    for node in qgraph["nodes"].values():
        node["category"] = node.get("category", "biolink:NamedThing")
    for edge in qgraph["edges"].values():
        edge["predicate"] = to_list(
            edge.get("predicate", "biolink:related_to"))


def to_kedge(row):
    """Convert operation to kedge."""
    row.pop("id")
    if row["predicate"].startswith("<"):
        kedge = {
            "subject": row.pop("target"),
            "predicate": row.pop("predicate")[2:-1],
            "object": row.pop("source"),
        }
    else:
        kedge = {
            "subject": row.pop("source"),
            "predicate": row.pop("predicate")[1:-2],
            "object": row.pop("target"),
        }
    kedge.update(row)
    return kedge


list_fields = ['category']
match_list = re.compile(r"\|(.*?)\|")


def custom_row_factory(cursor, row):
    """
    Convert row to dictionary and
    convert some of the fields to lists
    """
    row_output = {}
    for idx, col in enumerate(cursor.description):
        row_output[col[0]] = row[idx]

    for field in list_fields:
        if field not in row_output:
            continue
        values = match_list.finditer(row_output[field])
        row_output[field] = [v.group(1) for v in values]
    return row_output


class KnowledgeProvider():
    """Knowledge provider."""

    def __init__(
            self,
            arg: Union[str, aiosqlite.Connection],
    ):
        """Initialize."""
        if isinstance(arg, str):
            self.database_file = arg
            self.name = os.path.splitext(
                os.path.basename(self.database_file)
            )[0]
            self.db = None
        elif isinstance(arg, aiosqlite.Connection):
            self.database_file = None
            self.name = None
            self.db = arg
            self.db.row_factory = custom_row_factory
        else:
            raise ValueError(
                "arg should be of type str or aiosqlite.Connection"
            )

    async def __aenter__(self):
        """Enter context."""
        if self.db is not None:
            return self
        self.db = await aiosqlite.connect(self.database_file)
        self.db.row_factory = sqlite3.Row
        return self

    async def __aexit__(self, *args):
        """Exit context."""
        if not self.database_file:
            return
        tmp_db = self.db
        self.db = None
        await tmp_db.close()

    async def get_operations(self):
        """Get operations."""
        async with self.db.execute(
                "SELECT * FROM edges",
        ) as cursor:
            edges = await cursor.fetchall()

        async with self.db.execute(
                "SELECT * FROM nodes",
        ) as cursor:
            nodes = await cursor.fetchall()
        nodes = {
            node["id"]: node
            for node in nodes
        }

        ops = set()
        for edge in edges:
            source_node = nodes[edge["source"]]
            target_node = nodes[edge["target"]]

            operation_iterator = itertools.product(
                source_node["category"],
                [edge["predicate"]],
                target_node["category"],
            )

            ops.update(operation_iterator)

        return [
            {
                "source_type": op[0],
                "edge_type": op[1],
                "target_type": op[2],
            }
            for op in ops
        ]

    async def get_curie_prefixes(self):
        """Get CURIE prefixes."""
        async with self.db.execute(
                "SELECT * FROM nodes",
        ) as cursor:
            nodes = await cursor.fetchall()

        prefixes = defaultdict(set)
        for node in nodes:
            for category in node["category"]:
                prefixes[category].add(node["id"].split(":")[0])
        return {
            category: list(prefix_set)
            for category, prefix_set in prefixes.items()
        }

    async def get_kedges(self, **kwargs):
        """Get kedges by source id."""
        assert kwargs
        conditions = []
        for key, value in kwargs.items():
            if isinstance(value, list):
                placeholders = ", ".join("?" for _ in value)
                conditions.append(f"{key} in ({placeholders})")
            else:
                conditions.append(f"{key} = ?")
        conditions = " AND ".join(conditions)
        async with self.db.execute(
                "SELECT id, source, predicate, target FROM edges WHERE " + conditions,
                list(
                    x for value in kwargs.values()
                    for x in to_list(value)
                ),
        ) as cursor:
            rows = await cursor.fetchall()

        return {
            row["id"]: to_kedge(dict(row))
            for row in rows
        }

    async def expand_from_node(
            self,
            qgraph: Dict[str, Any],
            qnode_id: str,
            knode_id: str,
            knode: Dict[str, Any],
    ):
        """Expand from query graph node."""
        # if this is a leaf node, we're done
        if not qgraph["edges"]:
            return {
                "nodes": {
                    knode_id: knode
                },
                "edges": dict(),
            }, [{
                "node_bindings": {
                    qnode_id: [{
                        "id": knode_id,
                    }],
                },
                "edge_bindings": dict(),
            }]

        LOGGER.debug(
            "Expanding from node %s/%s...",
            qnode_id,
            knode_id,
        )

        kgraph = {"nodes": dict(), "edges": dict()}
        results = []
        for qedge_id, qedge in qgraph["edges"].items():
            # get kedges for qedge
            if qnode_id == qedge["subject"]:
                kedges = await self.get_kedges(
                    source=knode_id,
                    predicate=[
                        f"-{predicate}->"
                        for predicate in qedge["predicate"]
                    ],
                )
            elif qnode_id == qedge["object"]:
                kedges = await self.get_kedges(
                    source=knode_id,
                    predicate=[
                        f"<-{predicate}-"
                        for predicate in qedge["predicate"]
                    ],
                )
            else:
                continue

            for kedge_id, kedge in kedges.items():
                # validate kedge against qedge
                if not validate_edge(qedge, kedge):
                    LOGGER.debug(
                        "kedge %s does not satisfy qedge %s",
                        str(kedge),
                        str(qedge),
                    )
                    continue

                # recursively expand from edge
                kgraph_, results_ = await self.expand_from_edge(
                    {
                        "nodes": {
                            key: value
                            for key, value in qgraph["nodes"].items()
                            if key != qnode_id
                        },
                        "edges": qgraph["edges"],
                    },
                    qedge_id,
                    kedge_id,
                    kedge,
                )
                kgraph["nodes"].update(kgraph_["nodes"])
                kgraph["edges"].update(kgraph_["edges"])
                results.extend(results_)

        if not results:
            return kgraph, results

        # add node to results and kgraph
        kgraph["nodes"][knode_id] = knode
        results = [
            {
                "node_bindings": {
                    **result["node_bindings"],
                    qnode_id: [{
                        "id": knode_id,
                    }],
                },
                "edge_bindings": result["edge_bindings"],
            }
            for result in results
        ]
        return kgraph, results

    async def get_knode(self, knode_id: str) -> Tuple[str, Dict]:
        """Get knode by id."""
        async with self.db.execute(
                "SELECT * FROM nodes WHERE id = ?",
                [knode_id],
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            raise NoAnswersException()
        return row["id"], {k: v for k, v in dict(row).items() if k != "id"}

    async def expand_from_edge(
            self,
            qgraph: Dict[str, Any],
            qedge_id: str,
            kedge_id: str,
            kedge: Dict[str, Any],
    ):
        """Expand along a query graph edge.

        Only one endpoint should be present in the query graph.
        """
        LOGGER.debug(
            "Expanding from edge %s/%s...",
            qedge_id,
            kedge_id,
        )
        qedge = qgraph["edges"][qedge_id]

        # get the remaining endpoint (query-graph and knowledge-graph nodes)
        if qedge["object"] in qgraph["nodes"]:
            qnode_id = qedge["object"]
            knode_id, knode = await self.get_knode(kedge["object"])
        elif qedge["subject"] in qgraph["nodes"]:
            qnode_id = qedge["subject"]
            knode_id, knode = await self.get_knode(kedge["subject"])
        else:
            raise RuntimeError("Expanding from qedge with no endpoints?")
        qnode = qgraph["nodes"][qnode_id]

        kgraph = {"nodes": dict(), "edges": dict()}
        results = []

        # validate knode against qnode
        _knode = {**knode, "id": knode_id}
        if not validate_node(qnode, _knode):
            LOGGER.debug(
                "knode %s does not satisfy qnode %s",
                str(_knode),
                str(qnode),
            )
            return kgraph, results

        # recursively expand from the endpoint
        kgraph, results = await self.expand_from_node(
            {
                "nodes": qgraph["nodes"],
                "edges": {
                    key: value
                    for key, value in qgraph["edges"].items()
                    if key != qedge_id
                },
            },
            qnode_id,
            knode_id,
            knode,
        )
        if not results:
            return kgraph, results

        # add edge to results and kgraph
        kgraph["edges"][kedge_id] = kedge
        results = [
            {
                "node_bindings": result["node_bindings"],
                "edge_bindings": {
                    **result["edge_bindings"],
                    qedge_id: [{
                        "id": kedge_id,
                    }],
                },
            }
            for result in results
        ]
        return kgraph, results

    async def get_results(self, qgraph: Dict[str, Any]):
        """Get results and kgraph."""
        if is_cyclic(qgraph):
            raise ValueError("Query graph is cyclic.")
        normalize_qgraph(qgraph)
        # find fixed qnode
        qnode_id, qnode = next(
            (key, qnode)
            for key, qnode in qgraph["nodes"].items()
            if qnode.get("id", None) is not None
        )
        # look up associated knode(s)
        curies = to_list(qnode["id"])
        kgraph = {
            "nodes": dict(),
            "edges": dict(),
        }
        results = []
        for curie in curies:
            try:
                knode_id, knode = await self.get_knode(curie)
            except NoAnswersException:
                break
            kgraph_, results_ = await self.expand_from_node(
                qgraph,
                qnode_id,
                knode_id,
                knode,
            )
            kgraph["nodes"].update(kgraph_["nodes"])
            kgraph["edges"].update(kgraph_["edges"])
            results.extend(results_)
        return kgraph, results
