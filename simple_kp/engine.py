"""SQL query graph engine."""
import logging
import os
import sqlite3
from typing import Any, Dict, Union

import aiosqlite

from .util import is_cyclic, to_list, validate_edge, validate_node, NoAnswersException

LOGGER = logging.getLogger(__name__)


class KnowledgeProvider():
    """Knowledge provider."""

    def __init__(self, arg: Union[str, aiosqlite.Connection]):
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
            self.db.row_factory = sqlite3.Row
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
            edges = [dict(val) for val in await cursor.fetchall()]

        async with self.db.execute(
                "SELECT * FROM nodes",
        ) as cursor:
            nodes = [dict(val) for val in await cursor.fetchall()]
        nodes = {
            node["id"]: node
            for node in nodes
        }

        ops = set()
        for edge in edges:
            ops.add((
                nodes[edge["subject"]]["type"],
                "-" + edge["type"] + "->",
                nodes[edge["object"]]["type"],
            ))
            ops.add((
                nodes[edge["object"]]["type"],
                "<-" + edge["type"] + "-",
                nodes[edge["subject"]]["type"],
            ))
        return [
            {
                "source_type": op[0],
                "edge_type": op[1],
                "target_type": op[2],
            }
            for op in ops
        ]

    async def get_kedges(self, **kwargs):
        """Get kedges by source id."""
        assert kwargs
        conditions = " AND ".join(f"{key} = ?" for key in kwargs)
        async with self.db.execute(
                "SELECT * FROM edges WHERE " + conditions,
                list(kwargs.values()),
        ) as cursor:
            return [dict(val) for val in await cursor.fetchall()]

    async def expand_from_node(
            self,
            qgraph: Dict[str, Any],
            qnode_id: str,
            knode: Dict[str, Any],
    ):
        """Expand from query graph node."""
        # if this is a leaf node, we're done
        if not qgraph["edges"]:
            return {
                "nodes": {
                    knode["id"]: knode
                },
                "edges": dict(),
            }, [{
                "node_bindings": {
                    qnode_id: [{
                        "kg_id": knode["id"],
                    }],
                },
                "edge_bindings": dict(),
            }]

        LOGGER.debug(
            "Expanding from node %s/%s...",
            qnode_id,
            knode["id"],
        )

        kgraph = {"nodes": dict(), "edges": dict()}
        results = []
        for _, qedge in qgraph["edges"].items():
            # get kedges for qedge
            if qnode_id == qedge["subject"]:
                kedges = await self.get_kedges(subject=knode["id"])
            elif qnode_id == qedge["object"]:
                kedges = await self.get_kedges(object=knode["id"])
            else:
                continue

            for kedge in kedges:
                # validate kedge against qedge
                if not validate_edge(qedge, kedge):
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
                    qedge,
                    kedge,
                )
                kgraph["nodes"].update(kgraph_["nodes"])
                kgraph["edges"].update(kgraph_["edges"])
                results.extend(results_)

        if not results:
            return kgraph, results

        # add node to results and kgraph
        kgraph["nodes"][knode["id"]] = knode
        results = [
            {
                "node_bindings": {
                    **result["node_bindings"],
                    qnode_id: [{
                        "kg_id": knode["id"],
                    }],
                },
                "edge_bindings": result["edge_bindings"],
            }
            for result in results
        ]
        return kgraph, results

    async def get_knode(self, knode_id: str):
        """Get knode by id."""
        async with self.db.execute(
                "SELECT * FROM nodes WHERE id = ?",
                [knode_id],
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                raise NoAnswersException()
            return dict(row)

    async def expand_from_edge(
            self,
            qgraph: Dict[str, Any],
            qedge: Dict[str, Any],
            kedge: Dict[str, Any],
    ):
        """Expand along a query graph edge.

        Only one endpoint should be present in the query graph.
        """
        LOGGER.debug(
            "Expanding from edge %s/%s...",
            qedge["id"],
            kedge["id"],
        )

        # get the remaining endpoint (query-graph and knowledge-graph nodes)
        if qedge["object"] in qgraph["nodes"]:
            qnode_id = qedge["object"]
            knode = await self.get_knode(kedge["object"])
        elif qedge["subject"] in qgraph["nodes"]:
            qnode_id = qedge["subject"]
            knode = await self.get_knode(kedge["subject"])
        qnode = qgraph["nodes"][qnode_id]

        kgraph = {"nodes": dict(), "edges": dict()}
        results = []

        # validate knode against qnode
        if not validate_node(qnode, knode):
            return kgraph, results

        # recursively expand from the endpoint
        kgraph, results = await self.expand_from_node(
            {
                "nodes": qgraph["nodes"],
                "edges": {
                    key: value
                    for key, value in qgraph["edges"].items()
                    if key != qedge["id"]
                },
            },
            qnode_id,
            knode,
        )
        if not results:
            return kgraph, results

        # add edge to results and kgraph
        kgraph["edges"][kedge["id"]] = kedge
        results = [
            {
                "node_bindings": result["node_bindings"],
                "edge_bindings": {
                    **result["edge_bindings"],
                    qedge["id"]: [{
                        "kg_id": kedge["id"],
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
        # find fixed qnode
        qnode_id, qnode = next(
            (key, qnode)
            for key, qnode in qgraph["nodes"].items()
            if qnode.get("id", None)
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
                knode = await self.get_knode(curie)
            except NoAnswersException:
                break
            kgraph_, results_ = await self.expand_from_node(
                qgraph,
                qnode_id,
                knode,
            )
            kgraph["nodes"].update(kgraph_["nodes"])
            kgraph["edges"].update(kgraph_["edges"])
            results.extend(results_)
        return kgraph, results
