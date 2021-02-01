"""SQL query graph engine."""
import logging
import os
import sqlite3
from typing import Any, Dict, Tuple, Union

import aiosqlite

from .util import is_cyclic, to_list, validate_edge, validate_node, NoAnswersException

LOGGER = logging.getLogger(__name__)


class KnowledgeProvider():
    """Knowledge provider."""

    def __init__(
            self,
            arg: Union[str, aiosqlite.Connection],
            subject_to_object: bool = True,
            object_to_subject: bool = True,
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
            self.db.row_factory = sqlite3.Row
        else:
            raise ValueError(
                "arg should be of type str or aiosqlite.Connection"
            )
        self.subject_to_object = subject_to_object
        self.object_to_subject = object_to_subject

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
            if self.subject_to_object:
                ops.add((
                    nodes[edge["subject"]]["category"],
                    "-" + edge["predicate"] + "->",
                    nodes[edge["object"]]["category"],
                ))
            if self.object_to_subject:
                ops.add((
                    nodes[edge["object"]]["category"],
                    "<-" + edge["predicate"] + "-",
                    nodes[edge["subject"]]["category"],
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
                "SELECT id, subject, predicate, object FROM edges WHERE " + conditions,
                list(kwargs.values()),
        ) as cursor:
            rows = await cursor.fetchall()
        return {
            row["id"]: {
                k: v
                for k, v in dict(row).items()
                if k != "id"
            }
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
            if self.subject_to_object and qnode_id == qedge["subject"]:
                kedges = await self.get_kedges(subject=knode_id)
            elif self.object_to_subject and qnode_id == qedge["object"]:
                kedges = await self.get_kedges(object=knode_id)
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
