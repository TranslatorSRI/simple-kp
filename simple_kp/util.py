"""Query graph utilities."""
from collections import defaultdict


def to_list(scalar_or_list):
    """Enclose in list if necessary."""
    if not isinstance(scalar_or_list, list):
        return [scalar_or_list]
    return scalar_or_list


def is_cyclic(graph):
    """Detect if (undirected) graph has a cycle.

    Assume the graph is connected.
    """
    visited = {
        node['id']: False
        for node in graph['nodes']
    }
    connections = defaultdict(set)
    for edge in graph['edges']:
        connections[edge['source_id']].add(edge['target_id'])
        connections[edge['target_id']].add(edge['source_id'])

    def visit(node):
        """Visit node and neighbors, recursively."""
        if visited[node]:
            return True
        visited[node] = True
        neighbors = list(connections[node])
        for node_ in neighbors:
            connections[node].discard(node_)
            connections[node_].discard(node)
            if visit(node_):
                return True
        return False
    return visit(graph['nodes'][0]['id'])


def validate_node(qnode, knode):
    """Validate knode against qnode."""
    template = {
        key: value
        for key, value in qnode.items()
        if key not in ('id', 'curie') and value is not None
    }
    if qnode.get('curie', None):
        template['id'] = qnode['curie']
    return knode == {**knode, **template}


def validate_edge(qedge, kedge):
    """Validate kedge against qedge."""
    template = {
        key: value
        for key, value in qedge.items()
        if (
            key not in ('id', 'source_id', 'target_id')
            and value is not None
        )
    }
    return kedge == {**kedge, **template}
