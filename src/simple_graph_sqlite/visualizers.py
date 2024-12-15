#!/usr/bin/env python3

"""
visualizers.py

Functions to enable visualizations of graph data, starting with graphviz,
and extensible to other libraries.

"""

from graphviz import Digraph
from pysimplegraph import database as db
import json

from typing import List, Dict, Any, Optional, Callable, Sequence, Tuple, Union


def _as_dot_label(
    body: Dict[str, Any],
    exclude_keys: List[str],
    hide_key_name: bool,
    kv_separator: str,
) -> str:
    """
    Construct a node or edge label string from the given body dictionary.

    Parameters
    ----------
    body : Dict[str, Any]
        The dictionary of node or edge properties.
    exclude_keys : List[str]
        Keys to exclude from the label.
    hide_key_name : bool
        Whether to hide the property keys and only show their values.
    kv_separator : str
        The string used to separate keys and values when displayed.

    Returns
    -------
    str
        A label string for use with Graphviz nodes or edges.
    """
    keys = [k for k in body.keys() if k not in exclude_keys]
    if hide_key_name:
        fstring = "\\n".join(["{" + k + "}" for k in keys])
    else:
        fstring = "\\n".join([k + kv_separator + "{" + k + "}" for k in keys])
    return fstring.format(**body)


def _as_dot_node(
    body: Dict[str, Any],
    exclude_keys: Optional[List[str]] = None,
    hide_key_name: bool = False,
    kv_separator: str = " ",
) -> Tuple[str, str]:
    """
    Create a tuple representing a Graphviz node.

    Parameters
    ----------
    body : Dict[str, Any]
        The node properties, must include an "id".
    exclude_keys : Optional[List[str]], optional
        Keys to exclude from the label, by default [].
    hide_key_name : bool, optional
        Whether to hide the property keys in the label, by default False.
    kv_separator : str, optional
        Separator between key and value in the label, by default " ".

    Returns
    -------
    Tuple[str, str]
        A tuple of (node_name, node_label).
    """
    if exclude_keys is None:
        exclude_keys = []
    name = body["id"]
    exclude_keys.append("id")
    label = _as_dot_label(body, exclude_keys, hide_key_name, kv_separator)
    return str(name), label


def graphviz_visualize(
    db_file: str,
    dot_file: str,
    path: Sequence[Union[str, int]] = (),
    connections: Callable[
        [Union[str, int]], Callable[[Any], List[Tuple[Any, Any, str]]]
    ] = db.get_connections,
    format: str = "png",
    exclude_node_keys: Optional[List[str]] = None,
    hide_node_key: bool = False,
    node_kv: str = " ",
    exclude_edge_keys: Optional[List[str]] = None,
    hide_edge_key: bool = False,
    edge_kv: str = " ",
) -> None:
    """
    Visualize a subgraph of the database as a Graphviz diagram.

    Parameters
    ----------
    db_file : str
        Path to the database file.
    dot_file : str
        Path where the output diagram file is to be written.
    path : Sequence[Union[str, int]], optional
        A list of node identifiers to visualize.
    connections : Callable[[Union[str, int]], Callable[[Any], List[Tuple[Any, Any, str]]]], optional
        A function that returns a callable to retrieve connections for a given node.
        Defaults to db.get_connections.
    format : str, optional
        The format of the output diagram, by default "png".
    exclude_node_keys : Optional[List[str]], optional
        Keys to exclude from the node labels, by default [].
    hide_node_key : bool, optional
        Whether to hide node property keys in labels, by default False.
    node_kv : str, optional
        Node key-value separator, by default " ".
    exclude_edge_keys : Optional[List[str]], optional
        Keys to exclude from the edge labels, by default [].
    hide_edge_key : bool, optional
        Whether to hide edge property keys in labels, by default False.
    edge_kv : str, optional
        Edge key-value separator, by default " ".

    Returns
    -------
    None
        Renders the Graphviz diagram to the specified file.
    """
    if exclude_node_keys is None:
        exclude_node_keys = []
    if exclude_edge_keys is None:
        exclude_edge_keys = []

    ids: List[str] = []
    for i in path:
        i_str = str(i)
        ids.append(i_str)
        for edge in db.atomic(db_file, connections(i)):
            src, tgt, _ = edge
            if src not in ids:
                ids.append(src)
            if tgt not in ids:
                ids.append(tgt)

    dot = Digraph()

    visited: List[str] = []
    edges: List[Tuple[str, str, str]] = []
    for i in ids:
        if i not in visited:
            node = db.atomic(db_file, db.find_node(i))
            name, label = _as_dot_node(node, exclude_node_keys, hide_node_key, node_kv)
            dot.node(name, label=label)
            for edge in db.atomic(db_file, connections(i)):
                if edge not in edges:
                    src, tgt, prps = edge
                    props = json.loads(prps)
                    dot.edge(
                        str(src),
                        str(tgt),
                        label=_as_dot_label(
                            props, exclude_edge_keys, hide_edge_key, edge_kv
                        )
                        if props
                        else None,
                    )
                    edges.append(edge)
            visited.append(i)

    dot.render(dot_file, format=format)


def graphviz_visualize_bodies(
    dot_file: str,
    path: Sequence[Tuple[Union[str, int], str, str]] = (),
    format: str = "png",
    exclude_node_keys: Optional[List[str]] = None,
    hide_node_key: bool = False,
    node_kv: str = " ",
    exclude_edge_keys: Optional[List[str]] = None,
    hide_edge_key: bool = False,
    edge_kv: str = " ",
) -> None:
    """
    Visualize a path of traversed nodes and edges, where each element is a tuple
    containing (identifier, object_string, properties_string).

    Parameters
    ----------
    dot_file : str
        The path where the output diagram file will be written.
    path : Sequence[Tuple[Union[str, int], str, str]], optional
        The traversal path, where each element is a tuple:
        (identifier, obj, properties).
        If obj == "()", the tuple represents a node; if obj == "->" or "<-", an edge.
    format : str, optional
        The format of the output diagram, by default "png".
    exclude_node_keys : Optional[List[str]], optional
        Keys to exclude from the node labels, by default [].
    hide_node_key : bool, optional
        Whether to hide node property keys in labels, by default False.
    node_kv : str, optional
        Node key-value separator, by default " ".
    exclude_edge_keys : Optional[List[str]], optional
        Keys to exclude from the edge labels, by default [].
    hide_edge_key : bool, optional
        Whether to hide edge property keys in labels, by default False.
    edge_kv : str, optional
        Edge key-value separator, by default " ".

    Returns
    -------
    None
        Renders the Graphviz diagram to the specified file.
    """
    if exclude_node_keys is None:
        exclude_node_keys = []
    if exclude_edge_keys is None:
        exclude_edge_keys = []

    dot = Digraph()
    current_id: Optional[str] = None
    edges: List[Tuple[str, str, Dict[str, Any]]] = []
    for identifier, obj, properties in path:
        body = json.loads(properties)
        if obj == "()":
            name, label = _as_dot_node(body, exclude_node_keys, hide_node_key, node_kv)
            dot.node(name, label=label)
            current_id = body["id"]
        else:
            if current_id is not None:
                if obj == "->":
                    edge = (str(current_id), str(identifier), body)
                else:
                    edge = (str(identifier), str(current_id), body)

                if edge not in edges:
                    dot.edge(
                        edge[0],
                        edge[1],
                        label=_as_dot_label(
                            body, exclude_edge_keys, hide_edge_key, edge_kv
                        )
                        if body
                        else None,
                    )
                    edges.append(edge)

    dot.render(dot_file, format=format)
