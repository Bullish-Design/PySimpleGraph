#!/usr/bin/env python3

"""
database.py

A series of functions to leverage the (node, edge) schema of
json-based nodes, and edges with optional json properties,
using an atomic transaction wrapper function.

"""

import sqlite3
import json
import pathlib
from functools import lru_cache
from jinja2 import Environment, BaseLoader, select_autoescape

from typing import (
    Callable,
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    Iterable,
    Sequence,
)


@lru_cache(maxsize=None)
def read_sql(sql_file: str) -> str:
    """
    Read and return the contents of a SQL file.

    Parameters
    ----------
    sql_file : str
        The name of the SQL file to read.

    Returns
    -------
    str
        The SQL file contents as a string.
    """
    with open(pathlib.Path(__file__).parent.resolve() / "sql" / sql_file) as f:
        return f.read()


class SqlTemplateLoader(BaseLoader):
    """
    A Jinja2 template loader that reads SQL templates from files.
    """

    def get_source(
        self, environment: Environment, template: str
    ) -> Tuple[str, str, bool]:
        """
        Return the source code, filename, and whether the template is up-to-date.

        Parameters
        ----------
        environment : Environment
            The Jinja2 environment.
        template : str
            The name of the template to load.

        Returns
        -------
        Tuple[str, str, bool]
            The template source, the template name, and a boolean indicating it is up-to-date.
        """
        return read_sql(template), template, True


env = Environment(
    loader=SqlTemplateLoader(),
    autoescape=select_autoescape(),
)

clause_template = env.get_template("search-where.template")
search_template = env.get_template("search-node.template")
traverse_template = env.get_template("traverse.template")


def atomic(db_file: str, cursor_exec_fn: Callable[[sqlite3.Cursor], Any]) -> Any:
    """
    Execute a given function within an atomic database transaction.

    Parameters
    ----------
    db_file : str
        Path to the SQLite database file.
    cursor_exec_fn : Callable[[sqlite3.Cursor], Any]
        A function that takes a database cursor and performs operations.

    Returns
    -------
    Any
        The result of `cursor_exec_fn`.
    """
    connection = None
    try:
        connection = sqlite3.connect(db_file)
        cursor = connection.cursor()
        cursor.execute("PRAGMA foreign_keys = TRUE;")
        results = cursor_exec_fn(cursor)
        connection.commit()
    finally:
        if connection:
            connection.close()
    return results


def initialize(db_file: str, schema_file: str = "schema.sql") -> Any:
    """
    Initialize the database schema by executing the provided schema SQL file.

    Parameters
    ----------
    db_file : str
        Path to the SQLite database file.
    schema_file : str, optional
        Name of the SQL file containing the schema definition, by default 'schema.sql'.

    Returns
    -------
    Any
        The result of the initialization operation.
    """

    def _init(cursor: sqlite3.Cursor) -> None:
        cursor.executescript(read_sql(schema_file))

    return atomic(db_file, _init)


def _set_id(
    identifier: Optional[Union[str, int]], data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Set the 'id' field in the given data dictionary if an identifier is provided.

    Parameters
    ----------
    identifier : Optional[Union[str, int]]
        The identifier for the node.
    data : Dict[str, Any]
        The node data.

    Returns
    -------
    Dict[str, Any]
        The modified data dictionary with 'id' set if identifier is given.
    """
    if identifier is not None:
        data["id"] = identifier
    return data


def _insert_node(
    cursor: sqlite3.Cursor, identifier: Optional[Union[str, int]], data: Dict[str, Any]
) -> None:
    """
    Insert a single node into the database.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor.
    identifier : Optional[Union[str, int]]
        Node identifier.
    data : Dict[str, Any]
        Node properties as a dictionary.
    """
    cursor.execute(
        read_sql("insert-node.sql"), (json.dumps(_set_id(identifier, data)),)
    )


def add_node(
    data: Dict[str, Any], identifier: Optional[Union[str, int]] = None
) -> Callable[[sqlite3.Cursor], None]:
    """
    Return a callable that, when executed with a database cursor, inserts a node.

    Parameters
    ----------
    data : Dict[str, Any]
        Node properties.
    identifier : Optional[Union[str, int]]
        Node identifier, optional.

    Returns
    -------
    Callable[[sqlite3.Cursor], None]
        Function that inserts the node into the database.
    """

    def _add_node(cursor: sqlite3.Cursor) -> None:
        _insert_node(cursor, identifier, data)

    return _add_node


def add_nodes(
    nodes: List[Dict[str, Any]], ids: List[Optional[Union[str, int]]]
) -> Callable[[sqlite3.Cursor], None]:
    """
    Return a callable that, when executed with a database cursor, inserts multiple nodes.

    Parameters
    ----------
    nodes : List[Dict[str, Any]]
        A list of node property dictionaries.
    ids : List[Optional[Union[str, int]]]
        A list of node identifiers corresponding to the nodes.

    Returns
    -------
    Callable[[sqlite3.Cursor], None]
        Function that inserts multiple nodes into the database.
    """

    def _add_nodes(cursor: sqlite3.Cursor) -> None:
        cursor.executemany(
            read_sql("insert-node.sql"),
            [(json.dumps(_set_id(i, n)),) for i, n in zip(ids, nodes)],
        )

    return _add_nodes


def _upsert_node(
    cursor: sqlite3.Cursor, identifier: Union[str, int], data: Dict[str, Any]
) -> None:
    """
    Insert or update (upsert) a node in the database.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor.
    identifier : Union[str, int]
        Node identifier.
    data : Dict[str, Any]
        Node properties.
    """
    current_data = find_node(identifier)(cursor)
    if not current_data:
        _insert_node(cursor, identifier, data)
    else:
        updated_data = {**current_data, **data}
        cursor.execute(
            read_sql("update-node.sql"),
            (
                json.dumps(_set_id(identifier, updated_data)),
                identifier,
            ),
        )


def upsert_node(
    identifier: Union[str, int], data: Dict[str, Any]
) -> Callable[[sqlite3.Cursor], None]:
    """
    Return a callable that upserts a node when executed with a database cursor.

    Parameters
    ----------
    identifier : Union[str, int]
        Node identifier.
    data : Dict[str, Any]
        Node properties.

    Returns
    -------
    Callable[[sqlite3.Cursor], None]
        Function that upserts the node.
    """

    def _upsert(cursor: sqlite3.Cursor) -> None:
        _upsert_node(cursor, identifier, data)

    return _upsert


def upsert_nodes(
    nodes: List[Dict[str, Any]], ids: List[Union[str, int]]
) -> Callable[[sqlite3.Cursor], None]:
    """
    Return a callable that upserts multiple nodes when executed with a database cursor.

    Parameters
    ----------
    nodes : List[Dict[str, Any]]
        List of node property dictionaries.
    ids : List[Union[str, int]]
        Corresponding identifiers for the nodes.

    Returns
    -------
    Callable[[sqlite3.Cursor], None]
        Function that upserts multiple nodes.
    """

    def _upsert(cursor: sqlite3.Cursor) -> None:
        for id_val, node in zip(ids, nodes):
            _upsert_node(cursor, id_val, node)

    return _upsert


def connect_nodes(
    source_id: Union[str, int],
    target_id: Union[str, int],
    properties: Dict[str, Any] = {},
) -> Callable[[sqlite3.Cursor], None]:
    """
    Return a callable that creates an edge between two nodes.

    Parameters
    ----------
    source_id : Union[str, int]
        Identifier of the source node.
    target_id : Union[str, int]
        Identifier of the target node.
    properties : Dict[str, Any], optional
        Edge properties.

    Returns
    -------
    Callable[[sqlite3.Cursor], None]
        Function that inserts the edge into the database.
    """

    def _connect_nodes(cursor: sqlite3.Cursor) -> None:
        cursor.execute(
            read_sql("insert-edge.sql"),
            (
                source_id,
                target_id,
                json.dumps(properties),
            ),
        )

    return _connect_nodes


def connect_many_nodes(
    sources: List[Union[str, int]],
    targets: List[Union[str, int]],
    properties: List[Dict[str, Any]],
) -> Callable[[sqlite3.Cursor], None]:
    """
    Return a callable that creates multiple edges between sets of nodes.

    Parameters
    ----------
    sources : List[Union[str, int]]
        List of source node identifiers.
    targets : List[Union[str, int]]
        List of target node identifiers.
    properties : List[Dict[str, Any]]
        List of corresponding edge property dictionaries.

    Returns
    -------
    Callable[[sqlite3.Cursor], None]
        Function that inserts multiple edges.
    """

    def _connect_nodes(cursor: sqlite3.Cursor) -> None:
        cursor.executemany(
            read_sql("insert-edge.sql"),
            [(s, t, json.dumps(p)) for s, t, p in zip(sources, targets, properties)],
        )

    return _connect_nodes


def remove_node(identifier: Union[str, int]) -> Callable[[sqlite3.Cursor], None]:
    """
    Return a callable that removes a node and its connected edges.

    Parameters
    ----------
    identifier : Union[str, int]
        Node identifier.

    Returns
    -------
    Callable[[sqlite3.Cursor], None]
        Function that removes the node and associated edges.
    """

    def _remove_node(cursor: sqlite3.Cursor) -> None:
        cursor.execute(
            read_sql("delete-edge.sql"),
            (
                identifier,
                identifier,
            ),
        )
        cursor.execute(read_sql("delete-node.sql"), (identifier,))

    return _remove_node


def remove_nodes(
    identifiers: List[Union[str, int]],
) -> Callable[[sqlite3.Cursor], None]:
    """
    Return a callable that removes multiple nodes and their connected edges.

    Parameters
    ----------
    identifiers : List[Union[str, int]]
        List of node identifiers.

    Returns
    -------
    Callable[[sqlite3.Cursor], None]
        Function that removes the nodes and their edges.
    """

    def _remove_node(cursor: sqlite3.Cursor) -> None:
        cursor.executemany(
            read_sql("delete-edge.sql"),
            [
                (
                    identifier,
                    identifier,
                )
                for identifier in identifiers
            ],
        )
        cursor.executemany(
            read_sql("delete-node.sql"), [(identifier,) for identifier in identifiers]
        )

    return _remove_node


def _generate_clause(
    key: Optional[str],
    predicate: Optional[str] = None,
    joiner: Optional[str] = None,
    tree: bool = False,
    tree_with_key: bool = False,
) -> str:
    """
    Generate a WHERE clause snippet based on the given parameters.

    Parameters
    ----------
    key : Optional[str]
        The JSON key to filter on.
    predicate : Optional[str], default '='
        The comparison operator (e.g., '=', 'LIKE', '>', '<').
    joiner : Optional[str], default ''
        Logical operator to combine clauses (e.g., 'AND', 'OR').
    tree : bool, default False
        Whether to use a JSON tree query.
    tree_with_key : bool, default False
        Whether the tree query includes a key.

    Returns
    -------
    str
        The rendered clause snippet.
    """
    if predicate is None:
        predicate = "="
    if joiner is None:
        joiner = ""

    if tree:
        if tree_with_key:
            return clause_template.render(
                and_or=joiner, key=key, tree=tree, predicate=predicate
            )
        else:
            return clause_template.render(and_or=joiner, tree=tree, predicate=predicate)

    return clause_template.render(
        and_or=joiner, key=key, predicate=predicate, key_value=True
    )


def _generate_query(
    where_clauses: List[str],
    result_column: Optional[str] = None,
    key: Optional[str] = None,
    tree: bool = False,
) -> str:
    """
    Generate the search query SQL with optional JSON tree functionality.

    Parameters
    ----------
    where_clauses : List[str]
        A list of WHERE clause snippets.
    result_column : Optional[str], default 'body'
        Column to select (e.g. 'id' or 'body').
    key : Optional[str]
        A specific JSON key to query within the node data if tree is used.
    tree : bool, default False
        Whether the query uses a JSON tree function.

    Returns
    -------
    str
        The rendered SQL query string.
    """
    if result_column is None:
        result_column = "body"

    if tree:
        if key:
            return search_template.render(
                result_column=result_column,
                tree=tree,
                key=key,
                search_clauses=where_clauses,
            )
        else:
            return search_template.render(
                result_column=result_column, tree=tree, search_clauses=where_clauses
            )

    return search_template.render(
        result_column=result_column, search_clauses=where_clauses
    )


def find_node(
    identifier: Union[str, int],
) -> Callable[[sqlite3.Cursor], Dict[str, Any]]:
    """
    Return a callable that, when executed with a cursor, finds and returns a node by its identifier.

    Parameters
    ----------
    identifier : Union[str, int]
        Node identifier.

    Returns
    -------
    Callable[[sqlite3.Cursor], Dict[str, Any]]
        Function that returns the node data as a dictionary.
        Returns an empty dictionary if no node is found.
    """

    def _find_node(cursor: sqlite3.Cursor) -> Dict[str, Any]:
        query = _generate_query([clause_template.render(id_lookup=True)])
        result = cursor.execute(query, (identifier,)).fetchone()
        return {} if not result else json.loads(result[0])

    return _find_node


def _parse_search_results(results: List[Tuple[str]], idx=0) -> List[Dict[str, Any]]:
    """
    Parse search results from database into a list of dictionaries.

    Parameters
    ----------
    results : List[Tuple[str]]
        List of tuples, where each tuple is a row from the query.

    Returns
    -------
    List[Dict[str, Any]]
        List of node data dictionaries.
    """
    return [json.loads(item[idx]) for item in results]


def find_nodes(
    where_clauses: List[str],
    bindings: Sequence[Any],
    tree_query: bool = False,
    key: Optional[str] = None,
) -> Callable[[sqlite3.Cursor], List[Dict[str, Any]]]:
    """
    Return a callable that finds and returns multiple nodes matching given criteria.

    Parameters
    ----------
    where_clauses : List[str]
        WHERE clause SQL snippets.
    bindings : Sequence[Any]
        Values to bind to the query parameters.
    tree_query : bool, optional
        Whether to use JSON tree queries.
    key : Optional[str], optional
        A specific JSON key to query.

    Returns
    -------
    Callable[[sqlite3.Cursor], List[Dict[str, Any]]]
        Function that returns a list of node data dictionaries.
    """

    def _find_nodes(cursor: sqlite3.Cursor) -> List[Dict[str, Any]]:
        query = _generate_query(where_clauses, key=key, tree=tree_query)
        return _parse_search_results(cursor.execute(query, bindings).fetchall())

    return _find_nodes


def find_neighbors(with_bodies: bool = False) -> str:
    """
    Generate a traversal query to find all neighbors (inbound and outbound).

    Parameters
    ----------
    with_bodies : bool, optional
        Whether to include node bodies in the result.

    Returns
    -------
    str
        SQL query for finding neighbors.
    """
    return traverse_template.render(
        with_bodies=with_bodies, inbound=True, outbound=True
    )


def find_outbound_neighbors(with_bodies: bool = False) -> str:
    """
    Generate a traversal query to find outbound neighbors only.

    Parameters
    ----------
    with_bodies : bool, optional
        Whether to include node bodies in the result.

    Returns
    -------
    str
        SQL query for finding outbound neighbors.
    """
    return traverse_template.render(with_bodies=with_bodies, outbound=True)


def find_inbound_neighbors(with_bodies: bool = False) -> str:
    """
    Generate a traversal query to find inbound neighbors only.

    Parameters
    ----------
    with_bodies : bool, optional
        Whether to include node bodies in the result.

    Returns
    -------
    str
        SQL query for finding inbound neighbors.
    """
    return traverse_template.render(with_bodies=with_bodies, inbound=True)


def traverse(
    db_file: str,
    src: Union[str, int],
    tgt: Optional[Union[str, int]] = None,
    neighbors_fn: Callable[..., str] = find_neighbors,
    with_bodies: bool = False,
) -> Any:
    """
    Traverse the graph from a source node to an optional target node,
    collecting paths using a specified neighbors function.

    Parameters
    ----------
    db_file : str
        Path to the SQLite database file.
    src : Union[str, int]
        Source node identifier.
    tgt : Optional[Union[str, int]], optional
        Target node identifier to stop traversal at.
    neighbors_fn : Callable[..., str], optional
        A function that returns the SQL for neighbor queries, by default find_neighbors.
    with_bodies : bool, optional
        Whether to include node bodies in the traversal results.

    Returns
    -------
    Any
        The traversal path result.
    """

    def _traverse(cursor: sqlite3.Cursor) -> Any:
        path = []
        target = json.dumps(tgt)
        for row in cursor.execute(neighbors_fn(with_bodies=with_bodies), (src,)):
            if row:
                if with_bodies:
                    identifier, obj, _ = row
                    path.append(row)
                    if identifier == target and obj == "()":
                        break
                else:
                    identifier = row[0]
                    if identifier not in path:
                        path.append(identifier)
                        if identifier == target:
                            break
        return path

    return atomic(db_file, _traverse)


def connections_in() -> str:
    """
    Return the SQL for inbound connections search.

    Returns
    -------
    str
        SQL query to find inbound connections.
    """
    return read_sql("search-edges-inbound.sql")


def connections_out() -> str:
    """
    Return the SQL for outbound connections search.

    Returns
    -------
    str
        SQL query to find outbound connections.
    """
    return read_sql("search-edges-outbound.sql")


def get_connections_one_way(
    identifier: Union[str, int], direction: Callable[[], str] = connections_in
) -> Callable[[sqlite3.Cursor], List[Tuple]]:
    """
    Return a callable that retrieves either inbound or outbound connections for a node.

    Parameters
    ----------
    identifier : Union[str, int]
        Node identifier.
    direction : Callable[[], str], optional
        Function returning SQL for the desired connection direction (inbound or outbound).

    Returns
    -------
    Callable[[sqlite3.Cursor], List[Tuple]]
        Function that retrieves the connections.
    """

    def _get_connections(cursor: sqlite3.Cursor) -> List[Tuple]:
        return cursor.execute(direction(), (identifier,)).fetchall()

    return _get_connections


def get_connections(
    identifier: Union[str, int],
) -> Callable[[sqlite3.Cursor], List[Tuple]]:
    """
    Return a callable that retrieves both inbound and outbound connections for a node.

    Parameters
    ----------
    identifier : Union[str, int]
        Node identifier.

    Returns
    -------
    Callable[[sqlite3.Cursor], List[Tuple]]
        Function that retrieves both inbound and outbound connections.
    """

    def _get_connections(cursor: sqlite3.Cursor) -> List[Tuple]:
        return cursor.execute(
            read_sql("search-edges.sql"),
            (
                identifier,
                identifier,
            ),
        ).fetchall()

    return _get_connections
