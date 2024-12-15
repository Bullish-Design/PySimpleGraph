#!/usr/bin/env python3

# Imports ----------------------------------------
from __future__ import annotations
import os
import json
import uuid

# from threading import Lock
from pydantic import BaseModel, Field, model_validator
from typing import Any, Dict, Optional, Union, List, Tuple, Callable, Sequence, ClassVar


# Local Imports ----------------------------------
from pysimplegraph import database as db
from pysimplegraph import visualizers as viz

from dotenv import load_dotenv

# from simple_graph_sqlite.database import initialize

load_dotenv()

test_dir = os.getenv("TEST_DIR")
db_file = os.getenv("DB_FILE")
dot_file = os.getenv("DOT_FILE")

# Classes ----------------------------------------


def init_db(db_file: str, dot_file: str):
    graph_db = GraphDB(db_file=db_file, dot_file=dot_file)
    graph_db.initialize()
    return graph_db


def getDB() -> GraphDB:
    instance = GraphDB._instance
    if instance is None:
        instance = init_db(db_file, dot_file)  # GraphDB.get_instance()
        # raise RuntimeError("No GraphDB instance found. Initialize GraphDB first.")
    return instance


class GraphDB(BaseModel):
    db_file: str = Field(..., description="Path to the database file.")
    dot_file: str = Field(..., description="Path to the output diagram file.")
    _instance: ClassVar[Optional[GraphDB]] = None
    # _lock = Lock()

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def get_instance(cls) -> GraphDB:
        """
        Return a singleton instance of GraphDB. If one doesn't exist, create it.
        """
        if cls._instance is None:
            # with cls._lock:
            #    if cls._instance is None:
            initialized_self = cls(db_file=db_file, dot_file=dot_file).initialize()
            cls._instance = initialized_self
        return cls._instance

    def initialize(self, schema_file: str = "schema.sql") -> None:
        """
        Initialize the database using the provided schema file.
        """
        initialized_db = db.initialize(self.db_file, schema_file=schema_file)
        return initialized_db

    # --- Node Operations ---
    def upsert_node(self, node: Node) -> None:
        db.atomic(self.db_file, db.upsert_node(node.id, node.body))

    def remove_node(self, node: Node) -> None:
        db.atomic(self.db_file, db.remove_node(node.id))

    def find_node(self, identifier: Union[str, int]) -> Dict[str, Any]:
        return db.atomic(self.db_file, db.find_node(identifier))

    def find_nodes(
        self,
        where_clauses: List[str],
        bindings: List[Any],
        tree_query: bool = False,
        key: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return db.atomic(
            self.db_file, db.find_nodes(where_clauses, bindings, tree_query, key)
        )

    def upsert_nodes(self, nodes: list[Node]) -> None:
        bodies = []
        ids = []
        for node in nodes:
            bodies.append(node.body)
            ids.append(node.id)
        db.atomic(self.db_file, db.upsert_nodes(bodies, ids))

    # --- Edge Operations ---
    def connect_nodes(
        self,
        source_id: Union[str, int],
        target_id: Union[str, int],
        properties: Dict[str, Any] = {},
    ) -> None:
        db.atomic(self.db_file, db.connect_nodes(source_id, target_id, properties))

    def connect_many_nodes(
        self,
        sources: List[Union[str, int]],
        targets: List[Union[str, int]],
        properties: List[Dict[str, Any]],
    ) -> None:
        db.atomic(self.db_file, db.connect_many_nodes(sources, targets, properties))

    def get_connections(
        self, identifier: Union[str, int]
    ) -> List[Tuple[str, str, str]]:
        return db.atomic(self.db_file, db.get_connections(identifier))

    # --- Visualization ---
    def visualize(
        self,
        path: Sequence[Union[str, int]] = (),
        format: str = "png",
        exclude_node_keys: Optional[List[str]] = None,
        hide_node_key: bool = False,
        node_kv: str = " ",
        exclude_edge_keys: Optional[List[str]] = None,
        hide_edge_key: bool = False,
        edge_kv: str = " ",
        connections: Callable[
            [Union[str, int]], Callable[[Any], List[Tuple[Any, Any, str]]]
        ] = db.get_connections,
    ) -> None:
        viz.graphviz_visualize(
            db_file=self.db_file,
            dot_file=self.dot_file,
            path=path,
            connections=connections,
            format=format,
            exclude_node_keys=exclude_node_keys or [],
            hide_node_key=hide_node_key,
            node_kv=node_kv,
            exclude_edge_keys=exclude_edge_keys or [],
            hide_edge_key=hide_edge_key,
            edge_kv=edge_kv,
        )

    def visualize_bodies(
        self,
        path: Sequence[Tuple[Union[str, int], str, str]] = (),
        format: str = "png",
        exclude_node_keys: Optional[List[str]] = None,
        hide_node_key: bool = False,
        node_kv: str = " ",
        exclude_edge_keys: Optional[List[str]] = None,
        hide_edge_key: bool = False,
        edge_kv: str = " ",
    ) -> None:
        viz.graphviz_visualize_bodies(
            dot_file=self.dot_file,
            path=path,
            format=format,
            exclude_node_keys=exclude_node_keys or [],
            hide_node_key=hide_node_key,
            node_kv=node_kv,
            exclude_edge_keys=exclude_edge_keys or [],
            hide_edge_key=hide_edge_key,
            edge_kv=edge_kv,
        )


class Node(BaseModel):
    id: Optional[Union[str, int]] = Field(
        None, description="Unique identifier for the node."
    )
    body: Dict[str, Any] = Field(
        ..., description="Properties of the node as key-value pairs."
    )

    @model_validator(mode="before")  # pre=True)
    def ensure_id(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if "id" not in values or values["id"] is None:
            values["id"] = str(uuid.uuid4())
        return values

    def save(self) -> None:
        instance = getDB()
        instance.upsert_node(self)  # .id, self.body)

    def delete(self) -> None:
        instance = getDB()
        instance.remove_node(self)  # .id)

    @classmethod
    def from_db(cls, identifier: Union[str, int]) -> Node:
        instance = getDB()
        data = instance.find_node(identifier)
        if not data:
            raise ValueError(f"No node found with id: {identifier}")
        return cls(id=data.get("id", identifier), body=data)

    @classmethod
    def bulk_save(cls, nodes: List[Node]) -> None:
        instance = getDB()
        instance.upsert_nodes(nodes)

    @classmethod
    def search(
        cls,
        where_clauses: List[str],
        bindings: List[Any],
        tree_query: bool = False,
        key: Optional[str] = None,
    ) -> List[Node]:
        instance = getDB()
        results = instance.find_nodes(where_clauses, bindings, tree_query, key)
        return [cls(id=node.get("id"), body=node) for node in results]


class Edge(BaseModel):
    source_id: Union[str, int] = Field(..., description="Source node ID.")
    target_id: Union[str, int] = Field(..., description="Target node ID.")
    properties: Dict[str, Any] = Field(
        default_factory=dict, description="Edge properties."
    )

    def save(self) -> None:
        instance = getDB()
        instance.connect_nodes(self.source_id, self.target_id, self.properties)

    @classmethod
    def bulk_save(cls, edges: List[Edge]) -> None:
        instance = getDB()
        instance.connect_many_nodes(
            [e.source_id for e in edges],
            [e.target_id for e in edges],
            [e.properties for e in edges],
        )

    @classmethod
    def get_connections(cls, identifier: Union[str, int]) -> List[Edge]:
        instance = getDB()
        conn_results = instance.get_connections(identifier)
        return [
            cls(source_id=src, target_id=tgt, properties=json.loads(props))
            for src, tgt, props in conn_results
        ]


def nodes():
    return {
        1: {
            "name": "Apple Computer Company",
            "type": ["company", "start-up"],
            "founded": "April 1, 1976",
        },
        2: {"name": "Steve Wozniak", "type": ["person", "engineer", "founder"]},
        "3": {"name": "Steve Jobs", "type": ["person", "designer", "founder"]},
        4: {"name": "Ronald Wayne", "type": ["person", "administrator", "founder"]},
        5: {"name": "Mike Markkula", "type": ["person", "investor"]},
    }


def edges():
    return {
        1: [(4, {"action": "divested", "amount": 800, "date": "April 12, 1976"})],
        2: [(1, {"action": "founded"}), ("3", None)],
        "3": [(1, {"action": "founded"})],
        4: [(1, {"action": "founded"})],
        5: [(1, {"action": "invested", "equity": 80000, "debt": 170000})],
    }


def test():
    # Example usage:
    print(f"Starting example usage of {__file__}")
    # db_file = f"{test_dir}/test_db.sqlite"
    # dot_file = f"{test_dir}/test_graph.dot"
    print(f"Using database file: {db_file}\n")
    gv = GraphDB(db_file=db_file, dot_file=dot_file)
    gv.initialize()
    print(f"Initialized GraphDB: {gv}\n")
    nodes = nodes()
    edges = edges()
    print(f"\nAdding Nodes and edges:\n")
    for id, node in nodes.items():
        new_node = Node(id=id, body=node)
        gv.upsert_node(new_node)

    for src, targets in edges.items():
        for target in targets:
            tgt, label = target
            if label:
                new_edge = Edge(source_id=src, target_id=tgt, properties=label)
            else:
                new_edge = Edge(source_id=src, target_id=tgt)
            new_edge.save()

    node = Node.from_db("2")
    print(f"Node: {node}\n")
    node.body["new_prop"] = "value"
    node.save()
    print(f"Updated node: {node}\n")
    Node.bulk_save([Node(id="4", body={"foo": "bar"})])
    print(f"bulk save")
    edges = Edge.get_connections("3")
    print(f"Edges: {edges}\n")
    gv.visualize(path=["3"])
