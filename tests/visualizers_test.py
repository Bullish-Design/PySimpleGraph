from pathlib import Path
from filecmp import cmp

from _pytest.tmpdir import tmp_path
from pysimplegraph import database as db
from pysimplegraph import visualizers as viz
from pysimplegraph import database
from test_common import database_test_file, nodes, edges, apple
import pytest


def test_visualization(database_test_file, apple, tmp_path):
    dot_raw = tmp_path / "apple-raw.dot"
    viz.graphviz_visualize(database_test_file, dot_raw, [4, 1, 5])
    here = Path(__file__).parent.resolve()
    assert cmp(dot_raw, here / "fixtures" / "apple-raw.dot")
    dot = tmp_path / "apple.dot"
    viz.graphviz_visualize(
        database_test_file,
        dot,
        [4, 1, 5],
        exclude_node_keys=["type"],
        hide_edge_key=True,
    )
    assert cmp(dot, here / "fixtures" / "apple.dot")


def test_visualize_bodies(database_test_file, apple, tmp_path):
    dot_raw = tmp_path / "apple-raw.dot"
    path_with_bodies = db.traverse(database_test_file, 4, 5, with_bodies=True)
    viz.graphviz_visualize_bodies(dot_raw, path_with_bodies)
    here = Path(__file__).parent.resolve()
    assert cmp(dot_raw, here / "fixtures" / "apple-raw.dot")
    dot = tmp_path / "apple.dot"
    viz.graphviz_visualize_bodies(
        dot, path_with_bodies, exclude_node_keys=["type"], hide_edge_key=True
    )
    assert cmp(dot, here / "fixtures" / "apple.dot")


def main():
    # temp_path = tmp_path(Path(__file__))
    # print(f"Temp path -> {type(temp_path).__name__} : {temp_path}")
    # db_file = database_test_file()
    # apple_graph = apple
    # print(f"Database file -> {type(db_file).__name__} : {db_file}")
    print(f"\n\nRunning tests...")
    print(f"Running test_visualization...\n\n")
    test_visualization(database_test_file, apple, tmp_path)
    print(f"\n\nRunning test_visualize_bodies...\n\n")
    test_visualize_bodies(database_test_file, apple, tmp_path)
    print(f"\n\nAll tests complete!\n\n")


if __name__ == "__main__":
    pytest.main(["-v", __file__])
