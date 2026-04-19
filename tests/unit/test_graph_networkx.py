"""NetworkXGraphBackend unit tests.

Tests all 8 graph operations (4 core + 4 analytics) on an in-memory
NetworkX DiGraph. Same graph structure as the CTE backend tests:
cust_001 -> order_001, cust_002 -> order_001.

No infrastructure needed — pure in-memory.
"""

import pytest

nx = pytest.importorskip("networkx")

from defacto.query._graph import NetworkXGraphBackend


@pytest.fixture
def graph():
    """Small directed graph: two customers, one shared order."""
    G = nx.DiGraph()
    G.add_node("cust_001", entity_type="customer", name="Alice", mrr=99.0)
    G.add_node("cust_002", entity_type="customer", name="Bob", mrr=50.0)
    G.add_node("order_001", entity_type="order", amount=225.0, status="shipped")
    G.add_edge("cust_001", "order_001", relationship_type="placed_order")
    G.add_edge("cust_002", "order_001", relationship_type="placed_order")
    return G


@pytest.fixture
def backend(graph):
    return NetworkXGraphBackend(graph)


# ---------------------------------------------------------------------------
# Core operations
# ---------------------------------------------------------------------------


class TestTraverse:
    """traverse: BFS filtered by relationship type."""

    def test_directed(self, backend):
        df = backend.traverse("cust_001", "placed_order", depth=1)
        ids = set(df["entity_id"])
        assert "cust_001" in ids  # starting entity at depth 0
        assert "order_001" in ids  # reachable via placed_order

    def test_depth_zero(self, backend):
        """Depth 0 returns only the starting entity."""
        df = backend.traverse("cust_001", "placed_order", depth=0)
        assert len(df) == 1
        assert df.iloc[0]["entity_id"] == "cust_001"

    def test_wrong_relationship_type(self, backend):
        """No results when relationship type doesn't match."""
        df = backend.traverse("cust_001", "nonexistent", depth=5)
        # Only the starting entity (depth 0)
        assert len(df) == 1
        assert df.iloc[0]["entity_id"] == "cust_001"

    def test_undirected(self, backend):
        """Undirected traversal can go backwards along edges."""
        df = backend.traverse("order_001", "placed_order", depth=1, directed=False)
        ids = set(df["entity_id"])
        assert "order_001" in ids
        assert "cust_001" in ids or "cust_002" in ids

    def test_nonexistent_entity(self, backend):
        df = backend.traverse("ghost", "placed_order", depth=1)
        assert len(df) == 0

    def test_entity_type_enriched(self, backend):
        df = backend.traverse("cust_001", "placed_order", depth=1)
        order_row = df[df["entity_id"] == "order_001"]
        assert order_row.iloc[0]["entity_type"] == "order"


class TestPath:
    """path: shortest path between two entities."""

    def test_direct_path(self, backend):
        p = backend.path("cust_001", "order_001")
        assert p == ["cust_001", "order_001"]

    def test_path_through_shared(self, backend):
        """Path from cust_001 to cust_002 via shared order_001."""
        p = backend.path("cust_001", "cust_002")
        assert len(p) == 3
        assert p[0] == "cust_001"
        assert p[-1] == "cust_002"
        assert "order_001" in p

    def test_no_path(self, backend):
        p = backend.path("cust_001", "nonexistent")
        assert p == []

    def test_same_node(self, backend):
        p = backend.path("cust_001", "cust_001")
        assert p == ["cust_001"]


class TestNeighbors:
    """neighbors: all directly connected entities."""

    def test_outbound(self, backend):
        df = backend.neighbors("cust_001")
        outbound = df[df["direction"] == "outbound"]
        assert len(outbound) == 1
        assert outbound.iloc[0]["entity_id"] == "order_001"
        assert outbound.iloc[0]["relationship_type"] == "placed_order"

    def test_inbound(self, backend):
        """Order has inbound relationships from both customers."""
        df = backend.neighbors("order_001")
        inbound = df[df["direction"] == "inbound"]
        assert len(inbound) == 2
        ids = set(inbound["entity_id"])
        assert ids == {"cust_001", "cust_002"}

    def test_entity_type(self, backend):
        df = backend.neighbors("cust_001")
        assert df.iloc[0]["entity_type"] == "order"

    def test_nonexistent(self, backend):
        df = backend.neighbors("ghost")
        assert len(df) == 0


class TestRelated:
    """related: target entities via a specific relationship type."""

    def test_returns_target_properties(self, backend):
        df = backend.related("cust_001", "placed_order")
        assert len(df) == 1
        assert df.iloc[0]["entity_id"] == "order_001"
        assert df.iloc[0]["entity_type"] == "order"
        assert df.iloc[0]["amount"] == 225.0

    def test_wrong_type(self, backend):
        df = backend.related("cust_001", "nonexistent")
        assert len(df) == 0

    def test_nonexistent_entity(self, backend):
        df = backend.related("ghost", "placed_order")
        assert len(df) == 0


# ---------------------------------------------------------------------------
# Analytics operations
# ---------------------------------------------------------------------------


class TestConnectedComponents:
    """connected_components: weakly connected subgraphs."""

    def test_single_component(self, backend):
        """All three nodes are connected — one component."""
        df = backend.connected_components()
        assert len(df) == 3
        assert df["component_id"].nunique() == 1

    def test_disconnected(self):
        """Two isolated subgraphs — two components."""
        G = nx.DiGraph()
        G.add_node("a", entity_type="x")
        G.add_node("b", entity_type="x")
        G.add_node("c", entity_type="y")
        G.add_edge("a", "b", relationship_type="knows")
        # c is isolated
        backend = NetworkXGraphBackend(G)
        df = backend.connected_components()
        assert df["component_id"].nunique() == 2


class TestCommunities:
    """communities: community detection on the graph."""

    def test_louvain(self, backend):
        df = backend.communities(algorithm="louvain")
        assert len(df) == 3
        assert "community_id" in df.columns
        assert "entity_id" in df.columns

    def test_label_propagation(self, backend):
        df = backend.communities(algorithm="label_propagation")
        assert len(df) == 3

    def test_invalid_algorithm(self, backend):
        with pytest.raises(ValueError, match="Unknown algorithm"):
            backend.communities(algorithm="bogus")


class TestCentrality:
    """centrality: node importance scores."""

    def test_pagerank(self, backend):
        df = backend.centrality(algorithm="pagerank")
        assert len(df) == 3
        assert "score" in df.columns
        # Scores should sum to ~1.0 for pagerank
        assert abs(df["score"].sum() - 1.0) < 0.01

    def test_degree(self, backend):
        df = backend.centrality(algorithm="degree")
        assert len(df) == 3

    def test_betweenness(self, backend):
        df = backend.centrality(algorithm="betweenness")
        assert len(df) == 3

    def test_sorted_descending(self, backend):
        df = backend.centrality(algorithm="pagerank")
        scores = df["score"].tolist()
        assert scores == sorted(scores, reverse=True)

    def test_invalid_algorithm(self, backend):
        with pytest.raises(ValueError, match="Unknown algorithm"):
            backend.centrality(algorithm="bogus")


class TestSimilar:
    """similar: find structurally similar entities."""

    def test_similar_customers(self, backend):
        """cust_001 and cust_002 share a neighbor (order_001)."""
        df = backend.similar("cust_001", top_n=5)
        if not df.empty:
            # cust_002 should be the most similar (shared neighbor)
            assert df.iloc[0]["entity_id"] == "cust_002"
            assert df.iloc[0]["score"] > 0

    def test_nonexistent(self, backend):
        df = backend.similar("ghost")
        assert len(df) == 0

    def test_top_n_limits(self):
        """top_n limits the result count."""
        G = nx.DiGraph()
        G.add_node("hub", entity_type="x")
        for i in range(20):
            G.add_node(f"n{i}", entity_type="x")
            G.add_edge("hub", f"n{i}", relationship_type="link")
        backend = NetworkXGraphBackend(G)
        df = backend.similar("n0", top_n=5)
        assert len(df) <= 5


# ---------------------------------------------------------------------------
# Connection property
# ---------------------------------------------------------------------------


class TestConnection:
    """connection: raw DiGraph access."""

    def test_returns_graph(self, backend, graph):
        assert backend.connection is graph

    def test_is_digraph(self, backend):
        assert isinstance(backend.connection, nx.DiGraph)
