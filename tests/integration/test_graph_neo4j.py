"""Neo4jGraphBackend integration tests.

Requires Neo4j running: docker compose up -d neo4j
Tests auto-skip via pytest.mark.neo4j if Neo4j isn't available.

Tests the 4 core operations + query + connection. Analytics operations
raise NotImplementedError (require GDS plugin not in community edition).
"""

import pytest

from defacto.query._graph import Neo4jGraphBackend

pytestmark = pytest.mark.neo4j

NEO4J_URL = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "defacto_test")


@pytest.fixture
def neo4j_graph():
    """Populate Neo4j with test data, yield backend, cleanup after."""
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(NEO4J_URL, auth=NEO4J_AUTH)

    # Clean slate
    driver.execute_query("MATCH (n) DETACH DELETE n")

    # Create test graph: cust_001 -> order_001, cust_002 -> order_001
    driver.execute_query(
        "CREATE (c1:Customer {_defacto_id: 'cust_001', name: 'Alice', mrr: 99.0})"
        " CREATE (c2:Customer {_defacto_id: 'cust_002', name: 'Bob', mrr: 50.0})"
        " CREATE (o1:Order {_defacto_id: 'order_001', amount: 225.0, status: 'shipped'})"
        " CREATE (c1)-[:PLACED_ORDER]->(o1)"
        " CREATE (c2)-[:PLACED_ORDER]->(o1)"
    )

    backend = Neo4jGraphBackend(NEO4J_URL, auth=NEO4J_AUTH)
    yield backend
    backend.close()

    # Cleanup
    driver.execute_query("MATCH (n) DETACH DELETE n")
    driver.close()


# ---------------------------------------------------------------------------
# Core operations
# ---------------------------------------------------------------------------


class TestTraverse:
    """traverse: Cypher variable-length path traversal."""

    def test_directed(self, neo4j_graph):
        df = neo4j_graph.traverse("cust_001", "placed_order", depth=1)
        ids = set(df["entity_id"])
        assert "cust_001" in ids  # starting entity
        assert "order_001" in ids  # reachable via placed_order

    def test_depth_zero(self, neo4j_graph):
        """Depth 0 returns only the starting entity."""
        df = neo4j_graph.traverse("cust_001", "placed_order", depth=0)
        assert len(df) == 1
        assert df.iloc[0]["entity_id"] == "cust_001"

    def test_undirected(self, neo4j_graph):
        """Undirected traversal can go backwards along edges."""
        df = neo4j_graph.traverse(
            "order_001", "placed_order", depth=1, directed=False,
        )
        ids = set(df["entity_id"])
        assert "order_001" in ids
        # Should find at least one customer via reverse edge
        assert "cust_001" in ids or "cust_002" in ids


class TestPath:
    """path: Cypher shortestPath."""

    def test_direct_path(self, neo4j_graph):
        p = neo4j_graph.path("cust_001", "order_001")
        assert p == ["cust_001", "order_001"]

    def test_path_through_shared(self, neo4j_graph):
        """Path from cust_001 to cust_002 via shared order_001."""
        p = neo4j_graph.path("cust_001", "cust_002")
        assert len(p) == 3
        assert p[0] == "cust_001"
        assert p[-1] == "cust_002"
        assert "order_001" in p

    def test_no_path(self, neo4j_graph):
        p = neo4j_graph.path("cust_001", "nonexistent")
        assert p == []


class TestNeighbors:
    """neighbors: Cypher direct connections."""

    def test_outbound(self, neo4j_graph):
        df = neo4j_graph.neighbors("cust_001")
        outbound = df[df["direction"] == "outbound"]
        assert len(outbound) == 1
        assert outbound.iloc[0]["entity_id"] == "order_001"

    def test_inbound(self, neo4j_graph):
        """Order has inbound from both customers."""
        df = neo4j_graph.neighbors("order_001")
        inbound = df[df["direction"] == "inbound"]
        assert len(inbound) == 2
        ids = set(inbound["entity_id"])
        assert ids == {"cust_001", "cust_002"}


class TestRelated:
    """related: Cypher typed relationship MATCH."""

    def test_returns_target_properties(self, neo4j_graph):
        df = neo4j_graph.related("cust_001", "placed_order")
        assert len(df) == 1
        assert df.iloc[0]["entity_id"] == "order_001"
        assert df.iloc[0]["amount"] == 225.0


# ---------------------------------------------------------------------------
# Query + connection
# ---------------------------------------------------------------------------


class TestQuery:
    """query: raw Cypher execution."""

    def test_raw_cypher(self, neo4j_graph):
        df = neo4j_graph.query(
            "MATCH (n {_defacto_id: $id}) RETURN n._defacto_id AS id",
            id="cust_001",
        )
        assert len(df) == 1
        assert df.iloc[0]["id"] == "cust_001"


class TestConnection:
    """connection: raw Neo4j driver."""

    def test_returns_driver(self, neo4j_graph):
        driver = neo4j_graph.connection
        assert driver is not None
        # Verify it's a working driver
        driver.verify_connectivity()


# ---------------------------------------------------------------------------
# Analytics (GDS required — verify they raise)
# ---------------------------------------------------------------------------


class TestAnalyticsNotAvailable:
    """Analytics operations raise NotImplementedError without GDS."""

    def test_connected_components(self, neo4j_graph):
        with pytest.raises(NotImplementedError, match="GDS"):
            neo4j_graph.connected_components()

    def test_communities(self, neo4j_graph):
        with pytest.raises(NotImplementedError, match="GDS"):
            neo4j_graph.communities()

    def test_centrality(self, neo4j_graph):
        with pytest.raises(NotImplementedError, match="GDS"):
            neo4j_graph.centrality()

    def test_similar(self, neo4j_graph):
        with pytest.raises(NotImplementedError, match="GDS"):
            neo4j_graph.similar("cust_001")
