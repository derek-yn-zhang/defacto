"""Tests for the query layer — DefactoQuery, DefactoTable, TableCollection.

Tests use file-based SQLite (not :memory:) so the Ibis connection can
read what the state history backend writes. pytest's tmp_path fixture
handles cleanup automatically.
"""

import os

import pytest

from defacto._ddl import DDLGenerator, resolve_table_name, resolve_rel_table_name
from defacto.backends._state_history import SqliteStateHistory
from defacto.query import DefactoQuery, DefactoTable, TableCollection


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

ENTITY_DEFS = {
    "customer": {
        "properties": {
            "name": {"type": "string"},
            "mrr": {"type": "float"},
        },
        "relationships": [
            {
                "type": "placed_order",
                "target": "order",
                "cardinality": "has_many",
                "properties": {"total": {"type": "float"}},
            },
        ],
    },
    "order": {
        "properties": {
            "amount": {"type": "float"},
            "status": {"type": "string"},
        },
        "relationships": [
            {
                "type": "placed_by",
                "target": "customer",
                "cardinality": "belongs_to",
            },
        ],
    },
}

SNAPSHOTS = [
    {
        "entity_id": "cust_001",
        "entity_type": "customer",
        "state": "active",
        "properties": {"name": "Alice", "mrr": 99.0},
        "valid_from": "2026-01-15T10:00:00Z",
        "relationships": [
            {
                "relationship_type": "placed_order",
                "target_entity_id": "order_001",
                "properties": {"total": 150.0},
            },
        ],
    },
    {
        "entity_id": "cust_002",
        "entity_type": "customer",
        "state": "lead",
        "properties": {"name": "Bob", "mrr": 0.0},
        "valid_from": "2026-01-16T10:00:00Z",
        "relationships": [],
    },
    {
        "entity_id": "order_001",
        "entity_type": "order",
        "state": "completed",
        "properties": {"amount": 150.0, "status": "shipped"},
        "valid_from": "2026-01-15T10:00:00Z",
        "relationships": [],
    },
]

TOMBSTONES = [
    {
        "entity_id": "cust_002",
        "entity_type": "customer",
        "merged_into": "cust_001",
        "timestamp": "2026-02-01T10:00:00Z",
    },
]

# Second snapshot for cust_001 — upgrade from active to premium
UPGRADE_SNAPSHOT = {
    "entity_id": "cust_001",
    "entity_type": "customer",
    "state": "premium",
    "properties": {"name": "Alice", "mrr": 199.0},
    "valid_from": "2026-03-01T10:00:00Z",
    "relationships": [],
}


@pytest.fixture
def populated_db(tmp_path):
    """Create a SQLite state history with test data, return (db_path, defs)."""
    db_path = str(tmp_path / "test.db")
    sh = SqliteStateHistory(db_path)
    sh.ensure_tables(ENTITY_DEFS)
    sh.write_batch(SNAPSHOTS, [])
    sh.close()
    return db_path, {"entities": ENTITY_DEFS}


@pytest.fixture
def query(populated_db):
    """DefactoQuery connected to the populated test database."""
    db_path, defs = populated_db
    q = DefactoQuery(db_path, defs, "")
    yield q
    q.close()


# ---------------------------------------------------------------------------
# DDL — table name resolution
# ---------------------------------------------------------------------------


class TestTableNameResolution:
    """Test centralized table name functions."""

    def test_sqlite_entity_table(self):
        assert resolve_table_name("customer", "", "sqlite") == "customer_history"

    def test_postgres_entity_table(self):
        assert resolve_table_name("customer", "v1", "postgres") == "defacto_v1.customer_history"

    def test_duckdb_entity_table(self):
        assert resolve_table_name("customer", "v2", "duckdb") == "defacto_v2.customer_history"

    def test_sqlite_rel_table(self):
        assert resolve_rel_table_name("placed_order", "", "sqlite") == "rel_placed_order_history"

    def test_postgres_rel_table(self):
        assert resolve_rel_table_name("placed_order", "v1", "postgres") == "defacto_v1.rel_placed_order_history"


# ---------------------------------------------------------------------------
# DDL — relationship table generation
# ---------------------------------------------------------------------------


class TestRelationshipDDL:
    """Test that DDLGenerator creates relationship history tables."""

    def test_generates_rel_tables(self):
        ddl = DDLGenerator()
        stmts = ddl.generate({"entities": ENTITY_DEFS}, version="", backend="sqlite")
        rel_tables = [s for s in stmts if "rel_placed_order_history" in s]
        assert len(rel_tables) >= 1  # CREATE TABLE + indexes

    def test_entity_table_has_typed_columns(self):
        """Entity tables use {entity_type}_id and {entity_type}_state."""
        ddl = DDLGenerator()
        stmts = ddl.generate({"entities": ENTITY_DEFS}, version="", backend="sqlite")
        create = next(s for s in stmts if "CREATE TABLE" in s and "customer_history" in s)
        assert "customer_id TEXT NOT NULL" in create
        assert "customer_state TEXT NOT NULL" in create
        assert "entity_id" not in create

    def test_rel_table_has_system_columns(self):
        ddl = DDLGenerator()
        stmts = ddl.generate({"entities": ENTITY_DEFS}, version="", backend="sqlite")
        create = next(s for s in stmts if "CREATE TABLE" in s and "rel_placed_order" in s)
        assert "source_id TEXT NOT NULL" in create
        assert "target_id TEXT NOT NULL" in create
        assert "relationship_type TEXT NOT NULL" in create
        assert "valid_from" in create
        assert "valid_to" in create

    def test_rel_table_has_property_columns(self):
        ddl = DDLGenerator()
        stmts = ddl.generate({"entities": ENTITY_DEFS}, version="", backend="sqlite")
        create = next(s for s in stmts if "CREATE TABLE" in s and "rel_placed_order" in s)
        assert "total REAL" in create

    def test_rel_table_deduplication(self):
        """Both customer and order declare relationships — only one table each."""
        ddl = DDLGenerator()
        stmts = ddl.generate({"entities": ENTITY_DEFS}, version="", backend="sqlite")
        placed_order_creates = [
            s for s in stmts
            if "CREATE TABLE" in s and "rel_placed_order" in s
        ]
        placed_by_creates = [
            s for s in stmts
            if "CREATE TABLE" in s and "rel_placed_by" in s
        ]
        assert len(placed_order_creates) == 1
        assert len(placed_by_creates) == 1

    def test_rel_table_indexes(self):
        ddl = DDLGenerator()
        stmts = ddl.generate({"entities": ENTITY_DEFS}, version="", backend="sqlite")
        idx_stmts = [s for s in stmts if "idx_rel_placed_order" in s]
        assert len(idx_stmts) == 2  # source + target indexes

    def test_postgres_rel_table_in_schema(self):
        ddl = DDLGenerator()
        stmts = ddl.generate({"entities": ENTITY_DEFS}, version="v1", backend="postgres")
        create = next(s for s in stmts if "CREATE TABLE" in s and "rel_placed_order" in s)
        assert "defacto_v1.rel_placed_order_history" in create

    def test_no_rel_tables_without_relationships(self):
        """Entity defs without relationships produce no relationship tables."""
        defs = {
            "widget": {"properties": {"size": {"type": "int"}}},
        }
        ddl = DDLGenerator()
        stmts = ddl.generate({"entities": defs}, version="", backend="sqlite")
        rel_stmts = [s for s in stmts if "rel_" in s]
        assert len(rel_stmts) == 0


# ---------------------------------------------------------------------------
# State history — relationship writes
# ---------------------------------------------------------------------------


class TestRelationshipWrites:
    """Test that write_batch persists relationships."""

    def test_sqlite_writes_relationships(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        sh = SqliteStateHistory(db_path)
        sh.ensure_tables(ENTITY_DEFS)
        sh.write_batch(SNAPSHOTS, [])

        import sqlite3
        conn = sqlite3.connect(db_path)
        # Verify entity table uses typed column names
        entity_rows = conn.execute(
            "SELECT customer_id, customer_state FROM customer_history"
        ).fetchall()
        assert len(entity_rows) == 2

        # Verify relationship table
        rows = conn.execute(
            "SELECT source_id, target_id, total FROM rel_placed_order_history"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0] == ("cust_001", "order_001", 150.0)
        conn.close()
        sh.close()

    def test_sqlite_relationship_scd(self, tmp_path):
        """Writing a new version closes the previous one."""
        db_path = str(tmp_path / "test.db")
        sh = SqliteStateHistory(db_path)
        sh.ensure_tables(ENTITY_DEFS)
        sh.write_batch(SNAPSHOTS, [])

        # Update the relationship
        update = [{
            "entity_id": "cust_001",
            "entity_type": "customer",
            "state": "active",
            "properties": {"name": "Alice", "mrr": 99.0},
            "valid_from": "2026-02-01T10:00:00Z",
            "relationships": [{
                "relationship_type": "placed_order",
                "target_entity_id": "order_001",
                "properties": {"total": 200.0},
            }],
        }]
        sh.write_batch(update, [])

        import sqlite3
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            "SELECT source_id, total, valid_from, valid_to"
            " FROM rel_placed_order_history ORDER BY valid_from"
        ).fetchall()
        assert len(rows) == 2
        # First version closed
        assert rows[0][3] is not None  # valid_to set
        # Second version open
        assert rows[1][1] == 200.0
        assert rows[1][3] is None  # valid_to NULL
        conn.close()
        sh.close()


# ---------------------------------------------------------------------------
# DefactoQuery — core queries
# ---------------------------------------------------------------------------


class TestDefactoQuery:
    """Test DefactoQuery table/history/tables operations."""

    def test_table_returns_defacto_table(self, query):
        result = query.table("customer")
        assert isinstance(result, DefactoTable)

    def test_table_current_state(self, query):
        df = query.table("customer").execute()
        assert "customer_id" in df.columns
        assert "customer_state" in df.columns
        assert "name" in df.columns
        assert "mrr" in df.columns
        # Current state only — valid_to IS NULL
        assert len(df) == 2  # cust_001 and cust_002

    def test_table_filters_to_current(self, populated_db):
        """After update, table() returns only the latest version."""
        db_path, defs = populated_db
        sh = SqliteStateHistory(db_path)
        sh.ensure_tables(ENTITY_DEFS)
        sh.write_batch([UPGRADE_SNAPSHOT], [])
        sh.close()

        q = DefactoQuery(db_path, defs, "")
        df = q.table("customer").execute()
        alice = df[df["customer_id"] == "cust_001"]
        assert len(alice) == 1
        assert alice.iloc[0]["customer_state"] == "premium"
        assert alice.iloc[0]["mrr"] == 199.0
        q.close()

    def test_history_entity_type(self, populated_db):
        """history('customer') returns all versions."""
        db_path, defs = populated_db
        sh = SqliteStateHistory(db_path)
        sh.ensure_tables(ENTITY_DEFS)
        sh.write_batch([UPGRADE_SNAPSHOT], [])
        sh.close()

        q = DefactoQuery(db_path, defs, "")
        df = q.history("customer").execute()
        assert "valid_from" in df.columns
        assert "valid_to" in df.columns
        # cust_001 has 2 versions, cust_002 has 1
        assert len(df) == 3
        q.close()

    def test_history_filter_by_id(self, populated_db):
        """Filter history by typed ID column."""
        db_path, defs = populated_db
        sh = SqliteStateHistory(db_path)
        sh.ensure_tables(ENTITY_DEFS)
        sh.write_batch([UPGRADE_SNAPSHOT], [])
        sh.close()

        q = DefactoQuery(db_path, defs, "")
        t = q.history("customer")
        df = t.filter(t._expr.customer_id == "cust_001").execute()
        assert len(df) == 2  # original + upgrade
        q.close()

    def test_tables_all(self, query):
        """tables() with no args returns all entity types."""
        coll = query.tables()
        assert isinstance(coll, TableCollection)
        assert len(coll) == 2
        assert "customer" in coll
        assert "order" in coll

    def test_tables_specific(self, query):
        """tables('customer') returns only requested types."""
        coll = query.tables("customer")
        assert len(coll) == 1
        assert "customer" in coll

    def test_tables_list_arg(self, query):
        """tables(['customer', 'order']) works with a list."""
        coll = query.tables(["customer", "order"])
        assert len(coll) == 2

    def test_version_scoping(self, query):
        """version() returns a new DefactoQuery with different version."""
        v2 = query.version("v2")
        assert isinstance(v2, DefactoQuery)
        assert v2._version == "v2"
        assert v2 is not query

    def test_standalone_connect(self, populated_db):
        """DefactoQuery.connect() works as a standalone entry point."""
        db_path, defs = populated_db
        q = DefactoQuery.connect(db_path, defs, "")
        df = q.table("customer").execute()
        assert "customer_id" in df.columns
        assert len(df) == 2
        q.close()


# ---------------------------------------------------------------------------
# DefactoTable — chaining and temporal queries
# ---------------------------------------------------------------------------


class TestDefactoTable:
    """Test DefactoTable Ibis proxy and temporal methods."""

    def test_filter_chains(self, query):
        """Ibis filter passes through and returns DefactoTable."""
        result = query.table("customer").filter(
            query.table("customer")._expr.customer_state == "active"
        )
        assert isinstance(result, DefactoTable)
        df = result.execute()
        assert len(df) == 1
        assert df.iloc[0]["customer_id"] == "cust_001"

    def test_select_chains(self, query):
        """Ibis select passes through."""
        df = query.table("customer").select("customer_id", "customer_state").execute()
        assert list(df.columns) == ["customer_id", "customer_state"]

    def test_as_of(self, populated_db):
        """as_of returns entity state at a specific point in time."""
        db_path, defs = populated_db
        sh = SqliteStateHistory(db_path)
        sh.ensure_tables(ENTITY_DEFS)
        sh.write_batch([UPGRADE_SNAPSHOT], [])
        sh.close()

        q = DefactoQuery(db_path, defs, "")
        # Before upgrade — should see active state
        df = q.history("customer").as_of("2026-02-01T00:00:00Z").execute()
        alice = df[df["customer_id"] == "cust_001"]
        assert len(alice) == 1
        assert alice.iloc[0]["customer_state"] == "active"

        # After upgrade — should see premium state
        df = q.history("customer").as_of("2026-04-01T00:00:00Z").execute()
        alice = df[df["customer_id"] == "cust_001"]
        assert len(alice) == 1
        assert alice.iloc[0]["customer_state"] == "premium"
        q.close()

    def test_to_pandas(self, query):
        """to_pandas is an alias for execute."""
        df = query.table("customer").to_pandas()
        assert len(df) == 2

    def test_sql_inspection(self, query):
        """sql() returns the generated SQL without executing."""
        sql = query.table("customer").sql()
        assert isinstance(sql, str)
        assert "customer_history" in sql

    def test_repr(self, query):
        table = query.table("customer")
        assert "customer" in repr(table)

    def test_validation_catches_bad_column(self, query):
        """Validation gives clear error for unknown columns in SQL."""
        from defacto.errors import QueryError
        with pytest.raises(QueryError, match="Unknown column"):
            query.query(
                "SELECT nme FROM customer_history WHERE valid_to IS NULL"
            )

    def test_validation_catches_bad_table(self, query):
        """Validation gives clear error for unknown table names."""
        from defacto.errors import QueryError
        with pytest.raises(QueryError, match="Unknown table"):
            query.query("SELECT * FROM custmer_history").execute()

    def test_raw_query(self, query):
        """Raw SQL query works and returns chainable DefactoTable."""
        result = query.query(
            "SELECT customer_id, name FROM customer_history WHERE valid_to IS NULL"
        )
        assert isinstance(result, DefactoTable)
        df = result.execute()
        assert len(df) == 2
        assert "customer_id" in df.columns

    def test_validation_passes_good_query(self, query):
        """Valid queries pass validation without error."""
        df = query.table("customer").select("customer_id", "name").execute()
        assert len(df) == 2


# ---------------------------------------------------------------------------
# TableCollection — iteration and exports
# ---------------------------------------------------------------------------


class TestTableCollection:
    """Test TableCollection operations."""

    def test_iteration(self, query):
        coll = query.tables()
        types = [et for et, _ in coll]
        assert "customer" in types
        assert "order" in types

    def test_getitem(self, query):
        coll = query.tables()
        customer = coll["customer"]
        assert isinstance(customer, DefactoTable)

    def test_to_parquet(self, query, tmp_path):
        out = str(tmp_path / "export")
        query.tables().to_parquet(out)
        assert os.path.exists(os.path.join(out, "customer.parquet"))
        assert os.path.exists(os.path.join(out, "order.parquet"))

    def test_to_csv(self, query, tmp_path):
        out = str(tmp_path / "export")
        query.tables().to_csv(out)
        assert os.path.exists(os.path.join(out, "customer.csv"))
        assert os.path.exists(os.path.join(out, "order.csv"))

    def test_to_pandas_dict(self, query):
        result = query.tables().to_pandas()
        assert isinstance(result, dict)
        assert "customer" in result
        assert len(result["customer"]) == 2

    def test_history_mode(self, populated_db):
        db_path, defs = populated_db
        sh = SqliteStateHistory(db_path)
        sh.ensure_tables(ENTITY_DEFS)
        sh.write_batch([UPGRADE_SNAPSHOT], [])
        sh.close()

        q = DefactoQuery(db_path, defs, "")
        coll = q.tables("customer").history()
        df = coll["customer"].execute()
        assert len(df) == 3  # 2 versions of cust_001 + 1 of cust_002
        q.close()

    def test_relationships_excluded(self, query, tmp_path):
        """relationships=False excludes relationship tables."""
        out = str(tmp_path / "no_rels")
        query.tables(relationships=False).to_parquet(out)
        assert os.path.exists(os.path.join(out, "customer.parquet"))
        assert not os.path.exists(os.path.join(out, "rel_placed_order.parquet"))

    def test_relationships_in_parquet(self, query, tmp_path):
        """Relationships included by default in exports."""
        out = str(tmp_path / "export")
        query.tables().to_parquet(out)
        assert os.path.exists(os.path.join(out, "customer.parquet"))
        assert os.path.exists(os.path.join(out, "rel_placed_order.parquet"))

    def test_to_graph_json(self, query):
        result = query.tables().to_graph_json()
        assert "nodes" in result
        assert "edges" in result
        assert len(result["nodes"]) == 3  # 2 customers + 1 order
        assert len(result["edges"]) >= 1  # placed_order relationship

    def test_to_duckdb(self, query, tmp_path):
        """Export all tables into a DuckDB file."""
        out = str(tmp_path / "export.db")
        query.tables().to_duckdb(out)
        import duckdb
        conn = duckdb.connect(out)
        tables = [r[0] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()]
        assert "customer" in tables
        assert "order" in tables
        conn.close()

    def test_to_duckdb_with_relationships(self, query, tmp_path):
        """DuckDB export includes relationship tables when requested."""
        out = str(tmp_path / "export.db")
        query.tables().to_duckdb(out)
        import duckdb
        conn = duckdb.connect(out)
        tables = [r[0] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()]
        assert "rel_placed_order" in tables
        conn.close()

    def test_snapshot(self, query, tmp_path):
        """Snapshot writes flat tables (no SCD columns) to target."""
        out = str(tmp_path / "snapshot.db")
        query.tables("customer").snapshot(f"duckdb:///{out}")
        import duckdb
        conn = duckdb.connect(out)
        cols = [r[0] for r in conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'customer'"
        ).fetchall()]
        assert "customer_id" in cols
        assert "customer_state" in cols
        assert "valid_from" not in cols  # SCD columns dropped
        assert "valid_to" not in cols
        assert "merged_into" not in cols
        rows = conn.execute("SELECT * FROM customer").fetchall()
        assert len(rows) == 2
        conn.close()

    def test_to_networkx(self, query):
        """Export as NetworkX graph — entities as nodes, relationships as edges."""
        nx = pytest.importorskip("networkx")
        G = query.tables().to_networkx()
        assert isinstance(G, nx.DiGraph)
        assert len(G.nodes) == 3  # 2 customers + 1 order
        assert len(G.edges) >= 1


# ---------------------------------------------------------------------------
# Graph operations — CTE backend
# ---------------------------------------------------------------------------


class TestCteGraphBackend:
    """Test graph operations via recursive CTEs on SQLite."""

    @pytest.fixture
    def graph_db(self, tmp_path):
        """Create a SQLite DB with entities and relationships for graph tests."""
        db_path = str(tmp_path / "graph.db")
        sh = SqliteStateHistory(db_path)
        sh.ensure_tables(ENTITY_DEFS)

        # Create a small graph: cust_001 → order_001, cust_002 → order_001
        sh.write_batch([
            {
                "entity_id": "cust_001", "entity_type": "customer",
                "state": "active", "properties": {"name": "Alice", "mrr": 99.0},
                "valid_from": "2026-01-01T00:00:00Z",
                "relationships": [{
                    "relationship_type": "placed_order",
                    "target_entity_id": "order_001",
                    "properties": {"total": 150.0},
                }],
            },
            {
                "entity_id": "cust_002", "entity_type": "customer",
                "state": "active", "properties": {"name": "Bob", "mrr": 50.0},
                "valid_from": "2026-01-01T00:00:00Z",
                "relationships": [{
                    "relationship_type": "placed_order",
                    "target_entity_id": "order_001",
                    "properties": {"total": 75.0},
                }],
            },
            {
                "entity_id": "order_001", "entity_type": "order",
                "state": "completed", "properties": {"amount": 225.0, "status": "shipped"},
                "valid_from": "2026-01-01T00:00:00Z",
                "relationships": [],
            },
        ], [])
        conn = sh._conn
        sh_ref = sh  # keep reference so connection stays open
        yield db_path, conn, sh_ref
        sh_ref.close()

    def _backend(self, conn):
        from defacto.query._graph import CteGraphBackend
        return CteGraphBackend(
            conn, "", "sqlite", definitions={"entities": ENTITY_DEFS},
        )

    def test_traverse_directed(self, graph_db):
        db_path, conn, _ = graph_db
        backend = self._backend(conn)
        df = backend.traverse("cust_001", "placed_order", depth=1)
        assert len(df) >= 1
        ids = set(df["entity_id"])
        assert "cust_001" in ids  # starting entity
        assert "order_001" in ids  # reachable via placed_order

    def test_traverse_depth_zero(self, graph_db):
        """Depth 0 returns only the starting entity."""
        _, conn, _ = graph_db
        backend = self._backend(conn)
        df = backend.traverse("cust_001", "placed_order", depth=0)
        assert len(df) == 1
        assert df.iloc[0]["entity_id"] == "cust_001"

    def test_neighbors(self, graph_db):
        _, conn, _ = graph_db
        backend = self._backend(conn)
        df = backend.neighbors("cust_001")
        assert len(df) >= 1
        assert "order_001" in set(df["entity_id"])
        assert "outbound" in set(df["direction"])

    def test_neighbors_inbound(self, graph_db):
        """Order has inbound relationships from both customers."""
        _, conn, _ = graph_db
        backend = self._backend(conn)
        df = backend.neighbors("order_001")
        inbound = df[df["direction"] == "inbound"]
        assert len(inbound) == 2  # cust_001 and cust_002

    def test_path_exists(self, graph_db):
        """Path from cust_001 to order_001 via placed_order."""
        _, conn, _ = graph_db
        backend = self._backend(conn)
        p = backend.path("cust_001", "order_001")
        assert p == ["cust_001", "order_001"]

    def test_path_through_shared(self, graph_db):
        """Path from cust_001 to cust_002 via shared order_001."""
        _, conn, _ = graph_db
        backend = self._backend(conn)
        p = backend.path("cust_001", "cust_002")
        assert len(p) == 3
        assert p[0] == "cust_001"
        assert p[-1] == "cust_002"
        assert "order_001" in p

    def test_path_no_connection(self, graph_db):
        """No path returns empty list."""
        _, conn, _ = graph_db
        backend = self._backend(conn)
        p = backend.path("cust_001", "nonexistent")
        assert p == []

    def test_related(self, graph_db):
        """Related returns target entity rows with their properties."""
        _, conn, _ = graph_db
        backend = self._backend(conn)
        df = backend.related("cust_001", "placed_order")
        assert len(df) == 1
        # Returns the target entity's columns from order_history
        assert df.iloc[0]["order_id"] == "order_001"
        assert df.iloc[0]["order_state"] == "completed"
        assert df.iloc[0]["amount"] == 225.0

    def test_to_defacto_table(self, graph_db):
        """Graph results can be wrapped in DefactoTable for chaining."""
        db_path, conn, _ = graph_db
        backend = self._backend(conn)
        q = DefactoQuery(db_path, {"entities": ENTITY_DEFS}, "")
        df = backend.traverse("cust_001", "placed_order", depth=1)
        table = backend._to_defacto_table(df, q)
        assert isinstance(table, DefactoTable)
        result = table.execute()
        assert len(result) >= 1
        q.close()
