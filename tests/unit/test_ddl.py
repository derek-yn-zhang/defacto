"""Tests for DDLGenerator — centralized DDL generation for state history tables."""

import pytest

from defacto._ddl import DDLGenerator, TYPE_MAPS


CUSTOMER_DEF = {
    "entities": {
        "customer": {
            "properties": {
                "name": {"type": "string"},
                "mrr": {"type": "float"},
                "signup_count": {"type": "int"},
                "is_active": {"type": "bool"},
                "last_seen": {"type": "datetime"},
                "tags": {"type": "array"},
            },
        },
    },
}


class TestDDLGenerateSqlite:
    """SQLite dialect DDL generation."""

    def test_basic_table(self):
        ddl = DDLGenerator()
        stmts = ddl.generate(CUSTOMER_DEF, "v1", "sqlite")
        # Should produce CREATE TABLE + 3 indexes (current, valid_from, merged)
        assert len(stmts) == 4
        assert "CREATE TABLE IF NOT EXISTS customer_history" in stmts[0]
        assert "PRIMARY KEY (customer_id, valid_from)" in stmts[0]

    def test_all_sqlite_types(self):
        ddl = DDLGenerator()
        stmts = ddl.generate(CUSTOMER_DEF, "v1", "sqlite")
        table_stmt = stmts[0]
        assert "name TEXT" in table_stmt
        assert "mrr REAL" in table_stmt
        assert "signup_count INTEGER" in table_stmt
        assert "is_active INTEGER" in table_stmt  # bool → INTEGER in SQLite
        assert "last_seen TEXT" in table_stmt      # datetime → TEXT in SQLite
        assert "tags TEXT" in table_stmt            # array → TEXT in SQLite

    def test_system_columns(self):
        ddl = DDLGenerator()
        stmts = ddl.generate(CUSTOMER_DEF, "v1", "sqlite")
        table_stmt = stmts[0]
        assert "customer_id TEXT NOT NULL" in table_stmt
        assert "customer_state TEXT NOT NULL" in table_stmt
        assert "valid_from TEXT NOT NULL" in table_stmt
        assert "valid_to TEXT" in table_stmt
        assert "merged_into TEXT" in table_stmt

    def test_partial_index(self):
        ddl = DDLGenerator()
        stmts = ddl.generate(CUSTOMER_DEF, "v1", "sqlite")
        idx_stmt = stmts[1]
        assert "CREATE INDEX IF NOT EXISTS idx_customer_current" in idx_stmt
        assert "WHERE valid_to IS NULL" in idx_stmt

    def test_no_schema_for_sqlite(self):
        ddl = DDLGenerator()
        stmts = ddl.generate(CUSTOMER_DEF, "v1", "sqlite")
        # SQLite doesn't support schemas — no CREATE SCHEMA statement
        assert not any("CREATE SCHEMA" in s for s in stmts)

    def test_multiple_entity_types(self):
        ddl = DDLGenerator()
        defs = {
            "entities": {
                "customer": {"properties": {"name": {"type": "string"}}},
                "account": {"properties": {"plan": {"type": "string"}}},
            },
        }
        stmts = ddl.generate(defs, "v1", "sqlite")
        # 2 tables × (CREATE TABLE + 3 indexes) = 8 statements
        assert len(stmts) == 8
        assert any("customer_history" in s for s in stmts)
        assert any("account_history" in s for s in stmts)

    def test_empty_properties(self):
        ddl = DDLGenerator()
        defs = {"entities": {"customer": {"properties": {}}}}
        stmts = ddl.generate(defs, "v1", "sqlite")
        # Still gets system columns
        assert "customer_id TEXT NOT NULL" in stmts[0]
        assert "valid_from TEXT NOT NULL" in stmts[0]


class TestDDLGeneratePostgres:
    """Postgres dialect DDL generation."""

    def test_creates_schema(self):
        ddl = DDLGenerator()
        stmts = ddl.generate(CUSTOMER_DEF, "v1", "postgres")
        assert stmts[0] == "CREATE SCHEMA IF NOT EXISTS defacto_v1"

    def test_schema_qualified_table(self):
        ddl = DDLGenerator()
        stmts = ddl.generate(CUSTOMER_DEF, "v1", "postgres")
        table_stmt = stmts[1]
        assert "defacto_v1.customer_history" in table_stmt

    def test_postgres_types(self):
        ddl = DDLGenerator()
        stmts = ddl.generate(CUSTOMER_DEF, "v1", "postgres")
        table_stmt = stmts[1]
        assert "mrr DOUBLE PRECISION" in table_stmt
        assert "signup_count BIGINT" in table_stmt
        assert "is_active BOOLEAN" in table_stmt
        assert "last_seen TIMESTAMPTZ" in table_stmt
        assert "tags JSONB" in table_stmt


class TestDDLGenerateEdgeCases:
    """Edge cases and error handling."""

    def test_unknown_backend_raises(self):
        ddl = DDLGenerator()
        with pytest.raises(ValueError, match="Unsupported backend"):
            ddl.generate(CUSTOMER_DEF, "v1", "bigquery")

    def test_unknown_type_defaults_to_text(self):
        ddl = DDLGenerator()
        defs = {"entities": {"thing": {"properties": {"data": {"type": "custom_type"}}}}}
        stmts = ddl.generate(defs, "v1", "sqlite")
        assert "data TEXT" in stmts[0]

    def test_diff_raises(self):
        ddl = DDLGenerator()
        with pytest.raises(NotImplementedError):
            ddl.diff({}, {})
