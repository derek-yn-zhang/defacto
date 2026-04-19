"""DDL generation from entity definitions.

Generates SQL DDL (CREATE TABLE, ALTER TABLE) for SCD Type 2 state
history tables and relationship history tables. Backend-specific SQL
dialects (Postgres, SQLite, DuckDB).

This is the single source of truth for:
- Property type → SQL type mappings (TYPE_MAPS)
- Table name resolution (resolve_table_name, resolve_rel_table_name)

All state history backends and the query layer use these instead of
maintaining their own.

Postgres and DuckDB use per-version schemas: defacto_v1.customer_history.
SQLite uses flat table names: customer_history (no schema support).
"""

from __future__ import annotations

from typing import Any


# Defacto property types → SQL column types per backend dialect.
# This is the authoritative mapping — no other module should define its own.
TYPE_MAPS: dict[str, dict[str, str]] = {
    "sqlite": {
        "string": "TEXT",
        "int": "INTEGER",
        "float": "REAL",
        "bool": "INTEGER",
        "datetime": "TEXT",
        "array": "TEXT",
    },
    "postgres": {
        "string": "TEXT",
        "int": "BIGINT",
        "float": "DOUBLE PRECISION",
        "bool": "BOOLEAN",
        "datetime": "TIMESTAMPTZ",
        "array": "JSONB",
    },
    "duckdb": {
        "string": "VARCHAR",
        "int": "BIGINT",
        "float": "DOUBLE",
        "bool": "BOOLEAN",
        "datetime": "TIMESTAMP",
        "array": "JSON",
    },
}


def resolve_table_name(
    entity_type: str, version: str, backend: str, namespace: str = "defacto",
) -> str:
    """Build the fully qualified table name for an entity history table.

    Args:
        entity_type: Entity type name (e.g., 'customer').
        version: Definition version (e.g., 'v1'). Ignored for SQLite.
        backend: Backend type ('postgres', 'sqlite', or 'duckdb').
        namespace: Schema prefix. Default 'defacto'.

    Returns:
        Fully qualified table name (e.g., 'defacto_v1.customer_history').
    """
    if backend in ("postgres", "duckdb"):
        return f"{namespace}_{version}.{entity_type}_history"
    return f"{entity_type}_history"


def resolve_rel_table_name(
    rel_type: str, version: str, backend: str, namespace: str = "defacto",
) -> str:
    """Build the fully qualified table name for a relationship history table.

    Args:
        rel_type: Relationship type name (e.g., 'placed_order').
        version: Definition version (e.g., 'v1'). Ignored for SQLite.
        backend: Backend type ('postgres', 'sqlite', or 'duckdb').
        namespace: Schema prefix. Default 'defacto'.

    Returns:
        Fully qualified table name (e.g., 'defacto_v1.rel_placed_order_history').
    """
    if backend in ("postgres", "duckdb"):
        return f"{namespace}_{version}.rel_{rel_type}_history"
    return f"rel_{rel_type}_history"


def detect_backend(url: str) -> str:
    """Detect backend type from a database URL or file path.

    Bare file paths (no scheme) are treated as SQLite.

    Args:
        url: Database URL or file path.

    Returns:
        Backend type string ('sqlite', 'postgres', 'duckdb', etc.).
    """
    if "://" not in url:
        return "sqlite"
    return url.split("://")[0].lower()


class DDLGenerator:
    """Generates SQL DDL from entity definitions.

    Creates SCD Type 2 tables for each entity type with typed system
    columns ({type}_id, {type}_state, valid_from, valid_to, merged_into)
    plus one column per entity property. The composite key
    ({type}_id, valid_from) enables idempotent writes.

    Relationship history tables use (source_id, target_id, valid_from)
    as the composite key, with typed property columns.
    """

    def generate(
        self, definitions: dict[str, Any], version: str, backend: str,
        namespace: str = "defacto",
    ) -> list[str]:
        """Generate DDL statements to create all tables for a version.

        Creates SCD Type 2 tables for each entity type and relationship
        history tables for each declared relationship type.

        Args:
            definitions: Dict with "entities" key mapping entity type names
                to entity definitions (each having "properties" and optionally
                "relationships" dicts).
            version: Version name (used for schema name in Postgres).
            backend: Backend type ('postgres', 'sqlite', or 'duckdb').
            namespace: Schema prefix. Default 'defacto'.

        Returns:
            List of SQL statements (CREATE SCHEMA, CREATE TABLE, CREATE INDEX).

        Raises:
            ValueError: If backend is not supported.
        """
        if backend not in TYPE_MAPS:
            raise ValueError(
                f"Unsupported backend '{backend}'. Supported: {', '.join(TYPE_MAPS)}"
            )

        type_map = TYPE_MAPS[backend]
        entities = definitions.get("entities", definitions)
        statements: list[str] = []

        # Postgres/DuckDB: create version-specific schema
        if backend in ("postgres", "duckdb"):
            schema = f"{namespace}_{version}"
            statements.append(
                f"CREATE SCHEMA IF NOT EXISTS {schema}"
            )

        # Track relationship types to avoid generating duplicate tables
        # (both sides of a relationship declare it, but the table is shared)
        seen_rel_types: set[str] = set()

        for entity_type, entity_def in entities.items():
            # ── Entity history table ──
            table_name = resolve_table_name(entity_type, version, backend, namespace)
            id_col = f"{entity_type}_id"
            state_col = f"{entity_type}_state"

            columns = [f"{id_col} TEXT NOT NULL", f"{state_col} TEXT NOT NULL"]

            properties = entity_def.get("properties", {})
            for prop_name, prop_def in properties.items():
                prop_type = prop_def.get("type", "string")
                sql_type = type_map.get(prop_type, "TEXT")
                columns.append(f"{prop_name} {sql_type}")

            # Timestamp columns use native types per backend
            ts_types = {"postgres": "TIMESTAMPTZ", "duckdb": "TIMESTAMP"}
            ts_type = ts_types.get(backend, "TEXT")
            columns.extend([
                f"valid_from {ts_type} NOT NULL",
                f"valid_to {ts_type}",
                "merged_into TEXT",
                # Timing fields — flow from EntityState through snapshots for
                # cold start recovery and analytics queries.
                f"last_event_time {ts_type}",
                f"state_entered_time {ts_type}",
                f"created_time {ts_type}",
            ])

            col_sql = ", ".join(columns)
            statements.append(
                f"CREATE TABLE IF NOT EXISTS {table_name}"
                f" ({col_sql}, PRIMARY KEY ({id_col}, valid_from))"
            )

            # Partial index for current-state queries (valid_to IS NULL).
            # DuckDB doesn't support partial indexes — use a full index.
            idx_current = f"idx_{entity_type}_current"
            if backend == "duckdb":
                statements.append(
                    f"CREATE INDEX IF NOT EXISTS {idx_current}"
                    f" ON {table_name}({id_col}, valid_to)"
                )
            else:
                statements.append(
                    f"CREATE INDEX IF NOT EXISTS {idx_current}"
                    f" ON {table_name}({id_col})"
                    f" WHERE valid_to IS NULL"
                )

            # valid_from index for as_of() point-in-time queries and time-range scans
            statements.append(
                f"CREATE INDEX IF NOT EXISTS idx_{entity_type}_valid_from"
                f" ON {table_name}(valid_from)"
            )

            # Sparse index on merged_into for resolve_merges() lookups.
            # Most rows are NULL — only merged entities have a value.
            if backend == "duckdb":
                statements.append(
                    f"CREATE INDEX IF NOT EXISTS idx_{entity_type}_merged"
                    f" ON {table_name}(merged_into)"
                )
            else:
                statements.append(
                    f"CREATE INDEX IF NOT EXISTS idx_{entity_type}_merged"
                    f" ON {table_name}(merged_into)"
                    f" WHERE merged_into IS NOT NULL"
                )

            # ── Relationship history tables ──
            for rel in entity_def.get("relationships", []):
                rel_type = rel["type"]
                if rel_type in seen_rel_types:
                    continue
                seen_rel_types.add(rel_type)

                rel_table = resolve_rel_table_name(rel_type, version, backend, namespace)

                rel_columns = [
                    "source_id TEXT NOT NULL",
                    "target_id TEXT NOT NULL",
                    "relationship_type TEXT NOT NULL",
                ]

                # Relationship property columns
                for prop_name, prop_def in rel.get("properties", {}).items():
                    prop_type = prop_def.get("type", "string")
                    sql_type = type_map.get(prop_type, "TEXT")
                    rel_columns.append(f"{prop_name} {sql_type}")

                rel_columns.extend([
                    f"valid_from {ts_type} NOT NULL",
                    f"valid_to {ts_type}",
                ])

                rel_col_sql = ", ".join(rel_columns)
                statements.append(
                    f"CREATE TABLE IF NOT EXISTS {rel_table}"
                    f" ({rel_col_sql},"
                    f" PRIMARY KEY (source_id, target_id, valid_from))"
                )

                # Indexes for relationship lookups
                src_idx = f"idx_rel_{rel_type}_source"
                tgt_idx = f"idx_rel_{rel_type}_target"
                if backend == "duckdb":
                    statements.append(
                        f"CREATE INDEX IF NOT EXISTS {src_idx}"
                        f" ON {rel_table}(source_id, valid_to)"
                    )
                    statements.append(
                        f"CREATE INDEX IF NOT EXISTS {tgt_idx}"
                        f" ON {rel_table}(target_id, valid_to)"
                    )
                else:
                    statements.append(
                        f"CREATE INDEX IF NOT EXISTS {src_idx}"
                        f" ON {rel_table}(source_id)"
                        f" WHERE valid_to IS NULL"
                    )
                    statements.append(
                        f"CREATE INDEX IF NOT EXISTS {tgt_idx}"
                        f" ON {rel_table}(target_id)"
                        f" WHERE valid_to IS NULL"
                    )

        # ── Unified relationships VIEW ──
        # Unions all rel_{type}_history tables with common columns only.
        # Graph query operations (traverse, path, neighbors) use this
        # instead of querying N separate tables.
        if seen_rel_types:
            view_parts = []
            for rel_type in sorted(seen_rel_types):
                rel_table = resolve_rel_table_name(
                    rel_type, version, backend, namespace,
                )
                view_parts.append(
                    f"SELECT source_id, target_id,"
                    f" '{rel_type}' AS relationship_type,"
                    f" valid_from, valid_to"
                    f" FROM {rel_table}"
                )

            view_sql = " UNION ALL ".join(view_parts)
            if backend in ("postgres", "duckdb"):
                view_name = f"{namespace}_{version}.relationships"
                statements.append(
                    f"CREATE OR REPLACE VIEW {view_name} AS {view_sql}"
                )
            else:
                view_name = "relationships"
                # SQLite doesn't support CREATE OR REPLACE VIEW
                statements.append(f"DROP VIEW IF EXISTS {view_name}")
                statements.append(f"CREATE VIEW {view_name} AS {view_sql}")


        return statements

    def diff(
        self, old_definitions: dict[str, Any], new_definitions: dict[str, Any]
    ) -> list[str]:
        """Generate ALTER TABLE statements for schema evolution.

        Only produces ALTER statements for additive changes (new columns).
        Destructive changes (dropped columns, type changes) require a full
        table rebuild and are not handled here.

        Args:
            old_definitions: Previous version's definitions.
            new_definitions: New version's definitions.

        Returns:
            List of ALTER TABLE SQL statements. Empty if no additive changes.
        """
        raise NotImplementedError

    @staticmethod
    def _table_name(entity_type: str, version: str, backend: str) -> str:
        """Build the fully qualified table name for a backend.

        Delegates to module-level resolve_table_name() — kept for
        internal use within DDLGenerator.
        """
        return resolve_table_name(entity_type, version, backend)
