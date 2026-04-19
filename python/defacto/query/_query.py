"""Query coordinator — connects to any Ibis-supported store and provides
entity-aware query methods.

DefactoQuery is the read-path counterpart to the write-path backends.
It connects to wherever state history lives and returns lazy Ibis
expressions that materialize on .execute().

Two creation paths:
1. Built into every Defacto instance (uses the same database)
2. Standalone via DefactoQuery.connect() (any Ibis-supported URL)

The query layer is completely separate from the write path. It doesn't
need Pipeline, Publisher, Ledger, or Kafka. It just needs a database
URL, entity definitions, and a version name.
"""

from __future__ import annotations

from typing import Any

from defacto._ddl import detect_backend
from defacto.query._table import DefactoTable


# Where our URL scheme differs from Ibis's module name
_IBIS_BACKEND_ALIASES: dict[str, str] = {
    "postgresql": "postgres",
}


class DefactoQuery:
    """Query coordinator for reading entity state from any Ibis-supported store.

    Holds a lazy Ibis connection, entity definitions, and version context.
    Creates DefactoTable instances (Ibis expression proxies) for each query.

    Immutable — version() returns a new DefactoQuery, not a mutation.
    """

    def __init__(
        self,
        url: str,
        definitions: dict[str, Any],
        version: str,
        namespace: str = "defacto",
    ) -> None:
        """Initialize with a database URL, definitions, and version.

        The Ibis connection is created lazily on first query — if the
        query layer is never used, no connection is opened.

        Args:
            url: Database URL or file path. Bare paths are SQLite.
                Examples: '/path/defacto.db', 'postgresql://host/db',
                'duckdb:///path.db', 'bigquery://project/dataset'.
            definitions: Entity definitions dict with 'entities' key.
            version: Active version name (for schema/table resolution).
            namespace: Schema prefix. Default 'defacto'.
        """
        self._url = url
        self._definitions = definitions
        self._version = version
        self._ns = namespace
        self._backend = detect_backend(url)
        self._con: Any = None

    @classmethod
    def connect(
        cls,
        url: str,
        definitions: dict[str, Any],
        version: str,
    ) -> DefactoQuery:
        """Create a standalone query handle.

        Query entity state without a full Defacto pipeline — connect
        directly to wherever state history lives.

        Args:
            url: Database URL (Ibis-compatible).
            definitions: Entity definitions dict.
            version: Version name for table resolution.

        Returns:
            DefactoQuery ready for table/history/tables queries.
        """
        return cls(url, definitions, version)

    def table(self, entity_type: str) -> DefactoTable:
        """Current state of all entities of a type.

        Equivalent to ``SELECT * FROM {entity_type}_history WHERE valid_to IS NULL``.

        Args:
            entity_type: Entity type name (e.g., 'customer').

        Returns:
            DefactoTable wrapping a filtered Ibis expression.
        """
        t = self._resolve_entity_table(entity_type)
        expr = t.filter(t.valid_to.isnull())
        return DefactoTable(expr, self, entity_type=entity_type)

    def history(self, entity_type: str) -> DefactoTable:
        """Full SCD Type 2 history for an entity type.

        Returns the complete history table — all versions, including
        closed versions (valid_to IS NOT NULL) and merged entities.
        Filter by {entity_type}_id for a specific entity's history.

        Args:
            entity_type: Entity type name (e.g., 'customer').

        Returns:
            DefactoTable wrapping the unfiltered history table.

        Example::

            # All customer history
            m.history("customer").execute()

            # One customer's history
            m.history("customer").filter(_.customer_id == "cust_001").execute()
        """
        t = self._resolve_entity_table(entity_type)
        return DefactoTable(t, self, entity_type=entity_type)

    def tables(
        self, *entity_types: Any, relationships: bool = True,
    ) -> TableCollection:
        """Select multiple entity types as a collection.

        Accepts any combination of positional args, lists, or generators.
        If no args, selects all entity types from definitions.
        Relationships are included by default — they're tables too.

        Args:
            *entity_types: Entity type names. If empty, selects all.
            relationships: Include relationship tables. Default True.

        Returns:
            TableCollection for multi-table operations and exports.
        """
        from defacto.query._collection import TableCollection

        # Flatten args: tables("a", "b"), tables(["a", "b"]), tables(*gen)
        flat: list[str] = []
        for arg in entity_types:
            if isinstance(arg, str):
                flat.append(arg)
            else:
                flat.extend(arg)

        if not flat:
            flat = list(self._definitions.get("entities", {}).keys())

        table_dict = {et: self.table(et) for et in flat}
        return TableCollection(
            table_dict, self, include_relationships=relationships,
        )

    def version(self, v: str) -> DefactoQuery:
        """Return a new DefactoQuery scoped to a different version.

        Immutable — the original query object is unchanged.

        Args:
            v: Definition version name.

        Returns:
            New DefactoQuery targeting version v's tables.
        """
        return DefactoQuery(self._url, self._definitions, v)

    def query(self, sql: str) -> DefactoTable:
        """Execute raw SQL against the state history store.

        Validates the SQL against entity definitions first — catches
        unknown tables and columns with clear error messages before
        sending to the database.

        Args:
            sql: Raw SQL query string.

        Returns:
            DefactoTable wrapping the result (chainable, exportable).
        """
        self.validate_sql(sql)
        expr = self._connection.sql(sql)
        return DefactoTable(expr, self)

    @property
    def entity_definitions(self) -> dict[str, Any]:
        """Entity definitions dict — used by TableCollection for relationship discovery."""
        return self._definitions.get("entities", {})

    # ── Connection management ──

    @property
    def _connection(self) -> Any:
        """Lazy Ibis connection — created on first query."""
        if self._con is None:
            self._con = self._create_connection()
        return self._con

    def _create_connection(self) -> Any:
        """Create an Ibis connection for this backend.

        Uses ibis.connect() with the URL — it auto-detects the backend
        from the URL scheme.
        """
        import ibis

        return ibis.connect(self._url)

    # ── Table resolution ──

    def _resolve_entity_table(self, entity_type: str) -> Any:
        """Get an Ibis table reference for an entity history table.

        Handles schema-qualified names for Postgres/DuckDB and flat
        names for SQLite.
        """
        table_name = f"{entity_type}_history"
        if self._backend in ("postgres", "postgresql", "duckdb"):
            schema = f"{self._ns}_{self._version}"
            return self._connection.table(table_name, database=schema)
        return self._connection.table(table_name)

    def resolve_rel_table(self, rel_type: str) -> Any:
        """Get an Ibis table reference for a relationship history table."""
        table_name = f"rel_{rel_type}_history"
        if self._backend in ("postgres", "postgresql", "duckdb"):
            schema = f"{self._ns}_{self._version}"
            return self._connection.table(table_name, database=schema)
        return self._connection.table(table_name)

    def validate_sql(self, sql: str) -> None:
        """Validate generated SQL against entity definitions.

        Parses the SQL with sqlglot and checks that table and column
        references match known entity types and their properties. Raises
        QueryError with clear messages listing available names.

        Called automatically before execution. Silently returns if the
        SQL can't be parsed (non-standard syntax from some backends).

        Args:
            sql: SQL string to validate.

        Raises:
            QueryError: If unknown tables or columns are referenced.
        """
        import sqlglot
        from sqlglot import exp

        from defacto.errors import QueryError

        try:
            parsed = sqlglot.parse_one(sql)
        except sqlglot.errors.ParseError:
            return  # can't parse — skip validation

        entities = self.entity_definitions
        # Build set of known table names (without schema prefix)
        known_tables: set[str] = set()
        for et in entities:
            known_tables.add(f"{et}_history")
        for et_def in entities.values():
            for rel in et_def.get("relationships", []):
                known_tables.add(f"rel_{rel['type']}_history")
        known_tables.add("relationships")  # the unified VIEW

        # Build column sets per entity table
        table_columns: dict[str, set[str]] = {}
        for et, et_def in entities.items():
            cols = {f"{et}_id", f"{et}_state"}
            cols.update(et_def.get("properties", {}).keys())
            cols.update((
                "valid_from", "valid_to", "merged_into",
                "last_event_time", "state_entered_time", "created_time",
            ))
            table_columns[f"{et}_history"] = cols

        alias_map: dict[str, str] = {}
        errors: list[str] = []

        # Check table references
        for table in parsed.find_all(exp.Table):
            table_name = table.name
            if table.alias:
                alias_map[table.alias] = table_name

            if not table_name or table_name.startswith("_"):
                continue
            cte_names = {cte.alias for cte in parsed.find_all(exp.CTE)}
            if table_name not in known_tables and table_name not in cte_names:
                available = sorted(et for et in entities)
                errors.append(
                    f"Unknown table '{table_name}'. "
                    f"Available entity types: {available}"
                )

        # Determine which entity tables are referenced for column validation
        referenced = [
            t.name for t in parsed.find_all(exp.Table)
            if t.name in table_columns
        ]

        # Check column references
        for column in parsed.find_all(exp.Column):
            col_name = column.name
            col_table = column.table

            if col_table:
                real_table = alias_map.get(col_table, col_table)
            elif len(referenced) == 1:
                real_table = referenced[0]
            else:
                continue  # ambiguous — can't validate without table context

            if real_table and real_table in table_columns:
                valid = table_columns[real_table]
                if col_name not in valid:
                    entity_type = real_table.removesuffix("_history")
                    errors.append(
                        f"Unknown column '{col_name}' for entity '{entity_type}'. "
                        f"Available: {sorted(valid)}"
                    )

        if errors:
            raise QueryError(
                "Query validation errors:\n" + "\n".join(f"  - {e}" for e in errors),
                details={"errors": errors},
            )

    def close(self) -> None:
        """Close the Ibis connection if open."""
        if self._con is not None:
            # Ibis connections don't all have close(), but most do
            if hasattr(self._con, "disconnect"):
                self._con.disconnect()
            self._con = None
