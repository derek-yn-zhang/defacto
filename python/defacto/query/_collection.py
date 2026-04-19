"""TableCollection — multi-entity selection with export capabilities.

A collection of entity tables for multi-table operations: bulk exports
(Parquet directory, DuckDB file), graph exports (Neo4j, NetworkX), and
snapshots to fast read stores.

Iterable: ``for entity_type, table in collection``
Indexable: ``collection["customer"]``
"""

from __future__ import annotations

import datetime as dt
import decimal
import os
from typing import Any, Iterator

from defacto.query._table import DefactoTable


class TableCollection:
    """A collection of DefactoTable instances for multi-table operations.

    Created by DefactoQuery.tables(). Holds a dict of {entity_type: DefactoTable}
    plus flags for relationship inclusion and history mode.

    Immutable transformations — with_relationships() and history() return
    new collections, they don't mutate.
    """

    def __init__(
        self,
        tables: dict[str, DefactoTable],
        query: Any,
        *,
        include_relationships: bool = False,
        history_mode: bool = False,
    ) -> None:
        """Initialize with a dict of entity tables.

        Args:
            tables: Dict mapping entity type names to DefactoTable instances.
            query: DefactoQuery that created this collection.
            include_relationships: Whether relationship data is included.
            history_mode: Whether tables are full history (not just current).
        """
        self._tables = tables
        self._query = query
        self._include_relationships = include_relationships
        self._history_mode = history_mode

    def __iter__(self) -> Iterator[tuple[str, DefactoTable]]:
        """Iterate over (entity_type, DefactoTable) pairs."""
        return iter(self._tables.items())

    def __len__(self) -> int:
        """Number of entity types in the collection."""
        return len(self._tables)

    def __getitem__(self, entity_type: str) -> DefactoTable:
        """Get a specific entity type's table."""
        return self._tables[entity_type]

    def __contains__(self, entity_type: str) -> bool:
        """Check if an entity type is in the collection."""
        return entity_type in self._tables

    def history(self) -> TableCollection:
        """Return a new collection with full SCD Type 2 history.

        Replaces current-state tables (WHERE valid_to IS NULL) with
        full history tables (all versions).

        Returns:
            New TableCollection in history mode.
        """
        history_tables = {
            et: self._query.history(et) for et in self._tables
        }
        return TableCollection(
            history_tables,
            self._query,
            include_relationships=self._include_relationships,
            history_mode=True,
        )

    # ── Tabular exports ──

    def to_parquet(self, path: str) -> None:
        """Export to a Parquet directory — one file per entity type.

        Creates files like: path/customer.parquet, path/order.parquet.
        If relationships are included, also writes rel_{type}.parquet files.

        Args:
            path: Output directory path.
        """
        os.makedirs(path, exist_ok=True)
        for entity_type, table in self:
            table.to_parquet(os.path.join(path, f"{entity_type}.parquet"))

        if self._include_relationships:
            for rel_type, rel_table in self._relationship_tables():
                rel_table.to_parquet(
                    os.path.join(path, f"rel_{rel_type}.parquet")
                )

    def to_csv(self, path: str) -> None:
        """Export to a CSV directory — one file per entity type.

        If relationships are included, also writes rel_{type}.csv files.

        Args:
            path: Output directory path.
        """
        os.makedirs(path, exist_ok=True)
        for entity_type, table in self:
            table.to_csv(os.path.join(path, f"{entity_type}.csv"))

        if self._include_relationships:
            for rel_type, rel_table in self._relationship_tables():
                rel_table.to_csv(os.path.join(path, f"rel_{rel_type}.csv"))

    def to_duckdb(self, path: str) -> None:
        """Export all tables into a DuckDB database file.

        Materializes each table from the source and writes to DuckDB.
        Works across any source backend (SQLite → DuckDB, Postgres → DuckDB, etc.).

        Args:
            path: Output DuckDB file path.
        """
        import ibis

        target = ibis.duckdb.connect(path)
        for entity_type, table in self:
            target.create_table(entity_type, table.execute(), overwrite=True)

        if self._include_relationships:
            for rel_type, rel_table in self._relationship_tables():
                target.create_table(
                    f"rel_{rel_type}", rel_table.execute(), overwrite=True
                )

    def to_pandas(self) -> dict[str, Any]:
        """Execute all tables and return a dict of DataFrames.

        Returns:
            Dict mapping entity type names to pandas DataFrames.
        """
        return {et: table.execute() for et, table in self}

    # ── Graph exports ──

    def to_networkx(self) -> Any:
        """Export as a NetworkX directed graph.

        Entities become nodes (properties as attributes), relationships
        become directed edges. Requires with_relationships().

        Returns:
            networkx.DiGraph instance.
        """
        try:
            import networkx as nx
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "to_networkx() requires networkx. "
                "Install it with: pip install defacto[networkx]"
            ) from None

        G = nx.DiGraph()

        for entity_type, table in self:
            id_col = f"{entity_type}_id"
            for row in table.execute().to_dict("records"):
                entity_id = row.pop(id_col)
                props = {
                    k: _serialize_value(v)
                    for k, v in row.items()
                    if v is not None
                }
                G.add_node(entity_id, entity_type=entity_type, **props)

        if self._include_relationships:
            for rel_type, rel_table in self._relationship_tables():
                for row in rel_table.execute().to_dict("records"):
                    G.add_edge(
                        row["source_id"],
                        row["target_id"],
                        relationship_type=rel_type,
                    )

        return G

    def to_graph_json(self) -> dict[str, Any]:
        """Export as a JSON-serializable dict for D3/Cytoscape.

        Format: {"nodes": [...], "edges": [...]}

        Returns:
            Dict with 'nodes' and 'edges' keys.
        """
        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, Any]] = []

        for entity_type, table in self:
            id_col = f"{entity_type}_id"
            for row in table.execute().to_dict("records"):
                node: dict[str, Any] = {
                    "id": row.pop(id_col),
                    "entity_type": entity_type,
                }
                for k, v in row.items():
                    if v is not None:
                        node[k] = _serialize_value(v)
                nodes.append(node)

        if self._include_relationships:
            for rel_type, rel_table in self._relationship_tables():
                for row in rel_table.execute().to_dict("records"):
                    edges.append({
                        "source": row["source_id"],
                        "target": row["target_id"],
                        "type": rel_type,
                    })

        return {"nodes": nodes, "edges": edges}

    def to_neo4j(self, url: str, *, auth: tuple[str, str]) -> None:
        """Export to a Neo4j instance.

        Creates nodes with entity_type labels and all properties.
        Creates relationships with declared relationship types.
        Requires the neo4j package.

        Args:
            url: Neo4j bolt URL (e.g., 'bolt://localhost:7687').
            auth: (username, password) tuple.
        """
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(url, auth=auth)

        with driver.session() as session:
            for entity_type, table in self:
                id_col = f"{entity_type}_id"
                label = entity_type.title()
                for row in table.execute().to_dict("records"):
                    entity_id = row[id_col]
                    props = {
                        k: _serialize_value(v)
                        for k, v in row.items()
                        if v is not None
                    }
                    props["_defacto_id"] = entity_id
                    session.run(
                        f"MERGE (n:{label} {{_defacto_id: $id}}) SET n = $props",
                        id=entity_id,
                        props=props,
                    )

            if self._include_relationships:
                for rel_type, rel_table in self._relationship_tables():
                    cypher_type = rel_type.upper().replace(" ", "_")
                    for row in rel_table.execute().to_dict("records"):
                        session.run(
                            f"MATCH (a {{_defacto_id: $source}}) "
                            f"MATCH (b {{_defacto_id: $target}}) "
                            f"MERGE (a)-[:{cypher_type}]->(b)",
                            source=row["source_id"],
                            target=row["target_id"],
                        )

        driver.close()

    # ── Snapshot ──

    def snapshot(self, target: str, **kwargs: Any) -> None:
        """Export current state to a fast read store.

        Writes flat tables (no SCD columns) to the target, suitable
        for API-speed reads. Target is an Ibis-compatible URL.

        Args:
            target: Database URL (e.g., 'postgresql://fast-read/db',
                'duckdb:///snapshot.db').
            **kwargs: Additional args passed to ibis.connect().
        """
        import ibis

        target_con = ibis.connect(target, **kwargs)
        for entity_type, table in self:
            df = table.execute()
            # Drop SCD system columns — snapshot is current state only
            drop_cols = [c for c in ("valid_from", "valid_to", "merged_into") if c in df.columns]
            if drop_cols:
                df = df.drop(columns=drop_cols)
            target_con.create_table(entity_type, df, overwrite=True)

    # ── Private helpers ──

    def _relationship_tables(self) -> list[tuple[str, DefactoTable]]:
        """Get relationship tables for all entity types in this collection.

        Collects unique relationship types from definitions and resolves
        their Ibis table references.
        """
        entities = self._query.entity_definitions
        seen: set[str] = set()
        result: list[tuple[str, DefactoTable]] = []

        for entity_type in self._tables:
            entity_def = entities.get(entity_type, {})
            for rel in entity_def.get("relationships", []):
                rel_type = rel["type"]
                if rel_type not in seen:
                    seen.add(rel_type)
                    t = self._query.resolve_rel_table(rel_type)
                    # Filter to current relationships if not in history mode
                    if not self._history_mode:
                        t = t.filter(t.valid_to.isnull())
                    result.append((rel_type, DefactoTable(t, self._query)))

        return result


def _serialize_value(v: Any) -> Any:
    """Convert values to JSON-safe types for graph exports."""
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    if hasattr(v, "item"):  # numpy types
        return v.item()
    return v
