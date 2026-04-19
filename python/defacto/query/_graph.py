"""Graph query backends — read-only graph operations on entity relationships.

Graph backends provide traversal, pathfinding, and analytics operations
on the entity relationship graph. They are read-only — they don't write
data. They either query the relational store directly (CTE) or operate
on a previously exported graph (NetworkX, Neo4j).

Three implementations:
    CteGraphBackend      — recursive CTEs on the relationships VIEW (always available)
    NetworkXGraphBackend — in-memory graph loaded from to_networkx() (optional)
    Neo4jGraphBackend    — Cypher queries on a Neo4j instance (optional)

The CTE backend implements core operations only. NetworkX and Neo4j
implement core + analytics operations (community detection, centrality,
similarity).

Results are returned as DefactoTable instances wrapping ibis.memtable()
so they're chainable with filter/select/etc. regardless of backend.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import ibis
import pandas as pd

from defacto.query._table import DefactoTable


class GraphBackend(ABC):
    """Abstract interface for graph query operations.

    Core operations (traverse, path, neighbors, related) are required —
    all backends must implement them. Analytics operations have default
    implementations that raise NotImplementedError, since they're only
    practical on graph-native backends (NetworkX, Neo4j).
    """

    # ── Core operations (all backends) ──

    @abstractmethod
    def traverse(
        self,
        entity_id: str,
        relationship_type: str,
        *,
        depth: int = 1,
        directed: bool = True,
    ) -> pd.DataFrame:
        """Follow relationship chains from a starting entity.

        Returns all entities reachable within `depth` hops via the
        specified relationship type.

        Args:
            entity_id: Starting entity ID.
            relationship_type: Relationship type to follow.
            depth: Maximum hops to traverse. Default 1.
            directed: If True, follow edges in declared direction only.
                If False, follow edges in both directions.

        Returns:
            DataFrame with columns: entity_id, depth.
        """

    @abstractmethod
    def path(
        self,
        from_id: str,
        to_id: str,
        *,
        max_depth: int = 20,
    ) -> list[str]:
        """Find the shortest path between two entities.

        Follows any relationship type in either direction.

        Args:
            from_id: Starting entity ID.
            to_id: Target entity ID.
            max_depth: Maximum path length to search.

        Returns:
            Ordered list of entity IDs from source to target,
            or empty list if no path exists.
        """

    @abstractmethod
    def neighbors(self, entity_id: str) -> pd.DataFrame:
        """Get all directly connected entities regardless of relationship type.

        Args:
            entity_id: Entity to find neighbors for.

        Returns:
            DataFrame with columns: entity_id,
            relationship_type, direction ('outbound' or 'inbound').
        """

    @abstractmethod
    def related(
        self,
        entity_id: str,
        relationship_type: str,
    ) -> pd.DataFrame:
        """Get entities related through a specific relationship type.

        Returns target entities with their full properties and any
        relationship edge properties.

        Args:
            entity_id: Source entity ID.
            relationship_type: Relationship type to follow.

        Returns:
            DataFrame with target entity columns + relationship properties.
        """

    # ── Analytics operations (NetworkX / Neo4j only) ──

    def connected_components(self) -> pd.DataFrame:
        """Find connected components in the entity graph.

        Identifies isolated subgraphs — groups of entities connected
        to each other but not to other groups. Useful for data quality
        (finding orphans) and segmentation.

        Returns:
            DataFrame with columns: entity_id, entity_type, component_id.
        """
        raise NotImplementedError(
            "connected_components() requires a graph-native backend "
            "(NetworkX or Neo4j). CTE backend does not support this."
        )

    def communities(self, *, algorithm: str = "louvain") -> pd.DataFrame:
        """Detect communities in the entity graph.

        Finds natural groupings of entities based on relationship
        density — entities within a community are more connected to
        each other than to entities in other communities.

        Args:
            algorithm: Community detection algorithm.
                'louvain' (default), 'label_propagation'.

        Returns:
            DataFrame with columns: entity_id, entity_type, community_id.
        """
        raise NotImplementedError(
            "communities() requires a graph-native backend "
            "(NetworkX or Neo4j). CTE backend does not support this."
        )

    def centrality(self, *, algorithm: str = "pagerank") -> pd.DataFrame:
        """Compute centrality scores for entities.

        Identifies the most important or influential entities in the
        graph based on their relationship patterns.

        Args:
            algorithm: Centrality algorithm.
                'pagerank' (default), 'betweenness', 'degree'.

        Returns:
            DataFrame with columns: entity_id, entity_type, score.
            Sorted by score descending.
        """
        raise NotImplementedError(
            "centrality() requires a graph-native backend "
            "(NetworkX or Neo4j). CTE backend does not support this."
        )

    def similar(
        self,
        entity_id: str,
        *,
        top_n: int = 10,
        algorithm: str = "jaccard",
    ) -> pd.DataFrame:
        """Find entities structurally similar to a given entity.

        Similarity is based on shared neighbors — entities connected
        to the same other entities are considered similar.

        Args:
            entity_id: Entity to find similar entities for.
            top_n: Maximum number of similar entities to return.
            algorithm: Similarity algorithm. 'jaccard' (default).

        Returns:
            DataFrame with columns: entity_id, entity_type, score.
            Sorted by score descending.
        """
        raise NotImplementedError(
            "similar() requires a graph-native backend "
            "(NetworkX or Neo4j). CTE backend does not support this."
        )

    # ── Direct access ──

    @property
    @abstractmethod
    def connection(self) -> Any:
        """The underlying graph object or database connection.

        NetworkX: the DiGraph. Neo4j: the driver. CTE: the SQL connection.
        For direct operations beyond the structured API.
        """

    # ── Helpers ──

    def _to_defacto_table(self, df: pd.DataFrame, query: Any) -> DefactoTable:
        """Wrap a DataFrame result in a DefactoTable for chainable operations.

        Uses ibis.memtable() to create an Ibis expression from the
        materialized DataFrame, so filter/select/etc. work on the result.
        """
        expr = ibis.memtable(df)
        return DefactoTable(expr, query)


class CteGraphBackend(GraphBackend):
    """Graph operations via recursive CTEs on the relationships VIEW.

    Always available — no extra dependencies. Queries the relational
    store directly using the unified `relationships` VIEW that unions
    all per-type relationship history tables.

    Supports core operations only. Analytics operations (community
    detection, centrality, etc.) are not feasible with SQL CTEs and
    raise NotImplementedError.
    """

    def __init__(
        self,
        connection: Any,
        version: str,
        backend: str,
        definitions: dict[str, Any] | None = None,
        namespace: str = "defacto",
    ) -> None:
        """Initialize with a database connection and entity definitions.

        Args:
            connection: Raw database connection (sqlite3, psycopg, duckdb).
            version: Definition version for schema resolution.
            backend: Backend type ('sqlite', 'postgres', 'duckdb').
            definitions: Entity definitions dict with 'entities' key.
                Used to resolve entity types from relationship definitions.
            namespace: Schema prefix. Default 'defacto'.
        """
        self._conn = connection
        self._version = version
        self._backend = backend
        self._ns = namespace
        self._definitions = definitions or {}

        # Build lookup: relationship_type → target entity type
        self._rel_targets: dict[str, str] = {}
        for entity_type, entity_def in self._definitions.get("entities", {}).items():
            for rel in entity_def.get("relationships", []):
                self._rel_targets[rel["type"]] = rel["target"]

    @property
    def _view_name(self) -> str:
        """Fully qualified name of the relationships VIEW."""
        if self._backend in ("postgres", "postgresql", "duckdb"):
            return f"{self._ns}_{self._version}.relationships"
        return "relationships"

    def traverse(
        self,
        entity_id: str,
        relationship_type: str,
        *,
        depth: int = 1,
        directed: bool = True,
    ) -> pd.DataFrame:
        """Recursive CTE traversal on the relationships VIEW.

        Enriches results with entity_type from relationship definitions.
        Raises ValueError if the relationship type is not defined.
        """
        if relationship_type not in self._rel_targets:
            defined = list(self._rel_targets.keys())
            raise ValueError(
                f"Relationship type '{relationship_type}' is not defined. "
                f"Defined relationship types: {defined or 'none'}"
            )
        view = self._view_name
        ph = self._ph

        if directed:
            sql = (
                f"WITH RECURSIVE reachable(entity_id, depth) AS ("
                f"  SELECT {ph}, 0"
                f"  UNION"
                f"  SELECT r.target_id, re.depth + 1"
                f"  FROM reachable re"
                f"  JOIN {view} r ON r.source_id = re.entity_id"
                f"  WHERE re.depth < {ph}"
                f"    AND r.relationship_type = {ph}"
                f"    AND r.valid_to IS NULL"
                f") SELECT DISTINCT entity_id, depth FROM reachable"
            )
        else:
            sql = (
                f"WITH RECURSIVE reachable(entity_id, depth) AS ("
                f"  SELECT {ph}, 0"
                f"  UNION"
                f"  SELECT CASE WHEN r.source_id = re.entity_id"
                f"    THEN r.target_id ELSE r.source_id END,"
                f"    re.depth + 1"
                f"  FROM reachable re"
                f"  JOIN {view} r ON (r.source_id = re.entity_id"
                f"    OR r.target_id = re.entity_id)"
                f"  WHERE re.depth < {ph}"
                f"    AND r.relationship_type = {ph}"
                f"    AND r.valid_to IS NULL"
                f") SELECT DISTINCT entity_id, depth FROM reachable"
            )

        params = self._params(entity_id, depth, relationship_type)
        df = self._execute(sql, params)

        # Enrich with entity_type from relationship definitions
        target_type = self._rel_targets.get(relationship_type, "")
        if not df.empty and target_type:
            df["entity_type"] = df["depth"].apply(
                lambda d: target_type if d > 0 else self._source_type(relationship_type)
            )

        return df

    def _source_type(self, relationship_type: str) -> str:
        """Find the source entity type for a relationship type."""
        for entity_type, entity_def in self._definitions.get("entities", {}).items():
            for rel in entity_def.get("relationships", []):
                if rel["type"] == relationship_type:
                    return entity_type
        return ""

    def path(
        self,
        from_id: str,
        to_id: str,
        *,
        max_depth: int = 20,
    ) -> list[str]:
        """Recursive CTE shortest path search."""
        if not self._rel_targets:
            raise ValueError(
                "No relationships defined — path() requires at least one "
                "entity with a 'relationships' definition."
            )
        view = self._view_name

        # Path tracking via string concatenation — works on SQLite and Postgres.
        # from_id appears twice: as start entity and as initial path string.
        sql = (
            f"WITH RECURSIVE paths(entity_id, path, depth) AS ("
            f"  SELECT {self._ph}, {self._ph}, 0"
            f"  UNION ALL"
            f"  SELECT CASE WHEN r.source_id = p.entity_id"
            f"    THEN r.target_id ELSE r.source_id END,"
            f"    p.path || ',' || CASE WHEN r.source_id = p.entity_id"
            f"      THEN r.target_id ELSE r.source_id END,"
            f"    p.depth + 1"
            f"  FROM paths p"
            f"  JOIN {view} r ON (r.source_id = p.entity_id"
            f"    OR r.target_id = p.entity_id)"
            f"  WHERE p.depth < {self._ph}"
            f"    AND r.valid_to IS NULL"
            f"    AND p.path NOT LIKE '%' || CASE WHEN r.source_id = p.entity_id"
            f"      THEN r.target_id ELSE r.source_id END || '%'"
            f") SELECT path FROM paths"
            f" WHERE entity_id = {self._ph}"
            f" ORDER BY depth LIMIT 1"
        )
        params = self._params(from_id, from_id, max_depth, to_id)
        result = self._execute(sql, params)

        if result.empty:
            return []
        return result.iloc[0]["path"].split(",")

    def neighbors(self, entity_id: str) -> pd.DataFrame:
        """Direct connections via the relationships VIEW.

        Enriches with entity_type from relationship definitions.
        Raises ValueError if no relationships are defined.
        """
        if not self._rel_targets:
            raise ValueError(
                "No relationships defined — neighbors() requires at least one "
                "entity with a 'relationships' definition."
            )
        view = self._view_name
        ph = self._ph

        sql = (
            f"SELECT target_id AS entity_id, relationship_type,"
            f" 'outbound' AS direction"
            f" FROM {view}"
            f" WHERE source_id = {ph} AND valid_to IS NULL"
            f" UNION ALL"
            f" SELECT source_id AS entity_id, relationship_type,"
            f" 'inbound' AS direction"
            f" FROM {view}"
            f" WHERE target_id = {ph} AND valid_to IS NULL"
        )
        params = self._params(entity_id, entity_id)
        df = self._execute(sql, params)

        # Enrich with entity_type: outbound → target type, inbound → source type
        if not df.empty:
            df["entity_type"] = df.apply(
                lambda row: (
                    self._rel_targets.get(row["relationship_type"], "")
                    if row["direction"] == "outbound"
                    else self._source_type(row["relationship_type"])
                ),
                axis=1,
            )

        return df

    def related(
        self,
        entity_id: str,
        relationship_type: str,
    ) -> pd.DataFrame:
        """Join through relationship to target entity table.

        Returns target entity rows with their full properties plus
        any relationship edge properties. Uses the per-type relationship
        table (for properties) joined to the target entity history table.
        """
        from defacto._ddl import resolve_rel_table_name, resolve_table_name

        target_type = self._rel_targets.get(relationship_type)
        if not target_type:
            defined = list(self._rel_targets.keys())
            raise ValueError(
                f"Relationship type '{relationship_type}' is not defined. "
                f"Defined relationship types: {defined or 'none'}"
            )

        backend = self._backend
        if backend == "postgresql":
            backend = "postgres"

        rel_table = resolve_rel_table_name(
            relationship_type, self._version, backend, self._ns,
        )
        entity_table = resolve_table_name(
            target_type, self._version, backend, self._ns,
        )
        target_id_col = f"{target_type}_id"
        ph = self._ph

        sql = (
            f"SELECT e.*, r.relationship_type"
            f" FROM {rel_table} r"
            f" JOIN {entity_table} e"
            f"   ON e.{target_id_col} = r.target_id"
            f"   AND e.valid_to IS NULL"
            f" WHERE r.source_id = {ph}"
            f"   AND r.valid_to IS NULL"
        )
        params = self._params(entity_id)
        return self._execute(sql, params)

    @property
    def connection(self) -> Any:
        """The underlying SQL database connection."""
        return self._conn

    # ── Backend-portable SQL helpers ──

    @property
    def _ph(self) -> str:
        """Parameter placeholder for this backend. SQLite/DuckDB use ?, Postgres uses %s."""
        if self._backend in ("postgres", "postgresql"):
            return "%s"
        return "?"

    def _params(self, *args: Any) -> tuple:
        """Build a parameter tuple."""
        return args

    def _execute(self, sql: str, params: tuple) -> pd.DataFrame:
        """Execute SQL and return a DataFrame."""
        if self._backend in ("postgres", "postgresql"):
            import psycopg.rows
            with self._conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            return pd.DataFrame(rows) if rows else pd.DataFrame()
        elif self._backend == "duckdb":
            result = self._conn.execute(sql, list(params))
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame()
        else:
            # SQLite — use cursor directly, don't mutate connection state
            cursor = self._conn.execute(sql, params)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            if not rows:
                return pd.DataFrame()
            return pd.DataFrame(rows, columns=columns)


class NetworkXGraphBackend(GraphBackend):
    """Graph operations on an in-memory NetworkX DiGraph.

    Wraps a nx.DiGraph previously created via TableCollection.to_networkx().
    Supports both core and analytics operations — NetworkX has native
    implementations for community detection, centrality, and similarity.

    Nodes have ``entity_type`` and property attributes. Edges have
    ``relationship_type`` as an attribute. This is the structure produced
    by ``to_networkx()``.

    Usage::

        G = m.tables().with_relationships().to_networkx()
        backend = NetworkXGraphBackend(G)
        # Core operations
        backend.traverse("cust_001", "placed_order", depth=2)
        # Analytics
        backend.communities()
        backend.centrality(algorithm="pagerank")
    """

    def __init__(self, graph: Any) -> None:
        """Initialize with a NetworkX DiGraph.

        Args:
            graph: networkx.DiGraph created via to_networkx().
        """
        try:
            import networkx as nx
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "NetworkXGraphBackend requires networkx. "
                "Install it with: pip install defacto[networkx]"
            ) from None
        self._graph: nx.DiGraph = graph

    @property
    def connection(self) -> Any:
        """The underlying NetworkX DiGraph."""
        return self._graph

    # ── Core operations ──

    def traverse(self, entity_id: str, relationship_type: str, *,
                 depth: int = 1, directed: bool = True) -> pd.DataFrame:
        """BFS traversal filtered by relationship type."""
        G = self._graph
        if entity_id not in G:
            return pd.DataFrame(columns=["entity_id", "depth", "entity_type"])

        visited: dict[str, int] = {entity_id: 0}
        frontier = [entity_id]

        for d in range(1, depth + 1):
            next_frontier = []
            for node in frontier:
                # Get neighbors based on direction
                if directed:
                    candidates = G.successors(node)
                else:
                    candidates = list(G.successors(node)) + list(G.predecessors(node))
                for neighbor in candidates:
                    if neighbor in visited:
                        continue
                    # Check edge relationship type
                    edge = G.edges.get((node, neighbor), {})
                    if not directed and not edge:
                        edge = G.edges.get((neighbor, node), {})
                    if edge.get("relationship_type") == relationship_type:
                        visited[neighbor] = d
                        next_frontier.append(neighbor)
            frontier = next_frontier

        rows = [
            {
                "entity_id": nid,
                "depth": d,
                "entity_type": G.nodes[nid].get("entity_type", ""),
            }
            for nid, d in visited.items()
        ]
        return pd.DataFrame(rows)

    def path(self, from_id: str, to_id: str, *,
             max_depth: int = 20) -> list[str]:
        """Shortest path via undirected view (relationships navigable both ways)."""
        import networkx as nx

        if from_id not in self._graph or to_id not in self._graph:
            return []
        try:
            return nx.shortest_path(
                self._graph.to_undirected(as_view=True),
                from_id, to_id,
            )
        except nx.NetworkXNoPath:
            return []

    def neighbors(self, entity_id: str) -> pd.DataFrame:
        """All directly connected entities with direction."""
        G = self._graph
        if entity_id not in G:
            return pd.DataFrame(
                columns=["entity_id", "relationship_type", "direction", "entity_type"],
            )

        rows = []
        for target in G.successors(entity_id):
            edge = G.edges[entity_id, target]
            rows.append({
                "entity_id": target,
                "relationship_type": edge.get("relationship_type", ""),
                "direction": "outbound",
                "entity_type": G.nodes[target].get("entity_type", ""),
            })
        for source in G.predecessors(entity_id):
            edge = G.edges[source, entity_id]
            rows.append({
                "entity_id": source,
                "relationship_type": edge.get("relationship_type", ""),
                "direction": "inbound",
                "entity_type": G.nodes[source].get("entity_type", ""),
            })
        return pd.DataFrame(rows)

    def related(self, entity_id: str, relationship_type: str) -> pd.DataFrame:
        """Target entities connected via a specific relationship type."""
        G = self._graph
        if entity_id not in G:
            return pd.DataFrame()

        rows = []
        for target in G.successors(entity_id):
            edge = G.edges[entity_id, target]
            if edge.get("relationship_type") != relationship_type:
                continue
            props = dict(G.nodes[target])
            props["entity_id"] = target
            rows.append(props)
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    # ── Analytics operations ──

    def connected_components(self) -> pd.DataFrame:
        """Weakly connected components of the directed graph."""
        import networkx as nx

        rows = []
        for i, component in enumerate(nx.weakly_connected_components(self._graph)):
            for node in component:
                rows.append({
                    "entity_id": node,
                    "entity_type": self._graph.nodes[node].get("entity_type", ""),
                    "component_id": i,
                })
        return pd.DataFrame(rows)

    def communities(self, *, algorithm: str = "louvain") -> pd.DataFrame:
        """Community detection on an undirected view of the graph."""
        import networkx as nx

        undirected = self._graph.to_undirected()
        if algorithm == "louvain":
            partition = nx.community.louvain_communities(undirected)
        elif algorithm == "label_propagation":
            partition = nx.community.label_propagation_communities(undirected)
        else:
            raise ValueError(f"Unknown algorithm '{algorithm}'. Use 'louvain' or 'label_propagation'.")

        rows = []
        for community_id, members in enumerate(partition):
            for node in members:
                rows.append({
                    "entity_id": node,
                    "entity_type": self._graph.nodes[node].get("entity_type", ""),
                    "community_id": community_id,
                })
        return pd.DataFrame(rows)

    def centrality(self, *, algorithm: str = "pagerank") -> pd.DataFrame:
        """Centrality scores sorted by score descending."""
        import networkx as nx

        G = self._graph
        if algorithm == "pagerank":
            scores = nx.pagerank(G)
        elif algorithm == "betweenness":
            scores = nx.betweenness_centrality(G)
        elif algorithm == "degree":
            scores = nx.degree_centrality(G)
        else:
            raise ValueError(f"Unknown algorithm '{algorithm}'. Use 'pagerank', 'betweenness', or 'degree'.")

        rows = [
            {
                "entity_id": node,
                "entity_type": G.nodes[node].get("entity_type", ""),
                "score": score,
            }
            for node, score in scores.items()
        ]
        df = pd.DataFrame(rows)
        return df.sort_values("score", ascending=False).reset_index(drop=True)

    def similar(self, entity_id: str, *, top_n: int = 10,
                algorithm: str = "jaccard") -> pd.DataFrame:
        """Node similarity based on shared neighbors."""
        import networkx as nx

        G = self._graph
        if entity_id not in G:
            return pd.DataFrame(columns=["entity_id", "entity_type", "score"])

        undirected = G.to_undirected()
        others = [n for n in undirected.nodes if n != entity_id]
        if not others:
            return pd.DataFrame(columns=["entity_id", "entity_type", "score"])

        pairs = [(entity_id, other) for other in others]
        predictions = nx.jaccard_coefficient(undirected, pairs)

        rows = []
        for _, target, score in predictions:
            if score > 0:
                rows.append({
                    "entity_id": target,
                    "entity_type": G.nodes[target].get("entity_type", ""),
                    "score": score,
                })
        df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["entity_id", "entity_type", "score"])
        return df.sort_values("score", ascending=False).head(top_n).reset_index(drop=True)


class Neo4jGraphBackend(GraphBackend):
    """Graph operations via Cypher queries on a Neo4j instance.

    Connects to a Neo4j database where entity data was previously
    exported via TableCollection.to_neo4j(). Core operations use
    standard Cypher. Analytics operations require the Neo4j GDS
    (Graph Data Science) plugin (not included in community edition).

    Nodes are identified by ``_defacto_id`` property (set by to_neo4j).
    Entity types are stored as node labels (title-cased).

    Usage::

        backend = Neo4jGraphBackend("bolt://localhost:7687", auth=("neo4j", "test"))
        backend.traverse("cust_001", "placed_order", depth=2)
        backend.query("MATCH (n)-[:PLACED_ORDER]->(m) RETURN n, m")
    """

    def __init__(self, url: str, *, auth: tuple[str, str]) -> None:
        """Initialize with a Neo4j connection.

        Args:
            url: Neo4j bolt URL (e.g., 'bolt://localhost:7687').
            auth: (username, password) tuple.
        """
        self._url = url
        self._auth = auth
        self.__driver: Any = None

    @property
    def _driver(self) -> Any:
        """Lazy-init Neo4j driver on first use."""
        if self.__driver is None:
            from neo4j import GraphDatabase
            self.__driver = GraphDatabase.driver(self._url, auth=self._auth)
        return self.__driver

    @property
    def connection(self) -> Any:
        """The underlying Neo4j driver."""
        return self._driver

    def query(self, cypher: str, **params: Any) -> pd.DataFrame:
        """Execute a raw Cypher query and return results as a DataFrame.

        Args:
            cypher: Cypher query string.
            **params: Query parameters.

        Returns:
            DataFrame with query result columns.
        """
        records, _, _ = self._driver.execute_query(cypher, **params)
        if not records:
            return pd.DataFrame()
        return pd.DataFrame([dict(r) for r in records])

    # ── Core operations ──

    def traverse(self, entity_id: str, relationship_type: str, *,
                 depth: int = 1, directed: bool = True) -> pd.DataFrame:
        """Cypher variable-length path traversal."""
        rel_type = relationship_type.upper().replace(" ", "_")
        arrow = "->" if directed else "-"

        cypher = (
            f"MATCH path = (n {{_defacto_id: $id}})"
            f"-[:{rel_type}*1..{depth}]{arrow}(m) "
            f"WITH m, min(length(path)) AS d "
            f"RETURN m._defacto_id AS entity_id, d AS depth, "
            f"labels(m)[0] AS entity_type"
        )
        records, _, _ = self._driver.execute_query(cypher, id=entity_id)

        # Start with the source entity at depth 0
        source_label = self._node_label(entity_id)
        rows = [{"entity_id": entity_id, "depth": 0, "entity_type": source_label}]
        for r in records:
            rows.append({
                "entity_id": r["entity_id"],
                "depth": r["depth"],
                "entity_type": (r["entity_type"] or "").lower(),
            })
        return pd.DataFrame(rows)

    def path(self, from_id: str, to_id: str, *,
             max_depth: int = 20) -> list[str]:
        """Cypher shortestPath()."""
        cypher = (
            f"MATCH p = shortestPath("
            f"(a {{_defacto_id: $from_id}})-[*..{max_depth}]-(b {{_defacto_id: $to_id}}))"
            f" RETURN [n IN nodes(p) | n._defacto_id] AS path"
        )
        records, _, _ = self._driver.execute_query(
            cypher, from_id=from_id, to_id=to_id,
        )
        if not records:
            return []
        return list(records[0]["path"])

    def neighbors(self, entity_id: str) -> pd.DataFrame:
        """Cypher MATCH for all direct connections."""
        cypher = (
            "MATCH (n {_defacto_id: $id})-[r]->(m) "
            "RETURN m._defacto_id AS entity_id, type(r) AS relationship_type, "
            "'outbound' AS direction, labels(m)[0] AS entity_type "
            "UNION ALL "
            "MATCH (n {_defacto_id: $id})<-[r]-(m) "
            "RETURN m._defacto_id AS entity_id, type(r) AS relationship_type, "
            "'inbound' AS direction, labels(m)[0] AS entity_type"
        )
        records, _, _ = self._driver.execute_query(cypher, id=entity_id)
        if not records:
            return pd.DataFrame(
                columns=["entity_id", "relationship_type", "direction", "entity_type"],
            )
        rows = []
        for r in records:
            rows.append({
                "entity_id": r["entity_id"],
                "relationship_type": r["relationship_type"].lower().replace("_", " ")
                    if r["relationship_type"] else "",
                "direction": r["direction"],
                "entity_type": (r["entity_type"] or "").lower(),
            })
        return pd.DataFrame(rows)

    def related(self, entity_id: str, relationship_type: str) -> pd.DataFrame:
        """Cypher typed relationship MATCH returning target properties."""
        rel_type = relationship_type.upper().replace(" ", "_")
        cypher = (
            f"MATCH (n {{_defacto_id: $id}})-[:{rel_type}]->(m) "
            f"RETURN m"
        )
        records, _, _ = self._driver.execute_query(cypher, id=entity_id)
        if not records:
            return pd.DataFrame()
        rows = []
        for r in records:
            node = dict(r["m"])
            node["entity_id"] = node.pop("_defacto_id", None)
            rows.append(node)
        return pd.DataFrame(rows)

    # ── Analytics operations (require Neo4j GDS) ──

    def connected_components(self) -> pd.DataFrame:
        """Requires Neo4j GDS plugin (not available in community edition)."""
        raise NotImplementedError(
            "connected_components() requires the Neo4j Graph Data Science (GDS) "
            "plugin. Install GDS or use NetworkXGraphBackend for analytics."
        )

    def communities(self, *, algorithm: str = "louvain") -> pd.DataFrame:
        """Requires Neo4j GDS plugin (not available in community edition)."""
        raise NotImplementedError(
            "communities() requires the Neo4j Graph Data Science (GDS) "
            "plugin. Install GDS or use NetworkXGraphBackend for analytics."
        )

    def centrality(self, *, algorithm: str = "pagerank") -> pd.DataFrame:
        """Requires Neo4j GDS plugin (not available in community edition)."""
        raise NotImplementedError(
            "centrality() requires the Neo4j Graph Data Science (GDS) "
            "plugin. Install GDS or use NetworkXGraphBackend for analytics."
        )

    def similar(self, entity_id: str, *, top_n: int = 10,
                algorithm: str = "jaccard") -> pd.DataFrame:
        """Requires Neo4j GDS plugin (not available in community edition)."""
        raise NotImplementedError(
            "similar() requires the Neo4j Graph Data Science (GDS) "
            "plugin. Install GDS or use NetworkXGraphBackend for analytics."
        )

    # ── Helpers ──

    def _node_label(self, entity_id: str) -> str:
        """Look up the label (entity type) for a node."""
        records, _, _ = self._driver.execute_query(
            "MATCH (n {_defacto_id: $id}) RETURN labels(n)[0] AS label",
            id=entity_id,
        )
        if records:
            return (records[0]["label"] or "").lower()
        return ""

    def close(self) -> None:
        """Close the Neo4j driver connection."""
        if self.__driver is not None:
            self.__driver.close()
            self.__driver = None
