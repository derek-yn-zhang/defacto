"""Defacto query layer — read entity state from any Ibis-supported store.

The query layer is completely separate from the write path. It connects
to wherever state history lives and returns lazy Ibis expressions that
materialize on .execute().

Main types:
    DefactoQuery       — coordinator, creates table/history queries
    DefactoTable       — Ibis expression proxy with temporal methods
    TableCollection   — multi-entity selection with export capabilities

Graph backends:
    GraphBackend           — ABC for graph query operations
    CteGraphBackend        — recursive CTEs on relational store (default)
    NetworkXGraphBackend   — in-memory graph (stub)
    Neo4jGraphBackend      — Cypher queries on Neo4j (stub)
"""

from defacto.query._collection import TableCollection  # noqa: F401
from defacto.query._graph import (  # noqa: F401
    CteGraphBackend,
    GraphBackend,
    Neo4jGraphBackend,
    NetworkXGraphBackend,
)
from defacto.query._query import DefactoQuery  # noqa: F401
from defacto.query._table import DefactoTable  # noqa: F401
