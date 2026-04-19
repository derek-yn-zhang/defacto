"""DefactoTable — Ibis expression proxy with entity-model awareness.

Wraps any Ibis table expression. All standard Ibis methods (filter,
select, mutate, order_by, join, group_by, etc.) pass through via
__getattr__. Defacto adds temporal query methods (as_of, resolve_merges)
and export methods (to_parquet, to_csv, to_pandas, to_pyarrow).

Chaining works naturally — Ibis methods return new DefactoTable instances:
    m.table("customer").filter(_.customer_state == "active").select("customer_id", "mrr").execute()
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import ibis
from ibis.expr.types import Table
from ibis.expr.types.groupby import GroupedTable


class DefactoTable:
    """Proxy over an Ibis table expression with Defacto-specific methods.

    Standard Ibis operations pass through transparently. Any Ibis method
    that returns a table expression gets re-wrapped in a DefactoTable so
    the chain continues with Defacto methods available.
    """

    # Ibis types that should be re-wrapped when returned from proxied methods
    _WRAP_TYPES = (Table, GroupedTable)

    def __init__(
        self,
        expr: Any,
        query: Any,
        *,
        entity_type: str | None = None,
    ) -> None:
        """Initialize with an Ibis expression and query context.

        Uses object.__setattr__ to avoid triggering __getattr__ on
        internal attribute access.

        Args:
            expr: Ibis table expression.
            query: DefactoQuery that created this table (for context).
            entity_type: Entity type name, carried through for context
                in resolve_merges and relationship joins.
        """
        object.__setattr__(self, "_expr", expr)
        object.__setattr__(self, "_query", query)
        object.__setattr__(self, "_entity_type", entity_type)

    def __getattr__(self, name: str) -> Any:
        """Proxy all Ibis methods. Re-wrap table expression results.

        If the Ibis method returns a table or grouped expression, it's
        wrapped back in DefactoTable so chaining works. Otherwise the
        raw result (column expression, scalar, etc.) is returned.
        """
        attr = getattr(self._expr, name)
        if not callable(attr):
            return attr

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Unwrap DefactoTable args so Ibis sees raw expressions
            args = tuple(
                a._expr if isinstance(a, DefactoTable) else a for a in args
            )
            kwargs = {
                k: v._expr if isinstance(v, DefactoTable) else v
                for k, v in kwargs.items()
            }
            result = attr(*args, **kwargs)
            if isinstance(result, DefactoTable._WRAP_TYPES):
                return DefactoTable(
                    result, self._query, entity_type=self._entity_type
                )
            return result

        return wrapper

    def __repr__(self) -> str:
        if self._entity_type:
            return f"DefactoTable(entity_type={self._entity_type!r})"
        return f"DefactoTable({self._expr!r})"

    # ── Temporal queries ──

    def as_of(self, timestamp: str | datetime) -> DefactoTable:
        """Point-in-time state — entity state as it was at a given moment.

        Filters to versions that were valid at the given timestamp:
        ``valid_from <= timestamp AND (valid_to > timestamp OR valid_to IS NULL)``

        Args:
            timestamp: ISO 8601 string or datetime. The point in time to query.

        Returns:
            New DefactoTable filtered to the point-in-time snapshot.
        """
        if isinstance(timestamp, datetime):
            timestamp = timestamp.isoformat()

        t = self._expr
        filtered = t.filter(
            (t.valid_from <= timestamp)
            & (t.valid_to.isnull() | (t.valid_to > timestamp))
        )
        return DefactoTable(filtered, self._query, entity_type=self._entity_type)

    def resolve_merges(self) -> DefactoTable:
        """Unified timeline including pre-merge history from absorbed entities.

        Expands the history to include rows from entities that were merged
        into the entities in this result set. Useful for seeing the full
        history of a canonical entity including its pre-merge identities.

        Returns:
            New DefactoTable with merged entity history included.
        """
        if self._entity_type is None:
            raise ValueError(
                "resolve_merges() requires a typed entity query"
            )

        # Get the full history table (unfiltered) to find merge chains
        full = self._query._resolve_entity_table(self._entity_type)

        # Union: original results + rows where merged_into is in our entity set.
        # merged_into contains the canonical entity_id — rows with merged_into
        # matching any entity in our result are pre-merge history to include.
        id_col = f"{self._entity_type}_id"
        combined = self._expr.union(
            full.filter(
                full.merged_into.isin(
                    self._expr.select(id_col).distinct()
                )
            )
        )
        return DefactoTable(
            combined, self._query, entity_type=self._entity_type
        )

    # ── Execution ──

    def execute(self) -> Any:
        """Execute the query and return a pandas DataFrame.

        Validates the generated SQL against entity definitions before
        execution — catches typos in table/column names with clear
        error messages instead of raw database errors.

        Returns:
            pandas DataFrame with query results.
        """
        # Validate entity-typed queries (not graph memtable results).
        # DefactoError from validation propagates — that's the point.
        # Only skip if SQL generation itself fails (some Ibis expressions
        # don't produce standard SQL).
        if self._entity_type is not None and hasattr(self._query, "validate_sql"):
            try:
                sql = ibis.to_sql(self._expr)
            except Exception:
                sql = None
            if sql is not None:
                self._query.validate_sql(sql)
        return self._expr.execute()

    def to_pandas(self) -> Any:
        """Execute and return a pandas DataFrame. Alias for execute()."""
        return self._expr.execute()

    def to_pyarrow(self) -> Any:
        """Execute and return a PyArrow Table.

        Avoids pandas conversion overhead for large result sets.

        Returns:
            pyarrow.Table with query results.
        """
        return self._expr.to_pyarrow()

    # ── Exports ──

    def to_parquet(self, path: str) -> None:
        """Export query results to a Parquet file.

        Args:
            path: Output file path (e.g., 'customer.parquet').
        """
        self._expr.to_parquet(path)

    def to_csv(self, path: str) -> None:
        """Export query results to a CSV file.

        Args:
            path: Output file path (e.g., 'customers.csv').
        """
        self._expr.to_csv(path)

    # ── SQL inspection ──

    def sql(self) -> str:
        """Return the SQL that would be executed.

        Useful for debugging — see what query Ibis generates for
        this expression without executing it.

        Returns:
            SQL string.
        """
        return ibis.to_sql(self._expr)
