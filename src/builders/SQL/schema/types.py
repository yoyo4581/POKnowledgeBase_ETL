from dataclasses import dataclass, field
from typing import Mapping, Optional, Union
from abc import ABC, abstractmethod
from enum import Enum


class DBSchema(str, Enum):
    PRODUCTION = "dbo"
    STAGING = "staging"
    DIFF = "diff"


@dataclass(frozen=True)
class StagingQuery:
    """
    Everything the executor needs to run a dedup-guarded staging INSERT
    without knowing which strategy produced it.
    insert_cols: ordered columns being inserted (defines the SELECT ? ?  part)
    match_cols: ordered columns used for the dedup WHERE NOT EXISTS check
                (may be a strict subset of insert_cols, e.g. key-only)
    Params for one row = tuple(row[c] for c in insert_cols) + tuple(row[c] for c in match_cols)
    """
    sql: str
    insert_cols: tuple[str, ...]
    match_cols: tuple[str, ...]
    num_params: int


class SyncStrategy(ABC):
    def key_clause(self, schema) -> str:
        """ON-clause for MERGE. auto_id keys never reach staging (they're
        server-generated), so an auto_id table matches on its UniqueConstraint
        columns instead of its (nonexistent-in-staging) key."""
        if schema.auto_id:
            return self._natural_key_clause(schema)
        return f"ON target.{schema.key} = source.{schema.key}"

    def _natural_key_clause(self, schema) -> str:
        uq = next((c for c in schema.constraints if isinstance(c, UniqueConstraint)), None)
        if uq is None:
            raise ValueError(
                f"{schema.__table_name__}: auto_id table has no UniqueConstraint to MERGE-match on"
            )
        conds = " AND ".join(
            f"((target.{col} = source.{col}) OR (target.{col} IS NULL AND source.{col} IS NULL))"
            for col in uq.columns
        )
        return f"ON {conds}"

    @abstractmethod
    def match_clause(self, schema, target_table: str) -> str: ...

    @abstractmethod
    def diff_output_clause(self, schema, target_table: str, run_id: str) -> str: ...

    def insert_columns(self, schema) -> list[str]:
        return [c for c in schema.columns if not (schema.auto_id and c == schema.key)]

    def insert_values(self, schema) -> list[str]:
        return [f"source.{c}" for c in schema.columns if not (schema.auto_id and c == schema.key)]

    def dedup_match_columns(self, schema) -> tuple[str, ...]:
        """Columns used at staging-insert time to detect an already-staged
        duplicate row. Default: the table's single key. Strategies without
        a single-column identity (e.g. IdentityHashSync) must override."""
        assert schema.key is not None, (
            f"{type(self).__name__} has no schema.key and must override dedup_match_columns"
        )
        return (schema.key,)

@dataclass(frozen=True)
class IdentityHashSync(SyncStrategy):
    identity_hash: tuple[str, ...]
    coverage_scope_columns: tuple[str, ...]

    def key_clause(self, schema) -> str:
        return "ON target.identity_hash = source.identity_hash"

    def insert_columns(self, schema) -> list[str]:
        return super().insert_columns(schema) + ["identity_hash"]

    def insert_values(self, schema) -> list[str]:
        return super().insert_values(schema) + ["source.identity_hash"]

    def dedup_match_columns(self, schema) -> tuple[str, ...]:
        return self.identity_hash

    def match_clause(self, schema, target_table: str) -> str:
        if not self.coverage_scope_columns:
            raise ValueError(
                f"coverage_scope_columns must be set for IdentityHashSync on {target_table}; "
                "unconditional NOT MATCHED BY SOURCE deletes are disallowed."
            )
        coverage_cols = " AND ".join(
            f"target.{col} = s.{col}" for col in self.coverage_scope_columns
        )
        return f"""WHEN NOT MATCHED BY SOURCE AND EXISTS (
            SELECT 1 FROM staging.{target_table} s
            WHERE {coverage_cols}
        ) THEN DELETE"""

    def diff_output_clause(self, schema, target_table: str, run_id: str) -> str:
        output_cols = ", ".join(
            f"COALESCE(inserted.{c}, deleted.{c}) AS {c}" for c in schema.columns
        )
        col_list = ", ".join(schema.columns)
        return f"""OUTPUT '{run_id}' AS run_id, $action AS action, {output_cols}
            INTO diff.{target_table} (run_id, action, {col_list})"""


@dataclass(frozen=True)
class PrimaryCompositeKey:
    """A multi-column PRIMARY KEY. TableSchema.key normally holds a single
    column name; this marks the (rarer) case where the natural key spans
    more than one column, e.g. a pure junction table with no surrogate id."""
    columns: tuple[str, ...]

    def __post_init__(self):
        if len(self.columns) < 2:
            raise ValueError("PrimaryCompositeKey requires at least two columns")


@dataclass(frozen=True)
class CompositeKeySync(SyncStrategy):
    """
    For pure junction tables whose PrimaryCompositeKey covers every column,
    so there's no separate payload column left to UPDATE on a match (unlike
    DiffSync, which needs a key plus something besides the key). Matches on
    the real composite key columns directly in the MERGE ON clause -- no
    HASHBYTES/identity_hash column, unlike IdentityHashSync. A matched key
    means the row is already fully identical, so the only real transitions
    are INSERT (new combination) and DELETE (stale combination, scoped by
    coverage_scope_columns) -- the same shape as IdentityHashSync's
    match_clause, just without the hash.
    """
    coverage_scope_columns: tuple[str, ...]

    def key_clause(self, schema) -> str:
        assert isinstance(schema.key, PrimaryCompositeKey), (
            f"{schema.__table_name__}: CompositeKeySync requires a PrimaryCompositeKey"
        )
        conds = " AND ".join(f"target.{c} = source.{c}" for c in schema.key.columns)
        return f"ON {conds}"

    def dedup_match_columns(self, schema) -> tuple[str, ...]:
        assert isinstance(schema.key, PrimaryCompositeKey), (
            f"{schema.__table_name__}: CompositeKeySync requires a PrimaryCompositeKey"
        )
        return schema.key.columns

    def match_clause(self, schema, target_table: str) -> str:
        if not self.coverage_scope_columns:
            raise ValueError(
                f"coverage_scope_columns must be set for CompositeKeySync on {target_table}; "
                "unconditional NOT MATCHED BY SOURCE deletes are disallowed."
            )
        coverage_cols = " AND ".join(
            f"target.{col} = s.{col}" for col in self.coverage_scope_columns
        )
        return f"""WHEN NOT MATCHED BY SOURCE AND EXISTS (
            SELECT 1 FROM staging.{target_table} s
            WHERE {coverage_cols}
        ) THEN DELETE"""

    def diff_output_clause(self, schema, target_table: str, run_id: str) -> str:
        output_cols = ", ".join(
            f"COALESCE(inserted.{c}, deleted.{c}) AS {c}" for c in schema.columns
        )
        col_list = ", ".join(schema.columns)
        return f"""OUTPUT '{run_id}' AS run_id, $action AS action, {output_cols}
            INTO diff.{target_table} (run_id, action, {col_list})"""


@dataclass(frozen=True)
class DiffSync(SyncStrategy):
    diff_columns: tuple[str, ...]

    def match_clause(self, schema, target_table: str) -> str:
        diff_cond = " OR ".join(f"target.{c} <> source.{c}" for c in self.diff_columns)
        update_set = ", ".join(
            f"target.{c} = source.{c}" for c in schema.columns if c != schema.key
        )
        return f"WHEN MATCHED AND ({diff_cond}) THEN UPDATE SET {update_set}"

    def diff_output_clause(self, schema, target_table: str, run_id: str) -> str:
        return f"""OUTPUT '{run_id}' AS run_id, $action AS action,
            COALESCE(inserted.{schema.key}, deleted.{schema.key}) AS {schema.key}
            INTO diff.{target_table} (run_id, action, {schema.key})"""

@dataclass(frozen=True)
class DefaultSync(SyncStrategy):
    """No identity hash, no diff-column tracking — plain key-based upsert."""

    def match_clause(self, schema, target_table: str) -> str:
        update_set = ", ".join(
            f"target.{c} = source.{c}" for c in schema.columns if c != schema.key
        )
        return f"WHEN MATCHED THEN UPDATE SET {update_set}"

    def diff_output_clause(self, schema, target_table: str, run_id: str) -> str:
        return ""

@dataclass(frozen=True)
class DeferredResolution:
    match_columns: tuple[str, ...]
    staging_columns: tuple[str, ...]

    def __post_init__(self):
        if not (self.match_columns and self.staging_columns):
            raise ValueError(
                f"match_columns and staging_columns must both be set or both be None"
            )

@dataclass(frozen=True)
class ForeignKey:
    name: str
    columns: tuple[str, ...]           # local columns
    ref_table: str
    ref_columns: tuple[str, ...]       # referenced columns
    deferred: Optional[DeferredResolution] = None
    on_delete: Optional[str] = None    # "CASCADE", "SET NULL", etc.


    def to_sql(self) -> str:
        cols = ", ".join(self.columns)
        ref_cols = ", ".join(self.ref_columns)
        sql = (f"CONSTRAINT {self.name} FOREIGN KEY ({cols}) "
               f"REFERENCES {self.ref_table}({ref_cols})")
        if self.on_delete:
            sql += f" ON DELETE {self.on_delete}"
        return sql


@dataclass(frozen=True)
class UniqueConstraint:
    name: str
    columns: tuple[str, ...]

    def to_sql(self) -> str:
        return f"CONSTRAINT {self.name} UNIQUE ({', '.join(self.columns)})"


@dataclass(frozen=True)
class CheckConstraint:
    name: str
    expression: str  # raw SQL predicate, e.g. "start_pos <= end_pos"

    def to_sql(self) -> str:
        return f"CONSTRAINT {self.name} CHECK ({self.expression})"


Constraint = Union[ForeignKey, UniqueConstraint, CheckConstraint]

@dataclass(frozen=True)
class ColumnPlan:
    production_col: str     # real column name in the final table
    staging_col: str        #column actually populated at staging-insert time
    staging_type: str       #SQL type for staging column
    deffered_fk: Optional["ForeignKey"] = None  #set if this column's real value is resolved post-staging
    reused_column: bool = False


@dataclass(frozen=True)
class TableSchema:
    columns: Mapping[str, str]
    __table_name__: str
    key: Optional[str | PrimaryCompositeKey] = None          # PK column(s), if any
    auto_id: bool = False
    sync: SyncStrategy = field(default_factory=DefaultSync)
    constraints: tuple[Constraint, ...] = ()

    def __post_init__(self):
        if self.key is not None:
            key_cols = self.key.columns if isinstance(self.key, PrimaryCompositeKey) else (self.key,)
            missing = set(key_cols) - set(self.columns)
            if missing:
                raise ValueError(f"key column(s) {missing} not in columns")

        if isinstance(self.sync, DiffSync):
            if self.key is None:
                raise ValueError("DiffSync requires a key")
            missing = set(self.sync.diff_columns) - set(self.columns)
            if missing:
                raise ValueError(f"diff_columns not in columns: {missing}")

        if isinstance(self.sync, IdentityHashSync):
            if not self.sync.coverage_scope_columns:
                raise ValueError("coverage_scope columns cannot be empty")
            if not self.sync.identity_hash:
                raise ValueError("identity_hash cannot be empty")
            missing = set(self.sync.identity_hash) - set(self.columns)
            if missing:
                raise ValueError(f"identity_hash fields not in columns: {missing}")

        if isinstance(self.sync, CompositeKeySync):
            if not isinstance(self.key, PrimaryCompositeKey):
                raise ValueError("CompositeKeySync requires a PrimaryCompositeKey")
            if not self.sync.coverage_scope_columns:
                raise ValueError("coverage_scope columns cannot be empty")

@dataclass
class TableClearOrder:
    order: list[str]              # flat topological order, children before parents
    levels: list[list[str]]       # batched: each level has no deps within itself
    cycles: list[set[str]]        # multi-table circular FK groups (size > 1)
    self_refs: set[str]           # tables with self-referencing FKs (e.g. kegg_class)
