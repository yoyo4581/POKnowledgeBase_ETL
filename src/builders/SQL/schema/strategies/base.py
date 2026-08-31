from abc import ABC, abstractmethod
from src.builders.SQL.schema.types import (
    TableSchema, DiffSync, IdentityHashSync, CompositeKeySync, PrimaryCompositeKey, DBSchema, StagingQuery
)
from src.builders.SQL.schema.table_utils import resolve_staging_plan


# Step1: Every strategy should have its staging plan resolved. A staging plan is simple, it only controls what gets inserted.
class DbOpsStrategy(ABC):

    @abstractmethod
    def stage_data(self, schema: TableSchema) -> StagingQuery:
        """Handles staging, temporary tables, or file loading."""
        pass

    @abstractmethod
    def upsert_data(self, schema: TableSchema, run_id: str) -> list[str]:
        """Merges staged data into the final target table."""
        pass

    # ---------- shared helpers ----------

    def _guarded_create(self, full_name: str, body_sql: str) -> str:
        """Wraps a CREATE TABLE body in an existence check, so create_table calls are idempotent."""
        schema_name, table_name = full_name.split(".")
        return f"""
        IF NOT EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
        )
        BEGIN
            {body_sql}
        END
        """

    def _build_dedup_insert(
        self,
        schema: TableSchema,
        insert_cols: list[str],
        match_cols: list[str],
    ) -> StagingQuery:
        """
        Generates a dedup-guarded INSERT into staging.
        insert_cols: full set of columns to insert (must cover every staging column —
                     narrowing this to a "dedup-relevant" subset silently drops data).
        match_cols: columns compared in WHERE NOT EXISTS to detect a duplicate.
                    For single-column-keyed tables this MUST be just [schema.key]
                    — matching on more than the key risks two staging rows
                    sharing a key with different payloads, which MERGE rejects
                    outright.
        multi_col_match: True -> null-safe multi-column AND match (ISNULL-guarded),
                          used when there's no single key column to match on
                          (no key, auto_id, or a PrimaryCompositeKey).
                          False -> single equality match on match_cols[0].
        """
        col_insert = ", ".join(insert_cols)
        num_insert = len(insert_cols)

        multi_col_match = schema.auto_id or not schema.key or isinstance(schema.key, PrimaryCompositeKey)

        if multi_col_match:
            match_statement = " AND ".join(f"ISNULL(t.{c}, N'') = ISNULL(?, N'')" for c in match_cols)
        else:
            match_statement = f"t.{match_cols[0]} = ?"

        num_params = num_insert + len(match_cols)

        insert_sql = f"""
        INSERT INTO staging.{schema.__table_name__} ({col_insert})
        SELECT {", ".join(["?"] * num_insert)}
        WHERE NOT EXISTS (
            SELECT 1 FROM staging.{schema.__table_name__} t
            WHERE {match_statement}
        )
        """
        return StagingQuery(
            sql=insert_sql,
            insert_cols=tuple(insert_cols),
            match_cols=tuple(match_cols),
            num_params=num_params,
        )

    def compute_identity_hashes(self, schema: TableSchema, sync: IdentityHashSync) -> str:
        """Generates the UPDATE that fills staging.identity_hash from sync.identity_hash columns."""
        identity_cols = sync.identity_hash
        concat_expr = " + '|' + ".join(
            f"COALESCE(CAST({col} AS NVARCHAR(MAX)), '∅')" for col in identity_cols
        )
        return f"""
        UPDATE staging.{schema.__table_name__}
        SET identity_hash = HASHBYTES(
            'SHA2_256',
            {concat_expr}
        );
        """

    def _using_clause(self, schema: TableSchema) -> str:
        """Source of the MERGE's USING clause. Plain staging table by default;
        strategies that need to reshape staged rows (e.g. resolving deferred-FK
        shadow columns) override this to supply a derived table instead."""
        return f"staging.{schema.__table_name__}"

    def build_merge_query(self, schema: TableSchema, run_id: str) -> str:
        sync = schema.sync
        insert_cols = ", ".join(sync.insert_columns(schema))
        insert_vals = ", ".join(sync.insert_values(schema))
        key_clause = sync.key_clause(schema)
        match_clause = sync.match_clause(schema, schema.__table_name__)
        diff_out = sync.diff_output_clause(schema, schema.__table_name__, run_id)
        not_match = f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"

        return f"""
        MERGE INTO dbo.{schema.__table_name__} AS target
        USING {self._using_clause(schema)} AS source
        {key_clause}
        {match_clause}
        {not_match}
        {diff_out};
        """

    def _build_diff_table_sql(self, schema: TableSchema, full_name: str, key_columns: dict[str, str]) -> str:
        col_statement = ",\n    ".join(f"{col} {dtype}" for col, dtype in key_columns.items())
        body = f"""CREATE TABLE {full_name} (
                run_id UNIQUEIDENTIFIER DEFAULT NEWID(),
                action NVARCHAR(10),
                {col_statement}
            );"""
        return self._guarded_create(full_name, body)

    #--------indexing--------------
    def create_indexes(self, schema: TableSchema, kind: DBSchema) -> list[str]:
        """Generates CREATE INDEX statements needed for this schema/kind.
        Separate from create_table because indexes are additive DDL —
        keeping them apart makes it easy to add/drop indexes independently
        of table structure later."""
        full_name = f"{kind.value}.{schema.__table_name__}"
        statements = []

        if isinstance(schema.sync, IdentityHashSync):
            if kind in (DBSchema.STAGING, DBSchema.PRODUCTION):
                idx_name = f"ix_{schema.__table_name__}_{kind.value}_identity_hash"
                statements.append(self._guarded_create_index(idx_name, full_name, ("identity_hash",)))

        return statements

    def _guarded_create_index(self, index_name: str, full_name: str, columns: tuple[str, ...]) -> str:
        col_list = ", ".join(columns)
        return f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes WHERE name = '{index_name}'
        )
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX {index_name} ON {full_name} ({col_list}) WHERE identity_hash IS NOT NULL
        END
        """

    # ---------- create_table ----------

    def create_table(self, schema: TableSchema, kind: DBSchema) -> str:
        """Generates the CREATE TABLE statement for a schema."""
        full_name = f"{kind.value}.{schema.__table_name__}"

        if kind == DBSchema.DIFF:
            return self._build_diff_create(schema, full_name)
        if kind == DBSchema.STAGING:
            return self._build_staging_create(schema, full_name)
        return self._build_production_create(schema, full_name)

    def _build_staging_create(self, schema: TableSchema, full_name: str) -> str:
        """
        Staging columns come from resolve_staging_plan, not schema.columns directly:
        deferred-FK columns are staged under their shadow (natural-key) name/type,
        auto_id keys are excluded entirely, reused shadow columns (where a real
        column doubles as another FK's natural key) contribute no column of their
        own, and no PK/IDENTITY is ever applied here.
        """
        plan = resolve_staging_plan(schema)
        column_defs = [f"{p.staging_col} {p.staging_type}" for p in plan if not p.reused_column]
        if isinstance(schema.sync, IdentityHashSync):
            column_defs.append("identity_hash VARBINARY(32)")
        all_defs = ",\n     ".join(column_defs)
        body = f"CREATE TABLE {full_name} (\n   {all_defs}\n)"
        return self._guarded_create(full_name, body)

    def _build_diff_create(self, schema: TableSchema, full_name: str) -> str:
        match schema.sync:
            case DiffSync():
                assert schema.key is not None, "DiffSync requires a key (enforced in __post_init__)"
                key_columns = {schema.key: schema.columns[schema.key]}

            case IdentityHashSync():
                assert schema.sync.identity_hash, "IdentityHashSync requires identity_hash (enforced in __post_init__)"
                key_columns = {col: schema.columns[col] for col in schema.sync.identity_hash}

            case CompositeKeySync():
                assert isinstance(schema.key, PrimaryCompositeKey), "CompositeKeySync requires a PrimaryCompositeKey (enforced in __post_init__)"
                key_columns = {col: schema.columns[col] for col in schema.key.columns}

            case _:
                raise ValueError(
                    f"{schema.__table_name__}: sync strategy {type(schema.sync).__name__} "
                    f"has no diff-table representation"
                )

        return self._build_diff_table_sql(schema, full_name, key_columns)

    def _build_production_create(self, schema: TableSchema, full_name: str) -> str:
        column_defs = [f"{col_name} {col_type}" for col_name, col_type in schema.columns.items()]
        composite_pk_defs = []

        if schema.auto_id and schema.key:
            column_defs = [
                f"{schema.key} INT IDENTITY(1,1) PRIMARY KEY" if col_name == schema.key else col_def
                for col_name, col_def in zip(schema.columns.keys(), column_defs)
            ]
        elif isinstance(schema.key, PrimaryCompositeKey):
            composite_pk_defs.append(
                f"CONSTRAINT pk_{schema.__table_name__} PRIMARY KEY ({', '.join(schema.key.columns)})"
            )
        elif schema.key:
            column_defs = [
                f"{col_def} PRIMARY KEY" if col_name == schema.key else col_def
                for col_name, col_def in zip(schema.columns.keys(), column_defs)
            ]
        elif isinstance(schema.sync, IdentityHashSync):
            column_defs.append("identity_hash VARBINARY(32)")

        constraint_defs = [constraint_obj.to_sql() for constraint_obj in schema.constraints]
        all_defs = ",\n     ".join(column_defs + composite_pk_defs + constraint_defs)
        body = f"CREATE TABLE {full_name} (\n   {all_defs}\n)"
        return self._guarded_create(full_name, body)

    # ---------- wipe ----------

    def wipe_data(self, schema: TableSchema, kind: DBSchema) -> str:
        """Wipes rows (not structure) in any given table."""
        return f"DELETE FROM {kind.value}.{schema.__table_name__}"