from src.builders.SQL.schema.table_utils import resolve_staging_plan
from .base import DbOpsStrategy
from src.builders.SQL.schema.types import (
    TableSchema, StagingQuery, IdentityHashSync, DefaultSync, DiffSync, CompositeKeySync, PrimaryCompositeKey
)


class SQLStageUpsert(DbOpsStrategy):
    """DefaultSync: keyed tables, no diffing, no constraints-deferral."""

    def stage_data(self, schema: TableSchema) -> StagingQuery:
        assert isinstance(schema.key, str), "Schema must contain a single-column key"
        assert isinstance(schema.sync, DefaultSync), "Schema must contain a DefaultSync"
        insert_cols = list(schema.columns.keys())
        return self._build_dedup_insert(schema, insert_cols, match_cols=[schema.key])

    def upsert_data(self, schema: TableSchema, run_id: str) -> list[str]:
        assert schema.key is not None, "Schema must contain a key parameter."
        assert isinstance(schema.sync, DefaultSync), "Schema must contain a DefaultSync"
        return [self.build_merge_query(schema, run_id)]


class SQLStageUpsertDiff(DbOpsStrategy):
    """IdentityHashSync / DiffSync / CompositeKeySync tables with no deferred-FK constraints."""

    def stage_data(self, schema: TableSchema) -> StagingQuery:
        insert_cols = list(schema.columns.keys())
        match schema.sync:
            case IdentityHashSync():
                assert schema.key is None, "Schema must not contain a key"
                # Match on the full identity tuple: for keyless tables the identity
                # hash IS the row's identity, so two rows differing anywhere in it
                # are genuinely different rows, not a key collision to guard against.
            case DiffSync():
                assert schema.key, "Schema must contain a key"
                # Match on key ALONE, not key+diff_columns. Two staging rows sharing
                # a key (even with different diff-column values) would make MERGE
                # fail with "same row more than once" -- dedup must stay key-scoped.
                # insert_cols stays the FULL column set: narrowing it to diff_columns
                # silently drops any column not listed there (e.g. PathwayKGMLMeta's
                # last_checked never made it into staging under the old version).
            case CompositeKeySync():
                assert isinstance(schema.key, PrimaryCompositeKey), "Schema must contain a PrimaryCompositeKey"
                # Match on the full composite key: every column is part of the
                # key, so there's no separate payload to narrow the match to.
            case _:
                raise ValueError(
                    f"{schema.__table_name__}: sync strategy {type(schema.sync).__name__} "
                    f"should not use this strategy."
                )
        match_cols = list(schema.sync.dedup_match_columns(schema))
        return self._build_dedup_insert(schema, insert_cols, match_cols)

    def upsert_data(self, schema: TableSchema, run_id: str) -> list[str]:
        statements = []
        match schema.sync:
            case IdentityHashSync():
                assert schema.key is None, "Schema must not contain a key"
                statements.append(self.compute_identity_hashes(schema, schema.sync))
                statements.append(self.build_merge_query(schema, run_id))

            case DiffSync():
                assert schema.key, "Schema must contain a key"
                statements.append(self.build_merge_query(schema, run_id))

            case CompositeKeySync():
                assert isinstance(schema.key, PrimaryCompositeKey), "Schema must contain a PrimaryCompositeKey"
                statements.append(self.build_merge_query(schema, run_id))

            case _:
                raise ValueError(
                    f"{schema.__table_name__}: sync strategy {type(schema.sync).__name__} "
                    f"should not use this strategy."
                )
        return statements
