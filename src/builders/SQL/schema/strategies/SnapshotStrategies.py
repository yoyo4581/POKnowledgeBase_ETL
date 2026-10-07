from .base import DbOpsStrategy
from src.builders.SQL.schema.types import TableSchema, StagingQuery, SnapshotSync


class SQLSnapshotReplace(DbOpsStrategy):
    """
    SnapshotSync: stage the whole snapshot, then replace dbo with it.

    upsert_data emits DELETE + INSERT...SELECT rather than a MERGE. Both
    statements run on one cursor under a single commit (SQL_ETL.upsert_data),
    so the swap is atomic: readers see the old table or the new one, never an
    empty one, and a failure mid-load rolls back to the previous snapshot.

    This MUST be called exactly once, after every row is staged -- the
    consumer stages all batches first and swaps at the end (see
    consume_entrez_uniprot_map). Calling it per batch would make each batch
    delete the previous one's rows, which is the very failure SnapshotSync
    exists to rule out.
    """

    def stage_data(self, schema: TableSchema) -> StagingQuery:
        assert isinstance(schema.sync, SnapshotSync), "Schema must contain a SnapshotSync"
        insert_cols = list(schema.columns.keys())
        match_cols = list(schema.sync.dedup_match_columns(schema))
        return self._build_dedup_insert(schema, insert_cols, match_cols)

    def upsert_data(self, schema: TableSchema, run_id: str) -> list[str]:
        assert isinstance(schema.sync, SnapshotSync), "Schema must contain a SnapshotSync"
        table = schema.__table_name__
        cols = ", ".join(schema.columns.keys())
        return [
            f"DELETE FROM dbo.{table};",
            f"INSERT INTO dbo.{table} ({cols}) SELECT {cols} FROM staging.{table};",
        ]
