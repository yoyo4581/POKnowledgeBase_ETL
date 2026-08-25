from src.builders.SQL.schema.definitions import table_schemas
from src.builders.SQL.schema.types import DefaultSync, TableSchema
from src.builders.SQL.schema.strategies import (DBSchema, DbOpsStrategy, SQLStageUpsertDiff, SQLStageUpsert, SQLStageUpsertConstraints)


def _select_strategy(schema) -> DbOpsStrategy:
    """
    Selection order: constraints > sync type.

    Constraints take priority over sync type, since
    SQLStageUpsertConstraints degrades gracefully for tables with plain FKs
    (no match_columns) -- resolve_staging_plan returns an identity mapping and
    _using_clause falls back to the plain staging table unchanged, so
    reactions/reaction_participants route here safely alongside the deferred-FK
    tables (kegg_class, PathwayData).
 
    NOTE: for a constrained table whose sync is IdentityHashSync with a
    diff-relevant subset (e.g. an identity_hash narrower than all columns),
    SQLStageUpsertConstraints' dedup match will use the FULL column set, not
    just identity_hash -- unlike SQLStageUpsertDiff, which matches on
    identity_hash alone. Every table in table_schemas today happens to have
    identity_hash cover all its columns, so this doesn't currently bite, but
    it's a real behavioral gap if a future constrained IdentityHashSync table
    adds a column that shouldn't count toward dedup identity.
    """
    
    if schema.constraints:
        return SQLStageUpsertConstraints()
    if isinstance(schema.sync, DefaultSync):
        return SQLStageUpsert()
    return SQLStageUpsertDiff()


class TableManager:
    def __init__(self, schema: TableSchema, strategy: DbOpsStrategy):
        self.schema = schema
        self.strategy = strategy

    def create_production_environment(self) -> list[str]:
        return [self.strategy.create_table(self.schema, kind=DBSchema.PRODUCTION)] + \
                self.strategy.create_indexes(self.schema, kind=DBSchema.PRODUCTION)

    def create_staging_table(self) -> str:
        return self.strategy.create_table(self.schema, kind=DBSchema.STAGING)

    def create_diff_table(self) -> str | None:
        if isinstance(self.schema.sync, DefaultSync):
            return None
        return self.strategy.create_table(self.schema, kind=DBSchema.DIFF)

    def create_staging_environment(self) -> list[str]:
        """Full staging environment: staging table, plus diff table if the sync strategy needs one."""
        statements = [self.create_staging_table()] + self.strategy.create_indexes(self.schema, kind=DBSchema.STAGING)
        diff_sql = self.create_diff_table()
        if diff_sql is not None:
            statements.append(diff_sql)
        return statements

    def wipe_staging_environment(self) -> list[str]:
        statements = [self.strategy.wipe_data(self.schema, kind=DBSchema.STAGING)]
        if not isinstance(self.schema.sync, DefaultSync):
            wipe_diff = self.strategy.wipe_data(self.schema, kind=DBSchema.DIFF)
            statements.append(wipe_diff)
        return statements
 
table_managers: dict[str, TableManager] = {
    name: TableManager(schema, _select_strategy(schema))
    for name, schema in table_schemas.items()
}