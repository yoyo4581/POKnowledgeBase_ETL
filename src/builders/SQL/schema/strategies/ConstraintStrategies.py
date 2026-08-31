from src.builders.SQL.schema.table_utils import resolve_staging_plan
from src.builders.SQL.schema.types import TableSchema, StagingQuery, IdentityHashSync
from .base import DbOpsStrategy

class SQLStageUpsertConstraints(DbOpsStrategy):
    """
    Tables with deferred-FK constraints (match_columns/staging_columns), e.g. kegg_class.
    Staging carries shadow natural-key columns instead of the real FK value;
    the MERGE's USING clause resolves those shadows against PRODUCTION inline
    (see _using_clause), so no separate pre-merge UPDATE step is needed.
    """

    def stage_data(self, schema: TableSchema) -> StagingQuery:
        assert schema.constraints, "Schema must contain constraints"
        plan = resolve_staging_plan(schema)
        staging_cols = [p.staging_col for p in plan if not p.reused_column]

        if schema.auto_id or not schema.key:
            match_cols = []
            for p in plan:
                if p.deffered_fk is None:
                    match_cols.append(p.production_col)
                else:
                    match_cols.append(p.deffered_fk.deferred.staging_columns[0])
        else:
            match_cols = [schema.key]

        return self._build_dedup_insert(schema, staging_cols, match_cols)

    def _using_clause(self, schema: TableSchema) -> str:
        """
        Resolves deferred FK shadow columns inline via LEFT JOIN against
        PRODUCTION, so the MERGE's `source.<col>` binds to the real FK value
        instead of the shadow natural-key column staged under it.

        The natural key (deferred.match_columns) is not guaranteed unique on
        ref_table -- e.g. kegg_class.name is only unique combined with
        parent_id, and a leaf node (a pathway title) can legitimately sit
        under more than one parent when KEGG cross-lists it. A plain join
        would fan a single staging row out into one source row per match,
        which MERGE's WHEN NOT MATCHED THEN INSERT then tries to insert
        twice, violating the target's primary key. Joining against a
        ROW_NUMBER()-deduped view of ref_table instead picks exactly one
        match per natural key -- the earliest-created row (lowest ref
        column), i.e. first-encountered -- so the join can never fan out.

        Limitation: if the referenced row is introduced in the SAME batch as the
        row that points to it (e.g. a new kegg_class parent and its new child in
        one run), this will NOT resolve it — the parent has no class_id yet since
        it hasn't been inserted into production. This only resolves refs against
        rows that already exist in dbo.{ref_table}; a same-batch parent+child
        pair requires a second staging run after the parent's batch upserts.
        """
        plan = resolve_staging_plan(schema)
        if not any(p.deffered_fk is not None for p in plan):
            return super()._using_clause(schema)

        select_exprs = []
        joins = []
        alias_by_fk: dict[str, str] = {}

        for p in plan:
            if p.deffered_fk is None:
                select_exprs.append(f"c.{p.production_col}")
                continue

            fk = p.deffered_fk
            alias = alias_by_fk.get(fk.name)
            if alias is None:
                alias = f"fk_{fk.name}"
                alias_by_fk[fk.name] = alias
                join_cond = " AND ".join(
                    f"{alias}.{match_col} = c.{staging_col}"
                    for match_col, staging_col in zip(fk.deferred.match_columns, fk.deferred.staging_columns)
                )
                tie_break_col = fk.ref_columns[0]
                partition_cols = ", ".join(fk.deferred.match_columns)
                dedup_ref = (
                    f"(SELECT *, ROW_NUMBER() OVER ("
                    f"PARTITION BY {partition_cols} ORDER BY {tie_break_col}"
                    f") AS rn FROM dbo.{fk.ref_table}) AS {alias}"
                )
                joins.append(f"LEFT JOIN {dedup_ref} ON {join_cond} AND {alias}.rn = 1")

            idx = fk.columns.index(p.production_col)
            ref_col = fk.ref_columns[idx]
            select_exprs.append(f"{alias}.{ref_col} AS {p.production_col}")

        select_sql = ", ".join(select_exprs)
        join_sql = "\n        ".join(joins)
        return f"""(
        SELECT {select_sql}
        FROM staging.{schema.__table_name__} AS c
        {join_sql}
    )"""

    def upsert_data(self, schema: TableSchema, run_id: str) -> list[str]:
        statements = []

        if isinstance(schema.sync, IdentityHashSync):
            statements.append(self.compute_identity_hashes(schema, schema.sync))

        statements.append(self.build_merge_query(schema, run_id))
        return statements