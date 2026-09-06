import uuid
import logging
from typing import Literal, Optional, Iterator
import pyodbc

from functools import wraps
from src.builders.SQL.schema import table_schemas, resolve_clear_order, AnnotationTables
from src.builders.SQL.schema.types import TableSchema, IdentityHashSync, DiffSync, DefaultSync
from src.builders.SQL.schema import table_managers, TableManager

logger = logging.getLogger(__name__)

class SQLOperationError(Exception):
    def __init__(self, message: str, *, original: Exception, label: str):
        super().__init__(message)
        self.original = original
        self.label = label

def sql_safe(label=None):
    def decorator(fn):
        @wraps(fn)
        def wrapper(self, *args, **kwargs):
            try:
                return fn(self, *args, **kwargs)
            except pyodbc.Error as e:
                self.conn.rollback()
                op_label = label or fn.__name__
                logger.error(f"Error in {op_label} (args={args}, kwargs={kwargs}): {e}")
                raise SQLOperationError(f"SQL operation failed: {op_label}", original=e, label=op_label) from e
        return wrapper
    return decorator


class SQL_State:
    def __init__(self, conn: pyodbc.Connection, run_id: str):
        self.run_id = run_id
        self.conn = conn
        self._staging_ready: set[str] = set()   # per-run cache, avoids re-querying INFORMATION_SCHEMA
        self._production_verified = False
        self.ready = self._check_connection()
        if self.ready:
            self._enforce_production_state()

    def _check_connection(self) -> bool:
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except pyodbc.Error as e:
            logger.error("SQL_State connection check failed: %s", e)
            return False

    def _assert_ready(self):
        if not self.ready:
            raise RuntimeError("SQL_State is not ready (database connection failed)")

    def get_manager(self, target_table: str) -> TableManager:
        try:
            return table_managers[target_table]
        except KeyError:
            raise ValueError(f"No TableManager registered for '{target_table}'")

    # ------------------------------------------------------------------ #
    # Staging readiness (called by SQL_ETL.stage_data)
    # ------------------------------------------------------------------ #

    def ensure_staging_environment(self) -> bool:
        """
        Reset the staging area for a fresh pipeline run:
        1. Clears rows from every staging/diff table that already exists.
        2. Creates any staging tables that don't exist yet (FK-safe order).

        Intended to be called once at the start of a run (e.g. from
        SQL_ETL before the first stage_data call), not per-table like
        ensure_staging_ready.
        """
        self._assert_ready()

        missing = self.missing_tables_check(kind='staging')
        existing = [t for t in table_schemas if t not in missing]

        for table_name in existing:
            schema = table_schemas[table_name]
            if not isinstance(schema.sync, DefaultSync):
                self._clear_table_rows(table_name, "diff")
            self._clear_table_rows(table_name, "staging")

        if missing:
            logger.info("Creating missing staging tables: %s", missing)
            ordered = self._creation_order(missing)
            for table_name in ordered:
                manager = self.get_manager(table_name)
                for sql_staging in manager.create_staging_environment():
                    self.create_table(sql_staging, table_name=table_name)

            still_missing = self.missing_tables_check(kind='staging')
            if still_missing:
                raise RuntimeError(
                    f"Staging schema enforcement incomplete, still missing: {still_missing}"
                )

        self._staging_ready = set(table_schemas.keys())
        return True


    def ensure_staging_ready(self, table_name: str) -> bool:
        """Public entry point for SQL_ETL. Cheap on repeat calls within a run."""
        if table_name in self._staging_ready:
            return True

        missing = self.missing_tables_check(kind='staging')
        if table_name in missing:
            logger.info("Creating missing staging table: %s", table_name)
            manager = self.get_manager(table_name)
            for sql_staging in manager.create_staging_environment():
                self.create_table(sql_staging, table_name=table_name)

            still_missing = self.missing_tables_check(kind='staging')
            if table_name in still_missing:
                raise RuntimeError(f"Failed to create staging table '{table_name}'")

        self._staging_ready.add(table_name)
        return True

    # ------------------------------------------------------------------ #
    # Production readiness
    # ------------------------------------------------------------------ #

    def _creation_order(self, tables: list[str]) -> list[str]:
        """
        Order tables parent-before-child for CREATE TABLE.
        resolve_clear_order gives a child-before-parent order (safe for
        deletion / dropping FK-constrained tables); creation needs the
        reverse, filtered down to just the tables we actually need.
        """
        full_clear_order = resolve_clear_order(table_schemas).order
        wanted = set(tables)
        return [t for t in reversed(full_clear_order) if t in wanted]

    def _enforce_production_state(self):
        """Create any missing production tables, in FK-safe order, and verify."""
        missing = self.missing_tables_check(kind='dbo')
        if not missing:
            self._production_verified = True
            return

        logger.info("Missing production tables: %s", missing)
        ordered = self._creation_order(missing)

        for table_name in ordered:
            manager = self.get_manager(table_name)
            for sql_production in manager.create_production_environment():
            # reraise=True on create_table means this stops at the first
            # real failure instead of continuing to build in a possibly
            # broken order and masking the root cause.
                self.create_table(sql_production, table_name=table_name)

        still_missing = self.missing_tables_check(kind='dbo')
        if still_missing:
            raise RuntimeError(
                f"Production schema enforcement incomplete, still missing: {still_missing}"
            )
        self._production_verified = True

    @sql_safe(label="Unable to create sql data table")
    def create_table(self, sql: str, table_name: Optional[str] = None):
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
        self.conn.commit()
        logger.info("Created sql table%s", f" {table_name}" if table_name else "")
        return True

    def _batch_receive(self, fetch_query: str, table_name: str, batch_size: int = 10000)->Iterator[list[dict]]:
        with self.conn.cursor() as cursor:
            cursor.execute(fetch_query)
            columns = [col[0] for col in cursor.description]

            row_count = 0
            while rows := cursor.fetchmany(batch_size):
                row_count += len(rows)
                yield [dict(zip(columns, row)) for row in rows]

            if row_count == 0:
                raise ValueError(f"Empty data table {table_name}")

    @sql_safe(label="fetching data from SQL dbo or staging table")
    def fetch_data(self, table_name: str, kind: Literal["dbo", "staging", "diff"], batch_size: int = 10000)->Iterator[list[dict]]:
        schema = table_schemas[table_name]
        if isinstance(schema, DefaultSync) and kind == 'diff':
            raise ValueError("Table does not have a diff sync strategy")

        fetch_query = f"SELECT * FROM {kind}.{table_name}"
        yield from self._batch_receive(fetch_query, table_name)

    @sql_safe(label="fetching data from SQL dbo using diff table of entities")
    def fetch_diff_entities(self, table_name: str)->Iterator[list[dict]]:
        schema = table_schemas[table_name]
        assert isinstance(schema.sync, (DiffSync, IdentityHashSync)), ValueError("Table doesn't have a schema value")

        # Tables that key off entity_id (EntityPathMem, reaction_participants)
        # don't carry entity_type themselves, so pull it from entities -- callers
        # like PathwayMembership need it to know which node label entity_id refers to.
        entity_type_join = ""
        if table_name != "entities" and "entity_id" in schema.columns:
            entity_type_join = "INNER JOIN dbo.entities as e ON d.entity_id = e.entity_id"

        fetch_query = f"""
        SELECT *
        FROM diff.{table_name} as d
        INNER JOIN dbo.{table_name} as p
        ON d.entity_id = p.entity_id
        {entity_type_join}
        """
        yield from self._batch_receive(fetch_query, table_name)

    @sql_safe(label="fetching data from SQL diff table of interactions")
    def fetch_diff_interactions(self)->Iterator[list[dict]]:
        """
        interactions has no entity_id column (it keys on source_id/target_id),
        so it can't go through fetch_diff_entities's entity_id-based join.
        Interactions can occur between different entity types (e.g. Gene-Compound,
        not just Gene-Gene), so both endpoints need their own entity_type, pulled
        from dbo.entities via two separate joins.
        """
        fetch_query = """
        SELECT d.*,
               es.entity_type AS source_entity_type,
               et.entity_type AS target_entity_type
        FROM diff.interactions as d
        INNER JOIN dbo.entities as es ON d.source_id = es.entity_id
        INNER JOIN dbo.entities as et ON d.target_id = et.entity_id
        """
        yield from self._batch_receive(fetch_query, "interactions")


    @sql_safe(label="loading data from diff table")
    def load_from_diff(self, table_name: AnnotationTables):
        schema = table_schemas[table_name]
        assert schema.key is not None, ValueError("Table doesn't have a matching key id with entities entity_id")

        fetch_query = f"""
        SELECT *
        FROM diff.entities as d
        INNER JOIN dbo.{table_name} as p
        ON d.entity_id = p.{schema.key}
        """
        yield from self._batch_receive(fetch_query, table_name)

    def missing_tables_check(self, kind: Literal['dbo', 'staging']) -> list[str]:
        self._assert_ready()
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?",
                kind,
            )
            existing = {row[0] for row in cursor.fetchall()}

        missing = [t for t in table_schemas if t not in existing]
        if missing:
            logger.info("Missing tables (%s): %s", kind, missing)
        return missing

    # ------------------------------------------------------------------ #
    # Wipe / clear
    # ------------------------------------------------------------------ #

    def wipe_environment(self, kind: Literal["staging", "dbo"]):
        table_clear_order = resolve_clear_order(table_schemas)
        for target_table in table_clear_order.order:
            table_schema = table_schemas[target_table]
            if not isinstance(table_schema.sync, DefaultSync):
                self._clear_table_rows(target_table, "diff")

            if table_schema.constraints:
                self._drop_table_with_fks(target_table, schema=kind)
                continue

            self._clear_table_rows(target_table, kind)

        self._staging_ready.clear()
        self._production_verified = False

        if kind == "dbo":
            # constrained tables were structurally DROPped above, not just
            # cleared -- rebuild the schema so it's ready for the next run.
            self._enforce_production_state()

    def _clear_table_rows(self, table_name: str, schema: Literal["staging", "diff", "dbo"] = "staging"):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM {schema}.{table_name}")
            self.conn.commit()
            logger.info("Cleared %s table: %s", schema, table_name)

        except pyodbc.Error as e:
            self.conn.rollback()
            logger.error("Failed to clear %s.%s: %s", schema, table_name, e)
            raise

    def _drop_table_with_fks(self, table_name: str, schema: str = "dbo"):
        find_fks_sql = """
            SELECT
                fk.name AS fk_name,
                OBJECT_SCHEMA_NAME(fk.parent_object_id) AS child_schema,
                OBJECT_NAME(fk.parent_object_id) AS child_table
            FROM sys.foreign_keys fk
            WHERE fk.referenced_object_id = OBJECT_ID(?)
        """
        with self.conn.cursor() as cursor:
            cursor.execute(find_fks_sql, f"{schema}.{table_name}")
            referencing_fks = cursor.fetchall()

            for fk_name, child_schema, child_table in referencing_fks:
                logger.info("Dropping FK %s on %s.%s", fk_name, child_schema, child_table)
                cursor.execute(f"ALTER TABLE {child_schema}.{child_table} DROP CONSTRAINT {fk_name};")

            logger.info("Dropping table %s.%s", schema, table_name)
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table_name};")

        self.conn.commit()