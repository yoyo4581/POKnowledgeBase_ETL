import os
import uuid
from dotenv import load_dotenv
import pyodbc

from functools import wraps
import logging
from dataclasses import asdict
from typing import Sequence
from src.models.base import BaseSQLObject
from src.builders.SQL.SQLState import SQL_State

logger = logging.getLogger(__name__)


load_dotenv()

sql_conn_string = f"""
DRIVER={{ODBC Driver 18 for SQL Server}};
SERVER={os.getenv('sql_server')};
DATABASE={os.getenv('gene_database')};
TrustServerCertificate=yes;
UID={os.getenv('sql_uid')};
PWD={os.getenv('sql_pwd')};
Encrypt=no;
MARS_Connection=yes;
"""


def sql_safe(default=None, label=None):
    def decorator(fn):
        @wraps(fn)
        def wrapper(self, *args, **kwargs):
            try:
                return fn(self, *args, **kwargs)
            except pyodbc.Error as e:
                self.conn.rollback()
                logger.error(f"Error in {label or fn.__name__} (args={args}, kwargs={kwargs}): {e}")
                return default
        return wrapper
    return decorator


class SQL_ETL:
    """
    Thin executor: looks up the TableManager for a target table, asks its
    strategy for SQL, and runs it. Holds the only connection/cursor in this
    layer -- TableManager and DbOpsStrategy never touch the DB directly.
    """

    def __init__(self, run_id: str | None = None):
        self.run_id = run_id if run_id else str(uuid.uuid4())
        self._conn = None
        self._read_conn = None
        self._state = None

    @property
    def conn(self):
        if self._conn is None:
            self._conn = pyodbc.connect(sql_conn_string)
        return self._conn

    @property
    def read_conn(self):
        """
        Dedicated connection for long-lived streaming reads (fetch_data,
        fetch_diff_entities, ...), kept separate from `conn` so a write's
        commit on `conn` never invalidates an in-progress streaming fetch's
        open result set -- see SQL_State.__init__ for the failure mode.
        """
        if self._read_conn is None:
            self._read_conn = pyodbc.connect(sql_conn_string)
        return self._read_conn

    @property
    def sql_state(self):
        """Still used for reads (e.g. fetch_data on diff tables) -- writes now go through TableManager."""
        if self._state is None:
            self._state = SQL_State(self.conn, run_id=self.run_id, read_conn=self.read_conn)
        return self._state

    

    @sql_safe(default=None, label="staging data into table")
    def stage_data(self, target_table: str, data: Sequence[BaseSQLObject]):
        if not data:
            logger.error(f"No {target_table} data for this batch")
            return

        self.sql_state.ensure_staging_ready(target_table)
        manager = self.sql_state.get_manager(target_table)
        query = manager.strategy.stage_data(manager.schema)

        rows = [asdict(row) for row in data]
        try:
            params = [
                tuple(row[c] for c in query.insert_cols) + tuple(row[c] for c in query.match_cols)
                for row in rows
            ]
        except KeyError as e:
            raise ValueError(
                f"{target_table}: row is missing field {e} required by staging query "
                f"(insert_cols={query.insert_cols}, match_cols={query.match_cols}) -- "
                f"check that the BaseSQLObject model's field names match the staging "
                f"column names produced by resolve_staging_plan."
            )

        if params and len(params[0]) != query.num_params:
            raise ValueError(
                f"Param count mismatch for {target_table}: "
                f"query expects {query.num_params}, got {len(params[0])}"
            )

        with self.conn.cursor() as cursor:
            cursor.executemany(query.sql, params)
        self.conn.commit()
        logger.info(f"Staged {len(data)} rows into staging.{target_table} (duplicates skipped).")
        return True

    @sql_safe(default=None, label="upserting data into table")
    def upsert_data(self, target_table: str):
        manager = self.sql_state.get_manager(target_table)
        statements = manager.strategy.upsert_data(manager.schema, self.run_id)
        if isinstance(statements, str):
            statements = [statements]

        with self.conn.cursor() as cursor:
            for stmt in statements:
                cursor.execute(stmt)
        self.conn.commit()
        logger.info(f"Upserted staging.{target_table} into dbo.{target_table}.")
        return True

    @sql_safe(default=None, label="wiping staging data")
    def wipe_staging(self, target_table: str):
        """Clears staging.<target_table> (and diff.<target_table>, if the
        table's sync strategy uses one) -- call after upsert_data so the
        staged rows don't linger or get double-counted by the next batch."""
        manager = self.sql_state.get_manager(target_table)
        statements = manager.wipe_staging_environment()

        with self.conn.cursor() as cursor:
            for stmt in statements:
                cursor.execute(stmt)
        self.conn.commit()
        logger.info(f"Wiped staging.{target_table}.")
        return True