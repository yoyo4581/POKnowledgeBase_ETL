import os
import pyodbc
from typing import Literal, TYPE_CHECKING

from local_db.schema import *
from utils.colored_text import *

if TYPE_CHECKING:
    from local_db.SQL.SQLCaller import SQL_ETL


class SQLStateManager:
    """
    Will control all non-data intensive operations.
    Fetching, datatable creation, production table creation.
    And current table metadata.
    """
    def __init__(self, etl: "SQL_ETL"):
        self.etl = etl
        self.ready = False
        self._enforce_production_state()

    #-----------------
    # Internal guard
    #-----------------
    def _assert_ready(self):
        if not self.ready:
            print("SQLStateManager is not ready (database connection failed)")
    
    def _enforce_production_state(self):
        """
        Auto enforce production schema.
        Called from __init__ only.
        """
        self._assert_ready()
        missing = self._missing_tables_check(schema='dbo')
        if missing:
            print(f"Missing production tables: {missing}")
            self._produce_tables(missing)

        self.ready = True
            
    
    def _missing_tables_check(self, schema: Literal['dbo', 'staging']) -> list[str]:
        self._assert_ready()

        missing_tables = []
        for table_name in table_schemas.keys():

            with self.etl.conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT 1
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{schema}'
                        AND TABLE_NAME = '{table_name}'
                    """)
            
                if not cursor.fetchone():
                    missing_tables.append(table_name)

        print('missing_tables', missing_tables)
        return missing_tables

    def fetch_uniprot_id_mapping(self):
        """
        Fetches the current UniProt to Entrez mapping from the production table.
        Returns a list of tuples (entrez_id, uniprot_id).
        """
        self._assert_ready()
        query = "SELECT entrez_id, uniprot_id FROM dbo.EntrezUniprotMap"
        try:
            with self.etl.conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return [(row.entrez_id, row.uniprot_id) for row in rows]
        except pyodbc.Error as e:
            print(f"{RED} Error during fetching UniProt ID mapping: {e} {RESET}")
            return []

    def fetch_pathway_ids(self):
        pathway_fetch = """
        SELECT pathway_id, name
        FROM dbo.PathwayIds;
        """
        try:
            with self.etl.conn.cursor() as cursor:
                cursor.execute(pathway_fetch)
                rows = cursor.fetchall()
                return [PathwayIds(pathway_id=row.pathway_id, name=row.name) for row in rows]
            
        except pyodbc.Error as e:
            print(f"{RED} Error during fetching all pathways: {e} {RESET}")
            return None

    def is_occupied_diff(self, diff_table: str):
        try:
            with self.etl.conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT 1 FROM {diff_table}"
                )
                has_rows = cursor.fetchone() is not None
            return has_rows
        except pyodbc.Error as e:
            print(f"{RED} Error during check for {diff_table}: {e} {RESET}")

    
    def fetch_diff_table(self, diff_table: str):
        try:
            with self.etl.conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT * FROM diff.{diff_table}"
                )
                columns = [col[0] for col in cursor.description]
                diff_rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

                print(
                    f"{GREEN}Changes detected: {len(diff_rows)} {RESET}"
                )
                return diff_rows
        except pyodbc.Error as e:
            print(f"{RED} Error during fetch for {diff_table}: {e} {RESET}")
            return None

    def _build_tables(self, table_name: str, schema: Literal['dbo', 'staging']):
        table_schema = table_schemas[table_name]
        table_constraints = table_schema.get("constraints", [])
        key_col = table_schema.get("key", "")
        col_defs = []
        for col, typ in table_schema["columns"].items():
            if col == key_col:
                col_defs.append(f"{col} {typ} PRIMARY KEY")
            else:
                col_defs.append(f"{col} {typ}")
            
        if "identity_hash" in table_schema:
            col_defs.append("identity_hash VARBINARY(32)")

        fully_qualified = f"{schema}.{table_name}"

        try:
            ddl = f"""
            CREATE TABLE {fully_qualified} (
                {",\n   ".join(col_defs + table_constraints)}
            );
            """
            with self.etl.conn.cursor() as cursor:
                cursor.execute(ddl)

            self.etl.conn.commit()
            print(f"{GREEN}Table {fully_qualified} created successfully.{RESET}")
        except pyodbc.Error as e:
            print(f"{RED}Error creating table {fully_qualified}: {e}{RESET}")

    def _produce_tables(self, missing_tables=list[str]):
        """
        Create missing production tables.
        """
        for table_name in missing_tables:
            self._build_tables(table_name=table_name, schema='dbo')
            
    def create_diff_if_not_exists(self, target_table: str):
        if target_table in {"interactions", "EntityPathMem", "reaction_participants"}:
            col_schema = table_schemas[target_table]["columns"]
            col_statement = ",\n".join([f"{col} {typ}"for col, typ in col_schema.items()])
            create_diff = f"""
                IF NOT EXISTS (
                    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = 'diff' AND TABLE_NAME = '{target_table}'
                )
                BEGIN
                    CREATE TABLE diff.{target_table} (
                        run_id UNIQUEIDENTIFIER DEFAULT NEWID(),
                        action NVARCHAR(10),
                        {col_statement}
                    );
                END
            """
        else:
            key_col = table_schemas[target_table]["key"]
            key_attr = table_schemas[target_table]["columns"][key_col]
            create_diff = f"""
                IF NOT EXISTS (
                    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = 'diff' AND TABLE_NAME = '{target_table}'
                )
                BEGIN
                    CREATE TABLE diff.{target_table} (
                        run_id UNIQUEIDENTIFIER DEFAULT NEWID(),
                        action NVARCHAR(10),
                        {key_col} {key_attr}
                    );
                END
            """
        try:
            with self.etl.conn.cursor() as cursor:
                cursor.execute(create_diff)
            self.etl.conn.commit()
            print(f"{GREEN}Ensured diff table exists: diff.{target_table}{RESET}")
        except pyodbc.Error as e:
            print(f"{RED}Error ensuring diff table {target_table}: {e}{RESET}")

    def stage_tables(self):
        """
        Create any not found SQL staging table.
        """
        missing_staging = self._missing_tables_check(schema='staging')
        for target_table in missing_staging:
            self._build_tables(table_name=target_table, schema='staging')

    def wipe_staging_run(self):
        for target_table, table_schema in table_schemas.items():
            if 'constraints' in table_schema:
                self.drop_table_with_constraints(target_table, schema="staging")
                continue

            if 'identity_hash' in table_schema or 'diff_columns' in table_schema:
                self.drop_table(target_table, "diff")

            self.drop_table(target_table, "staging")

    def drop_table(self, table_name: str, schema: Literal["staging", "diff", "dbo"]="staging"):
        try:
            with self.etl.conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM {schema}.{table_name}")
            self.etl.conn.commit()
            print(f"Cleared {schema} table: {table_name}")
        except pyodbc.Error as e:
            self.etl.conn.rollback()
            print(f"{RED}Failed to clear {table_name}: {e}{RESET}")
            raise

    def drop_table_with_constraints(self, table_name: str, schema: str = "dbo"):
        """
        Drops a table after first finding and dropping any foreign key
        constraints from other tables that reference it.
        """

        # 1. Find all FK constraints in OTHER tables that reference this table
        find_fks_sql = """
            SELECT 
                fk.name AS fk_name,
                OBJECT_SCHEMA_NAME(fk.parent_object_id) AS child_schema,
                OBJECT_NAME(fk.parent_object_id) AS child_table
            FROM sys.foreign_keys fk
            WHERE fk.referenced_object_id = OBJECT_ID(?)
        """
        with self.etl.conn.cursor() as cursor:
            cursor.execute(find_fks_sql, f"{schema}.{table_name}")
            referencing_fks = cursor.fetchall()

            # 2. Drop each referencing FK
            for fk_name, child_schema, child_table in referencing_fks:
                drop_fk_sql = f"ALTER TABLE {child_schema}.{child_table} DROP CONSTRAINT {fk_name};"
                print(f"Dropping FK {fk_name} on {child_schema}.{child_table}")
                cursor.execute(drop_fk_sql)

            # 3. Drop the table itself (its own PK/unique/check/default constraints go with it)
            drop_table_sql = f"DROP TABLE IF EXISTS {schema}.{table_name};"
            print(f"Dropping table {schema}.{table_name}")
            cursor.execute(drop_table_sql)

        self.etl.conn.commit()
        