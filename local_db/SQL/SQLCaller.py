import pyodbc
import json
import os
import uuid

from collections import defaultdict
from typing import Literal, List, Any
from dotenv import load_dotenv
from functools import wraps

from local_db.SQL.SQLState import SQLStateManager
from utils.colored_text import RED, GREEN, RESET, YELLOW
from local_db.schema import table_schemas
from local_db.SQL.utils import sql_safe


load_dotenv()
sql_conn_string = f"""
    DRIVER={{ODBC Driver 18 for SQL Server}};
    SERVER={os.getenv('sql_server')};
    DATABASE={os.getenv('gene_database')};
    TrustServerCertificate=yes;
    UID={os.getenv('sql_uid')};
    PWD={os.getenv('sql_pwd')};
    Encrypt=no;
    """


class SQL_ETL:
    """
    Will receive, transform, insert, and upsert data.
    This class is in charge of all data insertion to the staging environment, including computing hashes.
    """
    def __init__(self, run_id: str = None):
        self.run_id = run_id if run_id else str(uuid.uuid4())
        self._conn = None
        self._state = None


    @property
    def conn(self):
        if self._conn is None:
            self._conn = pyodbc.connect(sql_conn_string)
        return self._conn


    @property
    def cursor(self):
        return self.conn.cursor()


    @property
    def sql_state(self):
        if self._state is None:
            self._state = SQLStateManager(self)
        return self._state


    @sql_safe(default=None, label='running annotation query')
    def _run_and_group(self, query: str) -> dict[tuple[Any, Any, str], list[Any]] | None:
        """
        Executes an annotation query and groups rows by (action, entity_id, entity_type),
        with remaining columns bundled into a payload dict.
        """
        print(query)
        grouped_data = defaultdict(list)
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            annotated_data = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
        payload_columns = columns[3:]
        for item in annotated_data:
            action, identifier, label = item[0], item[1], item[2]
            payload = dict(zip(payload_columns, item[3:]))
            grouped_data[(action, identifier, label)].append(payload)

        return grouped_data


    @sql_safe(default=None, label='loading UniProt map from diff')
    def load_uniprot_from_diff(self):
        map_query = f"""
        WITH FilteredMeta AS (
            SELECT d.action, d.entity_id, s.entity_type
            FROM diff.entities as d
            INNER JOIN staging.entities as s
            ON d.entity_id = s.entity_id
            WHERE s.entity_type = 'gene'
        )
        SELECT fe.action, fe.entity_id, c.uniprot_id
        FROM FilteredMeta as fe
        INNER JOIN dbo.EntrezUniprotMap as c
        ON fe.entity_id = c.entrez_id
        """
        with self.conn.cursor() as cursor:
            cursor.execute(map_query)
            annotated_data = cursor.fetchall()

        return [(item[0], item[1], item[2]) for item in annotated_data]


    @sql_safe(default=None, label='loading annotation from diff')
    def load_annotations_from_diff(self, target_table: Literal['CompoundData', 'GeneData', 'OrthoData']) -> dict[tuple[Any, Any, str], list[Any]]:
        entity_type = {'CompoundData': 'compound', 'GeneData': 'gene', 'OrthoData': 'ortholog'}[target_table]
        key = table_schemas[target_table]['key']

        annotation_query = f"""
        WITH FilteredMeta AS (
            SELECT d.action, d.entity_id AS entity_id, s.entity_type
            FROM diff.entities as d
            INNER JOIN staging.entities as s
            ON d.entity_id = s.entity_id
            WHERE s.entity_type = '{entity_type}'
        )
        SELECT fe.action, fe.entity_id, fe.entity_type, c.*
        FROM FilteredMeta as fe
        LEFT JOIN dbo.{target_table} as c
        ON fe.entity_id = c.{key}
        """
        return self._run_and_group(annotation_query)

    
    def load_reaction_annotations_from_diff(self) -> dict[tuple[Any, Any, str], list[Any]]:
        key = table_schemas['reactions']['key']

        annotation_query = f"""
        WITH FilteredMeta AS (
            SELECT d.action, d.entity_id AS entity_id, 'reactions' AS entity_type
            FROM diff.entities as d
        )
        SELECT fe.action, fe.entity_id, fe.entity_type, c.*
        FROM FilteredMeta as fe
        LEFT JOIN dbo.reactions as c
        ON fe.entity_id = c.{key}
        """
        return self._run_and_group(annotation_query)

    def stage_and_upsert(self, target_table: str, data: list):
        """
        Stages data, computes identity hash if needed, then upserts. When upserting requires a diff table will return signal.
        If table is interactions or EntityPathMem will sync immutable, meaning will create a diff table if interactions or EntityPathMem are altered/require deletion.

        Returns:
            1. True - if upserting has a diff table
            2. False - if upserting has no diff table
            3. None - if upserting did not work
        """
        if len(data)==0:
            print(f"{RED} No {target_table} data for this pathway")
            return
        staged = False

        has_hash = True if 'identity_hash' in table_schemas[target_table] else False
        has_diff = True if 'identity_hash' in table_schemas[target_table] or 'diff_columns' in table_schemas[target_table] else False

        # If not missing then stage, otherwise, build then stage.
        if not self.sql_state._missing_tables_check('staging'):
            staged = self.stage_batch(target_table, data)
        else:
            self.sql_state.stage_tables()
            staged = self.stage_batch(target_table, data)

        if staged:
            if has_hash:
                hashed = self.compute_identity_hashes(target_table)

            merge_query = self._build_merge_query(target_table)
            if has_diff:
                self.sql_state.create_diff_if_not_exists(target_table)

            has_diff = self.upsert_batch(merge_query, target_table, has_diff)
            if has_diff is None:
                print(f"{RED}Failed upserting for {target_table}.{RESET}")
        else:
            print(f"{RED} Error staging stage_and_upsert failed")

        return has_diff


    @sql_safe(default=None, label='Upserting data into target table')
    def upsert_batch(self, merge_query: str, target_table: str, has_diff: bool):
        with self.conn.cursor() as cursor:
            cursor.execute(merge_query)
        self.conn.commit()

        diff_rows = []
        if has_diff:
            diff_rows = self.sql_state.fetch_diff_table(target_table)
            print(f"{GREEN}Detected {len(diff_rows)} changes for {target_table}!{RESET}")

        return has_diff
    

    @sql_safe(default=None, label='Staging Batch')
    def stage_batch(
        self,
        target_table: str,
        data: list  # list of tuples with values in order of columns.keys()
    ):
        """
        Safely insert data into staging table in chunks, skipping existing keys,
        and respecting SQL Server's 2100 parameter limit.
        """
        # The key columns are used in the match_statement to make sure that there are no duplicates.
        # The col_insert is used to find all columns that will have their data inserted.
        # All columns aside from kegg_class and interactions will have their keys inserted.
        # The key for interactions is autogenerated, but we need to ensure that
        schema = table_schemas[target_table]
        key_col = schema.get("key", "")
        auto_id = schema.get("auto", False)
        columns = list(schema['columns'].keys())

        if auto_id or key_col == "":
            match_statement = " AND ".join([f't.{col} = ?' for col in schema["columns"].keys() if col != key_col])
            col_insert = ", ".join([col for col in columns if col != key_col])
            num_cols = len([col for col in columns if col != key_col])
            insert_sql = f"""
            INSERT INTO staging.{target_table} ({col_insert})
            SELECT {', '.join(['?'] * num_cols)}
            """
        else:
            match_statement = f't.{key_col} = ?'
            col_insert = ", ".join(columns)
            num_cols = len(columns)
            insert_sql = f"""
            INSERT INTO staging.{target_table} ({col_insert})
            SELECT {', '.join(['?'] * num_cols)}
            WHERE NOT EXISTS (
                SELECT 1 FROM staging.{target_table} t
                WHERE {match_statement}
            );
            """
        print(insert_sql)

        # Determine max chunk size within 2100 parameter limit
        max_chunk_size = 2100 // num_cols
        if max_chunk_size == 0:
            raise ValueError("Too many columns to safely insert any data.")

        def chunked(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        for chunk_idx, chunk in enumerate(chunked(data, max_chunk_size), start=1):
            # For each row, add an extra parameter (at the end) for the key in WHERE clause
            
            if auto_id or key_col == "":
                params_with_keys = [row for row in chunk]
            else:
                params_with_keys = [row + (row[columns.index(key_col)],) for row in chunk]

            with self.conn.cursor() as cursor:
                cursor.executemany(insert_sql, params_with_keys)
            self.conn.commit()
            print(f"{GREEN}Chunk {chunk_idx}: Inserted {len(chunk)} rows into staging.{target_table} (duplicates skipped).{RESET}")

        return True
        

    @sql_safe(default=None, label='upserting id map batch')
    def upsert_id_map_batch(self, batch: list[tuple[str, str]]):
        with self.conn.cursor() as cursor:
            cursor.executemany(
                """
                MERGE dbo.EntrezUniprotMap AS target
                USING (SELECT ? AS entrez_id, ? AS uniprot_id) AS src
                ON target.entrez_id = src.entrez_id AND target.uniprot_id = src.uniprot_id
                WHEN NOT MATCHED THEN
                    INSERT (entrez_id, uniprot_id) VALUES (src.entrez_id, src.uniprot_id);
                """,
                batch,
            )
        self.conn.commit()

    def _build_merge_query(self, target_table: str) -> str:
        schema = table_schemas[target_table]
        columns = list(schema["columns"].keys())
        key_col = schema.get("key")
        is_immutable = key_col is None
        has_hash = "identity_hash" in schema
        has_diff_cols = "diff_columns" in schema
        tracks_diff = is_immutable or has_diff_cols

        all_cols = columns + (["identity_hash"] if has_hash else [])
        insert_cols = ", ".join(all_cols)
        insert_vals = ", ".join(f"source.{c}" for c in all_cols)
        key_clause = (
            "ON target.identity_hash = source.identity_hash" if is_immutable
            else f"ON target.{key_col} = source.{key_col}"
        )
        not_match = f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"

        if is_immutable:
            match_clause = f"""WHEN NOT MATCHED BY SOURCE AND target.pathway_id IN
                (SELECT DISTINCT pathway_id FROM staging.{target_table}) THEN DELETE"""
            output_cols = ", ".join(f"COALESCE(inserted.{c}, deleted.{c}) AS {c}" for c in columns)
            diff_out = f"""OUTPUT '{self.run_id}' AS run_id, $action AS action, {output_cols}
                INTO diff.{target_table} (run_id, action, {", ".join(columns)})"""
        else:
            update_set = ", ".join(f"target.{c} = source.{c}" for c in columns if c != key_col)
            condition = (
                "AND target.identity_hash <> source.identity_hash" if has_hash
                else "AND " + " OR ".join(f"target.{c} <> source.{c}" for c in schema.get("diff_columns", [])) if has_diff_cols
                else ""
            )
            match_clause = f"WHEN MATCHED {condition} THEN UPDATE SET {update_set}"
            diff_out = (
                f"""OUTPUT '{self.run_id}' AS run_id, $action AS action,
                    COALESCE(inserted.{key_col}, deleted.{key_col}) AS {key_col}
                    INTO diff.{target_table} (run_id, action, {key_col})"""
                if has_hash or has_diff_cols else ""
            )

        merge_sql = f"""
        MERGE INTO dbo.{target_table} AS target
        USING staging.{target_table} AS source
        {key_clause}
        {match_clause}
        {not_match}
        {diff_out};
        """
        return merge_sql
        
        
    @sql_safe(default=False, label='computing identity hash')
    def compute_identity_hashes(self, target_table: str):
        """
        Compute identity_hash for staging table based on schema-defined identity_hash.
        Returns True if successful, False if an error occurred.
        """
        identity_cols = table_schemas[target_table].get("identity_hash")
        
        concat_expr = " + '|' + ".join(
            f"COALESCE(CAST({col} AS NVARCHAR(MAX)), '∅')"
            for col in identity_cols
        )

        sql_hash = f"""
        UPDATE staging.{target_table}
        SET identity_hash = HASHBYTES(
            'SHA2_256',
            {concat_expr}
        );
        """
        with self.conn.cursor() as cursor:
            cursor.execute(sql_hash)
        self.conn.commit()
        print(
            f"{GREEN} Computed identity_hash for staging.{target_table} using columns {identity_cols}.{RESET}"
        )
        return True
