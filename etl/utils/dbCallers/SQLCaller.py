from ..colored_text import RED, GREEN, RESET, YELLOW
import pyodbc
from ..schema_def import table_schemas


class ETL_Controller:
    def __init__(self):
        try:
            self.sql_conn = pyodbc.connect(
                "DRIVER={SQL Server};"
                "SERVER=DESKTOP-9TQJR1G\\SQLEXPRESS;"
                "DATABASE=etl_control_db;"
                "Trusted_Connection=yes;"
            )
            self.sql_cursor= self.sql_conn.cursor()
            print(f"{GREEN}Successfully connected to the database!{RESET}")
        except pyodbc.Error as e:
            print(f"{RED}Error connecting to Local SQL Express Server:{RESET}", e)
    
    def version_is_active(self, semver, version_name):
        try:
            result = self.sql_cursor.execute(
                "SELECT is_active FROM etl_datasets WHERE semver = ? AND dataset_name = ?", (semver, version_name)
            ).fetchone()[0]
            if result is None:
                return None
            
            return bool(result)
        except pyodbc.Error as e:
            print(f"{RED}Error checking version status: {e}{RESET}")
            return None

    def fetch_version_id(self, semver='1.0', version_name='PathwayOracle'):
        try:
            version_id = self.sql_cursor.execute(
                "SELECT version_id FROM etl_datasets WHERE semver = ? AND dataset_name = ?", (semver, version_name)
            ).fetchone()[0]
            print(f'{GREEN} Fetched version ID: {version_id} for version {semver} - {version_name}{RESET}')
            return version_id
        except pyodbc.Error as e:
            print(f"{RED}Error fetching version ID: {e}{RESET}")
            return None

    def create_version_entry(self, semver, version_name):
        try:
            self.sql_cursor.execute(
                """
                INSERT INTO etl_datasets (dataset_name, semver)
                VALUES (?, ?);
                """,
                (version_name, semver)
            )
            self.sql_conn.commit()
            print(f"{GREEN}Created new version entry: {version_name} - {semver}{RESET}")            
        except pyodbc.Error as e:
            print(f"{RED}Error creating version entry: {e}{RESET}")

    def set_batch_size(self, run_id, batch_size):
        try:
            self.sql_cursor.execute(
                """
                UPDATE etl_runs
                SET batch_size = ?
                WHERE run_id = ?
                    AND status IN ('STAGED');
                """, (batch_size, run_id)
            )
            self.sql_cursor.commit()
        except pyodbc.Error as e:
            print(f"{RED}Error setting batch_size: {e}{RESET}")

    def create_run_entry(self, version_id, batch_size, user_id):
        try:
            self.sql_cursor.execute(
                """
                INSERT INTO etl_runs (dataset_id, status, batch_size, created_by)
                VALUES (?, 'STAGED', ?, ?);
                """,
                (version_id, batch_size, user_id)
            )
            self.sql_conn.commit()
            print(f"{GREEN}Created new run entry for version_id {version_id}{RESET}")            
        except pyodbc.Error as e:
            print(f"{RED}Error creating run entry: {e}{RESET}")

    def fetch_prev_run(self, version_id):
        try:
            result = self.sql_cursor.execute(
                "SELECT run_id FROM etl_runs WHERE dataset_id = ?", version_id
            ).fetchone()
            print(f'{GREEN} Fetched data from version entry {version_id}, where run_id {result[0]}{RESET}')
            return result[0]
        
        except pyodbc.Error as e:
            print(f"{RED}Error fetching batch status: {e}{RESET}")
            return None
        
    def create_etl_entry(self, batch_id):
        try:
            self.sql_cursor.execute(
                """
                INSERT INTO etl_approval (version_id, batch_id, approved, approved_at, approved_by, notes, batch_num)
                VALUES (?, 0, GETDATE(), 'Yahya', NULL, 0);
                """,
                (batch_id)
            )
            self.sql_conn.commit()
        except pyodbc.Error as e:
            print(f"{RED}Error creating approval entry: {e}{RESET}")
        
    def approve_run(self, run_id, user_id):
        try:
            self.sql_cursor.execute(
                """
                UPDATE etl_approval
                SET approved = 1, approved_at = GETDATE(), approved_by = ?
                WHERE batch_id = ?
                """,
                (user_id, run_id)
            )
            self.sql_conn.commit()
        except pyodbc.Error as e:
            print(f"{RED}Error approving batch: {e}{RESET}")
    


    def is_approved(self, run_id):
        try:
            result = self.sql_cursor.execute(
                "SELECT approved FROM etl_approval WHERE run_id = ?", run_id
            ).fetchone()
            return result and result[0] == 1
        except pyodbc.Error as e:
            print(f"{RED}Error checking approval status: {e}{RESET}")
            return False
        
    def increment_batch_entry(self, batch_num, batch_id):
        try:
            self.sql_cursor.execute(
                """
                UPDATE etl_approval
                SET batch_num = ?
                WHERE batch_id = ?
                """,
                (batch_num, batch_id)
            )
            self.sql_conn.commit()
        except pyodbc.Error as e:
            print(f"{RED}Error checking approval status: {e}{RESET}")



class SQLCaller:
    def stage_tables(self):
        """
        Create any empty SQL staging table.
        """
        for target_table in table_schemas.keys():
            col_defs = ",\n    ".join(
                f"{col} {typ}{' PRIMARY KEY' if col == table_schemas[target_table]['key'] else ''}"
                for col, typ in table_schemas[target_table]["columns"].items()
            )

            fully_qualified = f"staging.{target_table}"

            try:
                # First check if the table exists
                self.sql_cursor.execute(
                    f"SELECT OBJECT_ID(?)", fully_qualified
                )
                result = self.sql_cursor.fetchone()
                if result[0] is None:
                    # Now create the table
                    ddl = f"""
                    CREATE TABLE {fully_qualified} (
                        {col_defs}
                    );
                    """
                    self.sql_cursor.execute(ddl)
                    self.sql_conn.commit()
                    print(f"{GREEN}Table {fully_qualified} created successfully.{RESET}")
                else:
                    # Pick up from where we left off
                    print(f"Staging table {fully_qualified} already exists and is occupied: {RESET}")
            except pyodbc.Error as e:
                print(f"{RED}Error creating table {fully_qualified}: {e}{RESET}")
                return False
            
    def stage_batch(
        self,
        target_table: str,
        data: list  # list of tuples with values in order of columns.keys()
    ):
        """
        Safely insert data into staging table in chunks, skipping existing keys,
        and respecting SQL Server's 2100 parameter limit.
        """
        special_tables = {'interactions', 'EntityPathMem'}
        try:
            columns = list(table_schemas[target_table]["columns"].keys())
            key_col = [table_schemas[target_table]["key"]] if target_table not in special_tables else table_schemas[target_table]["match_keys"]

            match_statement = ' AND '.join(f't.{key} = ?' for key in key_col)
            col_list = ", ".join(columns)
            num_cols = len(columns)

            # Determine max chunk size within 2100 parameter limit
            max_chunk_size = 2100 // num_cols
            if max_chunk_size == 0:
                raise ValueError("Too many columns to safely insert any data.")

            def chunked(lst, n):
                for i in range(0, len(lst), n):
                    yield lst[i:i + n]

            insert_sql = f"""
            INSERT INTO staging.{target_table} ({col_list})
            SELECT {', '.join(['?'] * num_cols)}
            WHERE NOT EXISTS (
                SELECT 1 FROM staging.{target_table} t
                WHERE {match_statement}
            );
            """

            for chunk_idx, chunk in enumerate(chunked(data, max_chunk_size), start=1):
                # For each row, add an extra parameter (at the end) for the key in WHERE clause
                params_with_keys = [row + tuple(row[columns.index(k)] for k in key_col) for row in chunk]
                self.sql_cursor.executemany(insert_sql, params_with_keys)
                self.sql_conn.commit()
                print(f"{GREEN}Chunk {chunk_idx}: Inserted {len(chunk)} rows into staging.{target_table} (duplicates skipped).{RESET}")

            return True

        except pyodbc.Error as e:
            print(f"{RED}Failed to insert into staging table {target_table}: {e}. Aborting batch insert.{RESET}")
            self.sql_cursor.rollback()
            return False
        
    def upsert_batch(
        self,
        target_table: str
    ):
        """
        Batch upsert data into SQL Server table via temp table + MERGE,
        while capturing a per-run diff (INSERT / UPDATE)
        """

        schema = table_schemas[target_table]

        if target_table=="interactions":
            key_cols = schema["match_keys"]
        else:
            key_cols = [schema["key"]]

        on_clause = " AND ".join(
            f"target.{col} = source.{col}"
            for col in schema["columns"].keys()
            if col not in key_cols
        )

        insert_cols = ", ".join(schema["columns"].keys())
        insert_vals = ", ".join(f"source.{col}" for col in schema["columns"].keys())

        diff_table = f"#diff_{target_table}"


        # Build update set clause, excluding primary key
        update_set = ", ".join(
            f"target.{col} = source.{col}"
            for col in table_schemas[target_table]["columns"].keys()
            if col != table_schemas[target_table]["key"]
        )

        # Columns for insert
        insert_cols = ", ".join(table_schemas[target_table]["columns"].keys())
        insert_vals = ", ".join(f"source.{col}" for col in table_schemas[target_table]["columns"].keys())

        create_diff_table = f"""
        CREATE TABLE {diff_table} (
            action NVARCHAR(10),
            entity_key NVARCHAR(255)
        );
        """

        # Build the MERGE statement
        merge_sql = f"""
        MERGE INTO dbo.{target_table} AS target
        USING staging.{target_table} AS source
        ON {on_clause}

        WHEN MATCHED 
            AND target.identity_hash <> source.identity_hash
        THEN UPDATE SET {update_set}

        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals});

        OUTPUT
            $action AS action,
            COALESCE(
                inserted.{key_cols[0]},
                deleted.{key_cols[0]}
            ) AS entity_key
        INTO {diff_table}
        """

        try:
            self.sql_cursor.execute(create_diff_table)
            self.sql_cursor.execute(merge_sql)
            self.sql_conn.commit()

            #-----Read diff result
            self.sql_cursor.execute(
                f"SELECT action, entity_key FROM {diff_table}"
            )
            diff_rows = self.sql_cursor.fetchall()
            print(
                f"{GREEN}Upsert complete for {target_table}."
                f"Chagnes detected: {len(diff_rows)} {RESET}"
            )
            return diff_rows
        
        except pyodbc.Error as e:
            self.sql_conn.rollback()
            print(f"{RED}Error during MERGE for {target_table}: {e}{RESET}")
            return None
        
        finally:
            # Cleanup: drop the staging table
            try:
                self.sql_cursor.execute(f"DROP TABLE staging.{target_table};")
                self.sql_cursor.execute(f"DROP TABLE IF EXISTS {diff_table}")
                self.sql_conn.commit()
                print(f"{GREEN}Dropped staging table: staging.{target_table}{RESET}")
            except pyodbc.Error as e:
                print(f"{RED}Failed to drop staging table staging.{target_table}: {e}{RESET}")


