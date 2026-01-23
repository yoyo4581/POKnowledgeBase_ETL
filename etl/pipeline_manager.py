from dotenv import load_dotenv
from neo4j import GraphDatabase
import pyodbc
import os
from pymilvus import MilvusClient, DataType
from .verify_graphDB import verify_genes
from .utils.dbCallers import EntrezCaller, KEGGCaller, SQLCaller
from .utils.colored_text import RED, GREEN, RESET, YELLOW
from datetime import datetime
from .utils.schema_def import table_schemas
from enum import Enum

class ETLError(Exception):
    pass

class VersionCompletedError(ETLError):
    pass

class RunApprovalRequiredError(ETLError):
    pass


class VersionState(Enum):
    NOT_FOUND = 0
    INCOMPLETE = 1
    COMPLETE = 2


class ETLPipelineManager:
    def __init__(self, user_id, semver, version_name):
        print(f"{GREEN}ETL Pipeline Manager initialized. Use this to manage your ETL pipelines.{RESET}")
        self.user_id = user_id
        self.semver = semver

        self.version_active = False
        self.sql_caller = SQLCaller.SQLCaller()
        self.etl_controller = SQLCaller.ETL_Controller()
        self.version_id, self.run_id = self.fetch_profile(semver=semver, version=version_name)

    def get_version_state(self, semver, version_name) -> VersionState:
        result = self.etl_controller.version_is_active(semver, version_name)

        if result is None:
            return VersionState.NOT_FOUND
        elif result is False:
            return VersionState.INCOMPLETE
        else:
            return VersionState.COMPLETE
        
    def _ensure_prev_run_approved(self, run_id):
        approved = self.etl_controller.is_approved(run_id)
        if not approved:
            raise RunApprovalRequiredError(
                f"Previous ETL run; run_id: {run_id} requires approval before proceeding."
            )

    def create_run_profile(self, version_id):
        self.etl_controller.create_run_entry(
            version_id=version_id,
            user_id=self.user_id
        )
        run_id = self.etl_controller.fetch_prev_run(
            version_id = version_id
        )
        print(f'{GREEN}Created run profile for version: {version_id} run: {run_id}{RESET}')
        return run_id

    def fetch_profile(self, semver, version_name):
        state = self.get_version_state(semver, version_name)

        if state == VersionState.NOT_FOUND:
            print(f"{YELLOW}No ETL version found. Creating new profile...{RESET}")
            self.etl_controller.create_version_entry(semver, version_name)

            version_id = self.etl_controller.fetch_version_id(semver, version_name)
            run_id = self.create_run_profile(version_id)

        elif state == VersionState.INCOMPLETE:
            print(f"{GREEN}Found existing ETL version with incomplete runs.{RESET}")
            version_id = self.etl_controller.fetch_version_id(semver, version_name)
            run_id = self.etl_controller.fetch_prev_run(version_id)
            self._ensure_prev_run_approved(run_id)

            print(f"{GREEN} PREVIOUS RUN APPROVED. Creating new run profile. {RESET}")
            run_id = self.create_run_profile(version_id)

        else:  # COMPLETE
            print(f"{GREEN}ETL version {semver} already completed and active.{RESET}")
            self.version_active = True
            version_id = self.etl_controller.fetch_version_id(semver, version_name)
            run_id = None  # or raise / require approval

        return version_id, run_id



    def run_pipeline(self, batch_size=20,stop_batch=None):
        if self.run_id:
            # the run_id had to already been declared upon creation.
            return
        
        batch_size = min(batch_size, 100) # The batch_size must not exceed the stop_batch number
        self.etl_controller.set_batch_size(self.run_id, batch_size)

        self.sql_caller.stage_tables()
        if self.batch_num is not None:
            self.extract(batch_size=batch_size)
        else:
            self.extract(batch_size=batch_size, batch_num=1, stop_batch=stop_batch)

        
        print("Data loaded into staging. Awaiting QA...")


    def extract(self, batch_size):
        """
        Extracts KEGG pathway data in batches.

        :param batch_size: Number of pathways per batch
        :param batch_num: Index of the batch to start from (e.g. 0 = start from beginning)
        :param max_batches: Max number of batches to process (optional)
        """
        print(f"{GREEN}Starting data extraction...{RESET}")

        # 1. Initialize KEGGCaller and fetch Pathway ID map
        kegg_sql_builder = KEGGCaller.KEGGSQLBuilder()
        self.pathway_data = kegg_sql_builder.fetch_pathway_ids()
        total_pathways = len(self.pathway_data)

        # Guard against invalid starting batch
        start_index = batch_num * batch_size
        if start_index >= total_pathways:
            raise ValueError(
                f"Starting index ({start_index}) is beyond total pathways ({total_pathways})."
            )

        print(f"{GREEN}Parsing {total_pathways} pathways starting from batch {batch_num}{RESET}")

        for batch_count, batch_start in enumerate(
            range(start_index, total_pathways, batch_size), start=batch_num
        ):
            batch_pathway_data = self.pathway_data[batch_start : batch_start + batch_size]

            # Build and stage tables
            b_tables = kegg_sql_builder.build_sql_pathway_tables(batch_pathway_data)
            b_tables2 = kegg_sql_builder.build_entity_tables(b_tables['entities'])
            all_tables = b_tables | b_tables2

            self.load_to_staging(all_tables)
            self.sql_caller.increment_batch_entry(batch_count)
            self.batch_num = batch_count
            print(f"✓ Processed Batch {batch_count} (start index: {batch_start})")

            # Stop if max_batches reached
            if stop_batch is not None and batch_count >= stop_batch:
                print(f"{YELLOW}Reached stop_batches = {stop_batch}. Halting extraction.{RESET}")
                break

        print(f"{GREEN}Finished processing pathway data{RESET}")

    def approve_batch(self):
        print(f"{GREEN}Approving batch {self.batch_id} for production load...{RESET}")
        self.sql_caller.approve_batch(self.batch_id)

    def is_approved(self):
        return self.sql_caller.is_approved(self.batch_id)

    def transform(self):
        print(f"{GREEN}Starting data transformation...{RESET}")
        return

    
    def load_to_staging(self, batch_tables: dict):
        print(f"{GREEN}Loading data to staging...{RESET}")
        
        for key, data in batch_tables.items():
            response = self.sql_caller.stage_batch(target_table=key, data=data)
            if not response:
                break
        

    def load_to_production(self):
        print(f"{GREEN}Loading data to production...{RESET}")
        for table in table_schemas.keys():
            batch_upserted = self.sql_caller.upsert_batch(target_table="table")
            if not batch_upserted:
                print(f"{RED} Failed to upsert to production table {table}")
        







