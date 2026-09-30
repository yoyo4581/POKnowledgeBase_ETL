from airflow.sdk import Asset, dag, task, AssetAlias
from airflow.exceptions import AirflowSkipException

from src.workflow.consumers import *
from src.workflow.producers import *

from src.builders.SQL.SQLCaller import SQL_ETL
from parsers.KEGG.KEGGCaller import KEGG_ETL
import logging
import os


PATHWAY_KGML_ALIAS = AssetAlias("kegg://pathway_kgml")

UUID = os.getenv('uuid')

logger = logging.getLogger(__name__)

@dag(schedule=None, catchup=False, tags=["SQL"])
def wipe_sql_environment():

    @task()
    def wipe_sql_data_tables():

        sql_caller = SQL_ETL(run_id=UUID)
        sql_caller.sql_state.wipe_environment(kind='staging')
        sql_caller.sql_state.wipe_environment(kind='dbo')

    wipe_sql_data_tables()


wipe_sql_environment()
