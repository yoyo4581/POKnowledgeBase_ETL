from local_db.schema import *
from local_db.SQL.SQLCaller import SQL_ETL
from local_db.UniProtCaller import UniProt_ETL
from local_db.Neo4jCaller import Neo4j_ETL


def consume_altered_genes_from_diff(sql_caller: SQL_ETL):
    return sql_caller.load_uniprot_from_diff()
