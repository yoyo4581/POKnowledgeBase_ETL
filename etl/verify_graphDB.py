from .utils import graph_queries
from .utils.colored_text import RED, GREEN, RESET




def verify_genes(graph_driver, sql_driver):
    """
    Verify all gene information from the Genes label in the graph database using the info stored in the local SQL database.
    """
    query = graph_queries.verify_gene_uids
    records, _, _ = graph_driver.execute_query(query)
    
    records = records[0]['null_gene_uids']
    if len(records) == 0:
        print("All genes have valid UIDs.")
    else:
        print(f"{RED} There are {len(records)} genes that have null UIDs{RESET}") 
        print("These genes need to be updated in the graph database:")
        print(f'{GREEN} Calling to Entrez to fetch the UIDs for these genes...{RESET}')
    
    return records

