from Bio import Entrez
import requests
import pprint
from ..colored_text import RED, GREEN, RESET


class EntrezCaller:
    '''
    An EntrezCaller:
    Generally it has different methods to fetch and parse data from the Entrez database.
    Returning data in a SQL-friendly format.
    It can fetch a entrezids for a list of gene names.
    Provides even more information about a gene.
    '''
    def __init__(self, email):
        self.email = email
        self.entrez = None

    def connect(self):
        Entrez.email = self.email
        self.entrez = Entrez

    def fetch_gene_uids(self, gene_names):
        if not self.entrez:
            raise Exception("Entrez connection not established. Call connect() first.")
        
        names_to_entrezId = {}
        undef_count = 0
        for i, name in enumerate(gene_names):
            query = f"{name}[Gene Name] AND Homo Sapiens [Organism]"
            handle = self.entrez.esearch(db="gene", term=query)
            record = self.entrez.read(handle)
            handle.close()
            if i%50==0:
                print(f"Processed {i} gene names...")

            if len(record['IdList'])>0:
                # Assuming we take the first ID found for each gene name
                names_to_entrezId[name] = record['IdList'][0]
            else:
                names_to_entrezId[name] = None
                undef_count += 1

        print(f"Found {len(gene_names) - undef_count} valid UIDs and {undef_count} undefined UIDs for the provided gene names.")
        return names_to_entrezId




        


