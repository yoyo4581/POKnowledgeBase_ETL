verify_gene_uids = """
MATCH (n:Genes)
WHERE n.uids IS NULL
RETURN Collect(n.name) as null_gene_uids;
"""