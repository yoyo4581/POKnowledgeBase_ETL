from local_db.schema import *
from local_db.SQL.SQLCaller import SQL_ETL
from local_db.UniProtCaller import UniProt_ETL
from local_db.Neo4jCaller import Neo4j_ETL
from local_db.BertEmbeddings import BioBertEmbeddings
from typing import Any


def produce_function_annotations_from_diff(
    altered_genes: list[tuple],
    sql_caller: SQL_ETL,
    uniprot_caller: UniProt_ETL,
    neo4j_caller: Neo4j_ETL,
    embed_gen: BioBertEmbeddings
):
    uniprot_covered = neo4j_caller.get_existing_uniprot_accessions()
    uniprot_accessions = [identifier for _, _, identifier in altered_genes if identifier not in uniprot_covered]

    batch_num = 200

    for idx_start in range(0, len(altered_genes), batch_num):
        batch_genes = altered_genes[idx_start:idx_start + batch_num]
        batch_uniprot = uniprot_accessions[idx_start:idx_start + batch_num]

        function_data = uniprot_caller.fetch_functions_from_uniprot(batch_uniprot)

        # Build a lookup keyed by accession -- confirm "Entry" is the actual
        # TSV header UniProt returns for the accession field (verify with a print
        # of function_data[0].keys(), since UniProt often renames it from "accession").
        function_by_accession = {
            row["Entry"]: row.get("Function [CC]", "").strip()
            for row in function_data
            if row.get("Function [CC]", "").strip()
        }

        valid_gene_function_pairs = [
            (gene_tuple, function_by_accession[identifier])
            for gene_tuple, identifier in zip(batch_genes, batch_uniprot)
            if identifier in function_by_accession
        ]

        if not valid_gene_function_pairs:
            continue

        valid_altered_genes = [pair[0] for pair in valid_gene_function_pairs]
        valid_function = [pair[1].removeprefix("FUNCTION: ") for pair in valid_gene_function_pairs]
        valid_uniprot = [gene[2] for gene in valid_altered_genes]

        sql_caller.stage_and_upsert(
            target_table="FunctionData",
            data=list(zip(valid_uniprot, valid_function))
        )

        embeddings = embed_gen.embed_documents(valid_function)

        payload = {
            gene_tuple: {"text": valid_function[i], "embedding": embeddings[i]}
            for i, gene_tuple in enumerate(valid_altered_genes)
        }

        neo4j_caller.update_functions(payload)



