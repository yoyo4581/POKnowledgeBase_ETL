"""
Dry-run harness for Neo4j_ETL. Mocks the driver/session so no live Neo4j
server is needed -- every call to session.execute_write is intercepted and
the (cypher, rows) it would have sent are printed instead of executed.

Covers the cases that matter for batch_cypher's grouping logic:
  - static-label edges/nodes (PathwayMembership, all node types)
  - _label_field-derived dynamic labels (Interactions: ACTIVATION vs
    INHIBITION must land in separate batches despite being the same class)
  - fully dynamic source_type/target_type/__label__ via @property
    (ReactionRelation: substrate/product/catalyst each need a separate batch)
  - multiple rows sharing one batch (two ACTIVATION edges, two Genes)
  - nullable fields being dropped from props (Gene.gene_synonym=None)

Run with: python neo4j_caller_tester.py
"""
import os
from unittest.mock import MagicMock

os.environ.setdefault("NEO4J_USERNAME", "test")
os.environ.setdefault("NEO4J_PASSWORD", "test")

from src.builders.Neo4j.Neo4jCaller import Neo4j_ETL
from src.builders.Neo4j.schema import nodes, edges


def make_mocked_etl() -> tuple[Neo4j_ETL, list[tuple[str, list[dict]]]]:
    """Returns an ETL instance whose driver/session are mocked, plus the
    list that captured (cypher, rows) calls will be appended to."""
    etl = Neo4j_ETL()
    etl._driver = MagicMock()
    session = MagicMock()
    etl._driver.session.return_value.__enter__.return_value = session

    calls: list[tuple[str, list[dict]]] = []
    session.execute_write.side_effect = lambda fn, cypher, rows: calls.append((cypher, rows))
    return etl, calls


def show(label: str, calls: list[tuple[str, list[dict]]]) -> None:
    print(f"\n=== {label}: {len(calls)} batch(es) ===")
    for cypher, rows in calls:
        match_lines = [l.strip() for l in cypher.splitlines() if "MERGE" in l or "MATCH" in l]
        for line in match_lines:
            print(f"  {line}")
        for row in rows:
            print(f"    {row}")


# ---- sample nodes -----------------------------------------------------

sample_nodes = [
    nodes.Function(uniprot_id="P12345", text="kinase activity", embedding="[0.1, 0.2]"),
    nodes.Ontology(
        id="GO:0006915", definition="apoptotic process", definition_xref=["PMID:123"],
        hasOboNamespace="biological_process", name="apoptosis", synonyms=["programmed cell death"],
    ),
    nodes.Pathway(id="path:hsa04010", name="MAPK signaling pathway", class_id="Environmental Information Processing"),
    nodes.Gene(id="hsa:673", name="BRAF", full_name="B-Raf proto-oncogene", gene_synonym=None),
    nodes.Gene(id="hsa:5290", name="PIK3CA", full_name="PI3K catalytic subunit alpha", gene_synonym="PI3K"),
    nodes.Compound(id="cpd:C00002", name="ATP", formula="C10H16N5O13P3", compound_synonyms="adenosine triphosphate", MOL_WEIGHT=507.18),
    nodes.Ortholog(id="ko:K04365", name="BRAF", full_name="B-Raf ortholog"),
    nodes.Reaction(
        id="rn:R00200", name="pyruvate kinase", definition="ATP + pyruvate <=> ADP + PEP",
        equation="C00002 + C00022 <=> C00008 + C00074", comment=None, reaction_type=None,
        pathway_id="path:hsa00010",
    ),
]

# ---- sample edges -------------------------------------------------------

sample_edges = [
    # static __label__, dynamic source_type via entity_type
    edges.PathwayMembership(source_id="hsa:673", target_id="path:hsa04010", entity_type="gene"),
    edges.PathwayMembership(source_id="hsa:5290", target_id="path:hsa04010", entity_type="gene"),
    edges.PathwayMembership(source_id="cpd:C00002", target_id="path:hsa04010", entity_type="compound"),

    # _label_field-derived dynamic label -- two rows share ACTIVATION,
    # one is INHIBITION, so this must split into two batches. Also covers a
    # Gene-Compound interaction, which needs its own batch since source_type
    # differs from the Gene-Gene rows despite sharing relation_type.
    edges.Interactions(source_id="hsa:673", target_id="hsa:5290", pathway_id="path:hsa04010", relation_type="ACTIVATION", source_entity_type="gene", target_entity_type="gene"),
    edges.Interactions(source_id="hsa:5290", target_id="hsa:673", pathway_id="path:hsa04010", relation_type="ACTIVATION", source_entity_type="gene", target_entity_type="gene"),
    edges.Interactions(source_id="hsa:673", target_id="hsa:999", pathway_id="path:hsa04012", relation_type="INHIBITION", source_entity_type="gene", target_entity_type="gene"),
    edges.Interactions(source_id="hsa:673", target_id="cpd:C00002", pathway_id="path:hsa04010", relation_type="ACTIVATION", source_entity_type="gene", target_entity_type="compound"),

    # fully dynamic source_type/target_type/__label__ via role -- three
    # distinct batches, one per role
    edges.ReactionRelation(source_id="cpd:C00002", target_id="rn:R00200", role="substrate"),
    edges.ReactionRelation(source_id="rn:R00200", target_id="cpd:C00008", role="product"),
    edges.ReactionRelation(source_id="hsa:673", target_id="rn:R00200", role="catalyst"),
]


if __name__ == "__main__":
    etl, calls = make_mocked_etl()
    etl.upsert_nodes(sample_nodes)
    show("upsert_nodes", calls)

    etl, calls = make_mocked_etl()
    etl.delete_nodes(sample_nodes)
    show("delete_nodes", calls)

    etl, calls = make_mocked_etl()
    etl.upsert_edges(sample_edges)
    show("upsert_edges", calls)

    etl, calls = make_mocked_etl()
    etl.delete_edges(sample_edges)
    show("delete_edges", calls)
