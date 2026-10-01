"""
Reactome table schemas -- the source set selected when pathway_source=reactome.

The alternative to kegg_definitions.py, not an addition to it: selecting a
source swaps this whole dict for that one. Source-neutral tables
(PathwayIds, entities, EntityPathMem, FunctionData, GOOntologyMeta) live in
shared_definitions.py and are merged in by definitions.py either way.

Six names here also exist in the KEGG set -- GeneData, CompoundData,
DrugData, reactions, PathwayData, reaction_participants -- with different
shapes. KEGG keys GeneData on `uid INT` (Entrez) and gives `reactions` a
definition/equation Reactome has no counterpart for. Same table name, other
schema, so the two sources need separate databases, not a shared one.
"""
from typing import Literal

from .types import (
    TableSchema, DiffSync, IdentityHashSync, ForeignKey,
)

reactome_table_schemas: dict[str, TableSchema] = {

    # ---- source state -------------------------------------------------
    "PathwaySBMLMeta": TableSchema(
        key="pathway_id",
        columns={
            "pathway_id": "VARCHAR(20)",
            "sbml_hash": "CHAR(32)",
            "last_checked": "DATETIME",
        },
        sync=DiffSync(diff_columns=("sbml_hash",)),
        __table_name__="PathwaySBMLMeta",
    ),

    # ---- per-entity annotation -----------------------------------------
    "EntityData": TableSchema(
        key="entity_id",
        columns={
            "entity_id": "VARCHAR(20)",
            "display_name": "NVARCHAR(512) NOT NULL",
            "compartment": "VARCHAR(100) NULL",
        },
        sync=DiffSync(diff_columns=("display_name", "compartment")),
        __table_name__="EntityData",
    ),

    # ---- topology -------------------------------------------------------
    #
    # IdentityHashSync scoped by pathway_id: a membership or participation
    # that stops being staged for a pathway must be DELETED, not merely left
    # behind. Structure is immutable per pathway -- an edge absent from the
    # current parse is an edge Reactome removed.
    "entity_membership": TableSchema(
        columns={
            "parent_id": "VARCHAR(20)",
            "child_id": "VARCHAR(20)",
            "rel": "VARCHAR(20)",
            "stoichiometry": "INT",
            "pathway_id": "VARCHAR(20)",
        },
        sync=IdentityHashSync(
            identity_hash=("parent_id", "child_id", "rel", "pathway_id"),
            coverage_scope_columns=("pathway_id",),
        ),
        __table_name__="entity_membership",
    ),
    "entity_identity": TableSchema(
        columns={
            "entity_id": "VARCHAR(20)",
            "reference_id": "VARCHAR(20)",
            "reference_type": "VARCHAR(30)",
        },
        sync=IdentityHashSync(
            identity_hash=("entity_id", "reference_id"),
            coverage_scope_columns=("entity_id",),
        ),
        __table_name__="entity_identity",
    ),
    "entity_moiety": TableSchema(
        columns={
            "entity_id": "VARCHAR(20)",
            "moiety_id": "VARCHAR(20)",
            "psi_mod": "VARCHAR(20) NULL",
        },
        sync=IdentityHashSync(
            identity_hash=("entity_id", "moiety_id"),
            coverage_scope_columns=("entity_id",),
        ),
        __table_name__="entity_moiety",
    ),

    # ---- annotation, replacing the KEGG-shaped versions ----------------
    "GeneData": TableSchema(
        key="uniprot_id",
        columns={
            "uniprot_id": "VARCHAR(20)",
            "gene_name": "VARCHAR(50) NOT NULL",
            "full_name": "NVARCHAR(MAX)",
            "gene_synonym": "NVARCHAR(512) NULL",
            "ensembl_gene": "VARCHAR(30) NULL",
        },
        sync=DiffSync(diff_columns=("gene_name", "full_name", "gene_synonym",
                                    "ensembl_gene")),
        __table_name__="GeneData",
    ),
    "CompoundData": TableSchema(
        key="compound_id",
        columns={
            "compound_id": "VARCHAR(20)",
            "compound_name": "NVARCHAR(MAX)",
            "formula": "VARCHAR(255) NULL",
            "compound_synonyms": "NVARCHAR(MAX) NULL",
        },
        sync=DiffSync(diff_columns=("compound_name", "formula")),
        __table_name__="CompoundData",
    ),
    "DrugData": TableSchema(
        key="drug_id",
        columns={
            "drug_id": "VARCHAR(20)",
            "drug_name": "NVARCHAR(MAX)",
            "drug_type": "VARCHAR(30)",
        },
        sync=DiffSync(diff_columns=("drug_name", "drug_type")),
        __table_name__="DrugData",
    ),
    "reactions": TableSchema(
        key="reaction_id",
        columns={
            "reaction_id": "VARCHAR(20)",
            "name": "NVARCHAR(MAX)",
            "compartment": "VARCHAR(100) NULL",
            "reaction_type": "VARCHAR(30)",
            "pathway_id": "VARCHAR(20)",
        },
        sync=DiffSync(diff_columns=("name", "compartment", "reaction_type")),
        __table_name__="reactions",
    ),
    "PathwayData": TableSchema(
        key="pathway_id",
        columns={
            "pathway_id": "VARCHAR(20)",
            "description": "NVARCHAR(MAX)",
        },
        sync=DiffSync(diff_columns=("description",)),
        constraints=(
            ForeignKey(
                name="fk_pathway_id",
                columns=("pathway_id",),
                ref_table="PathwayIds",
                ref_columns=("pathway_id",),
            ),
        ),
        __table_name__="PathwayData",
    ),
    "reaction_participants": TableSchema(
        columns={
            "reaction_id": "VARCHAR(20)",
            "entity_id": "VARCHAR(20)",
            "role": "VARCHAR(20)",
            "stoichiometry": "INT",
            "pathway_id": "VARCHAR(20)",
        },
        sync=IdentityHashSync(
            identity_hash=("reaction_id", "entity_id", "role", "pathway_id"),
            coverage_scope_columns=("pathway_id",),
        ),
        __table_name__="reaction_participants",
    ),
    # The pathway DAG: one row per (pathway, parent) edge, roots carrying a
    # NULL parent. ~2,900 edges over ~2,880 pathways.
    #
    # Keyed on stId rather than an auto-id matched by name, as kegg_class
    # was: Reactome event names are not unique, so a name match binds some
    # children under the wrong parent. With a real id the deferred FK
    # resolution disappears entirely.
    "pathway_class": TableSchema(
        columns={
            "stid": "VARCHAR(20)",
            "name": "NVARCHAR(512) NOT NULL",
            "parent_stid": "VARCHAR(20) NULL",
        },
        sync=IdentityHashSync(
            identity_hash=("stid", "parent_stid"),
            coverage_scope_columns=("stid",),
        ),
        __table_name__="pathway_class",
    ),

    # Secondary keys. Indexed on (xref_db, xref_id) so a DNA or RNA entity
    # can find its gene without a symbol match.
    "gene_xref": TableSchema(
        columns={
            "uniprot_id": "VARCHAR(20)",
            "xref_db": "VARCHAR(40)",
            "xref_id": "VARCHAR(40)",
        },
        sync=IdentityHashSync(
            identity_hash=("uniprot_id", "xref_db", "xref_id"),
            coverage_scope_columns=("uniprot_id",),
        ),
        __table_name__="gene_xref",
    ),

    # ---- layer 2 ---------------------------------------------------------
    #
    # Derived, so it needs delete-on-change more than the raw tables do: a
    # rule change must retract the edges it stops producing, and only a
    # coverage-scoped sync does that.
    "gene_edges": TableSchema(
        columns={
            "source_id": "VARCHAR(20)",
            "target_id": "VARCHAR(20)",
            "source_label": "VARCHAR(20)",
            "target_label": "VARCHAR(20)",
            "rel_type": "VARCHAR(30)",
            "mechanism": "VARCHAR(20)",
            "sign": "VARCHAR(10) NULL",
            "reaction_id": "VARCHAR(20) NULL",
            "via": "VARCHAR(20) NULL",
            "weight": "INT",
            "pathway_id": "VARCHAR(20)",
        },
        sync=IdentityHashSync(
            identity_hash=("source_id", "target_id", "rel_type", "mechanism",
                           "sign", "via", "pathway_id"),
            coverage_scope_columns=("pathway_id",),
        ),
        __table_name__="gene_edges",
    ),
}


AnnotationTables = Literal["CompoundData", "GeneData", "PathwayData", "reactions", "DrugData", "EntityData"]
