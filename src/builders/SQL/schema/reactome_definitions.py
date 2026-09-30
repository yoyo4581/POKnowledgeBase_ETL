"""
Reactome table schemas.

Kept in its own module so `definitions.py` stays untouched; merge with

    from .reactome_definitions import reactome_table_schemas
    table_schemas.update(reactome_table_schemas)

after the existing dict literal, then `validate_schema(table_schemas)`.

`entities`, `EntityPathMem` and `PathwayIds` are NOT redefined -- their
columns already take Reactome stIds and UniProt accessions unchanged.

The annotation tables ARE redefined, and the `update()` above is what
replaces them: KEGG keys GeneData on `uid INT` (Entrez) and gives `reactions`
a `definition`/`equation` that Reactome has no counterpart for. Since KEGG
retires, replacing is correct -- but it means the merge order matters and the
production tables need migrating, not just creating.
"""
from .types import (
    TableSchema, DiffSync, IdentityHashSync, ForeignKey, UniqueConstraint,
    DeferredResolution,
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
    "pathway_class": TableSchema(
        key="class_id",
        auto_id=True,
        columns={
            "class_id": "INT",
            "name": "NVARCHAR(255) NOT NULL",
            "parent_id": "INT NULL",
        },
        constraints=(
            ForeignKey(
                name="fk_pathway_class_parent",
                columns=("parent_id",),
                ref_table="pathway_class",
                ref_columns=("class_id",),
                deferred=DeferredResolution(
                    match_columns=("name",),
                    staging_columns=("parent_name",),
                ),
            ),
            UniqueConstraint(name="uq_pathway_class", columns=("name", "parent_id")),
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


# Tables Reactome fills that already exist for KEGG, listed so the overlap is
# explicit rather than discovered at upsert time. Reactome writes stIds and
# UniProt accessions into them; the column types already accommodate both.
SHARED_TABLES = ("entities", "EntityPathMem", "PathwayIds")

# Redefined above; merging replaces the KEGG shape. Production needs a
# migration, not just a create.
REPLACED_TABLES = ("GeneData", "CompoundData", "DrugData", "reactions",
                   "PathwayData", "reaction_participants")

# No Reactome source: Reactome has no ortholog concept, and its glycans are
# SimpleEntity rows with a ChEBI id, so they land in CompoundData.
RETIRED_TABLES = ("OrthoData", "GlycanData", "interactions",
                  "PathwayKGMLMeta", "kegg_class", "EntrezUniprotMap")
