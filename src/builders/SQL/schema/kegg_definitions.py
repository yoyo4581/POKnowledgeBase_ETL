"""
KEGG table schemas -- the source set selected when pathway_source=kegg.

Lifted verbatim out of definitions.py when Reactome arrived, so the two
sources sit side by side rather than one being grafted onto the other.
Selecting a source swaps this whole dict for reactome_definitions'; they are
alternatives, never merged. Source-neutral tables live in
shared_definitions.py.

Retired by the Reactome migration, kept here so switching back is a one-line
env change: OrthoData and GlycanData (Reactome has no ortholog concept and
its glycans are SimpleEntity rows with a ChEBI id), interactions (superseded
by gene_edges), EntrezUniprotMap (a Reactome Gene node IS its accession).
"""
from typing import Literal

from .types import (
    TableSchema, DiffSync, IdentityHashSync, CompositeKeySync,
    PrimaryCompositeKey, ForeignKey, UniqueConstraint, DeferredResolution,
)

kegg_table_schemas: dict[str, TableSchema] = {
    "PathwayKGMLMeta": TableSchema(
        key="pathway_id",
        columns={
            "pathway_id": "VARCHAR(20)",
            "kgml_hash": "CHAR(64)",
            "last_checked": "DATETIME",
        },
        sync=DiffSync(diff_columns=("kgml_hash",)),
        __table_name__ = "PathwayKGMLMeta"
    ),
    "interactions": TableSchema(
        columns={
            "source_id": "VARCHAR(50)",
            "target_id": "VARCHAR(50)",
            "relation_type": "VARCHAR(50)",
            "pathway_id": "VARCHAR(50)",
        },
        sync=IdentityHashSync(
            identity_hash=("source_id", "target_id", "relation_type", "pathway_id"), 
            coverage_scope_columns=("pathway_id",)
        ),
        __table_name__ = "interactions"
    ),
    # a genuinely static/reference table, e.g.
    "kegg_class": TableSchema(
        key="class_id",
        auto_id=True, # Auto id should mask the key column
        columns={
            "class_id": "INT", 
            "name": "NVARCHAR(255) NOT NULL", 
            "parent_id": "INT NULL"
        },
        constraints=(
            ForeignKey(
                name="fk_parent",
                columns=("parent_id",), # constraint on parent_id of production
                ref_table="kegg_class", # referencing the class_id of kegg_class table (must be a value in this column)
                ref_columns=("class_id",),
                deferred=DeferredResolution(
                    match_columns=('name',), # during staging use parent_name, its value should match production's name, then pull the class_id as the parent_id.
                    staging_columns=("parent_name",)
                )
            ),
            UniqueConstraint(
                name="uq_name_parent",
                columns=("name", "parent_id"),
            ),
        ),
        __table_name__ = "kegg_class"
    ),
    "PathwayData": TableSchema(
        key="pathway_id",
        columns = {
            "pathway_id": "VARCHAR(20)",
            "description": "NVARCHAR(MAX)",
            "class_id": "INT",
        },
        constraints=(
            ForeignKey(
                name="fk_pathway_id",
                columns=("pathway_id",), # constraint on pathway_id of production.
                ref_table="PathwayIds",
                ref_columns=("pathway_id",), # references the class_id of PathwayIds.
            ),
            ForeignKey(
                name="fk_class_id",
                columns=("class_id",), # constraint on the class_id of production
                ref_table="kegg_class",
                ref_columns=("class_id",), # references the class_id of kegg_class
                deferred=DeferredResolution(
                    match_columns=("name",), # during staging use description column, its value should match kegg_class name column, then pull the class_id as this table's class_id
                    staging_columns=("description",)
                )
            ),
        ),
        __table_name__ = "PathwayData"
    ),
    "OrthoData": TableSchema(
        key="ortho_id",
        columns={
            "ortho_id": "VARCHAR(20)",
            "ortho_name": "VARCHAR(20)",
            "full_name": "TEXT"
        },
        __table_name__ = "OrthoData"
    ),
    "CompoundData": TableSchema(
        key="compound_id",
        columns={
            "compound_id": "VARCHAR(20)",
            "compound_name": "VARCHAR(MAX)",
            "formula": "VARCHAR(MAX)",
            "compound_synonyms": "VARCHAR(MAX)",
            "MOL_WEIGHT": "NUMERIC(10,2)"
        },
        __table_name__ = "CompoundData"
    ),
    "DrugData": TableSchema(
        key="drug_id",
        columns={
            "drug_id": "VARCHAR(20)",
            "drug_name": "VARCHAR(MAX)",
            "formula": "VARCHAR(MAX)",
            "drug_synonyms": "VARCHAR(MAX)",
            "MOL_WEIGHT": "NUMERIC(10,2)"
        },
        __table_name__ = "DrugData"
    ),
    "GlycanData": TableSchema(
        key="glycan_id",
        columns={
            "glycan_id": "VARCHAR(20)",
            "glycan_name": "VARCHAR(MAX)",
            "composition": "VARCHAR(MAX)",
            "mass": "NUMERIC(10,2)"
        },
        __table_name__ = "GlycanData"
    ),
    "GeneData": TableSchema(
        key="uid",
        columns={
            "gene_name": "VARCHAR(50)",
            "uid": "INT",
            "full_name": "TEXT",
            "gene_synonym": "VARCHAR(255)"
        },
        __table_name__ = "GeneData"
    ),
    "reactions": TableSchema(
        key="reaction_id",
        columns={
            "reaction_id": "VARCHAR(20)",
            "name": "NVARCHAR(MAX)",
            "definition": "TEXT",
            "equation": "TEXT",
            "comment": "TEXT",
        },
        __table_name__ = "reactions"
    ),
    "EntrezUniprotMap": TableSchema(
        key=PrimaryCompositeKey(("entrez_id", "uniprot_id")),
        columns={
            "entrez_id": "INT",
            "uniprot_id": "VARCHAR(20)",
        },
        sync=CompositeKeySync(coverage_scope_columns=("entrez_id",)),
        __table_name__ = "EntrezUniprotMap"
    ),
    "reaction_participants": TableSchema(
        columns={
            "reaction_id": "VARCHAR(20)",
            "entity_id": "VARCHAR(20)",
            "role": "VARCHAR(20)",
            "pathway_id": "VARCHAR(20)"
        },
        sync=IdentityHashSync(
            identity_hash=("reaction_id", "entity_id", "role", "pathway_id"),
            coverage_scope_columns=("pathway_id",),
        ),
        constraints=(
            ForeignKey(
                name="rp_entity_id",
                columns=("entity_id",),
                ref_table="entities",
                ref_columns=("entity_id",),
            ),
            ForeignKey(
                name="rp_pathway_id",
                columns=("pathway_id",),
                ref_table="PathwayIds",
                ref_columns=("pathway_id",),
            ),
        ),
        __table_name__ = "reaction_participants"
    ),
}

AnnotationTables = Literal["CompoundData", "GeneData", "OrthoData", "PathwayData", "reactions", "DrugData", "GlycanData"]
