from .types import TableSchema
from .types import DiffSync, IdentityHashSync, CompositeKeySync, PrimaryCompositeKey, ForeignKey, UniqueConstraint, DeferredResolution
import re
from typing import Literal

table_schemas: dict[str, TableSchema] = {
    "PathwayIds": TableSchema(
        key="pathway_id",
        columns={"pathway_id": "VARCHAR(20)", "name": "VARCHAR(MAX)"},
        sync=DiffSync(diff_columns=("name",)),
        __table_name__ = "PathwayIds",
    ),
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
    "entities": TableSchema(
        key="entity_id",
        columns={"entity_id": "VARCHAR(20)", "entity_type": "VARCHAR(20)"},
        sync=DiffSync(diff_columns=("entity_type",)),
        __table_name__ = "entities"
    ),
    "EntityPathMem": TableSchema(
        columns={"pathway_id": "VARCHAR(20)", "entity_id": "VARCHAR(20)"},
        sync=IdentityHashSync(
            identity_hash=("pathway_id", "entity_id"), 
            coverage_scope_columns=("pathway_id",)),
        __table_name__ = "EntityPathMem"
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
            "description": "NVARCHAR(255)",
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
            "compound_synonms": "VARCHAR(MAX)",
            "MOL_WEIGHT": "NUMERIC(10,2)"
        },
        __table_name__ = "CompoundData"
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
    "FunctionData": TableSchema(
        key="uniprot_id",
        columns={
            "uniprot_id": "VARCHAR(20)",
            "function_text": "TEXT",
        },
        __table_name__ = "FunctionData"
    ),
    "reactions": TableSchema(
        key="reaction_id",
        columns={
            "reaction_id": "VARCHAR(20)",
            "name": "VARCHAR(200)",
            "definition": "TEXT",
            "equation": "TEXT",
            "comment": "TEXT",
            "reaction_type": "VARCHAR(20)",
            "pathway_id": "VARCHAR(20)"
        },
        constraints=(
            ForeignKey(
                name="r_pathway_id",
                columns=("pathway_id",),
                ref_table="PathwayIds",
                ref_columns=("pathway_id",),
            ),
        ),
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
    "GOOntologyMeta": TableSchema(
        columns={
            "source": "VARCHAR(10)",
            "etag": "VARCHAR(255)",
            "last_modified": "VARCHAR(50)",
            "node_count": "INT",
            "edge_count": "INT",
            "last_checked": "DATETIME",
        },
        sync=IdentityHashSync(
            identity_hash=("etag", "last_modified", "node_count", "edge_count"),
            coverage_scope_columns=("source",),
        ),
        __table_name__ = "GOOntologyMeta"
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

AnnotationTables = Literal["CompoundData", "GeneData", "OrthoData", "PathwayData", "reactions"]


def _base_type(col_type: str) -> str:
    """Extract the core SQL type, stripping NULL/NOT NULL and whitespace variance."""
    return re.sub(r"\s+(NOT\s+)?NULL\b", "", col_type, flags=re.IGNORECASE).strip().upper()

def validate_schema(table_schemas: dict[str, TableSchema]) -> None:
    for table_name, schema in table_schemas.items():
        for c in schema.constraints:
            if isinstance(c, ForeignKey):
                if c.ref_table not in table_schemas:
                    raise ValueError(f"{table_name}.{c.name}: unknown table '{c.ref_table}'")
                ref_schema = table_schemas[c.ref_table]
                missing = set(c.ref_columns) - set(ref_schema.columns)
                if missing:
                    raise ValueError(f"{table_name}.{c.name}: unknown columns {missing} in '{c.ref_table}'")

                for local_col, ref_col in zip(c.columns, c.ref_columns):
                    local_type = _base_type(schema.columns[local_col])
                    ref_type = _base_type(ref_schema.columns[ref_col])
                    if local_type != ref_type:
                        raise ValueError(
                            f"{table_name}.{c.name}: type mismatch — "
                            f"{table_name}.{local_col} is {schema.columns[local_col]}, "
                            f"{c.ref_table}.{ref_col} is {ref_schema.columns[ref_col]}"
                        )

                    
validate_schema(table_schemas)