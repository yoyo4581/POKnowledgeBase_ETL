"""
Tables that belong to no pathway source.

PathwayIds, entities and EntityPathMem are the CDC spine: both KEGG and
Reactome fill them, with their own id vocabularies but the same shape.
FunctionData is UniProt and GOOntologyMeta is GO -- neither moves when the
pathway source does.

Merged into whichever source set definitions.py selects.
"""
from .types import TableSchema, DiffSync, IdentityHashSync

shared_table_schemas: dict[str, TableSchema] = {
    "PathwayIds": TableSchema(
        key="pathway_id",
        columns={"pathway_id": "VARCHAR(20)", "name": "VARCHAR(MAX)"},
        sync=DiffSync(diff_columns=("name",)),
        __table_name__ = "PathwayIds",
    ),
    "entities": TableSchema(
        key="entity_id",
        columns={"entity_id": "VARCHAR(20)", "entity_type": "VARCHAR(30)"},
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
    "FunctionData": TableSchema(
        key="uniprot_id",
        columns={
            "uniprot_id": "VARCHAR(20)",
            "function_text": "TEXT",
        },
        __table_name__ = "FunctionData"
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
            identity_hash=("source", "etag", "last_modified", "node_count", "edge_count"),
            coverage_scope_columns=("source",),
        ),
        __table_name__ = "GOOntologyMeta"
    ),
}
