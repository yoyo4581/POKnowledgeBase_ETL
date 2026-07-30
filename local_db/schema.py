kgml_defaults = {
    "entity_type": "",
    "entities": [],
    "reaction": [],
    "substrates": [],
    "products": [],
    "relations": [],
    "relation_types": [],
    "reaction_type": None
}


from typing import NamedTuple
from enum import Enum

class EntityType(str, Enum):
    GENE = "gene"
    COMPOUND = "compound"
    ORTHOLOG = "ortholog"
    PATHWAYS = "pathways"
    REACTION = "reaction"


class Entity(NamedTuple):
    name: str
    type: str

class Interaction(NamedTuple):
    source_id: str
    target_id: str
    relation_type: str
    pathway_id: str

class EntityPathMem(NamedTuple):
    pathway_id: str
    entity_id: str

class Reaction(NamedTuple):
    reaction_id: str
    name: str
    definition: str
    equation: str
    comment: str
    reaction_type: str
    pathway_id: str

class ReactionP(NamedTuple):
    reaction_id: str
    entity_id: str
    role: str
    pathway_id: str

class PathwayIds(NamedTuple):
    pathway_id: str
    name: str

class PathwayData(NamedTuple):
    pathway_id: str
    name: str
    description: str


class GeneData(NamedTuple):
    gene_name: str
    uid: int
    full_name: str
    gene_synonym: str

class CompoundData(NamedTuple):
    compound_id: str
    compound_name: str
    formula: str
    compound_synonyms: str
    MOL_WEIGHT: float

class OrthoData(NamedTuple):
    ortho_id: str
    ortho_name: str
    full_name: str


table_schemas = {
    # Structural Tables
        "PathwayIds": {
            "key": "pathway_id",
            "columns": {
                "pathway_id": "VARCHAR(20)",
                "name": "VARCHAR(MAX)"
            }
        },
        "PathwayKGMLMeta": {
            "key": "pathway_id",
            "columns": {
                "pathway_id": "VARCHAR(20)",
                "kgml_hash": "CHAR(64)",
                "last_checked": "DATETIME"
            },
            "diff_columns": [
                "kgml_hash"
            ]
        },
        "entities": {
            "key": "entity_id",
            "columns": {
                "entity_id": "VARCHAR(20)",
                "entity_type": "VARCHAR(20)"
            },
            "diff_columns": [
                "entity_type"
            ]
        },
        "EntityPathMem": {
            "delete_stale": True,
            "columns": {
                "pathway_id": "VARCHAR(20)",
                "entity_id": "VARCHAR(20)"
            },
            "identity_hash": [
                "pathway_id",
                "entity_id"
            ]
        },
        "interactions": {
            "delete_stale": True,
            "columns": {
                "source_id": "VARCHAR(50)",
                "target_id": "VARCHAR(50)",
                "relation_type": "VARCHAR(50)",
                "pathway_id": "VARCHAR(50)"
            },
            "identity_hash": [
                "source_id",
                "target_id",
                "relation_type",
                "pathway_id"
            ]
        },


    # Annotation Tables
        "kegg_class": {
            "key": "class_id",
            "columns": {
                "class_id": "INT IDENTITY(1,1)",  # auto-increment primary key
                "name": "NVARCHAR(255) NOT NULL",
                "parent_id": "INT NULL"  # foreign key will be added later
            },
            "constraints": [
                "CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES kegg_class(class_id)",
                "CONSTRAINT uq_name_parent UNIQUE (name, parent_id)"
            ]
        },
        "PathwayData":{
            "key": "pathway_id",
            "columns": {
                "pathway_id": "VARCHAR(20) REFERENCES PathwayIds(pathway_id)",
                "description": "TEXT",
                "class_id": "INTEGER REFERENCES kegg_class(class_id)"
            }
        },
        "CompoundData": {
            "key": "compound_id",
            "columns": {
                "compound_id": "VARCHAR(20)",
                "compound_name": "VARCHAR(MAX)",
                "formula": "VARCHAR(MAX)",
                "compound_synonyms": "VARCHAR(MAX)",
                "MOL_WEIGHT": "NUMERIC(10,2)"
            }
        },
        "OrthoData": {
            "key": "ortho_id",
            "columns": {
                "ortho_id": "VARCHAR(20)",
                "ortho_name": "VARCHAR(20)", 
                "full_name": "TEXT"
            }
        },
        "FunctionData": {
            "key": "uniprot_id",
            "columns": {
                "uniprot_id": "VARCHAR(20)",
                "function_text": "TEXT",
            }
        },
        "reactions": {
            "key": "reaction_id",
            "columns": {
                "reaction_id": "VARCHAR(20)",
                "name": "VARCHAR(200)",
                "definition": "TEXT",
                "equation": "TEXT",
                "comment": "TEXT",
                "reaction_type": "VARCHAR(20)",
                "pathway_id": "VARCHAR(20)"
            },
            "constraints": [
                "CONSTRAINT r_path FOREIGN KEY (pathway_id) REFERENCES PathwayIds(pathway_id)"
            ]
        },
        "reaction_participants": {
            "delete_stale": True,
            "columns": {
                "reaction_id": "VARCHAR(20)",
                "entity_id": "VARCHAR(20)",
                "role": "VARCHAR(20)",
                "pathway_id": "VARCHAR(20)",
            },
            "identity_hash": [
                "reaction_id",
                "entity_id",
                "role",
                "pathway_id"
            ]
        },
        "GeneData": {
            "key": "uid",
            "columns": {
                "gene_name": "VARCHAR(50)",
                "uid": "INT",
                "full_name": "TEXT",
                "gene_synonym": "VARCHAR(255)"
            }
        },

}
