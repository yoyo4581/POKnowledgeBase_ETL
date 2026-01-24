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

class Entity(NamedTuple):
    name: str
    type: str

class Interaction(NamedTuple):
    source_id: str
    target_id: str
    relation_type: str
    pathway_id: str

class Reaction(NamedTuple):
    reaction_id: str
    reaction_type: str
    pathway_id: str

class ReactionP(NamedTuple):
    reaction_id: str
    entity_id: str
    role: str

class PathwayIds(NamedTuple):
    pathway_id: str
    name: str


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
        "PathwayIds": {
            "key": "pathway_id",
            "columns": {
                "pathway_id": "VARCHAR(20)",
                "name": "TEXT"
            }
        },
        "entities": {
            "key": "entity_id",
            "columns": {
                "entity_id": "VARCHAR(20)",
                "name": "TEXT"
            }
        },
        "interactions": {
            "key": None,
            "match_keys": ["source_id", "target_id", "relation_type", "pathway_id"],  # used for matching
            "columns": {
                "source_id": "VARCHAR(50)",
                "target_id": "VARCHAR(50)",
                "relation_type": "VARCHAR(50)",
                "pathway_id": "VARCHAR(50)"
            }
        },
        "CompoundData": {
            "key": "compound_id",
            "columns": {
                "compound_id": "VARCHAR(20)",
                "compound_name": "TEXT",
                "formula": "TEXT",
                "compound_synonyms": "TEXT",
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
        "reaction_participants": {
            "key": "reaction_id",
            "columns": {
                "reaction_id": "VARCHAR(20)",
                "entity_id": "VARCHAR(20)",
                "role": "VARCHAR(20)"
            }
        },
        "reactions": {
            "key": "reaction_id",
            "columns": {
                "reaction_id": "VARCHAR(20)",
                "reaction_type": "VARCHAR(20)",
                "pathway_id": "VARCHAR(20)"
            }
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
