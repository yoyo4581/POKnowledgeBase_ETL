from dataclasses import dataclass, asdict, fields
from typing import ClassVar, Iterable, Sequence, Optional
from itertools import groupby
from src.models.kegg import EntityType


@dataclass()
class BaseNeo4jNode:
    __label__: ClassVar[str] = ""   # set by each subclass
    __key__: ClassVar[str] = "id"   # override if identity key isn't `id`
    __column_map__: ClassVar[dict[str, str]] = {}
    __dynamic_label__: ClassVar[bool] = False

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not cls.__label__ and not cls.__dynamic_label__:
            raise TypeError(f"{cls.__name__} must define __label__")
        # __init_subclass__ fires when the raw class is built, before the
        # @dataclass decorator runs -- so fields(cls) would only see fields
        # inherited from already-decorated bases, not this subclass's own.
        # Its own __annotations__ dict is populated during class-body
        # execution regardless of the decorator, so check that instead.
        own_fields = cls.__dict__.get("__annotations__", {})
        if cls.__key__ not in own_fields:
            raise TypeError(f"{cls.__name__}.__key__ = {cls.__key__!r} is not a field")

    def resolve_label(self) -> str:
        lbl = getattr(self, "label", None) or self.__label__
        if not lbl.isidentifier():
            raise ValueError(f"unsafe label: {lbl!r}")
        return lbl

    def to_row(self) -> dict:
        data = asdict(self)
        data.pop("label", None)
        key_val = data.pop(self.__key__)
        props = {
            self.__column_map__.get(k, k): v
            for k, v in data.items()
            if v is not None
        }
        return {"id": key_val, "props": props}

    @classmethod
    def upsert_cypher(cls, label: str) -> str:
        return f"""
        UNWIND $rows AS row
        MERGE (n:`{label}` {{{cls.__key__}: row.id}})
        SET n += row.props
        """

    @classmethod
    def delete_cypher(cls, label: str) -> str:
        return f"""
        UNWIND $rows AS row
        MATCH (n:{label} {{{cls.__key__}: row.id}})
        DETACH DELETE n
        """

    @classmethod
    def batch_cypher(cls, nodes: Sequence["BaseNeo4jNode"], mode: str):
        """
        mode: 'upsert' or 'delete'. Yields (cypher, rows) per group.
        Unlike edges, a node's label/key are always static ClassVars (never
        derived from instance data), so grouping by Python type alone is
        enough to guarantee every node in a group shares one Cypher literal.
        """
        key = lambda n: (type(n).__name__, n.resolve_label())
        for (_, label), group in groupby(sorted(nodes, key=key), key=key):
            group = list(group)
            node_cls = type(group[0])
            cypher = (node_cls.upsert_cypher(label) if mode == "upsert"
                      else node_cls.delete_cypher(label))
            yield cypher, [n.to_row() for n in group]


@dataclass
class Function(BaseNeo4jNode):
    uniprot_id: str
    text: str
    embedding: str
    __key__: ClassVar[str] = "uniprot_id"
    __label__: ClassVar[str] = "Function"

@dataclass
class Ontology(BaseNeo4jNode):
    id: str
    definition: str
    definition_xref: list[str]
    hasOboNamespace: str
    name: str
    synonyms: list[str]
    __label__: ClassVar[str] = "Ontology"

@dataclass
class Pathway(BaseNeo4jNode):
    id: str
    name: str
    class_id: str
    __label__: ClassVar[str] = "Pathway"

@dataclass
class Gene(BaseNeo4jNode):
    id: str
    name: str
    full_name: str 
    gene_synonym: str | None
    __label__: ClassVar[str] = "Gene"
    __column_map__: ClassVar[dict[str, str]] = {
        "gene_name": "name",
        "uid": "id"
    } 

@dataclass
class Compound(BaseNeo4jNode):
    id: str
    name: str
    formula: str
    compound_synonyms: str
    MOL_WEIGHT: float
    __label__: ClassVar[str] = "Compound" 
    __column_map__: ClassVar[dict[str, str]] = {
        "compound_id": "id",
        "compound_name": "name"
    }

@dataclass
class Ortholog(BaseNeo4jNode):
    id: str
    name: str
    full_name: str
    __label__: ClassVar[str] = "Ortholog"
    __column_map__: ClassVar[dict[str, str]] = {
        "ortho_id": "id",
        "ortho_name": "name"
    }

@dataclass
class Reaction(BaseNeo4jNode):
    id: str
    name: str
    definition: str
    equation: str
    comment: str | None
    reaction_type: str | None
    pathway_id: str | None
    __label__: ClassVar[str] = "Reaction"
    __column_map__: ClassVar[dict[str, str]] = {
        "reaction_id": "id"
    }

@dataclass
class StructureNode(BaseNeo4jNode):
    __dynamic_label__: ClassVar[bool] = True
    id: str
    label: str

Entity_REGISTRY: dict[EntityType, type[BaseNeo4jNode]] = {
    EntityType.GENE: Gene,
    EntityType.COMPOUND: Compound,
    EntityType.ORTHOLOG: Ortholog,
    EntityType.REACTION: Reaction,
    EntityType.PATHWAY: Pathway
}


def build_neo4j_entity(entity_type: EntityType, data: dict) -> BaseNeo4jNode:
    cls = Entity_REGISTRY[entity_type]
    label = cls.__label__

    return StructureNode(id=data["entity_id"], label=label)


def build_neo4j_annotation(entity_type: EntityType, data: dict) -> BaseNeo4jNode:
    cls = Entity_REGISTRY[entity_type]
    valid_keys = {f.name for f in fields(cls)}
    filtered = {k: v for k, v in data.items() if k in valid_keys}
    return cls(**filtered)