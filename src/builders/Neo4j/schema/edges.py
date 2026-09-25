from src.models.kegg import EntityPathMem, Interaction, ReactionP
from dataclasses import dataclass, asdict, fields
from typing import ClassVar


from dataclasses import dataclass, asdict, fields
from typing import ClassVar, Optional, Literal, Sequence
from itertools import groupby
from abc import ABC, abstractmethod

@dataclass()
class BaseNeo4jEdge(ABC):
    """
    source_type/target_type/__label__ are ClassVar by default (fixed per
    edge type, baked into the Cypher literal). A subclass whose label
    varies per row instead sets _label_field to the name of the data field
    that feeds it -- the base __label__ property then reads that field off
    the instance -- see Interactions below. A subclass can still opt out
    of this and override __label__ itself (as a ClassVar or a @property)
    when the label needs custom derivation -- see ReactionRelation below.
    """
    source_id: str
    target_id: str
    source_key: str = "id"
    target_key: str = "id"

    source_type: ClassVar[str]
    target_type: ClassVar[str]
    _label_field: ClassVar[Optional[str]] = None
    _structural_fields: ClassVar[tuple] = ()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # A subclass either sets _label_field (dynamic label, validated
        # per-instance in __post_init__ below) or defines its own __label__
        # directly in its own class body (ClassVar or @property). Checking
        # cls.__dict__ rather than getattr avoids picking up the inherited
        # __label__ property itself, which would look truthy for everyone.
        if cls._label_field is not None or cls.__dict__.get("__label__"):
            return
        raise TypeError(f"{cls.__name__} must define __label__ or _label_field")

    def __post_init__(self):
        if self._label_field is not None and not getattr(self, self._label_field):
            raise ValueError(
                f"{self.__class__.__name__} instance has no {self._label_field} set"
            )

    @property
    def __label__(self) -> str:
        if self._label_field is None:
            raise NotImplementedError(
                f"{self.__class__.__name__} must override __label__ or set _label_field"
            )
        return getattr(self, self._label_field)

    @classmethod
    def _has_pathway_id(cls) -> bool:
        return any(f.name == "pathway_id" for f in fields(cls))

    @staticmethod
    def _quote(identifier: str) -> str:
        """
        Backtick-quote a label/type for splicing into Cypher literal text
        (relationship types can't be bound as query parameters). Needed
        because _label_field-derived labels come from external data (e.g.
        KGML relation_type values like "binding/association" or "indirect
        effect") and aren't valid unquoted Cypher identifiers. A backtick in
        the value itself would let it break out of the quoted identifier, so
        that's rejected outright rather than escaped.
        """
        if "`" in identifier:
            raise ValueError(f"unsafe Cypher identifier: {identifier!r}")
        return f"`{identifier}`"

    def to_row(self) -> dict:
        """Splits the dataclass into the identity key + everything else as props."""
        data = asdict(self)
        for key in ("source_id", "target_id", "source_key", "target_key"):
            data.pop(key)
        for key in self._structural_fields:
            data.pop(key, None)

        pathway_id = data.pop("pathway_id", None)

        props = {k: v for k, v in data.items() if v is not None}
        row = {"source_id": self.source_id, "target_id": self.target_id, "props": props}
        if self._has_pathway_id():
            row["pathway_id"] = pathway_id
        return row

    @classmethod
    def _match_clause(cls, verb: str, *, label: str, source_type: str, target_type: str,
                       source_key: str, target_key: str) -> str:
        return (
            f"{verb} (a:{cls._quote(source_type)} {{{source_key}: row.source_id}})"
            f"-[r:{cls._quote(label)}]->"
            f"(b:{cls._quote(target_type)} {{{target_key}: row.target_id}})"
        )

    @classmethod
    def _node_match_clause(cls, *, source_type: str, target_type: str,
                            source_key: str, target_key: str) -> str:
        # MATCH each endpoint separately so an already-connected-elsewhere
        # or not-yet-connected pair of existing nodes still gets reused.
        # A single MERGE across the whole a-[r]->b pattern only matches
        # when that exact triple already exists, so if the nodes exist but
        # this relationship doesn't yet, Neo4j creates duplicate nodes
        # instead of reusing the ones already in the graph.
        return (
            f"MATCH (a:{cls._quote(source_type)} {{{source_key}: row.source_id}})\n"
            f"        MATCH (b:{cls._quote(target_type)} {{{target_key}: row.target_id}})"
        )

    @classmethod
    def upsert_cypher(cls, *, label: str, source_type: str, target_type: str,
                       source_key: str, target_key: str) -> str:
        node_match = cls._node_match_clause(
            source_type=source_type, target_type=target_type,
            source_key=source_key, target_key=target_key,
        )
        quoted_label = cls._quote(label)
        if cls._has_pathway_id():
            return f"""
            UNWIND $rows AS row
            {node_match}
            MERGE (a)-[r:{quoted_label}]->(b)
            SET r += row.props
            SET r.pathway_ids = CASE
                WHEN row.pathway_id IN coalesce(r.pathway_ids, [])
                THEN coalesce(r.pathway_ids, [])
                ELSE coalesce(r.pathway_ids, []) + row.pathway_id
            END
            """
        return f"""
        UNWIND $rows AS row
        {node_match}
        MERGE (a)-[r:{quoted_label}]->(b)
        SET r += row.props
        """

    @classmethod
    def delete_cypher(cls, *, label: str, source_type: str, target_type: str,
                       source_key: str, target_key: str) -> str:
        match_clause = cls._match_clause(
            "MATCH", label=label, source_type=source_type, target_type=target_type,
            source_key=source_key, target_key=target_key,
        )
        if cls._has_pathway_id():
            return f"""
            UNWIND $rows AS row
            {match_clause}
            SET r.pathway_ids = [pid IN coalesce(r.pathway_ids, []) WHERE pid <> row.pathway_id]
            WITH r WHERE size(r.pathway_ids) = 0
            DELETE r
            """
        return f"""
        UNWIND $rows AS row
        {match_clause}
        DELETE r
        """

    @staticmethod
    def _group_key(edge: "BaseNeo4jEdge"):
        # Everything baked into the literal query text (not a $rows param)
        # must match for two edges to share one UNWIND batch.
        return (type(edge), edge.__label__, edge.source_type, edge.target_type,
                edge.source_key, edge.target_key)

    @classmethod
    @abstractmethod
    def from_sql(cls, data: dict)->"BaseNeo4jEdge":
        ...

    @classmethod
    def batch_cypher(cls, edges: Sequence["BaseNeo4jEdge"], mode: str):
        """mode: 'upsert' or 'delete'. Yields (cypher, rows) per group."""
        edges_sorted = sorted(edges, key=lambda e: repr(cls._group_key(e)))
        for _, group in groupby(edges_sorted, key=cls._group_key):
            group = list(group)
            template = group[0]
            kwargs = dict(
                label=template.__label__, source_type=template.source_type,
                target_type=template.target_type, source_key=template.source_key,
                target_key=template.target_key,
            )
            cypher = template.upsert_cypher(**kwargs) if mode == "upsert" else template.delete_cypher(**kwargs)
            yield cypher, [e.to_row() for e in group]


ENTITY_TYPE_LABELS = {
    "gene": "Gene",
    "compound": "Compound",
    "ortholog": "Ortholog",
    "reaction": "Reaction",
    "drug": "Drug",
    "glycan": "Glycan",
}

@dataclass()
class PathwayMembership(BaseNeo4jEdge):
    entity_type: str = ""
    __label__: ClassVar[str] = "belongs_to"
    target_type: ClassVar[str] = "Pathway"
    _structural_fields: ClassVar[tuple] = ("entity_type",)

    def __post_init__(self):
        if self.entity_type not in ENTITY_TYPE_LABELS:
            raise ValueError(f"PathwayMembership got unknown entity_type={self.entity_type!r}")

    @property
    def source_type(self) -> str:
        return ENTITY_TYPE_LABELS[self.entity_type]

    @classmethod
    def from_sql(cls, data)->"PathwayMembership":
        return cls(
            source_id = data["entity_id"],
            target_id = data["pathway_id"],
            entity_type = data["entity_type"],
        )


@dataclass()
class Interactions(BaseNeo4jEdge):
    pathway_id: str = ""
    relation_type: str = ""   # e.g. "ACTIVATION" / "INHIBITION" -- from KGML row
    source_entity_type: str = ""
    target_entity_type: str = ""
    _label_field: ClassVar[Optional[str]] = "relation_type"
    _structural_fields: ClassVar[tuple] = ("source_entity_type", "target_entity_type")

    def __post_init__(self):
        super().__post_init__()
        for endpoint, entity_type in (("source", self.source_entity_type), ("target", self.target_entity_type)):
            if entity_type not in ENTITY_TYPE_LABELS:
                raise ValueError(f"Interactions got unknown {endpoint}_entity_type={entity_type!r}")

    @property
    def source_type(self) -> str:
        return ENTITY_TYPE_LABELS[self.source_entity_type]

    @property
    def target_type(self) -> str:
        return ENTITY_TYPE_LABELS[self.target_entity_type]

    @classmethod
    def from_sql(cls, data)->"Interactions":
        valid_keys = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)

REACTION_ROLE_CONFIG = {
    "substrate": {"label": "SUBSTRATE_OF"},
    "product":   {"label": "PRODUCES"},
    "catalyst":  {"label": "CATALYZES"},
}

@dataclass()
class ReactionRelation(BaseNeo4jEdge):
    role: Literal["substrate", "product", "catalyst"] = "substrate"
    entity_type: str = ""
    _structural_fields: ClassVar[tuple] = ("role", "entity_type")

    def __post_init__(self):
        if self.role not in REACTION_ROLE_CONFIG:
            raise ValueError(f"ReactionRelation got unknown role={self.role!r}")
        if self.entity_type not in ENTITY_TYPE_LABELS:
            raise ValueError(f"ReactionRelation got unknown entity_type={self.entity_type!r}")

    @property
    def source_type(self) -> str:
        # product: reaction -> entity. substrate/catalyst: entity -> reaction.
        return "Reaction" if self.role == "product" else ENTITY_TYPE_LABELS[self.entity_type]

    @property
    def target_type(self) -> str:
        return "Reaction" if self.role in ("substrate", "catalyst") else ENTITY_TYPE_LABELS[self.entity_type]

    @property
    def __label__(self) -> str:
        return REACTION_ROLE_CONFIG[self.role]["label"]

    @classmethod
    def from_sql(cls, data) -> "ReactionRelation":
        role = data["role"]
        entity_type = data["entity_type"]
        return cls(
            source_id=data["entity_id"],
            target_id=data["reaction_id"],
            role=role,
            entity_type=entity_type,
        ) if role != "product" else cls(
            source_id = data["reaction_id"],
            target_id = data["entity_id"],
            role = role,
            entity_type = entity_type,
        )


Edge_REGISTRY: dict[str, type[BaseNeo4jEdge]] = {
    EntityPathMem.__table_name__: PathwayMembership,
    Interaction.__table_name__: Interactions,
    ReactionP.__table_name__: ReactionRelation
}

def build_neo4j_edges(table: str, data: dict) -> BaseNeo4jEdge:
    cls = Edge_REGISTRY[table]
    return cls.from_sql(data)