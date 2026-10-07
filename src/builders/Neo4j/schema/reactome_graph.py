"""
Neo4j node and edge builders for the Reactome graph.

Registries rather than edits to nodes.py/edges.py, so the KEGG ones can be
removed independently. Merge with:

    from .reactome_graph import REACTOME_ENTITY_REGISTRY, REACTOME_EDGE_REGISTRY
    Entity_REGISTRY.update(REACTOME_ENTITY_REGISTRY)
    Edge_REGISTRY.update(REACTOME_EDGE_REGISTRY)
"""
from dataclasses import dataclass, field, fields
from typing import ClassVar, Optional

from src.builders.Neo4j.schema.nodes import BaseNeo4jNode
from src.builders.Neo4j.schema.edges import BaseNeo4jEdge
from src.models.reactome import (
    EntityData, EntityIdentity, EntityMoiety, EntityPathMem, EntityType,
    GeneEdge, Membership, Participation, RelType, Role,
)


# --------------------------------------------------------------------------
# nodes
# --------------------------------------------------------------------------

class _EntityLabelled:
    """Adds the shared :Entity label to a Reactome node.

    Every structural edge matches its endpoints as (:Entity {id}), because
    an endpoint can be any of a dozen classes and the edge row never says
    which -- entity_membership carries two stIds and nothing else. Without
    a label they all share, those MATCHes find nothing and the MERGE
    silently creates no relationship, which is how 936k edge rows were
    "written" into an empty graph.

    MERGE stays on the class label and :Entity is added afterwards.
    Merging on :Entity instead would match none of the nodes written before
    this label existed and would create a second copy of every one of them.
    """

    @classmethod
    def upsert_cypher(cls, label: str) -> str:
        return f"""
        UNWIND $rows AS row
        MERGE (n:`{label}` {{{cls.__key__}: row.id}})
        SET n += row.props
        SET n:Entity
        """


@dataclass
class Gene(_EntityLabelled, BaseNeo4jNode):
    id: str
    name: str
    full_name: str
    gene_synonym: Optional[str]
    ensembl_gene: Optional[str] = None
    # GO annotation matches `row.uniprot_id IN g.uniprot_ids` (OntologyState).
    # KEGG-keyed Gene nodes got that list from EntrezUniprotMap; a
    # Reactome-keyed node IS its accession, so the list is [id] and GO
    # annotation keeps working without touching OntologyState.
    uniprot_ids: list[str] = field(default_factory=list)
    # entrez_ids is deliberately NOT a field here. It is an annotation
    # property, written only by entrez_uniprot_annotation from
    # dbo.EntrezUniprotMap, on its own (monthly) cadence. A field would be
    # worse than redundant: to_row() drops None but keeps [], so every
    # structural upsert of a Gene would SET entrez_ids = [] and silently
    # erase the qualifier until the annotation DAG ran again.
    __label__: ClassVar[str] = "Gene"
    __column_map__: ClassVar[dict[str, str]] = {"uniprot_id": "id", "gene_name": "name"}


@dataclass
class Compound(_EntityLabelled, BaseNeo4jNode):
    id: str
    name: str
    formula: Optional[str]
    compound_synonyms: Optional[str]
    __label__: ClassVar[str] = "Compound"
    __column_map__: ClassVar[dict[str, str]] = {"compound_id": "id", "compound_name": "name"}


@dataclass
class Drug(_EntityLabelled, BaseNeo4jNode):
    id: str
    name: str
    drug_type: str
    __label__: ClassVar[str] = "Drug"
    __column_map__: ClassVar[dict[str, str]] = {"drug_id": "id", "drug_name": "name"}


@dataclass
class Reaction(_EntityLabelled, BaseNeo4jNode):
    id: str
    name: str
    compartment: Optional[str]
    reaction_type: str
    __label__: ClassVar[str] = "Reaction"
    __column_map__: ClassVar[dict[str, str]] = {"reaction_id": "id"}


@dataclass
class Pathway(_EntityLabelled, BaseNeo4jNode):
    id: str
    name: str
    __label__: ClassVar[str] = "Pathway"
    __column_map__: ClassVar[dict[str, str]] = {"pathway_id": "id", "description": "name"}


@dataclass
class PhysicalEntity(_EntityLabelled, BaseNeo4jNode):
    """One state of a molecule. `WEE1` and `p-WEE1` are two of these, both
    IS_FORM_OF the one Gene -- which is what lets a query be about the gene
    without the graph having lost the state."""
    id: str
    name: str
    compartment: Optional[str]
    label: str = "Entity"
    __dynamic_label__: ClassVar[bool] = True
    __column_map__: ClassVar[dict[str, str]] = {"entity_id": "id", "display_name": "name"}


# Per Reactome class, so a query can ask for complexes without joining
# EntityData.
PHYSICAL_ENTITY_LABELS: dict[EntityType, str] = {
    EntityType.EWAS: "Protein",
    EntityType.COMPLEX: "Complex",
    EntityType.DEFINED_SET: "DefinedSet",
    EntityType.CANDIDATE_SET: "CandidateSet",
    EntityType.POLYMER: "Polymer",
    EntityType.SIMPLE_ENTITY: "SmallMolecule",
    EntityType.OTHER_ENTITY: "OtherEntity",
    EntityType.GENOME_ENCODED: "GenomeEncodedEntity",
    EntityType.CELL: "Cell",
}

REACTOME_ENTITY_REGISTRY: dict[EntityType, type[BaseNeo4jNode]] = {
    EntityType.GENE: Gene,
    EntityType.COMPOUND: Compound,
    EntityType.DRUG: Drug,
    EntityType.REACTION: Reaction,
    EntityType.PATHWAY: Pathway,
    **{t: PhysicalEntity for t in PHYSICAL_ENTITY_LABELS},
}

ENTITY_TYPE_LABELS: dict[str, str] = {
    EntityType.GENE.value: "Gene",
    EntityType.COMPOUND.value: "Compound",
    EntityType.DRUG.value: "Drug",
    EntityType.REACTION.value: "Reaction",
    EntityType.PATHWAY.value: "Pathway",
    **{t.value: label for t, label in PHYSICAL_ENTITY_LABELS.items()},
}


def build_reactome_node(entity_type: EntityType, data: dict) -> BaseNeo4jNode:
    """Structure pass: identity only. Properties arrive with the annotation
    row, which carries its own label via PHYSICAL_ENTITY_LABELS."""
    label = ENTITY_TYPE_LABELS[EntityType(entity_type).value]
    return PhysicalEntity(id=data["entity_id"], name=data.get("display_name", ""),
                          compartment=data.get("compartment"), label=label)


def build_reactome_annotation(entity_type: EntityType, data: dict,
                              as_physical: bool = False) -> BaseNeo4jNode:
    """`as_physical` for EntityData, whose rows are per-state physical
    entities whatever their entity_type says.

    EntityType.DRUG is doing two jobs: it is the identity type for a drug
    keyed on its Guide-to-Pharmacology accession, and it is also what
    ChemicalDrug/ProteinDrug/RNADrug map to as physical entities. So 1,083
    EntityData rows arrive typed `drug` carrying entity_id/display_name,
    and the Drug identity class wants drug_id/drug_name -- nothing maps and
    it is constructed with no arguments at all. SimpleEntity does not have
    this problem: it has its own SIMPLE_ENTITY type, distinct from the
    COMPOUND identity. The label is unaffected either way, so these still
    MERGE onto the nodes they already have.
    """
    cls = PhysicalEntity if as_physical else REACTOME_ENTITY_REGISTRY[EntityType(entity_type)]
    mapped = {cls.__column_map__.get(k, k): v for k, v in data.items()}
    valid = {f.name for f in fields(cls)}
    filtered = {k: v for k, v in mapped.items() if k in valid}
    if cls is PhysicalEntity:
        filtered["label"] = ENTITY_TYPE_LABELS[EntityType(entity_type).value]
    if cls is Gene and filtered.get("id"):
        filtered.setdefault("uniprot_ids", [filtered["id"]])
    return cls(**filtered)


# --------------------------------------------------------------------------
# edges
# --------------------------------------------------------------------------

MEMBERSHIP_LABELS = {
    "hasComponent": "HAS_COMPONENT",
    "hasMember": "HAS_MEMBER",
    "hasCandidate": "HAS_CANDIDATE",
    # A polymer physically contains its unit, so it rides the same edge
    # rather than adding a fourth type to every traversal. Kept distinct in
    # SQL, where the finer diff is free.
    "repeatedUnit": "HAS_COMPONENT",
}

# Direction carries meaning: everything feeding a reaction points AT it and
# products point away, so reaction order is one readable traversal --
# (a)-[:PRODUCT]->(e)-[:REACTANT]->(b).
ROLE_LABELS = {
    Role.REACTANT.value: "REACTANT",
    Role.PRODUCT.value: "PRODUCT",
    Role.CATALYST.value: "CATALYST",
    Role.INHIBITOR.value: "INHIBITOR",
    Role.STIMULATOR.value: "STIMULATOR",
}


@dataclass()
class MembershipEdge(BaseNeo4jEdge):
    """child -> parent, so descending an assembly walks against the arrow."""
    rel: str = "hasComponent"
    pathway_id: str = ""
    source_type: ClassVar[str] = "Entity"
    target_type: ClassVar[str] = "Entity"
    _structural_fields: ClassVar[tuple] = ("rel",)

    @property
    def __label__(self) -> str:
        return MEMBERSHIP_LABELS[self.rel]

    @classmethod
    def from_sql(cls, data) -> "MembershipEdge":
        return cls(source_id=data["child_id"], target_id=data["parent_id"],
                   rel=data["rel"], pathway_id=data.get("pathway_id", ""))


@dataclass()
class IdentityEdge(BaseNeo4jEdge):
    """entity -> gene/compound. The join that lets both layers exist at once."""
    reference_type: str = EntityType.GENE.value
    __label__: ClassVar[str] = "IS_FORM_OF"
    source_type: ClassVar[str] = "Entity"
    _structural_fields: ClassVar[tuple] = ("reference_type",)

    @property
    def target_type(self) -> str:
        return ENTITY_TYPE_LABELS[self.reference_type]

    @classmethod
    def from_sql(cls, data) -> "IdentityEdge":
        return cls(source_id=data["entity_id"], target_id=data["reference_id"],
                   reference_type=data["reference_type"])


@dataclass()
class MoietyEdge(BaseNeo4jEdge):
    psi_mod: Optional[str] = None
    __label__: ClassVar[str] = "HAS_MOIETY"
    source_type: ClassVar[str] = "Entity"
    target_type: ClassVar[str] = "Entity"

    @classmethod
    def from_sql(cls, data) -> "MoietyEdge":
        return cls(source_id=data["entity_id"], target_id=data["moiety_id"],
                   psi_mod=data.get("psi_mod"))


@dataclass()
class ParticipationEdge(BaseNeo4jEdge):
    role: str = Role.REACTANT.value
    stoichiometry: int = 1
    pathway_id: str = ""
    _structural_fields: ClassVar[tuple] = ("role",)

    @property
    def source_type(self) -> str:
        return "Reaction" if self.role == Role.PRODUCT.value else "Entity"

    @property
    def target_type(self) -> str:
        return "Entity" if self.role == Role.PRODUCT.value else "Reaction"

    @property
    def __label__(self) -> str:
        return ROLE_LABELS[self.role]

    @classmethod
    def from_sql(cls, data) -> "ParticipationEdge":
        forward = data["role"] != Role.PRODUCT.value
        return cls(
            source_id=data["entity_id"] if forward else data["reaction_id"],
            target_id=data["reaction_id"] if forward else data["entity_id"],
            role=data["role"], stoichiometry=data.get("stoichiometry", 1),
            pathway_id=data.get("pathway_id", ""))


@dataclass()
class PathwayEdge(BaseNeo4jEdge):
    __label__: ClassVar[str] = "IN_PATHWAY"
    source_type: ClassVar[str] = "Entity"
    target_type: ClassVar[str] = "Pathway"

    @classmethod
    def from_sql(cls, data) -> "PathwayEdge":
        return cls(source_id=data["entity_id"], target_id=data["pathway_id"])


@dataclass()
class GeneEdgeRelation(BaseNeo4jEdge):
    """Layer 2. The relationship TYPE carries the edge class, so a MATCH on
    ACTS_ON cannot pick up the chemistry or the membership edges."""
    rel_type: str = RelType.ACTS_ON.value
    mechanism: str = ""
    sign: Optional[str] = None
    weight: int = 1
    reaction_id: Optional[str] = None
    via: Optional[str] = None
    pathway_id: str = ""
    source_label: str = "Gene"
    target_label: str = "Gene"
    _structural_fields: ClassVar[tuple] = ("rel_type", "source_label", "target_label")

    @property
    def source_type(self) -> str:
        return self.source_label

    @property
    def target_type(self) -> str:
        return self.target_label

    @property
    def __label__(self) -> str:
        return self.rel_type

    @classmethod
    def from_sql(cls, data) -> "GeneEdgeRelation":
        valid = {f.name for f in fields(cls)}
        return cls(source_id=data["source_id"], target_id=data["target_id"],
                   **{k: v for k, v in data.items()
                      if k in valid and k not in ("source_id", "target_id")})


REACTOME_EDGE_REGISTRY: dict[str, type[BaseNeo4jEdge]] = {
    Membership.__table_name__: MembershipEdge,
    EntityIdentity.__table_name__: IdentityEdge,
    EntityMoiety.__table_name__: MoietyEdge,
    Participation.__table_name__: ParticipationEdge,
    EntityPathMem.__table_name__: PathwayEdge,
    GeneEdge.__table_name__: GeneEdgeRelation,
}
