from dataclasses import dataclass, field
from enum import Enum
import re
from typing import ClassVar, Optional
from src.models.base import BaseSQLObject
from datetime import datetime


class EntityType(str, Enum):
    """
    Reactome's own vocabulary, split by what the node IS rather than what it
    is made of.

    Identity nodes are shared across pathways and keyed by an external
    accession. Physical entities are per-state and keyed by stId: `WEE1` and
    `p-WEE1` are two of them, both pointing at the one GENE. Collapsing to the
    gene loses the state; collapsing to the state loses the identity.
    """
    # identity -- keyed by an external accession
    GENE = "gene"                   # UniProt
    COMPOUND = "compound"           # ChEBI
    DRUG = "drug"

    # physical entities -- keyed by Reactome stId
    EWAS = "ewas"
    COMPLEX = "complex"
    DEFINED_SET = "defined_set"
    CANDIDATE_SET = "candidate_set"
    POLYMER = "polymer"
    SIMPLE_ENTITY = "simple_entity"
    OTHER_ENTITY = "other_entity"
    GENOME_ENCODED = "genome_encoded_entity"

    # events
    REACTION = "reaction"
    PATHWAY = "pathway"


SCHEMA_CLASS_TO_ENTITY_TYPE: dict[str, EntityType] = {
    "EntityWithAccessionedSequence": EntityType.EWAS,
    "Complex": EntityType.COMPLEX,
    "DefinedSet": EntityType.DEFINED_SET,
    "CandidateSet": EntityType.CANDIDATE_SET,
    "Polymer": EntityType.POLYMER,
    "SimpleEntity": EntityType.SIMPLE_ENTITY,
    "OtherEntity": EntityType.OTHER_ENTITY,
    "GenomeEncodedEntity": EntityType.GENOME_ENCODED,
    "ChemicalDrug": EntityType.DRUG,
    "ProteinDrug": EntityType.DRUG,
    "RNADrug": EntityType.DRUG,
    "Reaction": EntityType.REACTION,
    "BlackBoxEvent": EntityType.REACTION,
    "Polymerisation": EntityType.REACTION,
    "Depolymerisation": EntityType.REACTION,
    "FailedReaction": EntityType.REACTION,
    "Pathway": EntityType.PATHWAY,
    "TopLevelPathway": EntityType.PATHWAY,
}


def entity_type_of(schema_class: str) -> EntityType:
    """Raises on an unrecognised class rather than defaulting.

    A silently mistyped entity still gets a node and still gets edges, so it
    looks fine and is wrong -- and Reactome adds classes.
    """
    try:
        return SCHEMA_CLASS_TO_ENTITY_TYPE[schema_class]
    except KeyError:
        raise ValueError(
            f"Unmapped Reactome schemaClass {schema_class!r}. Add it to "
            f"SCHEMA_CLASS_TO_ENTITY_TYPE -- do not let it default."
        ) from None


class Role(str, Enum):
    """A participant's role, from the SBO term on its <speciesReference>."""
    REACTANT = "reactant"
    PRODUCT = "product"
    CATALYST = "catalyst"
    INHIBITOR = "inhibitor"
    STIMULATOR = "stimulator"


class Membership_Rel(str, Enum):
    """Reactome's four child-bearing slots, kept distinct in SQL even though
    repeatedUnit rides the same Neo4j edge as hasComponent."""
    COMPONENT = "hasComponent"
    MEMBER = "hasMember"
    CANDIDATE = "hasCandidate"
    REPEATED_UNIT = "repeatedUnit"


class RelType(str, Enum):
    """Layer-2 edge classes. A relationship type rather than a property so a
    careless MATCH cannot mix the pathway network with the chemistry."""
    ACTS_ON = "ACTS_ON"
    ACTS_ON_CHEMICAL = "ACTS_ON_CHEMICAL"
    ASSOCIATED_WITH = "ASSOCIATED_WITH"


class Mechanism(str, Enum):
    CATALYSIS = "catalysis"
    REGULATION = "regulation"
    ASSOCIATION = "association"


# --------------------------------------------------------------------------
# node registry
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Entity(BaseSQLObject):
    """The CDC node registry: every node in the graph has a row here, and its
    diff is what decides when a node is created or dropped."""
    entity_id: str
    entity_type: EntityType
    __table_name__: ClassVar[str] = "entities"

    @classmethod
    def from_reactome(cls, datum: dict) -> "Entity":
        return cls(
            entity_id=datum["stId"],
            entity_type=entity_type_of(datum["schemaClass"]),
        )


@dataclass(frozen=True)
class EntityData(BaseSQLObject):
    """Per-state properties of a physical entity. Separate from Entity so a
    renamed or relocated entity is an annotation change, not a node change."""
    entity_id: str
    display_name: str
    compartment: Optional[str]
    __table_name__: ClassVar[str] = "EntityData"

    @classmethod
    def from_reactome(cls, datum: dict) -> "EntityData":
        # Reactome suffixes the compartment onto displayName. Strip it, or the
        # same fact lives in two columns and a name comparison depends on
        # which one a given entity happened to be resolved through.
        name = datum.get("displayName") or datum["stId"]
        compartments = datum.get("compartment") or []
        return cls(
            entity_id=datum["stId"],
            display_name=re.sub(r"\s*\[[^\[\]]*\]$", "", name),
            compartment=compartments[0].get("displayName") if compartments else None,
        )


# --------------------------------------------------------------------------
# identity
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Gene(BaseSQLObject):
    """Keyed by UniProt accession, not symbol: symbols collide, accessions do
    not, and UniProt is what GO annotation and UniProt function already key on."""
    entity_type: ClassVar[EntityType] = EntityType.GENE
    uniprot_id: str
    gene_name: str
    full_name: str
    gene_synonym: Optional[str]
    ensembl_gene: Optional[str]
    __table_name__: ClassVar[str] = "GeneData"

    @classmethod
    def from_reactome(cls, datum: dict) -> "Gene":
        names = datum.get("geneName") or []
        descriptions = datum.get("name") or []
        return cls(
            uniprot_id=datum["identifier"],
            gene_name=names[0] if names else datum["identifier"],
            full_name=descriptions[0] if descriptions else "undefined",
            gene_synonym=", ".join(names[1:]) if len(names) > 1 else None,
            ensembl_gene=datum.get("ensembl_gene"),
        )


@dataclass(frozen=True)
class GeneXref(BaseSQLObject):
    """Secondary keys for a gene, from the gene product's `referenceGene`.

    UniProt stays the identity because it is what GO and UniProt function
    key on, but Reactome models DNA, RNA and protein as separate entities and
    only the protein has a UniProt accession. This table is what lets a
    `CDKN1A gene` entity (ENSG00000124762) reach the same Gene node as the
    CDKN1A protein without guessing from a symbol.
    """
    uniprot_id: str
    xref_db: str
    xref_id: str
    __table_name__: ClassVar[str] = "gene_xref"


@dataclass(frozen=True)
class Compound(BaseSQLObject):
    entity_type: ClassVar[EntityType] = EntityType.COMPOUND
    compound_id: str
    compound_name: str
    formula: Optional[str]
    compound_synonyms: Optional[str]
    __table_name__: ClassVar[str] = "CompoundData"

    @classmethod
    def from_reactome(cls, datum: dict) -> "Compound":
        names = datum.get("name") or []
        return cls(
            compound_id=datum["identifier"],
            compound_name=names[0] if names else datum.get("displayName", "undefined"),
            formula=datum.get("formula"),
            compound_synonyms="; ".join(names[1:]) if len(names) > 1 else None,
        )


@dataclass(frozen=True)
class Drug(BaseSQLObject):
    entity_type: ClassVar[EntityType] = EntityType.DRUG
    drug_id: str
    drug_name: str
    drug_type: str
    __table_name__: ClassVar[str] = "DrugData"

    @classmethod
    def from_reactome(cls, datum: dict) -> "Drug":
        return cls(
            drug_id=datum["identifier"],
            drug_name=datum.get("displayName", "undefined"),
            drug_type=datum.get("schemaClass", "ChemicalDrug"),
        )


# --------------------------------------------------------------------------
# events
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Pathway(BaseSQLObject):
    entity_type: ClassVar[EntityType] = EntityType.PATHWAY
    pathway_id: str
    description: str
    __table_name__: ClassVar[str] = "PathwayData"

    @classmethod
    def from_reactome(cls, datum: dict) -> "Pathway":
        return cls(
            pathway_id=datum["stId"],
            description=datum.get("displayName", ""),
        )


@dataclass(frozen=True)
class Reaction(BaseSQLObject):
    entity_type: ClassVar[EntityType] = EntityType.REACTION
    reaction_id: str
    name: str
    compartment: Optional[str]
    reaction_type: str
    pathway_id: str
    __table_name__: ClassVar[str] = "reactions"

    @classmethod
    def from_reactome(cls, datum: dict) -> "Reaction":
        return cls(
            reaction_id=datum["stId"],
            name=datum.get("displayName", datum["stId"]),
            compartment=datum.get("compartment"),
            reaction_type=datum.get("schemaClass", "Reaction"),
            pathway_id=datum["pathway_id"],
        )


# --------------------------------------------------------------------------
# topology
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class EntityPathMem(BaseSQLObject):
    entity_id: str
    pathway_id: str
    __table_name__: ClassVar[str] = "EntityPathMem"


@dataclass(frozen=True)
class Membership(BaseSQLObject):
    """One child-of-assembly link. Not derivable from the SBML: bqbiol:hasPart
    arrives flattened, with a Complex and a DefinedSet serialised identically."""
    parent_id: str
    child_id: str
    rel: Membership_Rel
    stoichiometry: int
    pathway_id: str
    __table_name__: ClassVar[str] = "entity_membership"


@dataclass(frozen=True)
class Participation(BaseSQLObject):
    reaction_id: str
    entity_id: str
    role: Role
    stoichiometry: int
    pathway_id: str
    __table_name__: ClassVar[str] = "reaction_participants"


@dataclass(frozen=True)
class EntityIdentity(BaseSQLObject):
    """What a physical entity IS -- the IS_FORM_OF edge. `WEE1` and `p-WEE1`
    are two entities pointing at one UniProt accession; this table is what
    lets layer 2 traverse to the gene instead of flattening onto it."""
    entity_id: str
    reference_id: str
    reference_type: EntityType
    __table_name__: ClassVar[str] = "entity_identity"


@dataclass(frozen=True)
class EntityMoiety(BaseSQLObject):
    """A covalently attached group -- ubiquitin, SUMO, a lipoyl. Reached by
    hasModifiedResidue -> modification, and the reason layer 2 can tell a
    donated moiety from a destroyed substrate without a hand-written list."""
    entity_id: str
    moiety_id: str
    psi_mod: Optional[str]
    __table_name__: ClassVar[str] = "entity_moiety"


# --------------------------------------------------------------------------
# layer 2
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class GeneEdge(BaseSQLObject):
    """One projected claim, traceable to the reaction that licensed it.

    Derived from entities + EntityData + entity_membership + entity_moiety, so
    it is a SQL-to-SQL step needing no SBML. Staged rather than computed in
    Neo4j so a rule change deletes the edges it stops producing.
    """
    source_id: str
    target_id: str
    source_label: str
    target_label: str
    rel_type: RelType
    mechanism: Mechanism
    sign: Optional[str]
    reaction_id: Optional[str]
    via: Optional[str]
    weight: int
    pathway_id: str
    __table_name__: ClassVar[str] = "gene_edges"


# --------------------------------------------------------------------------
# source state
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class PathwayIds(BaseSQLObject):
    pathway_id: str
    name: str
    __table_name__: ClassVar[str] = "PathwayIds"


@dataclass(frozen=True)
class PathwayHierarchy(BaseSQLObject):
    """Reactome's event hierarchy, depth-first parent/child pairs."""
    name: str
    parent_name: Optional[str]
    __table_name__: ClassVar[str] = "pathway_class"


@dataclass(frozen=True)
class SBMLMetaData(BaseSQLObject):
    pathway_id: str
    sbml_hash: str
    last_checked: datetime
    __table_name__: ClassVar[str] = "PathwaySBMLMeta"


# --------------------------------------------------------------------------
# intermediate records -- not SQL
# --------------------------------------------------------------------------

@dataclass
class SBMLRecord:
    """One downloaded SBML file and the hash used to decide it changed."""
    pathway_code: str
    sbml_bytes: bytes
    metadata: SBMLMetaData


@dataclass
class PathwayRecord:
    """Everything one resolved pathway contributes, fanned out to tables by a
    consumer's extractor map. Structure and annotation arrive together because
    one resolve produces both."""
    pathway: Pathway
    entities: list[Entity] = field(default_factory=list)
    entity_data: list[EntityData] = field(default_factory=list)
    genes: list[Gene] = field(default_factory=list)
    gene_xrefs: list[GeneXref] = field(default_factory=list)
    compounds: list[Compound] = field(default_factory=list)
    drugs: list[Drug] = field(default_factory=list)
    reactions: list[Reaction] = field(default_factory=list)
    entity_path_mem: list[EntityPathMem] = field(default_factory=list)
    memberships: list[Membership] = field(default_factory=list)
    participations: list[Participation] = field(default_factory=list)
    identities: list[EntityIdentity] = field(default_factory=list)
    moieties: list[EntityMoiety] = field(default_factory=list)

    @property
    def gene_of(self) -> dict[str, str]:
        return {i.entity_id: i.reference_id for i in self.identities
                if i.reference_type is EntityType.GENE}

    @property
    def compound_of(self) -> dict[str, str]:
        return {i.entity_id: i.reference_id for i in self.identities
                if i.reference_type in (EntityType.COMPOUND, EntityType.DRUG)}
