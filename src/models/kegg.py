from dataclasses import dataclass, asdict, field, fields
from enum import Enum
from typing import ClassVar, Any, Dict, Optional
from src.models.base import BaseSQLObject
from datetime import datetime


class EntityType(str, Enum):
    GENE = "gene"
    COMPOUND = "compound"
    ORTHOLOG = "ortholog"
    REACTION = "reaction"
    PATHWAY = "pathway"
    GLYCAN = "glycan"
    DRUG = "drug"


@dataclass(frozen=True)
class Entity(BaseSQLObject):
    entity_id: str
    entity_type: EntityType
    __table_name__: ClassVar[str] = "entities"


@dataclass(frozen=True)
class Pathway(BaseSQLObject):
    # Child-specific attributes
    entity_type: ClassVar[EntityType] = EntityType.PATHWAY
    pathway_id: str
    description: str
    __table_name__: ClassVar[str] = "PathwayData"  

    @classmethod
    def from_kegg(cls, datum: dict) -> "Pathway":
        """Factory method to create a Pathway instance from KEGG data."""
        return cls(
            pathway_id=datum["ENTRY"].split()[0],
            description=datum.get("DESCRIPTION", "")
        )

@dataclass(frozen=True)
class Gene(BaseSQLObject):
    entity_type: ClassVar[EntityType] = EntityType.GENE
    gene_name: str
    uid: int
    full_name: str
    gene_synonym: Optional[str]
    __table_name__: ClassVar[str] = "GeneData"

    @classmethod
    def from_kegg(cls, datum: dict) -> "Gene":
        if 'SYMBOL' not in datum:
            datum['SYMBOL'] = 'LOC' + datum['ENTRY']

        symbols = [s.strip() for s in datum['SYMBOL'].split(',')]
        main_symbol = symbols[0]
        aliases = ", ".join(symbols[1:]) if len(symbols) > 1 else ''
        entrez_id = int(datum['ENTRY'].split()[0])
        full_name = datum['NAME'].replace("(RefSeq)", "").strip()
        return cls(
            gene_name=main_symbol,
            uid = entrez_id,
            full_name = full_name,
            gene_synonym = aliases
        )

@dataclass(frozen=True)
class Compound(BaseSQLObject):
    entity_type: ClassVar[EntityType] = EntityType.COMPOUND
    compound_id: str
    compound_name: str
    formula: str
    compound_synonyms: str
    MOL_WEIGHT: float
    __table_name__: ClassVar[str] = "CompoundData"

    @classmethod
    def from_kegg(cls, datum: dict)-> "Compound":
        weight = 0.0
        formula = 'NA'
        main_name = 'NA'
        synonyms = 'NA'
        id = datum['ENTRY'].split()[0]
        if 'MOL_WEIGHT' in datum:
            weight = round(float(datum['MOL_WEIGHT']), 2)
        elif 'MASS' in datum:
            weight = round(float(datum['MASS'].split()[0]), 2)

        if 'FORMULA' in datum:
            formula = datum['FORMULA']
        elif 'COMPOSITION' in datum:
            formula = datum['COMPOSITION']

        if 'NAME' in datum:
            name_data = datum['NAME']
            names = name_data.split(";")
            if len(names)>1:
                synonyms = ';'.join(datum['NAME'].split(';')[1:])
                main_name = names[0].strip()
            else:
                main_name = name_data
        return cls(
            compound_id = id,
            compound_name = main_name,
            formula = formula,
            compound_synonyms = synonyms,
            MOL_WEIGHT = weight
        )

@dataclass(frozen=True)
class Glycan(BaseSQLObject):
    entity_type: ClassVar[EntityType] = EntityType.GLYCAN
    glycan_id: str
    glycan_name: str
    composition: str
    mass: float
    __table_name__: ClassVar[str] = "GlycanData"

    @classmethod
    def from_kegg(cls, datum: dict) -> "Glycan":
        mass = 0.0
        if 'MASS' in datum:
            mass = round(float(datum['MASS'].split()[0]), 2)
        return cls(
            glycan_id=datum['ENTRY'].split()[0],
            glycan_name=datum.get('NAME', 'undefined'),
            composition=datum.get('COMPOSITION', 'undefined'),
            mass=mass
        )


@dataclass(frozen=True)
class Drug(BaseSQLObject):
    entity_type: ClassVar[EntityType] = EntityType.DRUG
    drug_id: str
    drug_name: str
    formula: str
    drug_synonyms: str
    MOL_WEIGHT: float
    __table_name__: ClassVar[str] = "DrugData"

    @classmethod
    def from_kegg(cls, datum: dict) -> "Drug":
        weight = 0.0
        formula = 'NA'
        main_name = 'NA'
        synonyms = 'NA'
        id = datum['ENTRY'].split()[0]
        if 'MOL_WEIGHT' in datum:
            weight = round(float(datum['MOL_WEIGHT']), 2)
        elif 'MASS' in datum:
            weight = round(float(datum['MASS'].split()[0]), 2)

        if 'FORMULA' in datum:
            formula = datum['FORMULA']
        elif 'COMPOSITION' in datum:
            formula = datum['COMPOSITION']

        if 'NAME' in datum:
            name_data = datum['NAME']
            names = name_data.split(";")
            if len(names)>1:
                synonyms = ';'.join(datum['NAME'].split(';')[1:])
                main_name = names[0].strip()
            else:
                main_name = name_data
        return cls(
            drug_id = id,
            drug_name = main_name,
            formula = formula,
            drug_synonyms = synonyms,
            MOL_WEIGHT = weight
        )


@dataclass(frozen=True)
class Ortholog(BaseSQLObject):
    entity_type: ClassVar[EntityType] = EntityType.ORTHOLOG    
    ortho_id: str
    ortho_name: str
    full_name: str
    __table_name__: ClassVar[str] = "OrthoData"

    @classmethod
    def from_kegg(cls, datum: dict)->"Ortholog":
        return cls(
            ortho_id= datum['ENTRY'].split()[0],
            ortho_name = datum['SYMBOL'].split(", ")[0],
            full_name = datum['NAME']
        )

@dataclass(frozen=True)
class Reaction(BaseSQLObject):
    entity_type: ClassVar[EntityType] = EntityType.REACTION
    reaction_id: str
    name: str
    definition: str
    equation: str
    comment: Optional[str]
    reaction_type: Optional[str]
    pathway_id: Optional[str]
    __table_name__: ClassVar[str] = 'reactions'

    @classmethod
    def from_kegg(cls, datum: dict)-> "Reaction":
        return cls(
            reaction_id=datum['ENTRY'].split()[0],
            name=datum.get('NAME', 'undefined'),
            definition=datum['DEFINITION'],
            equation=datum['EQUATION'],
            comment=datum.get('COMMENT', None),
            reaction_type = datum.get('REACTION_TYPE', None),
            pathway_id = datum.get('PATHWAY_ID', None)
        )

@dataclass(frozen=True)
class Interaction(BaseSQLObject):
    source_id: str
    target_id: str
    relation_type: str
    pathway_id: str
    __table_name__: ClassVar[str] = "interactions"

@dataclass(frozen=True)
class ReactionP(BaseSQLObject):
    reaction_id: str
    entity_id: str
    role: str
    pathway_id: str
    __table_name__: ClassVar[str] = "reaction_participants"

@dataclass(frozen=True)
class EntityPathMem(BaseSQLObject):
    entity_id : str
    pathway_id: str
    __table_name__: ClassVar[str] = "EntityPathMem"

@dataclass
class KGMLEntry:
    """Intermediate parse record for one <entry> tag in a KGML file."""
    entities: list[Entity] = field(default_factory=list)
    entity_path_mem: list[EntityPathMem] = field(default_factory=list)


@dataclass
class PathwayKGMLRecord:
    pathway: Pathway
    entities: list[Entity]
    entity_path_mem: list[EntityPathMem]
    relations: list[Interaction]
    reaction_participants: list[ReactionP]


@dataclass(frozen=True)
class KEGG_CLASS(BaseSQLObject):
    name: str
    parent_name: Optional[str]
    __table_name__: ClassVar[str] = "kegg_class"

@dataclass(frozen=True)
class PathwayIds(BaseSQLObject):
    pathway_id: str
    name: str
    __table_name__: ClassVar[str] = "PathwayIds"

@dataclass(frozen=True)
class KGMLMetaData(BaseSQLObject):
    pathway_id: str
    kgml_hash: str
    last_checked: datetime
    __table_name__: ClassVar[str] = "PathwayKGMLMeta"

@dataclass
class KGMLRecord:
    pathway_code: str
    kgml_bytes: bytes
    metadata: KGMLMetaData

