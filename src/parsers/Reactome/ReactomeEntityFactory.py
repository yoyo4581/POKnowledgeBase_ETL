from src.models.reactome import *


class ReactomeEntityFactory:
    """Builders keyed by EntityType, mirroring KEGGEntityFactory.

    Every parse rule lives on the model as `from_reactome`, so adding an
    entity type is a dataclass plus one REGISTRY line -- never a branch in
    the caller.
    """
    REGISTRY: dict

    @staticmethod
    def build_gene(data) -> list[Gene]:
        return [Gene.from_reactome(datum) for datum in data]

    @staticmethod
    def build_compound(data) -> list[Compound]:
        return [Compound.from_reactome(datum) for datum in data]

    @staticmethod
    def build_drug(data) -> list[Drug]:
        return [Drug.from_reactome(datum) for datum in data]

    @staticmethod
    def build_pathway(data) -> list[Pathway]:
        return [Pathway.from_reactome(datum) for datum in data]

    @staticmethod
    def build_reaction(data) -> list[Reaction]:
        return [Reaction.from_reactome(datum) for datum in data]

    @staticmethod
    def build_entity(data) -> list[Entity]:
        return [Entity.from_reactome(datum) for datum in data]

    @staticmethod
    def build_entity_data(data) -> list[EntityData]:
        return [EntityData.from_reactome(datum) for datum in data]


ReactomeEntityFactory.REGISTRY = {
    EntityType.GENE: {
        "table": Gene.__table_name__,
        "builder": ReactomeEntityFactory.build_gene,
    },
    EntityType.COMPOUND: {
        "table": Compound.__table_name__,
        "builder": ReactomeEntityFactory.build_compound,
    },
    EntityType.DRUG: {
        "table": Drug.__table_name__,
        "builder": ReactomeEntityFactory.build_drug,
    },
    EntityType.PATHWAY: {
        "table": Pathway.__table_name__,
        "builder": ReactomeEntityFactory.build_pathway,
    },
    EntityType.REACTION: {
        "table": Reaction.__table_name__,
        "builder": ReactomeEntityFactory.build_reaction,
    },
}

# Physical entities share one annotation table -- they have no identity of
# their own beyond a name and a compartment, and what they resolve TO is
# carried by IS_FORM_OF rather than by a per-class table.
PHYSICAL_ENTITY_TYPES = (
    EntityType.EWAS,
    EntityType.COMPLEX,
    EntityType.DEFINED_SET,
    EntityType.CANDIDATE_SET,
    EntityType.POLYMER,
    EntityType.SIMPLE_ENTITY,
    EntityType.OTHER_ENTITY,
    EntityType.GENOME_ENCODED,
)

for _entity_type in PHYSICAL_ENTITY_TYPES:
    ReactomeEntityFactory.REGISTRY[_entity_type] = {
        "table": EntityData.__table_name__,
        "builder": ReactomeEntityFactory.build_entity_data,
    }
