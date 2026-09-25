from src.models.kegg import *



class KEGGEntityFactory:
    REGISTRY: dict

    @staticmethod
    def build_gene(data)->list[Gene]:
        genes = []
        for datum in data:
            genes.append(Gene.from_kegg(datum))
        return genes
    
    @staticmethod
    def build_compound(data)->list[Compound]:
        compounds = []
        for datum in data:
            compounds.append(Compound.from_kegg(datum))
        return compounds
    
    @staticmethod
    def build_reaction(data)->list[Reaction]:
        reactions = []
        for datum in data:
            reactions.append(Reaction.from_kegg(datum))
        return reactions
    
    @staticmethod
    def build_ortholog(data)->list[Ortholog]:
        return [Ortholog.from_kegg(datum) for datum in data]
    
    @staticmethod
    def build_pathway(data)->list[Pathway]:
        return [Pathway.from_kegg(datum) for datum in data]

    @staticmethod
    def build_drug(data)->list[Drug]:
        return [Drug.from_kegg(datum) for datum in data]

    @staticmethod
    def build_glycan(data)->list[Glycan]:
        return [Glycan.from_kegg(datum) for datum in data]


KEGGEntityFactory.REGISTRY = {
    EntityType.GENE: {
        "table": "GeneData",
        "builder": KEGGEntityFactory.build_gene
    },
    EntityType.COMPOUND: {
        "table": "CompoundData",
        "builder": KEGGEntityFactory.build_compound
    },
    EntityType.ORTHOLOG: {
        "table": "OrthoData",
        "builder": KEGGEntityFactory.build_ortholog
    },
    EntityType.REACTION: {
        "table": "reactions",
        "builder": KEGGEntityFactory.build_reaction
    },
    EntityType.PATHWAY: {
        "table": "PathwayData",
        "builder": KEGGEntityFactory.build_pathway
    },
    EntityType.DRUG: {
        "table": "DrugData",
        "builder": KEGGEntityFactory.build_drug
    },
    EntityType.GLYCAN: {
        "table": "GlycanData",
        "builder": KEGGEntityFactory.build_glycan
    }
}

