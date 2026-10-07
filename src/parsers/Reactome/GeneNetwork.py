"""
The gene network projected off a resolved pathway.

One test does the work: an entity id in the reactant closure and absent from
the product closure has CHANGED. Everything else follows. The rules, the
evidence for each and the dead ends are in
`MODELING_NOTES.md`.

Nodes are keyed the way `entities` keys them -- UniProt for a gene, ChEBI for
a compound, stId for an assembly -- so the output stages straight into
`gene_edges` and joins GO and UniProt function with no bridge.
"""
from __future__ import annotations

import collections
from dataclasses import dataclass, field

from src.models.reactome import (
    EntityType, GeneEdge, Mechanism, Membership_Rel, PathwayRecord, RelType, Role,
)

# Cosubstrates: consumed, never attached, so `moieties` cannot find them.
# Cofactors that identify an enzyme (haem, FAD, Fe-S) are deliberately absent.
# Namespaced to match the compound ids the parser emits: an accession is
# only unique inside its database, so a bare "30616" no longer identifies
# ATP anywhere in the graph.
CURRENCY_CHEBI = frozenset({
    "chebi:15377", "chebi:15378", "chebi:29888",        # H2O, H+, OH-
    "chebi:30616", "chebi:456216",                      # ATP, ADP
    "chebi:43474", "chebi:33019",                       # Pi, PPi
    "chebi:16526", "chebi:15379",                       # CO2, O2
    "chebi:57540", "chebi:57945",                       # NAD(H)
    "chebi:58349", "chebi:57783",                       # NADP(H)
})

# ("Gene", "P04637") | ("Compound", "29654") | ("Entity", "R-HSA-6799192")
Node = tuple[str, str]

# A target resolving to one gene IS that gene; anything larger is a machine.
MIN_ASSEMBLY = 2

SIGN_OF_ROLE = {Role.INHIBITOR: "negative", Role.STIMULATOR: "positive"}

DIRECTED_MECHANISMS = (Mechanism.CATALYSIS, Mechanism.REGULATION)


@dataclass
class PathwayGraph:
    """Lookup view over one resolved pathway.

    Built either from a PathwayRecord in memory or from SQL rows, so layer 2
    can run in the same pass as the resolve or as a later SQL-to-SQL step
    without a second implementation.
    """
    pathway_id: str
    children: dict[str, list[str]] = field(default_factory=dict)
    gene_of: dict[str, str] = field(default_factory=dict)
    compound_of: dict[str, str] = field(default_factory=dict)
    moieties_of: dict[str, list[str]] = field(default_factory=dict)
    participants: dict[str, list[tuple[str, Role]]] = field(default_factory=dict)

    @classmethod
    def from_record(cls, record: PathwayRecord) -> "PathwayGraph":
        g = cls(pathway_id=record.pathway.pathway_id)
        for m in record.memberships:
            g.children.setdefault(m.parent_id, []).append(m.child_id)
        for mo in record.moieties:
            g.moieties_of.setdefault(mo.entity_id, []).append(mo.moiety_id)
        for p in record.participations:
            g.participants.setdefault(p.reaction_id, []).append((p.entity_id, p.role))
        g.gene_of = dict(record.gene_of)
        g.compound_of = dict(record.compound_of)
        return g

    @classmethod
    def from_rows(cls, pathway_id: str, memberships, moieties, participations,
                  identities) -> "PathwayGraph":
        """`identities` is entity_id -> (EntityType, accession) from
        EntityData joined to its IS_FORM_OF target."""
        g = cls(pathway_id=pathway_id)
        for r in memberships:
            g.children.setdefault(r["parent_id"], []).append(r["child_id"])
        for r in moieties:
            g.moieties_of.setdefault(r["entity_id"], []).append(r["moiety_id"])
        for r in participations:
            g.participants.setdefault(r["reaction_id"], []).append(
                (r["entity_id"], Role(r["role"])))
        for entity_id, (kind, accession) in identities.items():
            if kind is EntityType.GENE:
                g.gene_of[entity_id] = accession
            elif kind in (EntityType.COMPOUND, EntityType.DRUG):
                g.compound_of[entity_id] = accession
        return g

    def closure(self, stid: str, acc: set[str] | None = None) -> set[str]:
        """Every entity at or under `stid`. Unbounded -- membership nests to
        depth 10 and a cap silently loses whole complexes. `acc` is the cycle
        guard; membership is a DAG, so this terminates."""
        acc = acc if acc is not None else set()
        if stid in acc:
            return acc
        acc.add(stid)
        for child in self.children.get(stid, ()):
            self.closure(child, acc)
        return acc

    def members(self, ids: set[str]) -> set[Node]:
        """Gene and Compound nodes carried DIRECTLY by these entities.

        Direct matters: a container contributes nothing of its own, which is
        what lets the identity test discriminate. Genes and compounds share
        one set -- the namespaces are disjoint, so every subtraction below
        works untouched and the split happens only when an edge is emitted.
        """
        out: set[Node] = set()
        for i in ids:
            gene = self.gene_of.get(i)
            if gene:
                out.add(("Gene", gene))
            compound = self.compound_of.get(i)
            if compound and compound not in CURRENCY_CHEBI:
                out.add(("Compound", compound))
        return out

    def genes(self, ids: set[str]) -> set[str]:
        return {key for label, key in self.members(ids) if label == "Gene"}

    def donated(self, product_ids: set[str]) -> set[Node]:
        """Genes covalently DONATED by this reaction, not destroyed by it.

        Reactome records a conjugated form as hasModifiedResidue ->
        modification -> the moiety entity. Per reaction, so a reaction acting
        ON ubiquitin keeps its ubiquitin edges.
        """
        out: set[Node] = set()
        for pid in product_ids:
            for moiety in self.moieties_of.get(pid, ()):
                out |= {("Gene", g) for g in self.genes(self.closure(moiety))}
        return out


def _union(sets) -> set:
    out = set()
    for s in sets:
        out |= s
    return out


def derive(graph: PathwayGraph) -> list[GeneEdge]:
    """Every edge the pathway licenses, one reaction at a time."""
    seen: set[tuple] = set()
    out: list[GeneEdge] = []

    def emit(s: Node, t: Node, mech: Mechanism, sign, reaction, via=None) -> None:
        if s == t:
            return
        # Every edge needs a protein or an assembly; compound-to-compound is
        # chemistry the reaction already states.
        if s[0] == "Compound" and t[0] == "Compound":
            return
        rel = (RelType.ASSOCIATED_WITH if mech is Mechanism.ASSOCIATION
               else RelType.ACTS_ON if "Compound" not in (s[0], t[0])
               else RelType.ACTS_ON_CHEMICAL)
        key = (s[1], t[1], s[0], t[0], rel, mech, sign, reaction, via)
        if key in seen:
            return
        seen.add(key)
        out.append(GeneEdge(
            source_id=s[1], target_id=t[1], source_label=s[0], target_label=t[0],
            rel_type=rel, mechanism=mech, sign=sign, reaction_id=reaction,
            via=via, weight=1, pathway_id=graph.pathway_id))

    def as_assembly(candidates: set[str], target: set[Node]) -> set[Node]:
        """Name the participant when the target IS that participant's gene set.

        `candidates` is restricted to the role that DEFINED the target --
        searching every participant collapses an inhibitor's target onto a
        product and undoes the identity subtraction.
        """
        if len(target) < MIN_ASSEMBLY:
            return target
        for ent in sorted(candidates):
            if {("Gene", g) for g in graph.genes(graph.closure(ent))} == target:
                return {("Entity", ent)}
        return target

    for reaction_id, participants in graph.participants.items():
        by: dict[Role, set[str]] = collections.defaultdict(set)
        for entity_id, role in participants:
            by[role].add(entity_id)

        reactant_ids = _union(graph.closure(e) for e in by[Role.REACTANT])
        product_ids = _union(graph.closure(e) for e in by[Role.PRODUCT])
        changed = reactant_ids - product_ids
        substrate_side = by[Role.REACTANT] | by[Role.PRODUCT]

        # Donated, not destroyed: keeps enzyme -> ubiquitin out while leaving
        # proteasome -> substrate in.
        donated = graph.donated(product_ids)

        def acted_on(exclude: str | None = None) -> set[Node]:
            """What the reaction operates on: whatever changed, else the
            reactants being joined. Falls back on the absence of GENES, not of
            changed ids -- a set container carries no gene of its own and
            would otherwise read as "something changed"."""
            acted = graph.members(changed) - donated
            if acted:
                return acted
            others = [graph.closure(e) for e in by[Role.REACTANT] if e != exclude]
            return (graph.members(_union(others)) - donated) if others else set()

        changed_side = bool(graph.members(changed) - donated)

        # Enzyme = catalyst minus whatever of itself it consumed, PROTEINS
        # ONLY: a cofactor in an active site is machinery, not an actor.
        enzyme: set[Node] = set()
        for c in by[Role.CATALYST]:
            cc = graph.closure(c)
            enzyme |= {("Gene", g) for g in graph.genes(cc) - graph.genes(changed & cc)}

        for c in by[Role.CATALYST]:
            cands = substrate_side if changed_side else by[Role.REACTANT] - {c}
            targets = as_assembly(cands, acted_on(exclude=c) - enzyme)
            for s_ in sorted(enzyme):
                for t in sorted(targets):
                    emit(s_, t, Mechanism.CATALYSIS, None, reaction_id)

        # Subtract against the UNCOLLAPSED gene set -- subtracting from a
        # single Entity node removes nothing and silently disables the rule.
        if enzyme:
            target_genes = enzyme
            target = as_assembly(by[Role.CATALYST], enzyme)
        else:
            cands = substrate_side if changed_side else by[Role.REACTANT]
            target_genes = acted_on()
            target = as_assembly(cands, target_genes)
        for role in (Role.INHIBITOR, Role.STIMULATOR):
            for g in by[role]:
                agents = graph.members(graph.closure(g)) - target_genes
                for s_ in sorted(agents):
                    for t in sorted(target):
                        emit(s_, t, Mechanism.REGULATION, SIGN_OF_ROLE[role], reaction_id)

        # Association: no modifier, so neither side is the agent. Members link
        # to the assembly they form, not to each other; `via` records which
        # reactant carried each, so a join can require ra.via <> rb.via and
        # never pair two alternatives from one set.
        if not (by[Role.CATALYST] or by[Role.INHIBITOR] or by[Role.STIMULATOR]):
            if len(by[Role.REACTANT]) >= 2:
                for prod in sorted(by[Role.PRODUCT]):
                    for part in sorted(by[Role.REACTANT]):
                        for m in sorted(graph.members(graph.closure(part))):
                            emit(m, ("Entity", prod), Mechanism.ASSOCIATION,
                                 None, reaction_id, via=part)
    return out


def collapse(edges: list[GeneEdge]) -> list[GeneEdge]:
    """One row per (endpoints, rel_type, mechanism, sign, via), weighted by
    DISTINCT supporting reactions -- never derivation paths, which count
    assembly depth rather than evidence."""
    agg: dict[tuple, list[GeneEdge]] = {}
    for e in edges:
        key = (e.source_id, e.target_id, e.source_label, e.target_label,
               e.rel_type, e.mechanism, e.sign, e.via)
        agg.setdefault(key, []).append(e)
    out = []
    for key, group in agg.items():
        reactions = sorted({e.reaction_id for e in group if e.reaction_id})
        first = group[0]
        out.append(GeneEdge(
            source_id=first.source_id, target_id=first.target_id,
            source_label=first.source_label, target_label=first.target_label,
            rel_type=first.rel_type, mechanism=first.mechanism, sign=first.sign,
            reaction_id=reactions[0] if reactions else None,
            via=first.via, weight=len(reactions) or 1,
            pathway_id=first.pathway_id))
    return sorted(out, key=lambda c: (c.rel_type.value, c.source_id, c.target_id))
