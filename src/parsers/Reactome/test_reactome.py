"""
Golden cases: exact edge sets for reactions worked out by hand.

These test the RULES. Each case is one reaction whose correct output was
derived by reading the SBML, so a rule change that breaks one is visible
immediately -- which is what lets the model stay simple.

Written in gene symbols for readability and resolved to UniProt accessions
from the record itself, since accessions are the node keys but nobody can
read them.

    python -m src.parsers.Reactome.test_reactome

Needs the SBML on disk (data/SBML/<stId>.sbml) and network for the resolve.
Evidence for every case: src/parsers/Reactome/MODELING_NOTES.md §2, §4, §5.
"""
import sys

from src.parsers.Reactome.GeneNetwork import PathwayGraph
from src.parsers.Reactome.ReactomeCaller import Reactome_ETL

# reaction stId -> {(source, target, mechanism, sign)}, sources/targets as
# gene symbols, or an R-HSA- id for an assembly or a compound target.
GOLDEN: dict[str, dict[str, set]] = {
    "R-HSA-75035": {
        # CCNB1 is the same node on both sides; only CDK1 changes.
        "R-HSA-170070": {("WEE1", "CDK1", "catalysis", None),
                         ("CCNA1", "WEE1", "regulation", "negative"),
                         ("CCNA2", "WEE1", "regulation", "negative"),
                         ("CDK1", "WEE1", "regulation", "negative")},
        "R-HSA-75028": {("CHEK1", "WEE1", "catalysis", None)},
        "R-HSA-75809": {("CHEK2", "CDC25C", "catalysis", None)},
        # Seven isoforms linking to the complex; no isoform-isoform pair.
        "R-HSA-75016": {(x, "R-HSA-75005", "association", None)
                        for x in ("CDC25C", "SFN", "YWHAB", "YWHAE", "YWHAG",
                                  "YWHAH", "YWHAQ", "YWHAZ")},
        # Translocation changes compartment, not gene. Must stay silent.
        "R-HSA-9029987": set(),
    },
    "R-HSA-69620": {
        # Substrate inside the catalyst, regulator sharing a node with it.
        # Must NOT collapse: an enzyme target only collapses onto a catalyst.
        "R-HSA-6804879": {("MDM2", "TP53", "catalysis", None),
                          ("MDM4", "TP53", "catalysis", None),
                          ("CDKN2A", "MDM2", "regulation", "negative"),
                          ("CDKN2A", "MDM4", "regulation", "negative")},
        # Nothing changes identity; the target is exactly CCNA:CDK2.
        "R-HSA-187934": {(s, "R-HSA-141608", "catalysis", None)
                         for s in ("CDKN1A", "CDKN1B")},
        # Signed regulator, no catalyst: aims at what changed.
        "R-HSA-6803388": {("TP53", "CDKN1A", "regulation", "positive"),
                          ("ZNF385A", "CDKN1A", "regulation", "positive")},
        # Nothing changes either: collapses onto the {MDM4|p-MDM2} set.
        "R-HSA-6804741": {("ATM", "R-HSA-6804750", "regulation", "negative")},
    },
    # A moiety is identified from hasModifiedResidue, never from a list:
    # neither SUMO nor NEDD8 appears anywhere in the code, yet both are
    # suppressed as targets. No cases -- MOIETY_EXPECTATIONS is the assertion.
    "R-HSA-2990846": {},
    "R-HSA-8951664": {},
}

MOIETY_EXPECTATIONS = {
    "R-HSA-2990846": {"SUMO1", "SUMO2", "SUMO3"},
    "R-HSA-8951664": {"NEDD8", "UBB", "UBC", "UBA52", "RPS27A"},
}


def _symbolise(record):
    """accession -> symbol, and its inverse, from this pathway's genes."""
    to_symbol = {g.uniprot_id: g.gene_name for g in record.genes}
    return to_symbol


def check() -> bool:
    etl = Reactome_ETL()
    ok = True
    for pathway_id, cases in GOLDEN.items():
        record = next(etl.parse_pathway_record(pathway_id))
        graph = PathwayGraph.from_record(record)
        to_symbol = _symbolise(record)
        edges = etl.derive_gene_edges(graph)
        print(f"\n=== {pathway_id}  ({len(edges)} edges)")

        for reaction_id, want in cases.items():
            got = {
                (to_symbol.get(e.source_id, e.source_id),
                 to_symbol.get(e.target_id, e.target_id),
                 e.mechanism.value, e.sign)
                for e in edges if e.reaction_id == reaction_id
            }
            if got == want:
                print(f"  {reaction_id:<18}OK    {len(got)} edge(s)")
                continue
            ok = False
            print(f"  {reaction_id:<18}FAIL  expected {len(want)}, got {len(got)}")
            for e in sorted(want - got):
                print(f"      missing:    {e}")
            for e in sorted(got - want):
                print(f"      unexpected: {e}")

        expected_moieties = MOIETY_EXPECTATIONS.get(pathway_id)
        if expected_moieties is not None:
            found = set()
            for reaction_id, participants in graph.participants.items():
                product_ids = set()
                for entity_id, role in participants:
                    if role.value == "product":
                        product_ids |= graph.closure(entity_id)
                found |= {to_symbol.get(a, a) for _, a in graph.donated(product_ids)}
            status = "OK  " if found >= expected_moieties else "FAIL"
            ok &= found >= expected_moieties
            print(f"  {'moieties':<18}{status}  discovered {sorted(found)}")

    print("\nPASS" if ok else "\nFAIL")
    return ok


if __name__ == "__main__":
    sys.exit(0 if check() else 1)
