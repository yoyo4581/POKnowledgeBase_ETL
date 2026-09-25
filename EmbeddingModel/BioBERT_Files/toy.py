"""
Toy graph for exercising build_dataset()'s clean/propagate/split/mine logic
without a live Neo4j + SQL connection.

Run: python -m src.builders.Qdrant.toy
"""
from src.builders.Qdrant.dataset import Config, build_dataset


def toy_graph() -> dict:
    BP = "biological_process"
    terms = [
        {"term": "T:root", "name": "biological_process", "namespace": BP},
        {"term": "T:dr", "name": "DNA repair", "namespace": BP},
        {"term": "T:ber", "name": "base-excision repair", "namespace": BP},
        {"term": "T:ner", "name": "nucleotide-excision repair", "namespace": BP},
        {"term": "T:homeo", "name": "chemical homeostasis", "namespace": BP},
        {"term": "T:glc", "name": "glucose homeostasis", "namespace": BP},
        {"term": "T:ca", "name": "calcium ion homeostasis", "namespace": BP},
    ]
    hierarchy = [{"child": c, "parent": p, "rel": "is_a"} for c, p in [
        ("T:dr", "T:root"), ("T:ber", "T:dr"), ("T:ner", "T:dr"),
        ("T:homeo", "T:root"), ("T:glc", "T:homeo"), ("T:ca", "T:homeo")]]
    genes = [
        {"gene": "POLB", "text": "DNA polymerase that fills single-nucleotide gaps after damaged bases are excised."},
        {"gene": "XPA", "text": "Recognizes bulky DNA lesions and scaffolds the incision complex that removes them."},
        {"gene": "INS", "text": "Hormone that lowers blood glucose by promoting cellular uptake and storage."},
        {"gene": "GCK", "text": "Hexokinase that phosphorylates glucose and acts as the beta-cell glucose sensor."},
        {"gene": "CASR", "text": "G protein-coupled receptor that senses extracellular calcium and tunes PTH release."},
        {"gene": "TP53", "text": "Transcription factor that halts the cell cycle in response to DNA damage."},
    ]
    annotations = [
        {"gene": "POLB", "term": "T:ber", "qualifier": "involved_in", "evidence": "IDA"},
        {"gene": "XPA", "term": "T:ner", "qualifier": "involved_in", "evidence": "IMP"},
        {"gene": "XPA", "term": "T:ber", "qualifier": "NOT|involved_in", "evidence": "IDA"},
        {"gene": "INS", "term": "T:glc", "qualifier": "involved_in", "evidence": "IDA"},
        {"gene": "GCK", "term": "T:glc", "qualifier": "involved_in", "evidence": "IMP"},
        {"gene": "CASR", "term": "T:ca", "qualifier": "involved_in", "evidence": "IMP"},
        {"gene": "TP53", "term": "T:ner", "qualifier": "involved_in", "evidence": "IEA"},
        {"gene": "TP53", "term": "T:glc", "qualifier": "acts_upstream_of", "evidence": "IMP"},
    ]
    return {"annotations": annotations, "hierarchy": hierarchy, "terms": terms, "genes": genes}


def demo():
    cfg = Config(min_pos=1, max_pos=3, test_frac=0.0, p_curated_neg=1.0,
                 keep_namespaces=frozenset({"biological_process"}))
    data = build_dataset(**toy_graph(), cfg=cfg)
    print("stats:", data.stats, "\n")
    for r in data.rows:
        t, g, n, src = r["_meta"]
        print(f"{r['anchor']:<28} +{g:<5} -{n:<5} ({src})")


if __name__ == "__main__":
    demo()
