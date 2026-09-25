"""
End-to-end check of the whole stack on a 12-protein toy corpus, with stand-in
encoders, in a throwaway directory.

The point is to fail in 20 seconds instead of 2 hours. Encoding a real corpus
with BioBERT is the expensive step and it comes first, so a mistake anywhere
downstream -- a collection built without the IDF modifier, a grouped query the
embedded backend does not support, an MCP tool whose return type will not
serialize -- otherwise surfaces only after the GPU work is already spent.

What is real here: the collection schema, the upsert path, every query in
GeneRetriever, the MCP tools over a live in-memory session, the metrics, an
export/restore round trip, and -- wherever fastembed is installed -- the actual
BM25 encoder, including that its float64 document values and int query values
both survive into models.SparseVector.

What is faked is the dense encoder, because loading BioBERT would defeat the
purpose. The stand-in is a hashed bag of words, so texts sharing vocabulary do
land near each other and the ordering assertions below still mean something.

The retrieval numbers this prints are not evidence of anything. Twelve
documents and a toy dense encoder is not an experiment; which arm comes out
ahead here flips with the encoder and should be ignored.

    python smoke_test.py                 # real BM25 if fastembed is installed
    python smoke_test.py --stub-sparse   # force the stand-in
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import shutil
import sys
import tempfile
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))

import build.store as bc
import core.config as config
from analysis.evaluate import is_error, unwrap
from core.chunking import chunk_text, split_sentences
from core.metrics import aggregate, paired_bootstrap, per_query_metrics
from core.retriever import Retriever

DIM = 64
FAILURES: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    print(f"  {'PASS' if condition else 'FAIL'}  {label}" + (f"  [{detail}]" if detail else ""))
    if not condition:
        FAILURES.append(label)


# --- stand-in encoders --------------------------------------------------

def _terms(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def _slot(term: str, mod: int) -> int:
    return int(hashlib.md5(term.encode()).hexdigest(), 16) % mod


class HashEncoder:
    """Stands in for the fine-tuned BioBERT: hashed bag of words, L2-normalized."""

    def get_sentence_embedding_dimension(self) -> int:
        return DIM

    def encode(self, texts, normalize_embeddings=True, convert_to_numpy=True, **_):
        single = isinstance(texts, str)
        batch = [texts] if single else list(texts)
        out = np.zeros((len(batch), DIM), dtype=np.float32)
        for i, text in enumerate(batch):
            for term in _terms(text):
                out[i, _slot(term, DIM)] += 1.0
        if normalize_embeddings:
            out /= np.maximum(np.linalg.norm(out, axis=1, keepdims=True), 1e-12)
        return out[0] if single else out


class _Sparse:
    def __init__(self, indices, values):
        self.indices = np.asarray(indices, dtype=np.int64)
        self.values = np.asarray(values, dtype=np.float32)


class HashSparse:
    """Stands in for fastembed BM25: term frequencies; Qdrant applies IDF."""

    MOD = 1 << 20

    def _one(self, text: str, query: bool) -> _Sparse:
        counts: dict[int, float] = {}
        for term in _terms(text):
            counts[_slot(term, self.MOD)] = 1.0 if query else counts.get(_slot(term, self.MOD), 0.0) + 1.0
        if not counts:
            counts = {0: 0.0}
        return _Sparse(list(counts), list(counts.values()))

    def embed(self, texts, **_):
        return (self._one(t, False) for t in texts)

    def query_embed(self, text, **_):
        return iter([self._one(text, True)])


# --- toy corpus ---------------------------------------------------------

CORPUS = {
    "P11111": "Serine/threonine protein kinase that catalyzes the transfer of a phosphate group to "
            "substrate proteins. Required for cell cycle progression through mitosis. Binds ATP in "
            "a conserved pocket. Inhibited by staurosporine at nanomolar concentrations.",
    "P22222": "Protein kinase acting downstream of growth factor receptors. Phosphorylates histidine "
            "residues on the sensor domain. Activity requires magnesium ions. Loss of function "
            "causes defects in the mitotic spindle.",
    "P33333": "DNA polymerase involved in replication of the leading strand. Possesses 3'-5' "
             "exonuclease proofreading activity. Requires a primer and template. Mutations impair "
             "genome stability.",
    "P44444": "Translesion DNA polymerase that bypasses bulky adducts during replication. Lacks "
             "proofreading activity and is therefore error prone. Recruited to stalled forks by "
             "ubiquitinated PCNA.",
    "P55555": "ATP-dependent DNA helicase that unwinds duplex DNA ahead of the replication fork. "
            "Couples ATP hydrolysis to translocation along single-stranded DNA. Interacts with the "
            "primase complex.",
    "P66666": "Serine protease that cleaves peptide bonds C-terminal to basic residues. Secreted as "
             "an inactive zymogen. Activated by limited proteolysis in the extracellular space.",
    "P77777": "Cysteine protease involved in apoptotic signalling. Cleaves substrates after aspartate "
             "residues. Auto-processes into a heterodimer upon activation.",
    "P88888": "Transcription factor that binds a palindromic motif in target promoters. Activates "
           "transcription of stress response genes. Contains a basic leucine zipper domain.",
    "P99999": "Zinc finger transcription factor repressing developmental genes. Binds GC-rich elements. "
           "Recruits histone deacetylase complexes to chromatin.",
    "Q11111": "Membrane transporter mediating uptake of amino acids across the plasma membrane. "
              "Sodium dependent. Twelve predicted transmembrane helices.",
    "Q22222": "Structural constituent of the large ribosomal subunit. Contacts 23S ribosomal RNA. "
             "Required for assembly of the peptidyl transferase centre.",
    "Q33333": "Molecular chaperone that assists folding of nascent polypeptides. Binds exposed "
             "hydrophobic patches. ATP hydrolysis drives substrate release.",
}

QUERIES = {
    "GO:0004672": "protein kinase activity, transfer of phosphate to protein substrates",
    "GO:0003887": "DNA-directed DNA polymerase activity during replication",
    "GO:0008233": "peptidase activity, cleavage of peptide bonds",
    "GO:0003700": "DNA-binding transcription factor activity",
}
RELEVANT = {
    "GO:0004672": {"P11111", "P22222"},
    "GO:0003887": {"P33333", "P44444"},
    "GO:0008233": {"P66666", "P77777"},
    "GO:0003700": {"P88888", "P99999"},
}

# P11111 and P22222 share a gene -- two isoforms, one entrez id -- so the
# "entrez_id is not unique per record" case is exercised rather than assumed.
ENTREZ = {"5000": ["P11111", "P22222"], "5001": ["P33333"], "5002": ["P66666"]}
KINASES = {"P11111", "P22222"}


# --- the test ------------------------------------------------------------

def build(tmp: Path, encoder, sparse_encoder):
    entrez = {u: e for e, us in ENTREZ.items() for u in us}
    records, chunks = bc.shape(CORPUS, window=2, stride=1, entrez=entrez)
    client = config.open_client(storage=str(tmp / "store"))
    for name, payloads in ((config.RECORDS, records), (config.CHUNKS, chunks)):
        bc.ensure_collection(client, name, DIM, on_disk=False, recreate=True)
        dense = encoder.encode([p["text"] for p in payloads])
        sparse = list(sparse_encoder.embed([p["text"] for p in payloads]))
        bc.upsert(client, name, payloads, dense, sparse, bc.ids_for(name, payloads))
    return client, records, chunks


def pick_sparse_encoder(force_stub: bool = False):
    """
    Prefer the real BM25. The stub matches its interface, but only the real one
    proves fastembed's output actually satisfies models.SparseVector -- its
    document values are float64 and its query values are int, and both have to
    survive the round trip into Qdrant. Fall back quietly where fastembed is
    absent, so this still runs on a machine that never builds collections.
    """
    if force_stub:
        return HashSparse(), "stub"
    try:
        return config.load_sparse_encoder(), "fastembed Qdrant/bm25"
    except ImportError:
        return HashSparse(), "stub (fastembed not installed)"


async def main(force_stub: bool = False) -> int:
    tmp = Path(tempfile.mkdtemp(prefix="qdrant_smoke_"))
    encoder = HashEncoder()
    sparse_encoder, sparse_label = pick_sparse_encoder(force_stub)
    print(f"Sparse encoder: {sparse_label}")
    print(f"Scratch dir: {tmp}\n")

    print("chunking")
    sents = split_sentences(CORPUS["P11111"])
    check("splitter finds 4 sentences in P11111", len(sents) == 4, f"{len(sents)}")
    check("splitter keeps 3'-5' intact", "3'-5'" in " ".join(split_sentences(CORPUS["P33333"])))
    _, toy = chunk_text(CORPUS["P11111"], window=2, stride=1)
    check("windows cover every sentence", toy[-1]["sent_end"] == len(sents),
          f"last end {toy[-1]['sent_end']} of {len(sents)}")

    print("\nbuild")
    client, records, chunks = build(tmp, encoder, sparse_encoder)
    check("records collection populated",
          client.get_collection(config.RECORDS).points_count == len(records), f"{len(records)}")
    check("chunks collection populated",
          client.get_collection(config.CHUNKS).points_count == len(chunks), f"{len(chunks)}")

    if sparse_label.startswith("fastembed"):
        params = config.sparse_params(sparse_encoder)
        print("  bm25 params: " + ", ".join(f"{k}={v}" for k, v in params.items()))
        check("bm25 hyperparameters are all captured for the manifest",
              {"k", "b", "avg_len", "language"} <= set(params))
        doc = config.sparse_vector(sparse_encoder, CORPUS["P11111"])
        qry = config.sparse_vector(sparse_encoder, "protein kinase", query=True)
        check("bm25 document vector is non-empty", len(doc.indices) > 0,
              f"{len(doc.indices)} terms")
        check("bm25 query vector is flat 1.0 per term (Qdrant supplies IDF)",
              bool(qry.values) and all(float(v) == 1.0 for v in qry.values))
        check("bm25 is deterministic across calls",
              config.sparse_vector(sparse_encoder, CORPUS["P11111"]).values == doc.values)

    print("\nretrieval")
    r = Retriever(client, encoder, sparse_encoder,
                      prefetch_limit=50, shortlist=12, chunk_prefetch_limit=400)
    q = QUERIES["GO:0004672"]

    base = r.search(q, k=5, snippets_per_record=0, mode="record_hybrid")
    check("record_hybrid returns k", len(base) == 5, f"{len(base)}")
    check("record_hybrid attaches no snippets at spg=0", all(not b["snippets"] for b in base))
    check("record_hybrid ranks a kinase first", base[0]["uniprot_id"] in KINASES,
          base[0]["uniprot_id"])

    arm = r.search(q, k=5, snippets_per_record=2, mode="chunk_rerank")
    check("chunk_rerank returns k", len(arm) == 5, f"{len(arm)}")
    check("chunk_rerank ranks a kinase first", arm[0]["uniprot_id"] in KINASES, arm[0]["uniprot_id"])
    check("chunk_rerank reports both scores",
          arm[0]["chunk_score"] is not None and arm[0]["record_rank"] is not None)
    check("chunk_rerank attaches <= snippets_per_record", all(len(a["snippets"]) <= 2 for a in arm))
    check("chunk_rerank ordered by best chunk score",
          [a["chunk_score"] for a in arm if a["chunk_score"] is not None]
          == sorted([a["chunk_score"] for a in arm if a["chunk_score"] is not None], reverse=True))

    full = r.search(q, k=12, snippets_per_record=0, mode="chunk_rerank")
    check("chunk_rerank keeps the whole shortlist (no records dropped)",
          {a["uniprot_id"] for a in full} == {b["uniprot_id"] for b in
              r.search(q, k=12, snippets_per_record=0, mode="record_hybrid")},
          f"{len(full)} records")

    from core.retriever import CHUNK_AGGREGATIONS, _aggregate, _blom
    orders = {}
    for agg in CHUNK_AGGREGATIONS:
        got = r.search(q, k=12, snippets_per_record=0, mode=f"chunk_{agg}")
        orders[agg] = [g["uniprot_id"] for g in got]
        check(f"chunk_{agg} returns the full shortlist", len(got) == 12, f"{len(got)}")
        check(f"chunk_{agg} still ranks a kinase first", got[0]["uniprot_id"] in KINASES,
              got[0]["uniprot_id"])
    check("chunk_rerank is an alias for chunk_max",
          [g["uniprot_id"] for g in r.search(q, k=12, snippets_per_record=0,
                                             mode="chunk_rerank")] == orders["max"])
    check("aggregations do not all produce the same ordering",
          len({tuple(v) for v in orders.values()}) > 1,
          f"{len({tuple(v) for v in orders.values()})} distinct orderings")
    check("lognorm penalises a record for having more chunks",
          _blom(8) > _blom(3) > _blom(2) > _blom(1) == 0.0,
          f"blom 2/3/8 = {_blom(2):.2f}/{_blom(3):.2f}/{_blom(8):.2f}")

    class _H:
        def __init__(self, s): self.score = s
    hits = [_H(1.0), _H(0.5), _H(0.0)]
    check("top2 averages the two best", _aggregate(hits, "top2") == 0.75)
    check("max takes the best", _aggregate(hits, "max") == 1.0)
    check("lognorm subtracts the sampling advantage",
          _aggregate(hits, "lognorm") < _aggregate(hits, "max"))

    for mode in ("dense", "sparse"):
        got = r.search(q, k=3, snippets_per_record=0, mode=mode)
        check(f"{mode} branch returns results", len(got) == 3, f"{len(got)}")

    snips = r.snippets("P11111", "inhibited by small molecules", n=2)
    check("read_snippets returns windows", len(snips) == 2 and "text" in snips[0])

    exp = r.expand("P11111", 0, radius=1)
    check("expand widens the span", exp["sentences"][1] - exp["sentences"][0] >= 2, str(exp["sentences"]))
    check("expand reports edge flags", exp["at_start"] is True)
    wide = r.expand("P11111", 0, radius=99)
    check("expand clamps to the record", wide["sentences"] == [0, wide["of"]], str(wide["sentences"]))
    check("expand text matches the record",
          wide["text"] == " ".join(split_sentences(CORPUS["P11111"])))

    rec = r.read_record("P11111")
    check("read_record returns full text", rec["text"] == CORPUS["P11111"])
    try:
        r.read_record("NOPE")
        check("unknown accession raises", False)
    except LookupError:
        check("unknown accession raises", True)

    isoform = r.read_record("P22222")
    check("entrez_id rides along on records", isoform.get("entrez_id") == "5000",
          str(isoform.get("entrez_id")))
    check("isoforms of one gene share an entrez id but stay distinct records",
          rec.get("entrez_id") == isoform.get("entrez_id") and rec["text"] != isoform["text"])
    check("records with no entrez mapping omit the field",
          "entrez_id" not in r.read_record("Q33333"))
    check("search results carry entrez_id where present",
          any(x.get("entrez_id") == "5000" for x in arm),
          str([x.get("entrez_id") for x in arm]))
    check("uuid5 point id is recomputable from the accession alone",
          config.record_id("P11111") != config.record_id("P22222")
          and config.record_id("P11111") == config.record_id("P11111"))

    print("\nmcp")
    # One check below deliberately calls a tool with a bad accession. The server
    # logs that exception with a full traceback, which is correct of it and
    # very confusing here -- the test is asserting the error happens.
    logging.getLogger("mcp").setLevel(logging.CRITICAL)

    from mcp_server import build_server, open_session
    server = build_server(retriever=r)
    async with open_session(server) as session:
        names = {t.name for t in (await session.list_tools()).tools}
        check("all five tools exposed",
              names == {"search_proteins", "read_snippets", "expand_snippet",
                        "read_record", "collection_info"}, ", ".join(sorted(names)))

        res = await session.call_tool("search_proteins",
                                      {"query": q, "k": 5, "snippets_per_record": 1,
                                       "mode": "chunk_rerank"})
        payload = unwrap(res)
        rows = payload.get("results", [])
        check("search_proteins round-trips as one JSON object",
              isinstance(payload, dict) and len(rows) == 5, str(type(payload)))
        check("search_proteins echoes the mode it ran", payload.get("mode") == "chunk_rerank")
        check("search_proteins result carries snippets with chunk_idx",
              bool(rows) and "chunk_idx" in rows[0]["snippets"][0])

        top = rows[0]
        res = await session.call_tool("expand_snippet",
                                      {"uniprot_id": top["uniprot_id"],
                                       "chunk_idx": top["snippets"][0]["chunk_idx"], "radius": 1})
        check("expand_snippet round-trips", "text" in unwrap(res))

        res = await session.call_tool("read_snippets",
                                      {"uniprot_id": top["uniprot_id"], "query": "activation", "n": 2})
        check("read_snippets round-trips", len(unwrap(res)["snippets"]) == 2)

        res = await session.call_tool("collection_info", {})
        info = unwrap(res)
        check("collection_info counts points",
              info[config.RECORDS]["points"] == len(CORPUS), json.dumps(info[config.RECORDS]))

        res = await session.call_tool("read_record", {"uniprot_id": "NOPE"})
        check("tool errors surface as an error result", is_error(res))

        runs = {}
        for mode in ("record_hybrid", "chunk_rerank"):
            runs[mode] = {}
            for qid, text in QUERIES.items():
                res = await session.call_tool("search_proteins", {
                    "query": text, "k": 10, "snippets_per_record": 0, "mode": mode})
                runs[mode][qid] = [row["uniprot_id"] for row in unwrap(res)["results"]]

    print("\nmetrics")
    scores = {m: {qid: per_query_metrics(ranked, RELEVANT[qid], (1, 3, 5, 10), 10)
                  for qid, ranked in run.items()} for m, run in runs.items()}
    summary = {m: aggregate(s) for m, s in scores.items()}
    for m, s in summary.items():
        print(f"    {m:14} ndcg@10={s['ndcg@10']:.4f}  recall@5={s['recall@5']:.4f}  "
              f"accuracy@1={s['accuracy@1']:.4f}")
    check("metrics computed for both arms", set(summary) == {"record_hybrid", "chunk_rerank"})
    check("ndcg in [0,1]", all(0.0 <= s["ndcg@10"] <= 1.0 for s in summary.values()))
    perfect = per_query_metrics(["A", "B", "C"], {"A", "B"}, (1, 2), 3)
    check("perfect ranking scores 1.0", perfect["ndcg@2"] == 1.0 and perfect["recall@2"] == 1.0
          and perfect["mrr@1"] == 1.0 and perfect["map@3"] == 1.0)
    miss = per_query_metrics(["X", "Y"], {"A"}, (1, 2), 2)
    check("missed ranking scores 0.0", all(v == 0.0 for v in miss.values()))

    boot = paired_bootstrap([s["ndcg@10"] for s in scores["chunk_rerank"].values()],
                            [s["ndcg@10"] for s in scores["record_hybrid"].values()],
                            n_resamples=500)
    check("bootstrap returns an interval around the delta",
          boot["ci_lo"] <= boot["delta"] <= boot["ci_hi"],
          f"{boot['delta']:+.4f} [{boot['ci_lo']:+.4f}, {boot['ci_hi']:+.4f}]")

    print("\nexport / restore")
    rec_dense = encoder.encode([p["text"] for p in records])
    ch_dense = encoder.encode([p["text"] for p in chunks])
    manifest = {"created": "smoke", "model": "hash", "dim": DIM, "window": 2, "stride": 1,
                "splitter": "biomed-regex-v1"}
    bc.write_export(tmp / "export", manifest, records, rec_dense, chunks, ch_dense)
    back_manifest, data = bc.read_export(tmp / "export")
    check("export round-trips record count", len(data[config.RECORDS][0]) == len(records))
    check("export round-trips vectors bit-exact",
          np.array_equal(data[config.RECORDS][1], rec_dense))
    check("export round-trips the sentences payload",
          data[config.RECORDS][0][0]["sentences"] == records[0]["sentences"])
    check("manifest survives", back_manifest["splitter"] == "biomed-regex-v1")

    client.close()
    restored = config.open_client(storage=str(tmp / "store2"))
    for name in (config.RECORDS, config.CHUNKS):
        payloads, dense = data[name]
        bc.ensure_collection(restored, name, DIM, on_disk=False, recreate=True)
        sparse = list(sparse_encoder.embed([p["text"] for p in payloads]))
        bc.upsert(restored, name, payloads, dense, sparse, bc.ids_for(name, payloads))
    check("restored store has the same record count",
          restored.get_collection(config.RECORDS).points_count == len(records))
    r2 = Retriever(restored, encoder, sparse_encoder,
                       prefetch_limit=50, shortlist=12, chunk_prefetch_limit=400)
    check("restored store returns the same top record",
          r2.search(q, k=1, snippets_per_record=0, mode="chunk_rerank")[0]["uniprot_id"]
          == arm[0]["uniprot_id"])
    restored.close()

    shutil.rmtree(tmp, ignore_errors=True)
    print(f"\n{'FAILED: ' + ', '.join(FAILURES) if FAILURES else 'All checks passed.'}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main("--stub-sparse" in sys.argv)))
