"""
The retrieval layer the MCP tools wrap and the evaluation measures.

Two collections, one entity: every UniProt accession has one record (its whole
function text) and N snippets (overlapping sentence windows of that same text).
A record and its snippets are joined by the `uniprot_id` payload field.

The accession is the identity everywhere in this file. Point ids are
uuid5(accession), which Qdrant accepts where the raw accession would be
rejected, and which stays computable from the accession alone -- that is what
lets expand() jump from a chunk straight to its parent record. But the hash is
one-way, so `uniprot_id` has to be on the payload too: a search returns points
the caller never named, and the payload is the only thing that can say what was
found. It also carries the shortlist filter and the group_by below, neither of
which can read a point id.

`entrez_id` comes back on record lookups when the store was built with a
mapping. It is the key back to Neo4j, where annotations live at gene
granularity, and it is deliberately not unique here -- several isoforms of one
gene are several records, with genuinely different function text.

Search modes
------------
  record_hybrid   Baseline. RRF over dense (fine-tuned BioBERT) + BM25 on
                  function_records. Snippets are attached to the results as
                  evidence but do NOT influence the ordering.

  chunk_*         The arms under test. Stage 1 is exactly record_hybrid, but
                  widened to `shortlist` records instead of k. Stage 2 runs the
                  same hybrid query over function_chunks restricted to those
                  accessions, and re-orders them by an aggregate of their chunk
                  scores. Same candidate set as the baseline, same metrics,
                  different ordering -- so any metric delta is attributable to
                  the re-ranking and nothing else.

                  chunk_max (= chunk_rerank) ranks by the single best chunk;
                  chunk_top2, chunk_lognorm and chunk_rrf are the alternatives
                  described at CHUNK_AGGREGATIONS below. Which aggregate is used
                  matters more than (window, stride) on this data -- see the
                  note there.

  dense / sparse  Single-branch record-level searches. Not part of the headline
                  comparison; useful for reading a result that surprises you.

The one parameter that can quietly invalidate the experiment is
chunk_prefetch_limit. Stage 2 pulls that many chunks per branch before fusion,
and those chunks have to cover `shortlist` records at roughly n_chunks each.
Set it too low and records near the bottom of the shortlist contribute no
chunks at all, fall back to their record ordering, and chunk re-ranking looks
weaker than it is. Rule of thumb: chunk_prefetch_limit >= shortlist * mean
chunks per record.
"""
from __future__ import annotations

from qdrant_client import models

from core.config import CHUNKS, DENSE, RECORDS, SPARSE, chunk_id, record_id, sparse_vector

# How a record's chunk scores collapse into the one number that re-orders it.
#
# This turned out to be the parameter that matters, not (window, stride). The
# discrimination diagnostic measured sigma_within = 2.1 x sigma_across on this
# store: a record's own sentences vary twice as much as records vary from each
# other. Under max-pooling that asymmetry is fatal, because E[max of n draws]
# grows with n -- two records of identical true relevance separate by ~0.10
# (half the entire best-to-worst spread of a shortlist) when one has 10 chunks
# and the other has 2. Max-pooling therefore ranks substantially by record
# length, with relevance as a secondary term.
#
#   max      hits[0]. The original. Maximal n-bias, maximal variance.
#   top2     mean of the two best chunks. Halves the variance and flattens the
#            n-bias, at the cost of some genuine localization.
#   lognorm  max minus the expected max for that record's own chunk count and
#            observed spread, so the sampling advantage is subtracted rather
#            than inherited. Scale-free: it uses each record's own dispersion,
#            which matters because these are RRF scores, not cosines.
#   rrf      fuse the record rank and the best-chunk rank the same way dense
#            and sparse are already fused. Damps chunk noise with the record
#            signal instead of replacing one with the other.
CHUNK_AGGREGATIONS = ("max", "top2", "lognorm", "rrf")
RRF_K = 60          # the constant Qdrant's own FusionQuery uses

MODES = ("record_hybrid", "chunk_rerank", "dense", "sparse",
         "chunk_max", "chunk_top2", "chunk_lognorm", "chunk_rrf")

# Enough to identify a hit and to follow it up, without dragging the full text
# and the sentences list back on every search.
RECORD_BRIEF = ["uniprot_id", "entrez_id", "n_chunks"]
CHUNK_BRIEF = ["chunk_idx", "text"]


class Retriever:
    def __init__(self, client, encoder, sparse_encoder, prefetch_limit: int = 200,
                 shortlist: int = 100, chunk_prefetch_limit: int = 1000,
                 agg_group_size: int = 8):
        self.client = client
        self.encoder = encoder
        self.sparse_encoder = sparse_encoder
        self.pl = prefetch_limit
        self.shortlist = shortlist
        self.cpl = chunk_prefetch_limit
        self.agg_group_size = agg_group_size

    # --- encoding -------------------------------------------------------

    def _dense(self, q: str) -> list[float]:
        # show_progress_bar=False matters here, not in the builder: this runs
        # once per query, and sentence-transformers renders a separate one-batch
        # tqdm for each call. Over an evaluation sweep that is thousands of
        # progress bars, which in a notebook costs real time and can swamp the
        # output pane.
        return self.encoder.encode(q, normalize_embeddings=True,
                                   show_progress_bar=False).tolist()

    def _sparse(self, q: str) -> models.SparseVector:
        return sparse_vector(self.sparse_encoder, q, query=True)

    def _prefetch(self, dq, sq, flt=None, limit=None) -> list[models.Prefetch]:
        limit = limit or self.pl
        return [
            models.Prefetch(query=dq, using=DENSE, filter=flt, limit=limit),
            models.Prefetch(query=sq, using=SPARSE, filter=flt, limit=limit),
        ]

    # --- tool: search_proteins ------------------------------------------

    def search(self, query: str, k: int = 10, snippets_per_record: int = 1,
               mode: str = "chunk_rerank", shortlist: int | None = None) -> list[dict]:
        if mode not in MODES:
            raise ValueError(f"Unknown mode {mode!r}; expected one of {MODES}.")
        dq, sq = self._dense(query), self._sparse(query)

        if mode in ("dense", "sparse"):
            return self._single_branch(dq, sq, mode, k, snippets_per_record)
        if mode == "record_hybrid":
            recs = self._record_hybrid(dq, sq, k)
            return self._attach_snippets(dq, sq, recs, snippets_per_record)

        # chunk_rerank is kept as an alias for chunk_max so earlier runs and
        # saved configs keep meaning what they meant.
        aggregation = "max" if mode == "chunk_rerank" else mode.split("_", 1)[1]
        return self._chunk_rerank(dq, sq, k, snippets_per_record,
                                  shortlist or self.shortlist, aggregation)

    def _record_hybrid(self, dq, sq, limit: int):
        return self.client.query_points(
            RECORDS, prefetch=self._prefetch(dq, sq),
            query=models.FusionQuery(fusion=models.Fusion.RRF),
            limit=limit, with_payload=RECORD_BRIEF,
        ).points

    def _single_branch(self, dq, sq, mode: str, k: int, snippets_per_record: int):
        recs = self.client.query_points(
            RECORDS, query=dq if mode == "dense" else sq,
            using=DENSE if mode == "dense" else SPARSE,
            limit=k, with_payload=RECORD_BRIEF,
        ).points
        return self._attach_snippets(dq, sq, recs, snippets_per_record)

    def _best_chunks(self, dq, sq, accessions: list[str], group_size: int,
                     limit: int | None = None):
        """
        Hybrid chunk search confined to `accessions`, grouped per accession.
        Returns {uniprot_id: [hits]} with hits in descending score order.

        group_by takes a payload field -- there is no group-by-point-id -- which
        is one of the reasons uniprot_id has to be on the chunk payload and not
        only folded into the chunk's uuid.
        """
        if not accessions or group_size < 1:
            return {}
        flt = models.Filter(must=[models.FieldCondition(
            key="uniprot_id", match=models.MatchAny(any=accessions))])
        groups = self.client.query_points_groups(
            CHUNKS, prefetch=self._prefetch(dq, sq, flt, limit=limit or self.cpl),
            query=models.FusionQuery(fusion=models.Fusion.RRF),
            query_filter=flt, group_by="uniprot_id",
            limit=len(accessions), group_size=group_size,
            with_payload=CHUNK_BRIEF,
        ).groups
        return {g.id: sorted(g.hits, key=lambda h: h.score, reverse=True) for g in groups}

    def _attach_snippets(self, dq, sq, recs, snippets_per_record: int) -> list[dict]:
        """Evidence only -- the incoming record order is preserved exactly."""
        accessions = [p.payload["uniprot_id"] for p in recs]
        by_acc = self._best_chunks(dq, sq, accessions, snippets_per_record)
        return [{
            **_identity(p.payload),
            "record_score": p.score,
            "n_chunks": p.payload["n_chunks"],
            "snippets": _snippets(by_acc.get(p.payload["uniprot_id"], [])),
        } for p in recs]

    def _chunk_rerank(self, dq, sq, k: int, snippets_per_record: int,
                      shortlist: int, aggregation: str = "max") -> list[dict]:
        if aggregation not in CHUNK_AGGREGATIONS:
            raise ValueError(f"Unknown aggregation {aggregation!r}; "
                             f"expected one of {CHUNK_AGGREGATIONS}.")
        recs = self._record_hybrid(dq, sq, shortlist)
        if not recs:
            return []
        accessions = [p.payload["uniprot_id"] for p in recs]
        by_id = {p.payload["uniprot_id"]: p for p in recs}
        record_rank = {a: i for i, a in enumerate(accessions)}

        # top2 needs two hits and lognorm needs enough of a record's chunks to
        # estimate its own dispersion, so the group has to be wider than the
        # snippets we return. Capped, since a record with 40 chunks does not
        # need all of them to estimate a spread.
        group_size = max(self.agg_group_size, snippets_per_record, 1)
        by_acc = self._best_chunks(dq, sq, accessions, group_size)

        chunk_score = {a: _aggregate(h, aggregation) for a, h in by_acc.items()}

        if aggregation == "rrf":
            # Fuse the two rankings rather than letting either one win outright.
            chunk_rank = {a: i for i, a in enumerate(
                sorted(by_acc, key=lambda a: (-by_acc[a][0].score, record_rank[a])))}
            key = {a: 1.0 / (RRF_K + record_rank[a] + 1) + 1.0 / (RRF_K + chunk_rank[a] + 1)
                   for a in by_acc}
        else:
            key = chunk_score

        # Records whose chunks never surfaced in the prefetch keep their record
        # order and go after everything that did score -- dropping them would
        # silently shrink the candidate set relative to the baseline and inflate
        # the measured difference.
        scored = sorted(by_acc, key=lambda a: (-key[a], record_rank[a]))
        unscored = [a for a in accessions if a not in by_acc]

        out = []
        for acc in (scored + unscored)[:k]:
            hits = by_acc.get(acc, [])
            p = by_id[acc]
            out.append({
                **_identity(p.payload),
                "record_score": p.score,
                "record_rank": record_rank[acc],
                "chunk_score": chunk_score.get(acc),
                "n_chunks": p.payload["n_chunks"],
                "snippets": _snippets(hits[:snippets_per_record]),
            })
        return out

    # --- tool: read_snippets --------------------------------------------

    def snippets(self, uniprot_id: str, query: str, n: int = 3) -> list[dict]:
        dq, sq = self._dense(query), self._sparse(query)
        flt = models.Filter(must=[models.FieldCondition(
            key="uniprot_id", match=models.MatchValue(value=uniprot_id))])
        hits = self.client.query_points(
            CHUNKS, prefetch=self._prefetch(dq, sq, flt, limit=50),
            query=models.FusionQuery(fusion=models.Fusion.RRF),
            query_filter=flt, limit=n, with_payload=CHUNK_BRIEF,
        ).points
        return _snippets(hits)

    # --- tool: expand_snippet -------------------------------------------

    def expand(self, uniprot_id: str, chunk_idx: int, radius: int = 1) -> dict:
        ch = self.client.retrieve(CHUNKS, ids=[chunk_id(uniprot_id, chunk_idx)],
                                  with_payload=["sent_start", "sent_end"])
        if not ch:
            raise LookupError(f"No chunk {chunk_idx} for {uniprot_id!r}.")
        span = ch[0].payload
        rec = self.client.retrieve(RECORDS, ids=[record_id(uniprot_id)],
                                   with_payload=["sentences"])
        if not rec:
            raise LookupError(f"No record for {uniprot_id!r}.")
        sents = rec[0].payload["sentences"]

        start = max(0, span["sent_start"] - radius)
        end = min(len(sents), span["sent_end"] + radius)
        text = " ".join(sents[start:end])
        return {"uniprot_id": uniprot_id, "chunk_idx": chunk_idx, "sentences": [start, end],
                "of": len(sents), "at_start": start == 0, "at_end": end == len(sents),
                "text": text, "chars": len(text)}

    # --- tool: read_record ----------------------------------------------

    def read_record(self, uniprot_id: str) -> dict:
        rec = self.client.retrieve(RECORDS, ids=[record_id(uniprot_id)],
                                   with_payload=["text", "n_chunks", "entrez_id"])
        if not rec:
            raise LookupError(f"No record for {uniprot_id!r}.")
        p = rec[0].payload
        out = {"uniprot_id": uniprot_id, "text": p["text"], "chars": len(p["text"]),
               "n_chunks": p["n_chunks"]}
        if p.get("entrez_id"):
            out["entrez_id"] = p["entrez_id"]
        return out


def _blom(n: int) -> float:
    """
    Expected maximum of n standard normals, via Blom's approximation
    Phi^-1((n - 0.375) / (n + 0.25)). Accurate enough for a correction term and
    far cheaper than simulating, which this would otherwise have to do per
    record per query.
    """
    from statistics import NormalDist
    if n < 2:
        return 0.0
    return NormalDist().inv_cdf((n - 0.375) / (n + 0.25))


def _aggregate(hits, how: str) -> float:
    """One record's chunk hits (descending) -> the single score that ranks it."""
    scores = [h.score for h in hits]
    if not scores:
        return float("-inf")
    if how in ("max", "rrf"):
        return scores[0]
    if how == "top2":
        return sum(scores[:2]) / min(2, len(scores))
    if how == "lognorm":
        # Subtract the advantage a record gets purely from having had more
        # draws. Uses the record's own observed dispersion, so it is scale-free
        # -- these are RRF fusion scores, not cosine similarities.
        if len(scores) < 2:
            return scores[0]
        from statistics import pstdev
        return scores[0] - _blom(len(scores)) * pstdev(scores)
    raise ValueError(how)


def _identity(payload: dict) -> dict:
    """The accession, plus the entrez id when the store carries one."""
    out = {"uniprot_id": payload["uniprot_id"]}
    if payload.get("entrez_id"):
        out["entrez_id"] = payload["entrez_id"]
    return out


def _snippets(hits) -> list[dict]:
    return [{"chunk_idx": h.payload["chunk_idx"], "text": h.payload["text"],
             "score": h.score, "chars": len(h.payload["text"])} for h in hits]
