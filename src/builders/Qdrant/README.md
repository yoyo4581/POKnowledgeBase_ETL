# Protein function retrieval: hybrid search + chunk re-ranking

Vector store, MCP toolkit, and evaluation harness for the question this
experiment exists to answer: **does adding a chunk-level re-ranking step on top
of traditional hybrid search improve retrieval accuracy?**

Everything assumes the fine-tuned BioBERT from `../GO_Contrastive.py` already
exists and is loadable with `SentenceTransformer(path)`.

## Layout

```
core/         shared by everything; no entry points
  config.py       collection names, client + encoder wiring, point ids
  chunking.py     biomedical sentence splitter, sentence-window chunking
  retriever.py    the four operations and every search mode
  metrics.py      IR metrics + paired bootstrap (pure functions)
  cli.py          shared CLI args, retriever factory, eval-set loading

build/        producing the store
  store.py        build a corpus into both collections; restore an export

analysis/     measuring it (none of these write to the store)
  evaluate.py        the experiment: arms, IR metrics, bootstrap
  analyze.py         post-hoc on per_query.jsonl -- no store, no model needed
  discrimination.py  does the encoder resolve sentences within one record?
  context_cost.py    tokens read, snippets vs whole records

mcp_server.py   the product: MCP toolkit over the store
smoke_test.py   whole stack on a 12-protein toy corpus, ~20s
```

Every script under `build/` and `analysis/` is runnable from any working
directory — each calls `core.cli.bootstrap()` to put the package root on
`sys.path`.

## Data model

The unit is the **protein**, not the gene. `FunctionData` is keyed by
`uniprot_id`, and [dataset.py](../BioBERT_Files/dataset.py) keeps isoforms as
separate corpus entries on purpose — a gene with several mapped accessions can
have genuinely different function text per accession, and annotations are
expanded onto every one of them.

Every accession has one **record** (its whole function text) and N **snippets**
(overlapping sentence windows of that text), joined by the `uniprot_id` payload
field:

```
function_records   id = uuid5(uniprot_id)
                   payload: uniprot_id, entrez_id?, text, sentences[], n_chunks
                   vectors: dense (BioBERT), sparse (BM25)

function_chunks    id = uuid5(f"{uniprot_id}#{chunk_idx}")
                   payload: uniprot_id, chunk_idx, text, sent_start, sent_end
                   vectors: dense (BioBERT), sparse (BM25)
```

### Why the accession is hashed into a UUID

Qdrant accepts exactly two point-id types: unsigned 64-bit integer, or UUID.
`"P04637"` is rejected as *"not a valid UUID"*, and so is `"7157-0"` — a type
restriction, not a character one. Both the server and the embedded backend
enforce it identically, so this cannot pass in Colab and fail in Docker.

`uuid5` rather than a counter because it is a pure function of the accession:
any process holding an accession computes the point id with no lookup table.
That is what lets `expand_snippet` jump from a chunk straight to its parent
record, and what lets the export regenerate ids from payloads alone.

### Why `uniprot_id` is also on the payload

The hash is one-way. `read_record` / `read_snippets` / `expand_snippet` all
start from an accession you already hold, so the hash covers them. **Search does
not** — it returns points you never named, as a UUID plus a payload, and the
payload is the only channel that can say what was found. It also carries the
shortlist filter and the `group_by` that chunk re-ranking depends on; neither
can read a point id. The cost is ~10 bytes beside a 3KB vector.

`entrez_id` is optional, records only, and **not unique** — several isoforms of
one gene are several records. It is the key back to Neo4j, where annotations
live at gene granularity.

### Records with no `entrez_id` are a distinct population

[dataset.py](../BioBERT_Files/dataset.py) builds `protein_rows` from **every**
`FunctionData` row, but expands GO annotations only onto accessions found in
`EntrezUniprotMap`:

```python
annotations = [... for uniprot_id in entrez_to_uniprots.get(a["gene"], ())]
```

An accession missing from that map therefore has function text but no
annotations. It is in `eval_corpus`, embeddable and retrievable — but never in
`pos[t]`, so **never in `eval_relevant` for any term**, so never a correct
answer. It is also in `all_genes`, the pool random training negatives are drawn
from.

Two consequences:

- **In evaluation** they are structural distractors. They take rank slots and
  cannot be right, capping achievable precision for reasons unrelated to
  retrieval quality. Both arms see them identically, so the chunk-rerank delta
  is unaffected; only the absolute numbers move.
- **In training** they are potential label noise. The false-negative guard is
  `blocked = any_pos[t]`, but an unmapped accession has no annotations
  *recorded*, which is not the same as being genuinely unannotated. One that is
  truly annotated to the anchor term and merely missing from the crosswalk gets
  mined as a random negative.

Don't filter them out: they belong in production, and
`InformationRetrievalEvaluator` scored against this same corpus during training,
so removing them would make the numbers *less* comparable. `build/store.py
build` reports the count when `--entrez-map` is given.

`sentences[]` lives on the record so a snippet can be widened back into its
surrounding context. That only works if both machines split sentences
identically, which is why the splitter is a dependency-free regex and why its
name is stamped into the export manifest.

## Search modes

| Mode | Ranking |
| --- | --- |
| `record_hybrid` | **Default.** RRF over dense + BM25 on `function_records`. Snippets attach as evidence but do not affect order. |
| `chunk_max` | Re-rank the shortlist by each record's single best chunk. `chunk_rerank` is an alias. |
| `chunk_top2` | …by the mean of its two best chunks. |
| `chunk_lognorm` | …by its best chunk, minus the expected max for its own chunk count and spread. |
| `chunk_rrf` | …by fusing the record rank with the best-chunk rank. |
| `dense` / `sparse` | Single-branch record-level searches, for diagnosing a surprising result. |

Every `chunk_*` mode draws from the same candidate set as `record_hybrid`, so
any metric delta is attributable to the re-ranking and nothing else. Snippets
come back in every mode — the mode changes protein **order**, not whether you
get passage evidence.

**All four `chunk_*` modes measured null** against `record_hybrid` on this
corpus (476 held-out GO terms; no 95% interval excluded zero). `record_hybrid`
is the default for that reason; the chunk modes are kept because they are cheap
to re-test on a different corpus or encoder. See "What was measured" below.

## Running it

### 1. Build, in Colab

```bash
pip install -r requirements.txt

python build/store.py build \
    --corpus data/qdrant/go_contrastive/eval_corpus.json \
    --model  biobert-go-retrieval \
    --storage qdrant_store --export qdrant_export \
    --window 2 --stride 1
    # optional: --entrez-map entrez_to_uniprots.json
```

**One store serves both the experiment and production.** Despite its name,
`eval_corpus.json` is the *whole* corpus — `build_dataset()` splits on GO terms,
not on proteins:

```python
eval_queries  = {t: name for t in test}      # held-out terms
eval_relevant = {t: pos[t] for t in test}    # held-out judgments
eval_corpus   = dict(gene_text)              # every protein with function text
```

Every text in `train.jsonl` is `gene_text[g]` for some `g`, so the triplets
contribute no protein the corpus doesn't already hold. Nothing needs merging,
and the relevance judgments are complete with respect to everything indexed —
so the metrics stay directly comparable to `InformationRetrievalEvaluator`'s.

#### Optional: `--entrez-map`

`eval_corpus.json` carries no entrez ids (`export_dataset()` never writes
`entrez_to_uniprots`), so `entrez_id` is optional enrichment rather than a
requirement — build today without it, add it on a later rebuild. The flag takes
either direction:

```python
# on the ETL side, from _load_protein_data()'s own output
json.dump(entrez_to_uniprots, open("entrez_to_uniprots.json", "w"))
```

It buys the join back to Neo4j, where annotations live at gene granularity.

This writes two artifacts, because neither format does both jobs:

- **`qdrant_store/`** — an embedded `QdrantClient(path=...)` folder. Zip it,
  download it, query it anywhere with no server. Not loadable into a Qdrant
  server.
- **`qdrant_export/`** — dense vectors as `.npy`, payloads as `.jsonl`, plus a
  manifest. This is what the ETL pipeline consumes.

Sparse vectors are deliberately not exported. The sparse params carry
`Modifier.IDF`, so what Qdrant stores per point is BM25 term frequencies — a
pure function of the payload text. `restore` re-derives them, which is exactly
equivalent to shipping them.

Then `zip -r qdrant.zip qdrant_store qdrant_export` and download.

### 2. Evaluate

```bash
python analysis/evaluate.py \
    --data-dir data/qdrant/go_contrastive \
    --model    biobert-go-retrieval \
    --storage  qdrant_store \
    --out      eval_results
```

Queries are GO term names and relevance is "this protein is annotated to this
term" — the same held-out set `GO_Contrastive.py` selects checkpoints on, so
the numbers are directly comparable to the ones printed during training.

Search runs **through MCP**, over the SDK's in-memory transport: real server
object, real tool schemas, real JSON round-trip, no subprocess. The evaluation
therefore measures the path the application will actually take.

Output is a metric table (accuracy@k, precision@k, recall@k, MRR@k, NDCG@k,
MAP@k), per-arm median latency, and a paired bootstrap on NDCG@10 so the result
reads as "chunk re-ranking helps by X ± Y" rather than as two bare numbers.
`eval_results/per_query.jsonl` holds every ranking and score for error analysis.

### 3. Load into the ETL pipeline

```bash
python build/store.py restore --export qdrant_export --url http://localhost:6333
```

### 4. Serve the MCP toolkit

```bash
BIOBERT_MODEL_PATH=biobert-go-retrieval QDRANT_PATH=qdrant_store python mcp_server.py
```

Or as a client config entry:

```json
{"mcpServers": {"protein-retrieval": {
  "command": "python",
  "args": ["C:/Users/Yahya/Documents/BioBERT_Finetune/Qdrant/mcp_server.py"],
  "env": {"QDRANT_PATH": "qdrant_store", "BIOBERT_MODEL_PATH": "biobert-go-retrieval"}}}}
```

Tools: `search_proteins`, `read_snippets`, `expand_snippet`, `read_record`,
`collection_info`. They are shaped as a funnel — everything after
`search_proteins` is the agent deciding the initial retrieval did not settle the
question, which is what the chunk collection exists to support.

Every tool speaks UniProt accessions. The `uuid5` point id never surfaces: a
result's `uniprot_id` feeds straight back into the next call.

### 5. Check the plumbing without paying for encoding

```bash
python smoke_test.py
```

Builds a 12-protein toy corpus with stand-in encoders in a temp directory and
exercises the collection schema, every query, all five MCP tools over a live
session, the metrics, and an export/restore round trip. Twenty seconds instead
of two hours.

## Knobs that can quietly invalidate the experiment

- **`--chunk-prefetch-limit`** must be at least `shortlist × mean chunks per
  record`. Set it too low and records near the bottom of the shortlist
  contribute no chunks, silently keep their baseline ordering, and chunk
  re-ranking looks weaker than it is. `analysis/evaluate.py` warns when it looks
  too small.
- **`--shortlist` must be ≥ `--top-k`**, or `chunk_rerank` is scored on a
  shorter ranked list than the baseline. The eval refuses to run otherwise.
- **`--window` / `--stride`**: a window covering the whole record collapses the
  chunk collection back into the record collection — the degenerate case where
  re-ranking can show no benefit by construction.
- **The encoder must be the same one the collections were built with.** Nothing
  can detect a mismatch; results will just be quietly poor.

## fastembed

`fastembed` supplies the BM25 half of every hybrid query, so it is needed on
**every** machine in the chain, not just the build box:

| Machine | Needs | Why |
| --- | --- | --- |
| Colab (build) | fastembed + BioBERT + GPU | encodes both collections |
| ETL host (restore) | fastembed only | re-derives sparse; dense comes from the `.npy` |
| App / MCP server | fastembed + BioBERT | encodes the *query* on both branches |

Two things worth knowing:

- **BM25 does not use the GPU.** It is pure Python — tokenize, stem, weight — with
  no ONNX graph. Your GPU accelerates the BioBERT dense pass only; the BM25 pass
  is CPU-bound and runs alongside it.
- **First use downloads the `Qdrant/bm25` model data from HuggingFace** (stopwords
  and stemmer, ~18 files). Set `FASTEMBED_CACHE_PATH` to a mounted volume, or bake
  the cache into the image, before running this anywhere air-gapped.

### Why sparse vectors are re-derived rather than exported

fastembed stores BM25 term frequencies normalized by **fixed** hyperparameters —
`k=1.2`, `b=0.75`, `avg_len=256.0`, English, stemmer on. `avg_len` is a constant,
not measured from your corpus, which is exactly what makes a document's sparse
vector a pure function of its own text and makes re-deriving on restore
equivalent to shipping the vectors.

That guarantee holds only while those values match on both machines. So `build`
stamps them (and the fastembed version) into the manifest as `bm25_params`, and
`restore` refuses to run if they have drifted — a changed default would not raise
anything on its own, it would just make hybrid search quietly worse than the
version you evaluated. Override with `--allow-param-drift` if you mean it.

## Environment notes

- `mcp` 1.x (`FastMCP`) and 2.x (`MCPServer`) are both supported via a shim in
  `mcp_server.py`. 2.x renamed the class and moved to snake_case result fields.
- Embedded mode holds an exclusive lock on the storage folder — one process at
  a time. Point `QDRANT_URL` at a server if the application needs more.
- `create_payload_index` is a no-op in embedded mode (the backend scans
  instead). The call is kept because the same code path has to produce a
  correct **server** collection on restore.

## What was measured

Recorded so the next person doesn't re-run it.

| finding | evidence |
| --- | --- |
| Chunk re-ranking gives no measurable retrieval gain | 4 aggregations x 476 queries; no 95% interval excluded zero. Best was `chunk_top2` at +0.0399 accuracy@3, p=0.076 — and that is one of four comparisons, so it does not survive correction. |
| Because most records are too short to chunk | median 3 sentences; 21% are single-sentence. At `window=2` 39% of records yield one chunk identical to the record, and 78-87% of queries are unchanged in every arm. |
| The encoder *can* resolve sentences | `analysis/discrimination.py`: true-query within-record spread is 1.65x an unrelated query's; best chunk beats the whole record 65% of the time. |
| But that resolution is not relevance-aligned | sigma_within = 2.1x sigma_across. Max-pooling therefore amplifies within-record noise, and E[max of n] promotes long records by ~half a shortlist's spread regardless of relevance. Correcting for it (`chunk_lognorm`) did not help, so the chunk signal itself is not more discriminative of relevance than the record signal. |
| Most likely cause | the model is trained with **whole records** as positives (`positive: gene_text[g]`, mean pooling), so sentence embeddings are off-distribution. Fixing this means chunk-level positives in the contrastive dataset — a different change from the `min_pos` one. |
| What chunking is worth here | context reduction at identical retrieval quality. `analysis/context_cost.py` measures it. |

Untested and still open: **chunk-first retrieval** — searching `function_chunks`
with no record shortlist, so a record whose whole-text embedding was never
competitive can still surface. Every mode above re-orders a record shortlist and
so cannot improve recall. Check `recall@100` on `record_hybrid` first: if it is
high, the shortlist already contains the relevant records and only ordering is
at fault, and chunk-first has nothing to add.
