# Reactome parser: refactor plan

Reactome **replaces** KEGG. No KEGG source data is retained. What is kept is
KEGG's *architecture* — `<Source>_State` / `<Source>_ETL` / `<Source>Factory`,
models carrying their own parse rule, diff tables driving Neo4j — and the
entity model shapes, re-sourced from Reactome.

Modelling rules are not repeated here; they are in
`MODELING_NOTES.md` and survive unchanged. This file is shape.

---

## 1. Scope

| | |
|---|---|
| **retires** | `KEGGCaller`, `KEGGEntityFactory`, `models/kegg.py`, KGML parsing, `interactions`, `PathwayKGMLMeta`, `kegg_class`, the BRITE hierarchy fetch, **and `EntrezUniprotMap`** (§2) |
| **kept, re-sourced** | the entity taxonomy — Gene, Pathway, Compound, Drug, Reaction — now built `from_reactome` |
| **kept as-is** | `entities` as the CDC node registry · `BaseSQLObject` · `_stage_and_upsert` · `table_schemas` sync strategies · `consume_neo4j_*` · the GO and UniProt callers |
| **added** | Reactome physical entities, the membership tree, moieties, SBO-typed participation, and the layer-2 gene network |

Two KEGG models have no Reactome source: **Ortholog** (no equivalent concept)
and **Glycan** (Reactome carries them as `SimpleEntity` with a ChEBI id, so
they fold into Compound). Both retire unless you want them kept as empty
tables.

Table names are free to change.

---

## 2. Genes are keyed by UniProt accession

```
entities.entity_id   gene            = UniProt accession   (P04637)
                     compound/drug   = ChEBI id
                     physical entity = Reactome stId       (R-HSA-69488)
                     reaction        = Reactome stId
                     pathway         = Reactome stId
```

This is the change that simplifies the most downstream:

- `Function.__key__` is already `uniprot_id`. Gene → Function is now a direct
  join, not a mapped one.
- GO annotation comes from `goa-human.gaf`, UniProt-keyed. Same.
- **`EntrezUniprotMap` and the `entrez_uniprot_annotation` DAG both retire.**
  `produce_gene_uniprot_annotations` exists only to hang a `uniprot_ids`
  property on Entrez-keyed Gene nodes; when the Gene node *is* the UniProt
  node there is nothing to attach.
- The symbol-collision guard in layer 2's loader disappears — accessions are
  unique by construction, symbols are not.

`layer1.Entity.gene` already carries `Ref("uniprot", accession)`; layer 2 just
used the symbol as the key. Symbol becomes a node property, not an identity.

---

## 3. Architecture being mirrored

```
Reactome_State       pure I/O. Session + limiter as CLASS attrs so every
                     instance and thread shares one clock.
                     fetch_pathway_ids · fetch_pathway_sbml · query_ids ·
                     download_sbml_temp_file · compute_sbml_hash ·
                     read_sbml_temp_file · cleanup_sbml_temp_files
Reactome_ETL         owns the state; all parsing lives here as parse_*
                     generators. No I/O except through self.reactome_state.
ReactomeFactory      build_<type>(data) -> list[Model] and
                     REGISTRY: EntityType -> {"table", "builder"}
models/reactome.py   frozen dataclasses on BaseSQLObject:
                       __table_name__ ClassVar · entity_type ClassVar ·
                       from_reactome(cls, datum)
producers/consumers  produce_* is a thin yield-from; consume_* funnels into
                     _stage_and_upsert(records, sql_caller, extractors)
table_schemas        the sync strategy produces the diff table
consume_neo4j_*      diff rows -> build_neo4j_entity / build_neo4j_edges
```

---

## 4. One parse step, not two

Your redaction is right, and it is worth writing down why, because it is the
one place the shape differs from KEGG.

KEGG splits structure from annotation because **KGML is self-contained**: the
file is the topology, so annotation is a genuinely separate API pass that only
adds node properties.

Reactome resolves a pathway in **one pass** that yields both. The SBML gives
species, compartments, UniProt/ChEBI refs, reactions and SBO-typed
participation; `/data/query/ids` fills in `schema_class`, the membership tree,
gene symbols and moieties — because `bqbiol:hasPart` is flattened and unusable
(notes F4). Splitting those into two stages would call the expensive endpoint
twice for nothing.

So there is no separate annotation DAG. One `reactome_structure` stage stages
every structure *and* annotation table from a single resolve, and layer 2
follows as a SQL-to-SQL projection.

Consequence worth knowing: that stage creates **interior entities** — a
`DefinedSet`'s member complexes — which exist nowhere in the SBML and only
appear once the tree is walked. They stage into `entities` like any other
node, so the node registry is populated from the resolve, not from the file.

---

## 5. Models

`src/models/reactome.py`, `from_reactome(cls, datum)` mirroring `from_kegg`.

**`EntityType` — Reactome's own classes**

```python
class EntityType(str, Enum):
    GENE            = "gene"             # EWAS -> UniProt
    COMPOUND        = "compound"         # SimpleEntity -> ChEBI
    DRUG            = "drug"             # ChemicalDrug | ProteinDrug | RNADrug
    COMPLEX         = "complex"
    DEFINED_SET     = "defined_set"
    CANDIDATE_SET   = "candidate_set"
    POLYMER         = "polymer"
    OTHER_ENTITY    = "other_entity"
    GENOME_ENCODED  = "genome_encoded_entity"
    REACTION        = "reaction"
    PATHWAY         = "pathway"
```

Per-class rather than one `PHYSICAL_ENTITY`, so `build_neo4j_entity` can give
each its own label and a query can ask for complexes without joining
`EntityData`.

**Source state**

| model | table | sync |
|---|---|---|
| `ReactomePathwayIds` | `PathwayIds` | `DiffSync(name)` |
| `SBMLMetaData` | `PathwaySBMLMeta` | `DiffSync(sbml_hash)` |
| `PathwayHierarchy` | `pathway_class` | replaces `kegg_class`, from `/data/eventsHierarchy/9606` |

**Structure + annotation — one resolve**

| model | table | sync |
|---|---|---|
| `Entity` | `entities` | `DiffSync(entity_type)` |
| `EntityData` | `EntityData` | `DiffSync(display_name, compartment, schema_class)` |
| `EntityPathMem` | `EntityPathMem` | `IdentityHashSync` |
| `Membership` | `entity_membership` | `IdentityHashSync(parent_id, child_id, rel; scope=pathway_id)` |
| `EntityMoiety` | `entity_moiety` | `IdentityHashSync(entity_id, moiety_id)` |
| `Reaction` | `reactions` | `DiffSync(name, compartment)` |
| `Participation` | `reaction_participants` | `IdentityHashSync(reaction_id, entity_id, role; scope=pathway_id)` |

`role` is `reactant | product | catalyst | inhibitor | stimulator`, from the
SBO term — KEGG's `catalyst | substrate | product` widened.

Annotation models (`Gene`, `Compound`, `Drug`, `Pathway`, `Reaction`) keep
their KEGG table shapes and gain `from_reactome`, fed from
`referenceEntity` / `displayName` rather than flat files.

**Projection — layer 2**

```python
@dataclass(frozen=True)
class GeneEdge(BaseSQLObject):
    source_id: str          # entities.entity_id
    target_id: str
    source_label: str       # Gene | Compound | Entity
    target_label: str
    rel_type: str           # ACTS_ON | ACTS_ON_CHEMICAL | ASSOCIATED_WITH
    mechanism: str          # catalysis | regulation | association
    sign: Optional[str]     # negative | positive | None
    reaction_id: Optional[str]
    pathway_id: str
    weight: int
    __table_name__: ClassVar[str] = "gene_edges"
```

`IdentityHashSync(source_id, target_id, rel_type, mechanism, sign;
scope=pathway_id)` — so a rule change deletes the edges it stops producing,
which derived edges need more than raw ones do.

**Intermediate (plain dataclasses, not SQL)**

```python
SBMLRecord         pathway_code, sbml_bytes, metadata
PathwayRecord      pathway, entities, entity_data, path_mem, memberships,
                   moieties, reactions, participations
```

---

## 6. DAGs

| DAG | trigger | tasks |
|---|---|---|
| `reactome_meta_build` | manual | ensure SQL env → pathway hierarchy → pathway ids → fetch SBML, hash, diff, download changed → emit `reactome://pathway_sbml_batched` with `changed_ids` |
| `reactome_structure` | that asset | resolve changed pathways (SBML + `/data/query/ids`) → all structure and annotation tables → Neo4j nodes then edges → emit `reactome://structure_complete` |
| `reactome_gene_edges` | that asset | derive layer 2 from SQL → `gene_edges` → Neo4j |

Same three-stage shape as KEGG, with annotation merged into structure (§4) and
projection added.

---

## 7. Where the current code lands

| `reactome-staging/` | becomes |
|---|---|
| `layer1.parse_sbml` | `Reactome_ETL.parse_sbml_structure` |
| `layer1.resolve` | `Reactome_ETL.resolve_entities` — same stage |
| `layer1` dataclasses | `src/models/reactome.py` on `BaseSQLObject` |
| `layer1.write_mechanism`, `CONSTRAINTS` | deleted — Neo4j via diff tables |
| `entity_cache.json`, `wipe_cache` | **deleted** — an ad-hoc version of the annotation tables, and every bug it caused (shared across pathways, silently reattaching another pathway's 571 entities) is what `DiffSync` prevents |
| `layer2.derive`, `collapse` | `Reactome_ETL.derive_gene_edges`, reading SQL |
| `layer2.CURRENCY`, `DUPES` guard | deleted — moieties are derived, UniProt keys cannot collide |
| `layer2.GOLDEN` / `check()`, `validate.py` | keep as tests |
| `build.py` | deleted — the DAG orchestrates |

---

## 8. Invariants the refactor must not lose

Each cost a real bug; `MODELING_NOTES.md` has the evidence.

| | |
|---|---|
| Batch `/data/query/ids` at **20** | it silently caps there and still returns HTTP 200 |
| **Never cache a negative** | an id the API did not return may just have been lost; persisting `Unknown` fossilised 80 of 154 entries |
| No bounded membership traversal | nesting reaches depth 10 |
| Read `repeatedUnit` as a membership slot | 1,391 Polymers; `monoSUMO1` is one |
| Never key entity type on a species `sboTerm` | the mapping inverts between exports |
| Subtract before collapsing to an assembly | otherwise identity subtraction silently stops |
| Persist `HAS_MOIETY` | layer 2 reads it; without it the graph cannot reproduce layer 2 |

**Tests to carry over:** the nine golden cases (`layer2.py --check`) assert exact
edge sets for hand-worked reactions — they test the rules. `validate.py`
re-derives layer 2 from the persisted graph — it tests the data. Both found
bugs the other could not, and neither caught the one that diffing output
against a baseline did.

---

## 9. Built — what the implementation found

Files: `src/models/reactome.py`, `src/parsers/Reactome/{ReactomeCaller,
ReactomeEntityFactory,GeneNetwork,test_reactome}.py`,
`src/builders/SQL/schema/reactome_definitions.py`,
`src/builders/Neo4j/schema/reactome_graph.py`,
`src/workflow/reactome_flow.py`, `dags/reactome_routine.py`.

**Regression: identical to the `reactome-staging` reference on all five
corpora** — 14 / 533 / 1078 / 473 / 1658 edges, exact set match after
translating symbols to accessions. Nine golden cases pass, 17,752 rows pass
`BaseSQLObject` validation.

### UniProt keying needed a secondary key

Reactome models the central dogma explicitly: a gene, its transcript and its
protein are three entities with three reference types, and only
`ReferenceGeneProduct` is UniProt-keyed -- UniProt is a protein database and
has no accession for a stretch of DNA.

```
R-HSA-69488   TP53          ReferenceGeneProduct   UniProt  P04637
R-HSA-3786256 CDKN1A gene   ReferenceDNASequence   ENSEMBL  ENSG00000124762
R-HSA-6803386 CDKN1A mRNA   ReferenceRNASequence   ENSEMBL  ENST00000244741
```

Keying on UniProt alone loses 31 edges, including `TP53 stimulates CDKN1A
transcription` -- not because TP53 is unresolvable but because the *target* is
the gene entity.

**`gene_xref` (uniprot_id, xref_db, xref_id)** carries the secondary keys, and
they come from Reactome structurally: a gene product's `referenceGene` lists
its ENSEMBL, NCBI Gene, OMIM, UCSC, KEGG and COSMIC ids. So a DNA entity finds
its gene by join, not by symbol. `GeneData.ensembl_gene` holds the ENSEMBL one
for convenience.

**mRNA still needs the symbol.** `ReferenceRNASequence` and
`ReferenceDNASequence` carry no upward link at all -- no `referenceGene`, no
`crossReference`. `geneName` is the only field the three reference types
share. So the symbol fallback survives for transcripts alone, scoped to one
pathway, and unbridged symbols are logged rather than guessed.

Cost: `referenceGene` is absent from a *nested* `referenceEntity` but present
when the gene product's own dbId is queried, so it batches at 20 like
everything else -- 274 genes in 14 calls, not 274. Memoised per run; xrefs do
not vary by pathway. ~3,200 xref rows per pathway, deduplicated by
`IdentityHashSync` scoped on `uniprot_id`.

`NCBI Gene` and `KEGG` ids arrive free, which is a ready-made join back to the
old KEGG-keyed tables for migration checking.

### The merge must go inside definitions.py, nowhere else

`src/builders/SQL/schema/__init__.py` imports `table_registry`, which builds
`table_managers` from `table_schemas` **at import time**. Importing any
submodule of the package runs `__init__` first, so a merge in caller code --
even one that runs before `table_registry` is named -- is already too late:

```python
# definitions.py, AFTER the table_schemas literal and BEFORE anything else
from .reactome_definitions import reactome_table_schemas
table_schemas.update(reactome_table_schemas)
validate_schema(table_schemas)
```

Merging anywhere else leaves a half-configured registry whose failure mode is
`ValueError: No TableManager registered for 'gene_edges'` at stage time, with
the replaced tables silently still KEGG-shaped. Verified: with the merge in
place, all 17 models map cleanly; without it, 13 of them fail.

### A 403 is not always a block

`ReactomeBlockedError` fires on 403, but Reactome also 403s a default urllib
User-Agent. `requests` and `curl` both get 200 for the same URL. Worth knowing
before treating one as an IP block.

---

## 10. Still open

**For you**

- **Ortholog and Glycan** retire, unless you want the tables kept.
- **Rate limiting the ContentService.** Nothing published. Five pathways ran
  unthrottled at batches of 20 without a refusal; a full run is ~2,700, so I
  would put KEGG's `_RateLimiter` in front of it anyway.
- **Import path** — `from parsers.KEGG…` vs `from src.parsers.GO…`. Pick one;
  I will use `src.parsers.Reactome` unless told otherwise.

**In the model** (from `MODELING_NOTES.md` §8, unchanged by the refactor):
fan-out policy, autocatalysis, the unknown-modifier-SBO default to `CATALYST`
(should raise), `RNADrug`, and whether to emit reaction succession.
