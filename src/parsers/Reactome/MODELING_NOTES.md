# Reactome → gene network: the spec

What the Reactome SBML licenses, and the rules that follow. This is the
reasoning; the code implements it and should not repeat it.

Corpora used to derive and test every rule below:

| file | pathway | reactions | why it is here |
|---|---|---|---|
| `small.sbml` | R-HSA-75035 Chk1/Chk2 inactivation of CyclinB:Cdk1 | 5 | small enough to check by hand |
| `big.sbml` | R-HSA-69620 Cell Cycle Checkpoints | 63 | signalling |
| `medium.sbml` | R-HSA-1428517 Aerobic respiration | 117 | metabolic — a different regime |
| `sumo.sbml` | R-HSA-2990846 SUMOylation | 141 | broke the currency list |
| `nedd.sbml` | R-HSA-8951664 Neddylation | 44 | independent check of the fix |

---

## 1. Ground rules

| | |
|---|---|
| **G1** | Derive from SBML structure or Reactome schema. Never from `<notes>` prose. |
| **G2** | Layer 1 transcribes; layer 2 projects. Every layer-2 edge traces to a layer-1 path. |
| **G3** | Prefer a rule that follows from the data model over one that suppresses a symptom. |
| **G4** | No silent drops. A filter that loses rows without saying so is worse than an error. |
| **G5** | Simple beats descriptive. A rule checkable by hand on one reaction beats a better one nobody can audit. |

---

## 2. The model

**The identity test.** An entity id present in the reactant closure and absent
from the product closure has *changed*. Reactome gives a modified protein a new
stId (`TP53` → `PolyUb-TP53`), so this is set difference and nothing more.

```
changed    = ids(reactants, transitively) − ids(products, transitively)
donated    = genes reachable from hasModifiedResidue.modification on any product
acted_on   = genes(changed) − donated,  else the reactants being joined
enzyme     = genes(catalyst) − genes(changed ∩ catalyst)          proteins only

CATALYSIS    enzyme → acted_on − enzyme                    unsigned
REGULATION   genes(regulator) − target → target            signed, from SBO only
             target = enzyme, else acted_on
ASSOCIATION  no modifier: every member → the assembly formed, tagged `via`
```

**Collapse to the assembly.** If a target gene set is exactly some
participant's gene set (≥2 genes), name the participant rather than its
members. Candidates are restricted to the role that defined the target —
enzyme targets to catalysts, substrate targets to the side `acted_on` used.
Subtract *before* collapsing.

**Why each piece exists**

| | |
|---|---|
| agency = the modifier role | a participant with no `modifierSpeciesReference` is passive |
| regulators target the enzyme | `SBO:0000020` slows the reaction; the enzyme performs it |
| subtract before crossing | a gene in both sets is one molecule in two roles |
| pairs only across participants | two members of one `DefinedSet` can never be paired |
| enzymes are proteins | a cofactor in an active site is machinery, not an actor |
| every edge needs a protein | compound↔compound is chemistry the reaction already states |
| no bounded traversal | membership nests to depth 10; `*0..4` silently lost 34 genes from one complex |

---

## 3. Schema

```
(:Gene|:Compound)-[:ASSOCIATED_WITH {reaction, via, weight}]->(:Entity)
(:Gene)-[:ACTS_ON {mechanism, sign, reactions, weight, fanout}]->(:Gene|:Entity)
(:Gene|:Compound)-[:ACTS_ON_CHEMICAL {...}]->(:Gene|:Compound|:Entity)
```

Three types, not one type with flags: a property can be forgotten in a `MATCH`,
a relationship type cannot. `-[:ACTS_ON]->` yields a protein-only, genuinely
directed graph with no further filtering.

`mechanism ∈ {catalysis, regulation, association}` · `sign ∈ {negative,
positive, null}`, non-null only from `SBO:0000020`/`0000459` · `weight` =
distinct supporting reactions, never path multiplicity.

**Recovering gene↔gene:**

```cypher
MATCH (a:Gene)-[e:ACTS_ON]->(t)
OPTIONAL MATCH (t)<-[:HAS_COMPONENT|HAS_MEMBER|HAS_CANDIDATE*0..]-(:Entity)-[:IS_FORM_OF]->(g:Gene)
WITH a, e, coalesce(g, t) AS b WHERE b:Gene AND a <> b
RETURN a.name, e.mechanism, e.sign, b.name;

MATCH (a:Gene)-[ra:ASSOCIATED_WITH]->(cx)<-[rb:ASSOCIATED_WITH]-(b:Gene)
WHERE ra.via <> rb.via AND a.name < b.name
RETURN a.name, b.name, cx.stid;
```

Verified exact against the pre-collapse edge sets: `big` 257/257 and 351/351,
`medium` 1150/1150 and 912/912, zero self-loops.

---

## 4. What the source actually provides

| | fact | evidence |
|---|---|---|
| **F1** | Modifier `sboTerm` is reliable: `0000013` catalyst, `0000020` inhibitor, `0000459`/`0000461` stimulator. | stable across all five corpora, 285+ references |
| **F2** | Sign exists **only** for `0000020`/`0000459`. `0000013` is unsigned. | CHEK1→WEE1 activates, CHEK2→CDC25C inactivates; same term |
| **F3** | Reaction *shape* cannot supply sign either. | `187934` "Inactivation by p27/p21" and `176264` "Recruitment of Rad9-Hus1-Rad1" are structurally identical, opposite signs |
| **F4** | `bqbiol:hasPart` is flattened and identical for sets and complexes. | `species_141608` (Complex) lists CCNA1, CCNA2, CDK2 as peers though the first two are alternatives |
| **F5** | Stoichiometry does not balance. | `6804741`: three monomers in, two out — both branches flattened into one list |
| **F6** | A `speciesReference` `id` is a label, not a reference. | `speciesreference_6804879_input_68524` points at nothing |
| **F7** | The pathway boundary is a confounder. **No rule may infer from an absent edge.** | APC/C is an E3 ligase but appears only as a reactant in `big.sbml`; its catalysis is in another pathway |
| **F8** | Membership nests to depth 10. | `*0..4` truncated 20 entities — 34 genes from the kinetochore, 10 from the proteasome, splitting one complex in half |
| **F9** | Donated moieties are identifiable from `hasModifiedResidue.modification`. | §5 |
| **F10** | `Polymer` holds contents in `repeatedUnit`, a fourth membership slot. | 1,391 in Reactome; `monoSUMO1` is one |
| **F11** | `ent.compound` is *any* non-UniProt id, not a chemical test. | `CDKN1A gene` carries an Ensembl accession |

---

## 5. Donated moieties

```
PolyUb-TP53 --hasModifiedResidue--> GroupModifiedResidue
                                      modification → DefinedSet PolyUb → UBB/UBC/UBA52/RPS27A
                                      psiMod       → MOD:01148
```

> A gene consumed by a reaction is a **donated moiety**, not a destroyed
> substrate, if it is reachable from `hasModifiedResidue.modification` on any
> **product** of that reaction.

Per reaction, so a reaction acting *on* ubiquitin keeps its ubiquitin edges.
SUMO1/2/3 drop from 300/182/106 targets to 4/4/4, and those twelve are SUMO
maturation and SENP cleavage.

**Not available from the SBML.** `bqbiol:hasVersion` carries the PSI-MOD term
but only for top-level *simple* species: `big.sbml` reports zero
ubiquitinylations while containing `PolyUb-TP53 Tetramer`, because that species
is a Complex and the modification sits on an interior entity. The
ContentService has it for every entity.

Validated on Neddylation, where `NEDD8` appears nowhere in the code:
218 → 3 edges, while UBE2M (163), CUL1 (73) and CUL3/CUL5 (61) are untouched.

`CURRENCY_CHEBI` (H₂O, H⁺, ATP/ADP, Pi, CO₂, O₂, NAD(P)(H)) stays hand-written:
a cosubstrate is not modelled as a modification. Cofactors that identify an
enzyme (haem, FAD, Fe-S) are deliberately excluded from it.

---

## 6. Results

| | reactions | `ACTS_ON` | `ACTS_ON_CHEMICAL` | `ASSOCIATED_WITH` | round trip |
|---|---|---|---|---|---|
| `small.sbml` | 5 | 6 | 0 | 8 | PASS |
| `big.sbml` | 63 | 225 | 0 | 310 | PASS |
| `medium.sbml` | 117 | 513 | 327 | 260 | PASS |
| `sumo.sbml` | 141 | 1011 | 0 | 22 | PASS |
| `nedd.sbml` | 44 | 842 | 12 | 857 | PASS |

Sign density is a property of the pathway, not the method: 2% in checkpoint
signalling, 28% in respiration.

A third of a metabolic pathway yields nothing, correctly — 39 of
`medium.sbml`'s 117 reactions act only on metabolites, and a gene graph has
nowhere to put `ACO2 isomerizes citrate`.

**Two tests, different jobs.** `layer2.py --check` asserts exact edge sets for
nine hand-worked reactions: it tests the *rules*. `validate.py` reads layer 1
back out of Neo4j, re-derives and compares: it tests the *data*. Verified
against deliberate corruption — a missing `hasMember`, a dropped `INHIBITOR`
role and a fabricated edge are each caught.

---

## 7. Dead ends

Kept so they are not re-derived.

| | |
|---|---|
| **species `sboTerm` types entities** | Held perfectly on `big.sbml`. `SBO:0000297` means *Complex* there and *single protein* in `small`/`medium`; sets carry no term at all in two of five corpora. Not portable across exports. |
| **sign from catalytic topology** | "Unmodified form catalyses, modified form does not ⇒ inactivating." Fails on `187934` (neither form catalyses in-corpus) and `141423` (APC/C's catalysis is in another pathway). Generalised into F7. |
| **co-realizability (set = XOR)** | Correct but redundant: partitioning by participant already prevents pairing two members of one set. |
| **bidirectional association pairs** | Every association edge was mutual (702/702), so the second copy carried nothing. |
| **`binding` from static complex membership** | 6,708 rows on 63 reactions. An edge from a reaction is grounded in an event; one from co-listing is grounded in nothing. |
| **`directed` property** | Neo4j has no undirected relationship, so it promised what it could not enforce, and was redundant with `mechanism`. |
| **a hand-written currency list** | Covered ubiquitin because `big.sbml` contained ubiquitin. Cost 690 edges (40%) on SUMOylation. Replaced by §5. |
| **frequency as a moiety test** | Ub is consumed 3× in `big.sbml`, CDC25A 4×; consumed/produced ranks Ub *below* CDC25A, because `69600` releases free Ub. |

---

## 8. Open

- **Fan-out policy.** `69600` gives 32 proteasome→CDC25A edges; assemblies
  absorb most of the rest. `fanout` records the *source* side, but the
  dilution is target-side and nothing records that.
- **Autocatalysis.** Three reactions emit nothing because catalyst and target
  resolve to the same genes. `6804724` *MDM2 ubiquitinates MDM4* should give
  `MDM2 → MDM4`; both subunits are ubiquitinated so `enzyme` is empty, and
  nothing structural marks MDM2 as the ligase.
- **Unknown modifier SBO defaults to CATALYST** (the `SBO_ROLE` lookup in
  layer 1). An unhandled term becomes an unsigned agent instead of an error.
  Should raise. `0000461` has never appeared and is untested.
- **Drug subclasses.** `ProteinDrug` (102) resolves as a Gene, probably right.
  `RNADrug` (2) vanishes.
- **Reaction succession** (KEGG's `ECrel`): a product becoming another
  reaction's reactant. Trivial to detect, not emitted. Worth it, or noise?

---

## 9. For the rewrite

Moving to the `src/models` + `<Source>EntityFactory` paradigm:

- **One pathway per file is assumed.** `pw.stid` comes from the SBML model id
  and every reaction gets one `IN_PATHWAY` edge. A full run needs
  accumulation, not `--wipe` per pathway.
- **The resolution cache must be per-pathway.** It is keyed by stId and
  shared, and `resolve()` reattaches every entry — a cache warmed on
  `big.sbml` made `small.sbml` report 571 entities instead of 36 and wrote the
  wrong ones into the graph. Derive the path from the pathway stId.
- **Two network passes per pathway**: entities, then modified residues. Batch
  at 20 — the endpoint silently caps there and still answers HTTP 200. Never
  cache a negative; an id the API did not return may simply have been lost.
- **KEGG correspondence.** KGML stores layer 2 only, so its relation types are
  a specification for what layer 2 should hold:

  | KGML | ours |
  |---|---|
  | `PPrel` + phosphorylation / ubiquitination | `catalysis` |
  | `PPrel` + activation / inhibition | `regulation` (signed) |
  | `PPrel` + binding/association | `association` |
  | `ECrel` | not emitted (§8) |

  KEGG omits the proteasome entirely — its cell cycle map draws ubiquitination
  as SKP2 → CDKN1B, the E3 ligase, not the 26S machine. That curatorial
  omission is why the prior KB never had this fan-out problem.
