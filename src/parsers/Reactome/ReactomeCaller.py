import hashlib
import logging
import re
import shutil
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.models.reactome import *
from src.parsers.Reactome.GeneNetwork import PathwayGraph, collapse, derive
from src.parsers.Reactome.ReactomeEntityFactory import ReactomeEntityFactory

logger = logging.getLogger(__name__)

REACTOME_PATH = Path("data/SBML/")
SPECIES = "9606"

SBML_NS = "http://www.sbml.org/sbml/level3/version1/core"
# Event-hierarchy node types that are pathways. CellLineagePath is a Pathway
# subclass in Reactome's schema and nests inside itself, so dropping it would
# take 14 real pathways and their whole sub-structure with it. Everything
# absent here -- Reaction, BlackBoxEvent, FailedReaction, Polymerisation,
# Depolymerisation, CellDevelopmentStep -- is reaction-level and never
# contains a pathway.
PATHWAY_TYPES = frozenset({"Pathway", "TopLevelPathway", "CellLineagePath"})
RDF_RESOURCE = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource"
BQ_NS = "{http://biomodels.net/biology-qualifiers/}"

# SBO terms on <modifierSpeciesReference>. Plain XML attributes, not RDF --
# the role is stated in the file, never inferred. 0000461 is an essential
# activator, which Reactome emits for a Requirement.
SBO_ROLE: dict[str, Role] = {
    "SBO:0000013": Role.CATALYST,
    "SBO:0000020": Role.INHIBITOR,
    "SBO:0000459": Role.STIMULATOR,
    "SBO:0000461": Role.STIMULATOR,
}

MEMBERSHIP_SLOTS = ("hasComponent", "hasMember", "hasCandidate", "repeatedUnit")

CHEMICAL_CLASSES = frozenset({"SimpleEntity", "ChemicalDrug", "ProteinDrug",
                              "RNADrug", "Polymer"})


class _RateLimiter:
    """Caps the combined request rate across every thread sharing one
    instance. A pool's worker count bounds concurrency, not burstiness."""

    def __init__(self, max_per_second: float):
        self._min_interval = 1.0 / max_per_second
        self._lock = threading.Lock()
        self._last_call = 0.0

    def wait(self) -> None:
        with self._lock:
            sleep_for = self._min_interval - (time.monotonic() - self._last_call)
            if sleep_for > 0:
                time.sleep(sleep_for)
            self._last_call = time.monotonic()


def _build_reactome_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        backoff_factor=1.0,
        # 520-527 are Cloudflare's own codes for "the origin misbehaved" --
        # 521 origin refused the connection, 525 TLS handshake failed. They
        # are transient and retry clean, but they are not in urllib3's usual
        # list, so without them one blip drops a pathway for the whole run.
        status_forcelist=(429, 500, 502, 503, 504,
                          520, 521, 522, 523, 524, 525, 526, 527),
        allowed_methods=("GET", "POST"),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


class ReactomeBlockedError(Exception):
    """403 from the ContentService. Treated as an IP-level block, not a
    skippable per-request failure -- deliberately not a RequestException, so a
    caller that catches-and-continues on a bad id cannot swallow this too."""


class Reactome_State:
    """Pure I/O. Session and limiter are class attributes so every instance
    and every thread shares one connection pool and one clock."""

    _session = _build_reactome_session()
    # Reactome publishes no rate limit. A full run is ~2,000 pathways and the
    # resolve issues a request per 20 entities, so the volume is comparable
    # to what got us soft-blocked elsewhere. Throttled on principle.
    _rate_limiter = _RateLimiter(max_per_second=5)
    _hierarchy: list[dict] | None = None

    def __init__(self):
        self.base_url = "https://reactome.org/ContentService"

    def _request(self, method: str, url: str, timeout: float = 120.0, **kwargs):
        self._rate_limiter.wait()
        response = self._session.request(method, url, timeout=timeout, **kwargs)
        if response.status_code == 403:
            raise ReactomeBlockedError(
                f"Reactome returned 403 for {url} -- treat as an IP-level block. "
                "Stop issuing further requests.")
        return response

    def fetch_event_hierarchy(self) -> list[dict]:
        """The whole human event tree. Both the pathway registry and the
        pathway hierarchy come out of this one payload, so it is memoised --
        it is ~4 MB and two producers read it in the same run."""
        if self._hierarchy is not None:
            return self._hierarchy
        url = f"{self.base_url}/data/eventsHierarchy/{SPECIES}"
        response = self._request("GET", url)
        if not response.ok:
            logger.error("Failed to fetch event hierarchy - status %d", response.status_code)
            return []
        self._hierarchy = response.json()
        return self._hierarchy

    def fetch_pathway_sbml(self, pathway_id: str) -> bytes:
        url = f"{self.base_url}/exporter/event/{pathway_id}.sbml"
        response = self._request("GET", url)
        if not response.ok:
            raise RuntimeError(f"Failed to fetch SBML for {pathway_id} "
                               f"- status {response.status_code}")
        return response.content

    # /data/query/ids SILENTLY CAPS AT 20 OBJECTS -- HTTP 200, no error, no
    # pagination header, everything past the 20th dropped.
    QUERY_IDS_MAX = 20

    def query_ids(self, ids: list[str], attempts: int = 3) -> list[dict]:
        """One batch, retried. Returns whatever came back; the caller decides
        what a short answer means -- it must never be cached as absence."""
        url = f"{self.base_url}/data/query/ids"
        last = None
        for attempt in range(attempts):
            try:
                response = self._request(
                    "POST", url, data=",".join(ids),
                    headers={"Content-Type": "text/plain", "accept": "application/json"})
                if response.ok:
                    got = response.json()
                    return got if isinstance(got, list) else [got]
                last = f"HTTP {response.status_code}"
            except ReactomeBlockedError:
                raise
            except Exception as e:                      # noqa: BLE001 - retried
                last = f"{type(e).__name__}: {e}"
            if attempt + 1 < attempts:
                time.sleep(2 ** attempt)
        logger.warning("Batch of %d failed after %d attempts (%s)", len(ids), attempts, last)
        return []

    def download_sbml_temp_file(self, pathway_id: str, file: bytes) -> None:
        REACTOME_PATH.mkdir(parents=True, exist_ok=True)
        file_path = REACTOME_PATH / f"{pathway_id}.sbml"
        tmp_path = file_path.with_suffix(".sbml.tmp")
        tmp_path.write_bytes(file)
        tmp_path.replace(file_path)

    def compute_sbml_hash(self, content: bytes) -> str:
        return hashlib.md5(content).hexdigest()

    def read_sbml_temp_file(self, pathway_id: str) -> bytes:
        return (REACTOME_PATH / f"{pathway_id}.sbml").read_bytes()

    def cleanup_sbml_temp_files(self) -> None:
        if REACTOME_PATH.exists():
            shutil.rmtree(REACTOME_PATH)


def _as_dicts(value) -> list[dict]:
    """A one-or-many Reactome slot, always as a list.

    The ContentService serialises the same slot as a dict when it holds one
    value and as a list when it holds several, so reading either shape with
    .get() is a crash waiting for the first instance of the other.
    """
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    return []


def _residue_moieties(residue: dict) -> list[dict]:
    """What one modified residue donates: (moiety entity, PSI-MOD) pairs.

    A residue with no `modification` donates nothing, and that is the
    common case rather than an edge case -- ReplacedResidue (3.4k of them)
    records a point mutation, "L-alanine 60 replaced with L-glutamic acid",
    which changes a residue without attaching anything. Those carry two
    psiMod terms, the residue lost and the residue gained, which is the
    list shape _as_dicts exists for.
    """
    psi = [p["identifier"] for p in _as_dicts(residue.get("psiMod")) if p.get("identifier")]
    out = []
    for i, m in enumerate(_as_dicts(residue.get("modification"))):
        stid = m.get("stId")
        if not stid:
            continue
        psi_mod = psi[i] if i < len(psi) else (psi[0] if psi else None)
        if str(stid).startswith("R-"):
            # A protein moiety (SUMO, ubiquitin) is a PhysicalEntity, walked
            # like any other.
            out.append({"modification": stid, "ref": None, "psi_mod": psi_mod})
        elif str(stid).startswith("chebi:") and m.get("identifier"):
            # A chemical one is a ReferenceMolecule. Every ReferenceEntity
            # stId is '<prefix>:<accession>' and never R-, so these are not
            # entities to walk; the node is the compound keyed on the bare
            # accession, exactly as a SimpleEntity's compound is.
            out.append({"modification": m["identifier"], "ref": m, "psi_mod": psi_mod})
        else:
            logger.warning("moiety modification %r is neither a Reactome entity "
                           "nor a ChEBI reference - skipped", stid)
    return out


def _walk_pathways(nodes: list[dict], parent_stid: str | None = None
                   ) -> Iterator[tuple[str, str, bool, str | None]]:
    """Every pathway in the event hierarchy as (stId, name, is_leaf, parent).

    Emits one tuple per (pathway, parent) arrival, duplicates included --
    deduping is the caller's job, because the two callers dedupe on
    different things. A leaf is a pathway with no pathway children; its
    reaction children are what the SBML export carries.
    """
    for node in nodes:
        children = node.get("children", [])
        if node.get("type") not in PATHWAY_TYPES:
            continue
        stid = node.get("stId")
        if not stid:
            continue
        is_leaf = not any(c.get("type") in PATHWAY_TYPES for c in children)
        yield stid, node.get("name") or node.get("displayName", ""), is_leaf, parent_stid
        yield from _walk_pathways(children, stid)


def _quals(species: ET.Element) -> dict[str, list[str]]:
    """bqbiol qualifiers on one species, as {qualifier: [db:id, ...]}."""
    out: dict[str, list[str]] = {}
    for el in species.iter():
        if not el.tag.startswith(BQ_NS):
            continue
        refs = [li.get(RDF_RESOURCE) for li in el.iter() if li.get(RDF_RESOURCE)]
        out[el.tag[len(BQ_NS):]] = [r for r in refs if r]
    return out


def _own_stid(el: ET.Element, fallback: str) -> str:
    """The element's OWN Reactome stId, from its direct <annotation>.

    The SBML ids are bare numbers (`pathway_75035`, `reaction_9029987`) while
    everything else in the graph keys on stIds, so take the stId where the
    file states it. Restricted to bqbiol:is under the element's own
    annotation: iterating the subtree picks up a child species, and the
    isHomologTo bag next to it is full of R-MMU/R-RNO ids.
    """
    ann = el.find(f"{{{SBML_NS}}}annotation")
    if ann is not None:
        for node in ann.iter(f"{BQ_NS}is"):
            for li in node.iter():
                m = re.search(r"reactome:(R-[A-Z]{3}-\d+)", li.get(RDF_RESOURCE) or "")
                if m:
                    return m.group(1)
    return fallback


def _parse_resource(url: str) -> tuple[str, str] | None:
    """Reactome writes identifiers.org URIs for UniProt and Reactome but a
    legacy EBI URL for ChEBI. Handle both or every compound vanishes."""
    m = re.search(r"identifiers\.org/([A-Za-z0-9.]+)[:/]([^/\s]+)$", url)
    if m:
        return m.group(1).lower(), m.group(2)
    m = re.search(r"chebiId=CHEBI:(\d+)", url)
    return ("chebi", m.group(1)) if m else None


class Reactome_ETL:
    def __init__(self):
        self.reactome_state = Reactome_State()
        # ReferenceGeneProduct dbId -> its cross-references. A gene's xrefs
        # are the same in every pathway, so memoised for the whole run.
        self._xref_cache: dict[int, list[dict]] = {}

    # ---- flat sources -------------------------------------------------

    def parse_pathway_ids(self, nodes: list[dict]) -> Iterator[PathwayIds]:
        """Leaf pathways -- the ones whose SBML is actually ingested.

        A pathway's SBML export contains every reaction in its subtree, so
        fetching intermediates too would resolve the same reaction once per
        ancestor. Ancestry is not lost: pathway_class holds the full DAG
        above these, so a leaf's parents are one traversal away.
        """
        seen: set[str] = set()
        for stid, name, is_leaf, _ in _walk_pathways(nodes):
            if is_leaf and stid not in seen:
                seen.add(stid)
                yield PathwayIds(pathway_id=stid, name=name)

    def parse_event_hierarchy(self, nodes: list[dict]) -> Iterator[PathwayHierarchy]:
        """Pathway -> parent edges, keyed by stId.

        Two traps in this feed. It carries every event, and reactions
        outnumber pathways six to one -- unfiltered it turns a pathway
        classification into a flattened copy of Reactome. And it is a DAG
        served as a tree: a pathway reachable under several parents has its
        whole subtree repeated per parent, so the same (child, parent) pair
        arrives many times.

        stId, not name, because names are not unique -- ~19k events share
        ~18.8k names, and matching a parent by name binds some children to
        the wrong one.
        """
        seen: set[tuple[str, str | None]] = set()
        for stid, name, _, parent_stid in _walk_pathways(nodes):
            if (stid, parent_stid) in seen:
                continue
            seen.add((stid, parent_stid))
            yield PathwayHierarchy(stid=stid, name=name, parent_stid=parent_stid)

    # ---- structure, from the SBML file ---------------------------------

    def _parse_sbml(self, xml_content: bytes) -> tuple[Pathway, dict[str, dict], list[Reaction],
                                                       list[Participation]]:
        root = ET.fromstring(xml_content)
        model = root.find(f"{{{SBML_NS}}}model")
        if model is None:
            raise ValueError("SBML has no <model>")

        pathway_id = _own_stid(model, (model.get("id") or "").replace("pathway_", ""))
        pathway = Pathway(pathway_id=pathway_id, description=model.get("name") or "")

        compartments = {
            c.get("id"): c.get("name")
            for c in (model.find(f"{{{SBML_NS}}}listOfCompartments") or [])
        }

        species: dict[str, dict] = {}
        by_sbml_id: dict[str, str] = {}
        for s in (model.find(f"{{{SBML_NS}}}listOfSpecies") or []):
            refs = {}
            for url in _quals(s).get("is", []):
                parsed = _parse_resource(url)
                if parsed:
                    refs.setdefault(parsed[0], parsed[1])
            stid = refs.get("reactome") or s.get("id")
            species[stid] = {
                "stId": stid,
                "displayName": s.get("name") or stid,
                "compartment": compartments.get(s.get("compartment")),
                "uniprot": refs.get("uniprot"),
                "chebi": refs.get("chebi"),
            }
            by_sbml_id[s.get("id")] = stid

        reactions: list[Reaction] = []
        participations: list[Participation] = []
        for r in (model.find(f"{{{SBML_NS}}}listOfReactions") or []):
            reaction_id = _own_stid(r, (r.get("id") or "").replace("reaction_", ""))
            reactions.append(Reaction.from_reactome({
                "stId": reaction_id,
                "displayName": r.get("name") or reaction_id,
                "compartment": compartments.get(r.get("compartment")),
                "schemaClass": "Reaction",
                "pathway_id": pathway_id,
            }))
            for tag, role in (("listOfReactants", Role.REACTANT),
                              ("listOfProducts", Role.PRODUCT)):
                for x in (r.find(f"{{{SBML_NS}}}{tag}") or []):
                    participations.append(Participation(
                        reaction_id=reaction_id,
                        entity_id=by_sbml_id[x.get("species")],
                        role=role,
                        stoichiometry=int(float(x.get("stoichiometry") or 1)),
                        pathway_id=pathway_id))
            for x in (r.find(f"{{{SBML_NS}}}listOfModifiers") or []):
                sbo = x.get("sboTerm")
                if sbo not in SBO_ROLE:
                    raise ValueError(
                        f"{reaction_id}: unmapped modifier sboTerm {sbo!r}. Add it to "
                        f"SBO_ROLE -- defaulting it to CATALYST makes an unsigned "
                        f"agent out of a regulator, which looks fine and is wrong.")
                participations.append(Participation(
                    reaction_id=reaction_id,
                    entity_id=by_sbml_id[x.get("species")],
                    role=SBO_ROLE[sbo],
                    stoichiometry=1,
                    pathway_id=pathway_id))

        return pathway, species, reactions, participations

    # ---- resolve, from the ContentService -------------------------------

    def resolve_entities(self, stids: set[str]) -> dict[str, dict]:
        """Reactome's real tree plus the accessions and moieties hanging off
        it, breadth-first from the SBML's top-level species.

        The SBML cannot supply this: bqbiol:hasPart arrives flattened, with a
        Complex and a DefinedSet serialised identically, so an assembly's
        structure is dissolved by the time it reaches the file.
        """
        state = self.reactome_state
        resolved: dict[str, dict] = {}
        unresolved: set[str] = set()

        frontier = set(stids)
        while todo := sorted(frontier - resolved.keys() - unresolved):
            for i in range(0, len(todo), state.QUERY_IDS_MAX):
                chunk = todo[i:i + state.QUERY_IDS_MAX]
                objs = state.query_ids(chunk)
                for o in objs:
                    resolved[o["stId"]] = o
                # NEVER record a negative: an id the API did not return is
                # either absent or was lost to a bad response, and we cannot
                # tell which. Persisting the guess fossilises it.
                missing = set(chunk) - {o["stId"] for o in objs}
                if missing:
                    logger.warning("%d id(s) not returned, will retry next run: %s",
                                   len(missing), sorted(missing)[:5])
                    unresolved |= missing
            frontier = {c["stId"]
                        for sid in todo if sid in resolved
                        for slot in MEMBERSHIP_SLOTS
                        for c in (resolved[sid].get(slot) or [])
                        if isinstance(c, dict) and c.get("stId")}

        # Second pass: what does each modified residue attach? This is what
        # replaces a hand-written currency list -- see MODELING_NOTES.md §5.
        residues = {r["dbId"] for o in resolved.values()
                    for r in (o.get("hasModifiedResidue") or [])
                    if isinstance(r, dict) and r.get("dbId")}
        moiety_of: dict[int, list[dict]] = {}
        if residues:
            todo = sorted(str(d) for d in residues)
            for i in range(0, len(todo), state.QUERY_IDS_MAX):
                for o in state.query_ids(todo[i:i + state.QUERY_IDS_MAX]):
                    moiety_of[o.get("dbId")] = _residue_moieties(o)
        for o in resolved.values():
            o["_moieties"] = [
                m for r in (o.get("hasModifiedResidue") or [])
                if isinstance(r, dict)
                for m in moiety_of.get(r.get("dbId"), [])
            ]

        # Moiety entities are usually not components of anything in the
        # pathway, so the walk above never saw them and they carry no genes.
        # Only protein moieties are walked -- a chemical one is a ChEBI
        # reference, not a Reactome entity, and resolving its pseudo-stId
        # would put a ReferenceMolecule in the entity table.
        frontier = {m["modification"] for o in resolved.values()
                    for m in o["_moieties"] if not m["ref"]}
        while todo := sorted(frontier - resolved.keys() - unresolved):
            for i in range(0, len(todo), state.QUERY_IDS_MAX):
                chunk = todo[i:i + state.QUERY_IDS_MAX]
                objs = state.query_ids(chunk)
                for o in objs:
                    o.setdefault("_moieties", [])
                    resolved[o["stId"]] = o
                unresolved |= set(chunk) - {o["stId"] for o in objs}
            frontier = {c["stId"]
                        for sid in todo if sid in resolved
                        for slot in MEMBERSHIP_SLOTS
                        for c in (resolved[sid].get(slot) or [])
                        if isinstance(c, dict) and c.get("stId")}

        return resolved

    def fetch_gene_xrefs(self, db_ids: set[int]) -> dict[int, list[dict]]:
        """`referenceGene` for each gene product, batched.

        /data/query/ids returns a NESTED referenceEntity shallow, but asking
        for the gene product's own dbId returns it in full -- so this reuses
        the same batched, retried path as everything else rather than one
        request per gene.

        This is what makes the DNA bridge structural: the CDKN1A protein
        points at ENSG00000124762, so a `CDKN1A gene` entity reaches the same
        Gene node without a symbol match. mRNA still needs the symbol --
        ReferenceRNASequence carries no link back, and Reactome offers
        nothing else.
        """
        state = self.reactome_state
        todo = sorted(str(d) for d in db_ids - self._xref_cache.keys())
        for i in range(0, len(todo), state.QUERY_IDS_MAX):
            for o in state.query_ids(todo[i:i + state.QUERY_IDS_MAX]):
                self._xref_cache[o.get("dbId")] = [
                    {"db": x.get("databaseName"), "id": x.get("identifier")}
                    for x in (o.get("referenceGene") or [])
                    if isinstance(x, dict) and x.get("identifier")
                ]
        return {d: self._xref_cache.get(d, []) for d in db_ids}

    # ---- one pathway, end to end ----------------------------------------

    def parse_pathway_record(self, pathway_id: str) -> Iterator[PathwayRecord]:
        """Structure and annotation together: Reactome resolves both in one
        pass, so splitting them would call the expensive endpoint twice."""
        xml_content = self.reactome_state.read_sbml_temp_file(pathway_id)
        pathway, species, reactions, participations = self._parse_sbml(xml_content)
        resolved = self.resolve_entities(set(species))
        yield self._build_record(pathway, species, reactions, participations, resolved)

    def _build_record(self, pathway, species, reactions, participations,
                      resolved: dict[str, dict]) -> PathwayRecord:
        pid = pathway.pathway_id
        record = PathwayRecord(pathway=pathway, reactions=reactions,
                               participations=participations)
        seen_reference: set[tuple[str, str]] = set()

        # A gene's identity is its UniProt accession, but an mRNA or a gene
        # entity (ReferenceRNASequence / ReferenceDNASequence) carries an
        # Ensembl id and no accession of its own. `MT-CO1 mRNA is translated`
        # and `TP53 stimulates CDKN1A transcription` are then edgeless, since
        # neither endpoint resolves. Bridge them on geneName, scoped to this
        # pathway and only for entities that have no accession -- an
        # Ensembl-keyed Gene node would be a second node for one gene.
        uniprot_of_symbol: dict[str, str] = {}
        gene_products: dict[int, str] = {}
        for obj in resolved.values():
            ref = obj.get("referenceEntity") or {}
            if (ref.get("databaseName") or "").lower().startswith("uniprot"):
                for symbol in (ref.get("geneName") or []):
                    uniprot_of_symbol.setdefault(symbol, ref["identifier"])
                if ref.get("dbId"):
                    gene_products[ref["dbId"]] = ref["identifier"]

        # Structural secondary keys. ENSG -> UniProt comes from Reactome
        # itself, so a DNA entity needs no symbol match; the symbol is only
        # the fallback for mRNA, which has no upward link at all.
        xrefs = self.fetch_gene_xrefs(set(gene_products))
        uniprot_of_xref: dict[str, str] = {}
        ensembl_of_uniprot: dict[str, str] = {}
        for db_id, uniprot in gene_products.items():
            for x in xrefs.get(db_id, []):
                uniprot_of_xref.setdefault(x["id"], uniprot)
                record.gene_xrefs.append(GeneXref(
                    uniprot_id=uniprot, xref_db=x["db"] or "unknown", xref_id=x["id"]))
                if x["db"] == "ENSEMBL":
                    ensembl_of_uniprot.setdefault(uniprot, x["id"])

        unbridged: set[str] = set()
        for stid, obj in resolved.items():
            record.entities.append(Entity.from_reactome(obj))
            record.entity_data.append(EntityData.from_reactome(obj))
            if stid in species:
                record.entity_path_mem.append(EntityPathMem(entity_id=stid, pathway_id=pid))

            for slot in MEMBERSHIP_SLOTS:
                counts: dict[str, int] = {}
                for c in (obj.get(slot) or []):
                    # A homodimer lists the same component twice, and the
                    # serialiser emits the repeat as a bare dbId back-
                    # reference. The ints are stoichiometry, not partners.
                    key = c.get("stId") if isinstance(c, dict) else None
                    if key:
                        counts[key] = counts.get(key, 0) + 1
                for child_id, n in counts.items():
                    record.memberships.append(Membership(
                        parent_id=stid, child_id=child_id,
                        rel=Membership_Rel(slot), stoichiometry=n, pathway_id=pid))

            for m in obj.get("_moieties", []):
                record.moieties.append(EntityMoiety(
                    entity_id=stid, moiety_id=m["modification"], psi_mod=m["psi_mod"]))
                # A chemical moiety's node is the compound itself, and the
                # walk never visits it, so register it here or HAS_MOIETY
                # points at a node nothing created.
                if m["ref"] and (EntityType.COMPOUND, m["modification"]) not in seen_reference:
                    seen_reference.add((EntityType.COMPOUND, m["modification"]))
                    record.entities.append(Entity(entity_id=m["modification"],
                                                  entity_type=EntityType.COMPOUND))
                    record.compounds.append(Compound.from_reactome(m["ref"]))

            ref = obj.get("referenceEntity") or {}
            identifier, db = ref.get("identifier"), (ref.get("databaseName") or "").lower()
            if not identifier:
                continue
            if db.startswith("uniprot"):
                kind, reference_id = EntityType.GENE, identifier
            elif (db.startswith(("chebi", "guide to pharmacology"))
                  or obj.get("schemaClass") in CHEMICAL_CLASSES):
                kind = EntityType.DRUG if obj.get("schemaClass", "").endswith("Drug") \
                    else EntityType.COMPOUND
                reference_id = identifier
            else:
                # A DNA or RNA entity. Reactome keys those on Ensembl, and
                # only the protein carries a UniProt accession.
                symbols = ref.get("geneName") or []
                bridged = uniprot_of_xref.get(identifier) or next(
                    (uniprot_of_symbol[s] for s in symbols if s in uniprot_of_symbol), None)
                if bridged is None:
                    unbridged.update(symbols or [identifier])
                    continue
                kind, reference_id, ref = EntityType.GENE, bridged, {
                    **ref, "identifier": bridged}

            record.identities.append(EntityIdentity(
                entity_id=stid, reference_id=reference_id, reference_type=kind))
            if (kind, reference_id) in seen_reference:
                continue
            seen_reference.add((kind, reference_id))
            record.entities.append(Entity(entity_id=reference_id, entity_type=kind))
            if kind is EntityType.GENE:
                record.genes.append(Gene.from_reactome(
                    {**ref, "ensembl_gene": ensembl_of_uniprot.get(reference_id)}))
            elif kind is EntityType.DRUG:
                record.drugs.append(Drug.from_reactome({**ref, "schemaClass": obj["schemaClass"]}))
            else:
                record.compounds.append(Compound.from_reactome(ref))

        if unbridged:
            logger.info("%s: %d sequence entity symbol(s) with no UniProt in this "
                        "pathway, left unidentified: %s",
                        pid, len(unbridged), sorted(unbridged)[:5])

        record.entities.append(Entity(entity_id=pid, entity_type=EntityType.PATHWAY))
        for reaction in reactions:
            record.entities.append(
                Entity(entity_id=reaction.reaction_id, entity_type=EntityType.REACTION))
        return record

    # ---- layer 2 ---------------------------------------------------------

    def derive_gene_edges(self, graph: PathwayGraph) -> list[GeneEdge]:
        return collapse(derive(graph))

    # ---- factory dispatch -------------------------------------------------

    def data_model_entities(self, metadata: dict, type_name: EntityType) -> dict[str, Any]:
        handler = ReactomeEntityFactory.REGISTRY[type_name]
        return {handler["table"]: handler["builder"](list(metadata.values()))}
