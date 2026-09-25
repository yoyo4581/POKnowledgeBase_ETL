from bs4 import BeautifulSoup, Tag
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple
from typing import Literal, Iterator
from pathlib import Path
import logging

from src.models.kegg import *
from src.parsers.KEGGEntityFactory import KEGGEntityFactory


logger = logging.getLogger(__name__)

KEGG_PATH = Path("data/KGML/") # last modified tag


class _RateLimiter:
    """
    Caps the combined request rate across every thread sharing one instance.
    A ThreadPoolExecutor's worker count only bounds concurrency (how many
    requests are in flight at once) -- it doesn't stop those workers from
    firing in a burst. KEGG is a shared public API with no documented rate
    limit, so this is what actually keeps us from looking like abuse and
    getting soft-throttled, independent of pool size.
    """
    def __init__(self, max_per_second: float):
        self._min_interval = 1.0 / max_per_second
        self._lock = threading.Lock()
        self._last_call = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            sleep_for = self._min_interval - (now - self._last_call)
            if sleep_for > 0:
                time.sleep(sleep_for)
            self._last_call = time.monotonic()


def _build_kegg_session() -> requests.Session:
    """
    A session shared across requests so the retry-configured adapter (and
    its underlying connection pool) is reused rather than rebuilt per call.
    Retries cover exactly the failure modes KEGG has actually produced in
    this pipeline: connection-level errors (DNS blips, refused connections
    -- e.g. the NameResolutionError that killed a task outright with no
    retry at all) via `connect`, plus 429/5xx via `status_forcelist`.
    `respect_retry_after_header` honors a Retry-After KEGG sends on a 429
    instead of guessing a backoff.
    """
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _find_tag(parent: Tag | BeautifulSoup, name: str) -> Tag:
    """soup.find()/tag.find() can return a Tag, a NavigableString, or None.
    Narrows to Tag, raising if the required child element is absent."""
    found = parent.find(name)
    if not isinstance(found, Tag):
        raise ValueError(f"Expected a <{name}> tag, found none")
    return found


def _find_optional_tag(parent: Tag, name: str) -> Tag | None:
    found = parent.find(name)
    return found if isinstance(found, Tag) else None


def _require_attr(tag: Tag, attr: str) -> str:
    """tag.get()/tag[...] type as str | list[str] | None (bs4 supports multi-valued
    attributes). KGML attributes are always single strings; narrow and validate."""
    value = tag.get(attr)
    if not isinstance(value, str):
        raise ValueError(f"<{tag.name}> missing required string attribute '{attr}'")
    return value


KEGG_PREFIX_TO_ENTITY_TYPE: dict[str, EntityType] = {
    "cpd": EntityType.COMPOUND,
    "gl": EntityType.GLYCAN,
    "dr": EntityType.DRUG,
    "rn": EntityType.REACTION,
    "hsa": EntityType.GENE,
    "ko": EntityType.ORTHOLOG,
    "path": EntityType.PATHWAY,
}


def _parse_kegg_ref(token: str) -> tuple[str, EntityType]:
    """
    Splits one 'prefix:code' KEGG reference (e.g. 'gl:G00115') into its bare
    code and the EntityType its prefix denotes. KGML groups compounds,
    glycans, and drugs under the same <entry type="compound">/<substrate>/
    <product> tags, so the tag's own type isn't reliable for entity typing --
    the prefix on each individual name token is what actually distinguishes
    them. Uses partition (not split(":")[1]) so a value can't silently split
    wrong if it ever contains more than one colon.
    """
    prefix, sep, code = token.partition(":")
    if not sep:
        raise ValueError(f"KEGG reference {token!r} has no ':' prefix")
    entity_type = KEGG_PREFIX_TO_ENTITY_TYPE.get(prefix)
    if entity_type is None:
        raise ValueError(f"Unknown KEGG prefix {prefix!r} in reference {token!r}")
    return code, entity_type


def _parse_kegg_refs(raw: str) -> list[tuple[str, EntityType]]:
    """
    Splits a whitespace-separated list of KEGG references -- e.g. a KGML
    entry's `name` attribute can hold several cross-referenced ids for one
    node ('cpd:C00022 gl:G00115') -- into (code, EntityType) pairs. Always
    split on whitespace before touching any individual token; taking the raw
    attribute value whole (or blindly stripping one hardcoded prefix from it)
    silently produces a mangled multi-token string like 'dr:D00195 cpd:C06174'
    instead of two separate references.
    """
    return [_parse_kegg_ref(tok) for tok in raw.split()]


class KEGGBlockedError(Exception):
    """
    Raised when KEGG returns 403 -- observed in practice to be an IP-level
    block, not a per-request rejection: a real run got 403 on a batch of
    valid gene ids, then kept getting 403 on completely unrelated glycan
    batches immediately after, with nothing wrong with any of the requests
    themselves. Deliberately NOT a subclass of requests.RequestException --
    callers that catch-and-continue on ordinary request failures (a bad id,
    a transient timeout) must not catch this one the same way. Continuing
    to send requests during an active block window is what turns a short
    block into a long one, so every caller of _get must let this propagate
    and stop, not log it and move on to the next batch/table/pathway.
    """
    pass


class KEGG_State:
    # Shared by every KEGG_State instance -- the connection pool inside the
    # session and the rate limiter's "last call" clock only mean something
    # if every caller (including concurrent ThreadPoolExecutor workers, each
    # of which may construct its own KEGG_ETL()/KEGG_State()) goes through
    # the same session and the same clock.
    #
    # KEGG's documented ceiling (https://www.kegg.jp/kegg/rest/) is "up to
    # 3 times per second, otherwise your access will be blocked". A real run
    # got blocked while nominally sitting at exactly 3/sec -- but that was
    # while base_url was still http://, which KEGG's BigIP 301s to https://;
    # requests followed that redirect as a second real request per call,
    # invisible to this limiter, so actual traffic was ~6/sec: double their
    # stated limit despite the code believing it was compliant. Now that
    # base_url goes straight to https:// (no redirect, one real request per
    # call), 2/sec keeps a margin under the documented 3/sec ceiling instead
    # of sitting right on it.
    _session = _build_kegg_session()
    _rate_limiter = _RateLimiter(max_per_second=2)

    def __init__(self):
        # KEGG's BigIP now 301s every http:// request to https:// -- requests
        # follows that redirect as a second real HTTP call made internally
        # by urllib3, which never passes back through _rate_limiter.wait().
        # That silently doubled real request volume against KEGG's
        # infrastructure and sent it as an unthrottled back-to-back pair
        # (rate-limited request, then an instant unthrottled follow-up) --
        # exactly the kind of burst pattern WAF abuse detection looks for.
        # Going straight to https:// removes the redirect hop entirely.
        self.base_url = "https://rest.kegg.jp"

    def _get(self, url: str, timeout: float = 30.0) -> requests.Response:
        self._rate_limiter.wait()
        response = self._session.get(url, timeout=timeout)
        if response.status_code == 403:
            raise KEGGBlockedError(
                f"KEGG returned 403 for {url} -- treat as an IP-level block, not a "
                "retryable/skippable failure. Stop issuing further requests."
            )
        return response

    def fetch_brite_hierarchy(self, brite_id: str = "br08901")->dict:
        url = f"{self.base_url}/get/br:{brite_id}/json"
        response = self._get(url)
        if not response.ok:
            logger.error("Failed to fetch BRITE hierarchy %s - status %d", brite_id, response.status_code)
            return {}
        return response.json()


    def fetch_pathway_kgml(self, pathway_code: str):
        url = f"{self.base_url}/get/{pathway_code}/kgml"
        response = self._get(url)
        if not response.ok:
            raise Exception(f"Failed to fetch data for {pathway_code}")
        return response.content

    def fetch_pathway_ids(self):
        url = f"{self.base_url}/list/pathway/hsa"
        response = self._get(url)
        pathway_data = []
        if response.ok:
            logger.info("Successfilly fetched KEGG pathway IDs.")
        else:
            logger.error(f"Failed to fetch KEGG pathway IDs - Status: {response.status_code}")
        return response.text
    

    def download_kgml_temp_file(self, pathway_id: str, file: bytes):
        KEGG_PATH.mkdir(parents=True, exist_ok=True)

        file_path = KEGG_PATH / f"{pathway_id}.xml"
        tmp_path = file_path.with_suffix(".xml.tmp")

        tmp_path.write_bytes(file)
        tmp_path.replace(file_path)


    def compute_kgml_hash(self, xml_content: bytes)->str:
        import hashlib
        return hashlib.md5(xml_content).hexdigest()

    def read_kgml_temp_file(self, pathway_code: str)->bytes:
        return (KEGG_PATH / f"{pathway_code}.xml").read_bytes()

    def cleanup_kgml_temp_files(self):
        import shutil
        if KEGG_PATH.exists():
            shutil.rmtree(KEGG_PATH)


class KEGG_ETL:
    def __init__(self):
        self.kegg_state = KEGG_State()

    def parse_flatten_brite(self, node: dict, parent_name: str | None=None)-> Iterator[KEGG_CLASS]:
        """Parses and models kegg pathway class pairs, depth-first."""
        import re

        name = re.sub(r"^\s*\d+\s*", "", node['name'])
        yield KEGG_CLASS.from_dict({"name":name, "parent_name": parent_name})

        for child in node.get("children", []):
            yield from self.parse_flatten_brite(child, parent_name=name)

    def parse_pathway_ids(self, pathway_text:str)->Iterator[PathwayIds]:
        """Parses pathway Ids text from kegg and models to PathwayIds objects"""
        for line in pathway_text.strip().split("\n"):
            if not line.strip():
                continue
            pathway_id, description = line.split("\t", 1)
            yield PathwayIds(pathway_id, description)

    # Gates which KGML <entry> tags get processed at all. Not used to derive
    # entity type any more -- KEGG lumps compounds/glycans/drugs under
    # type="compound" (and possibly type="drug"/"glycan" on some diagrams),
    # so the real type comes from each name token's own prefix instead; see
    # _parse_kegg_ref.
    KGML_ENTRY_TYPES = {"gene", "ortholog", "compound", "drug", "glycan"}

    def _parse_kgml_entries(self, soup: BeautifulSoup, pathway_id: str) -> tuple[dict[str, KGMLEntry], list[Entity]]:
        entry_map: dict[str, KGMLEntry] = {}
        reaction_entities: list[Entity] = []
        for entry in soup.find_all("entry"):
            entity_type = entry.get("type")
            if entity_type not in self.KGML_ENTRY_TYPES:
                continue

            entry_id = _require_attr(entry, "id")
            refs = _parse_kegg_refs(_require_attr(entry, "name"))
            kgml_entry = KGMLEntry(
                entities=[Entity(code, etype) for code, etype in refs],
                entity_path_mem=[EntityPathMem(code, pathway_id) for code, etype in refs],
            )

            if entry.has_attr("reaction"):
                # This entry's own identity is whatever `name` says (a gene,
                # ortholog, ...); `reaction` just cross-references which
                # reaction(s) it's involved in for the diagram. Keep those out
                # of kgml_entry.entities -- both _parse_kgml_reaction_participants
                # (catalyst derivation) and _parse_kgml_relations (interaction
                # endpoints) read that list, and a reaction id showing up there
                # gets treated as if it were the entry's own identity, producing
                # e.g. a reaction "catalyzing" itself or another reaction.
                # Tracked separately purely so it still lands in `entities`.
                reaction_entities.extend(
                    Entity(code, etype) for code, etype in _parse_kegg_refs(_require_attr(entry, "reaction"))
                )

            entry_map[entry_id] = kgml_entry
        return entry_map, reaction_entities

    def _parse_kgml_relations(self, soup: BeautifulSoup, entry_map: dict[str, KGMLEntry], pathway_id: str) -> list[Interaction]:
        interactions = []
        for rel in soup.find_all("relation"):
            source = _require_attr(rel, "entry1")
            target = _require_attr(rel, "entry2")
            if source not in entry_map or target not in entry_map:
                continue

            sub_type = _find_optional_tag(rel, "subtype")
            relation_type = _require_attr(sub_type, "name") if sub_type is not None else "undefined"
            for source_syn in entry_map[source].entities:
                for target_syn in entry_map[target].entities:
                    interactions.append(Interaction(source_id=source_syn.entity_id,
                                                    target_id=target_syn.entity_id,
                                                    relation_type=relation_type,
                                                    pathway_id=pathway_id))
        return interactions

    def _parse_kgml_reaction_participants(
        self, soup: BeautifulSoup, entry_map: dict[str, KGMLEntry], pathway_id: str
    ) -> tuple[list[ReactionP], list[Entity]]:
        """
        Unlike relations, substrate/product compounds come straight off the
        <reaction> tag's own name attribute rather than an <entry> lookup, so
        they aren't guaranteed to already be in entry_map -- KEGG frequently
        omits off-diagram cofactors (water, ATP/ADP, NAD+/NADH, ...) from the
        entry list even though a reaction still references them. Any such
        compound is synthesized as its own Entity so it lands in `entities`
        before reaction_participants is staged, keeping the FK satisfied.
        """
        known_entity_ids = {
            entity.entity_id for kgml_entry in entry_map.values() for entity in kgml_entry.entities
        }
        reaction_participants = []
        synthesized_entities: dict[str, Entity] = {}

        def _track_compound(compound_id: str, entity_type: EntityType) -> None:
            if compound_id not in known_entity_ids and compound_id not in synthesized_entities:
                synthesized_entities[compound_id] = Entity(compound_id, entity_type)

        for rx in soup.find_all("reaction"):
            rx_entry_id = _require_attr(rx, "id")
            if rx_entry_id not in entry_map:
                continue

            reaction_ids = [code for code, _ in _parse_kegg_refs(_require_attr(rx, "name"))]
            substrates = rx.find_all("substrate")
            products = rx.find_all("product")
            for reaction_id in reaction_ids:
                for catalyst in entry_map[rx_entry_id].entities:
                    reaction_participants.append(ReactionP(
                        reaction_id=reaction_id,
                        entity_id=catalyst.entity_id,
                        role="catalyst",
                        pathway_id=pathway_id
                    ))
                for substrate in substrates:
                    for entity_id, entity_type in _parse_kegg_refs(_require_attr(substrate, "name")):
                        _track_compound(entity_id, entity_type)
                        reaction_participants.append(ReactionP(
                            reaction_id=reaction_id,
                            entity_id=entity_id,
                            role="substrate",
                            pathway_id=pathway_id
                        ))
                for product in products:
                    for entity_id, entity_type in _parse_kegg_refs(_require_attr(product, "name")):
                        _track_compound(entity_id, entity_type)
                        reaction_participants.append(ReactionP(
                            reaction_id=reaction_id,
                            entity_id=entity_id,
                            role="product",
                            pathway_id=pathway_id
                        ))
        return reaction_participants, list(synthesized_entities.values())

    def _parse_kgml_to_entry_map(self, xml_content)-> PathwayKGMLRecord:
        soup = BeautifulSoup(xml_content, "xml")

        pathway_entry = _find_tag(soup, "pathway")
        pathway_id = _require_attr(pathway_entry, "name").split("path:")[1]
        pathway = Pathway(pathway_id= pathway_id,
                        description= _require_attr(pathway_entry, "title"))
        pathway_entity = Entity(entity_id=pathway_id, entity_type=EntityType.PATHWAY)

        entry_map, reaction_entities = self._parse_kgml_entries(soup, pathway_id)
        interactions = self._parse_kgml_relations(soup, entry_map, pathway_id)
        reaction_participants, synthesized_entities = self._parse_kgml_reaction_participants(soup, entry_map, pathway_id)

        entities = (
            [entity for kgml_entry in entry_map.values() for entity in kgml_entry.entities]
            + [pathway_entity]
            + reaction_entities
            + synthesized_entities
        )
        entity_path_mem = [epm for kgml_entry in entry_map.values() for epm in kgml_entry.entity_path_mem]
        return PathwayKGMLRecord(pathway, entities, entity_path_mem, interactions, reaction_participants)


    def parse_kgml_structure(self, pathway_id):
        xml_content = self.kegg_state.read_kgml_temp_file(pathway_id)
        yield self._parse_kgml_to_entry_map(xml_content)

    def parse_kegg_flatfile(self, entry_text: str) -> dict:
        """
        General parser for KEGG flat files.
        Handles multiline fields, nested sections, and grouped entries.
        """
        parsed = {}
        current_key = None
        current_value_lines = []

        def store_current_key():
            if current_key:
                value = "\n".join(current_value_lines).strip()
                if current_key in parsed:
                    if isinstance(parsed[current_key], list):
                        parsed[current_key].append(value)
                    else:
                        parsed[current_key] = [parsed[current_key], value]
                else:
                    parsed[current_key] = value

        for line in entry_text.splitlines():
            if not line.strip():
                continue

            key = line[0:12].strip()
            value = line[12:].rstrip()

            if key:
                store_current_key()
                current_key = key
                current_value_lines = [value]
            else:
                current_value_lines.append(value)

        store_current_key()  # Store the last key-value pair
        return parsed


    
    def parse_kegg_txt(self, codes: list[str], dtype: str)->dict:
        """
        Fetches and parses KEGG flat file data in a single batch.
        Returns a dict mapping KEGG entry codes to parsed metadata.
        Assumes codes are already batched upstream.
        """
        def preparse(send_url, code_str, expected_count):
            response = self.kegg_state._get(send_url)
            parsed_data = []

            if response.ok:
                entries = response.text.strip().split("///")
                for entry_text in entries:
                    entry_text = entry_text.strip()
                    if not entry_text:
                        continue
                    parsed = self.parse_kegg_flatfile(entry_text)
                    parsed_data.append(parsed)
                if len(parsed_data) < expected_count:
                    # KEGG's /get/ doesn't error on a partial match -- it
                    # returns 200 with fewer entries than requested (e.g. a
                    # withdrawn/obsolete id, or a batch over its per-request
                    # cap). Silently accepting that count mismatch is exactly
                    # how the batch-size-20 bug above dropped entries with no
                    # trace, so flag it instead of trusting response.ok alone.
                    logger.warning(
                        f"KEGG returned {len(parsed_data)} entries for {expected_count} requested "
                        f"codes: {code_str}"
                    )
            else:
                logger.info(f"Failed to fetch batch: {code_str} — Status: {response.status_code}")

            return parsed_data
        def get_prefix(code: str, dtype: str) -> str:
            # entity_type now comes from each KGML token's own prefix (see
            # _parse_kegg_ref), so compound/glycan/drug are already separated
            # by the time entities reach here -- no need to re-derive it from
            # the code's leading letter.
            prefixes = {
                "compound": "cpd:",
                "glycan": "gl:",
                "drug": "dr:",
                "reaction": "",
                "pathway": "path:",
                "gene": "hsa:",  # Or make species dynamic if needed
                "ortholog": "ko:",
            }
            if dtype not in prefixes:
                raise ValueError(f"Unsupported dtype: {dtype}")
            return prefixes[dtype]

        text_url = self.kegg_state.base_url + "/get/"
        metadata = {}

        # KEGG's /get/ silently caps at 10 entries per request -- asking for
        # more doesn't error, it just returns 200 with only the first 10
        # entries, dropping the rest with no trace. Confirmed against the
        # live API: an 11-code request came back with exactly 10 entries.
        KEGG_GET_BATCH_LIMIT = 10
        # Bounds concurrency; the actual request pace is capped separately
        # by kegg_state's shared rate limiter (see _get), so raising this
        # widens how many batches can be *queued up* waiting on that limiter
        # rather than how fast requests actually leave the machine. Kept low
        # to also limit simultaneous open connections to KEGG, which a
        # abuse-detecting proxy can flag independently of request rate.
        MAX_WORKERS = 3

        batches = [
            codes[i : i + KEGG_GET_BATCH_LIMIT]
            for i in range(0, len(codes), KEGG_GET_BATCH_LIMIT)
        ]

        def fetch_batch(batch_codes: list[str]) -> list[dict]:
            code_str = "+".join(f"{get_prefix(code, dtype)}{code}" for code in batch_codes)
            send_url = text_url + code_str
            return preparse(send_url, code_str, len(batch_codes))

        completed = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_batch, batch): batch for batch in batches}
            for future in as_completed(futures):
                completed += 1
                try:
                    parsed_entries = future.result()
                except KEGGBlockedError:
                    # Do NOT treat like an ordinary failed batch. Cancel
                    # every not-yet-started future (already-running ones
                    # can't be interrupted, but this stops queueing more)
                    # and abort the whole call -- annotation for every
                    # other entity type must stop too, not just this dtype.
                    logger.error(
                        f"KEGG block detected on {dtype} batch {futures[future]} -- "
                        f"aborting remaining {len(futures) - completed} batch(es)."
                    )
                    executor.shutdown(cancel_futures=True)
                    raise
                except requests.exceptions.RequestException as e:
                    # Retries (see _build_kegg_session) are already exhausted
                    # by this point -- one persistently unreachable batch
                    # shouldn't take down annotation for every other batch
                    # that's already succeeded or still in flight.
                    logger.error(f"Batch {futures[future]} failed after retries: {e}")
                    continue

                for parsed in parsed_entries:
                    entry_id = parsed.get("ENTRY", "").split()[0]
                    if entry_id == "":
                        logger.info(f"Missing ENTRY in parsed record: {parsed}")

                    if entry_id:
                        metadata[entry_id] = parsed

                if completed % 25 == 0:
                    logger.info(f"Processing {dtype}: {completed}/{len(batches)} batches")

        entity_type = EntityType(dtype)
        modeled_entities = self.data_model_entities(metadata, entity_type)
        return modeled_entities


    def data_model_entities(self, metadata: dict, type_name: EntityType)->dict[str, Any]:

        handler = KEGGEntityFactory.REGISTRY[type_name]
        table = handler["table"]
        builder = handler["builder"]

        return {
            table: builder(list(metadata.values()))
        }
