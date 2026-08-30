from bs4 import BeautifulSoup, Tag
import requests
from typing import List, Tuple
from typing import Literal, Iterator
from pathlib import Path
import logging

from src.models.kegg import *
from src.parsers.KEGGEntityFactory import KEGGEntityFactory


logger = logging.getLogger(__name__)

KEGG_PATH = Path("data/KGML/") # last modified tag


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

class KEGG_State:
    def __init__(self):
        self.base_url = "http://rest.kegg.jp"

    def fetch_brite_hierarchy(self, brite_id: str = "br08901")->dict:
        url = f"{self.base_url}/get/br:{brite_id}/json"
        response = requests.get(url)
        if not response.ok:
            logger.error("Failed to fetch BRITE hierarchy %s - status %d", brite_id, response.status_code)
            return {}
        return response.json()
    
    
    def fetch_pathway_kgml(self, pathway_code: str):
        url = f"http://rest.kegg.jp/get/{pathway_code}/kgml"
        response = requests.get(url)
        if not response.ok:
            raise Exception(f"Failed to fetch data for {pathway_code}")
        return response.content
    
    def fetch_pathway_ids(self):
        url = f"{self.base_url}/list/pathway/hsa"
        response = requests.get(url)
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

    KGML_ENTRY_TYPES = {"gene", "ortholog", "compound"}

    def _parse_kgml_entries(self, soup: BeautifulSoup, pathway_id: str) -> dict[str, KGMLEntry]:
        entry_map: dict[str, KGMLEntry] = {}
        for entry in soup.find_all("entry"):
            entity_type = entry.get("type")
            if entity_type not in self.KGML_ENTRY_TYPES:
                continue

            entry_id = _require_attr(entry, "id")
            entity_ids = [name.split(":")[1] for name in _require_attr(entry, "name").split()]
            kgml_entry = KGMLEntry(
                entities=[Entity(entity_id, EntityType(entity_type)) for entity_id in entity_ids],
                entity_path_mem=[EntityPathMem(entity_id, pathway_id) for entity_id in entity_ids],
            )

            if entry.has_attr("reaction"):
                reaction_id = _require_attr(entry, "reaction").split(":")[1]
                kgml_entry.entities.append(Entity(reaction_id, EntityType.REACTION))

            entry_map[entry_id] = kgml_entry
        return entry_map

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

        def _track_compound(compound_id: str) -> None:
            if compound_id not in known_entity_ids and compound_id not in synthesized_entities:
                synthesized_entities[compound_id] = Entity(compound_id, EntityType.COMPOUND)

        for rx in soup.find_all("reaction"):
            rx_entry_id = _require_attr(rx, "id")
            if rx_entry_id not in entry_map:
                continue

            reaction_ids = [name.removeprefix("rn:") for name in _require_attr(rx, "name").split()]
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
                    entity_id = _require_attr(substrate, "name").removeprefix("cpd:")
                    _track_compound(entity_id)
                    reaction_participants.append(ReactionP(
                        reaction_id=reaction_id,
                        entity_id=entity_id,
                        role="substrate",
                        pathway_id=pathway_id
                    ))
                for product in products:
                    entity_id = _require_attr(product, "name").removeprefix("cpd:")
                    _track_compound(entity_id)
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

        entry_map = self._parse_kgml_entries(soup, pathway_id)
        interactions = self._parse_kgml_relations(soup, entry_map, pathway_id)
        reaction_participants, synthesized_entities = self._parse_kgml_reaction_participants(soup, entry_map, pathway_id)

        entities = [entity for kgml_entry in entry_map.values() for entity in kgml_entry.entities] + [pathway_entity] + synthesized_entities
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
        def preparse(send_url, code_str):
            response = requests.get(send_url)
            parsed_data = []

            if response.ok:
                entries = response.text.strip().split("///")
                for entry_text in entries:
                    entry_text = entry_text.strip()
                    if not entry_text:
                        continue
                    parsed = self.parse_kegg_flatfile(entry_text)
                    parsed_data.append(parsed)
            else:
                logger.info(f"Failed to fetch batch: {code_str} — Status: {response.status_code}")

            return parsed_data
        def get_prefix(code: str, dtype: str) -> str:
            if dtype == "compound":
                if code.startswith("C"):
                    return "cpd:"
                elif code.startswith("G"):
                    return "gl:"
                elif code.startswith("D"):
                    return ""
                else:
                    raise ValueError(f"Unknown compound-like code: {code}")
            elif dtype == "reaction":
                return ""
            elif dtype == "pathway":
                return "path:"
            elif dtype == "gene":
                return "hsa:"  # Or make species dynamic if needed
            elif dtype == "ortholog":
                return "ko:"
            else:
                raise ValueError(f"Unsupported dtype: {dtype}")

        text_url = self.kegg_state.base_url + "/get/"
        metadata = {}
        
        for batch_start in range(0, len(codes), 20):
            batch_codes = codes[batch_start : batch_start+20]

            # Compose batch URL with correct per-code prefix
            code_str = "+".join(f"{get_prefix(code, dtype)}{code}" for code in batch_codes)
            send_url = text_url + code_str
            parsed_entries = preparse(send_url, code_str)

            # Map each parsed entry to its KEGG ID
            for parsed in parsed_entries:
                entry_id = parsed.get("ENTRY", "").split()[0]
                if entry_id=="":
                    logger.info(f"Missing ENTRY in parsed record: {parsed}")

                if entry_id:
                    metadata[entry_id] = parsed

            if batch_start % 250==0:
                logger.info(f'Processing {dtype} from {batch_start}/{len(codes)}')

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
