from bs4 import BeautifulSoup
import requests
from typing import List, Tuple
from local_db.schema import *
from utils.colored_text import *
from typing import Literal
from pathlib import Path

from KEGG.KEGGEntityFactory import KEGGEntityFactory

KEGG_PATH = Path("data/KGML/") # last modified tag

class KEGG_State:
    def __init__(self):
        self.base_url = "http://rest.kegg.jp"
    
    def fetch_pathway_ids(self) ->List[PathwayIds] | None:
        '''
        Retrieves pathway_id and description of all human specific pathways.
        '''
        url = f"{self.base_url}/list/pathway/hsa"
        response = requests.get(url)
        pathway_data = []
        if response.ok:
            print(f"{GREEN}Successfully fetched KEGG pathway IDs.{RESET}")
            for line in response.text.strip().split("\n"):
                entry, description = line.split("\t")
                pathway_id = entry
                pathway_data.append(PathwayIds(pathway_id, description))

            self.seen_pathways = set([code for code, desc in pathway_data])
            return pathway_data
        else:
            print(f"{RED}Failed to fetch KEGG pathway IDs — Status: {response.status_code}{RESET}")
            return None
    
    def fetch_pathway_kgml(self, pathway_code):
        url = f"http://rest.kegg.jp/get/{pathway_code}/kgml"
        response = requests.get(url)

        if not response.ok:
            raise Exception(f"Failed to fetch data for {pathway_code}")

        return response.content

    def download_kgml_temp_files(self, pathway_ids: list)->bool:
        if KEGG_PATH.exists():
            return True

        KEGG_PATH.mkdir(parents=True, exist_ok=True)

        for pathway_code in pathway_ids:
            file_path = KEGG_PATH/ f"{pathway_code}.xml"
            if file_path.exists():
                continue
            xml_content = self.fetch_pathway_kgml(pathway_code)
            file_path.write_bytes(xml_content)

        return True

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
    '''
    Fetching pathway data, this includes Pathway KGML, and every Entity file, works collaboratively with child parsers:
    1. Downloads current XML files from KEGG for every Pathway ID.
    2. Calls to get info on compounds, drugs, and enzymes.
    '''
    def __init__(self):
        self.kegg_state = KEGG_State()
    
    def parse_kgml_nested_pathways(self, xml_content):
        soup = BeautifulSoup(xml_content, "xml")
        pathway_data = []
        for entry in soup.find_all("entry"):
            if entry.get("type") == "map":
                pathway_id = entry.get("name", "").removeprefix("path:")
                graphics = entry.find("graphics")
                if graphics:
                    graphics_name = graphics.get("name")
                    pathway_data.append(PathwayIds(pathway_id, graphics_name))
        
        return pathway_data
    
    def parse_kgml_to_entry_map(self, xml_content):
        soup = BeautifulSoup(xml_content, "xml")

        entry_map = {}
        for entry in soup.find_all("entry"):
            
            if entry.get("type") != "map": # skip any pathways
                
                entry_id = entry.get("id")
                entry_map[entry_id] = {
                    "entities": entry.get("name", "").split(),
                    "reaction": entry.get("reaction", "").split() if entry.get("reaction") else [],
                    "entity_type": entry.get("type")
                }

        for reaction in soup.find_all("reaction"):
            rid = reaction.get("id")
            entry = entry_map.get(rid, {})
            entry["substrates"] = [s.get("name") for s in reaction.find_all("substrate")]
            entry["products"] = [p.get("name") for p in reaction.find_all("product")]
            entry["reaction_type"] = reaction.get("type")
            entry_map[rid] = entry  # in case it wasn’t already in the map

        for rel in soup.find_all("relation"):
            src = rel.get("entry1")
            tgt = rel.get("entry2")
            if src in entry_map and tgt in entry_map:
                entry_map[src].setdefault("relations", []).append(entry_map[tgt]['entities'])
                subtype = rel.find("subtype")
                entry_map[src].setdefault("relation_types", []).append(subtype.get("name") if subtype else "unknown")
        
        return entry_map
    
    def parse_map_sql(self, entry_map, pathway_id):

        entity_table = []
        entity_path_mem_table = []
        interactions_table = []
        reaction_table = []
        reaction_ptable = []
        seen_reactions = set()

        def clean_id(identifier):
            return identifier.split(":")[1] if ":" in identifier else identifier

        for entry_id, entry in entry_map.items():
            entity_type, entities, reaction_ids, substrates, products, relation_targets, relation_types, reaction_type = [
                entry.get(key, default) for key, default in kgml_defaults.items()
            ]

            for entity in entities:
                entity_code = clean_id(entity)
                entity_table.append(Entity(entity_code, "pathway" if entity_type == "map" else entity_type))
                entity_path_mem_table.append(EntityPathMem(pathway_id, entity_code))
                # Handle interactions
                for idx, targets in enumerate(relation_targets):
                    for target in targets:
                        interactions_table.append(Interaction(entity_code, clean_id(target), relation_types[idx], pathway_id))

                # Handle reaction participants
                for reaction_id_raw in reaction_ids or []:
                    reaction_id = clean_id(reaction_id_raw)

                    if reaction_id not in seen_reactions:
                        reaction_table.append(Reaction(reaction_id, None, None, None, None, reaction_type, pathway_id))
                        seen_reactions.add(reaction_id)

                    if entity_type == "ortholog":
                        reaction_ptable.append(ReactionP(reaction_id, entity_code, entity_type,pathway_id))
                        continue

                    for s in substrates:
                        reaction_ptable.append(ReactionP(reaction_id, clean_id(s), "substrate", pathway_id))
                    for p in products:
                        reaction_ptable.append(ReactionP(reaction_id, clean_id(p), "product", pathway_id))


        return entity_table, entity_path_mem_table, interactions_table, reaction_table, reaction_ptable
    
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


    
    def parse_kegg_txt(self, codes: list[str], dtype: str):
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
                print(f"{RED}Failed to fetch batch: {code_str} — Status: {response.status_code}{RESET}")

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
                    print(f"{RED}Missing ENTRY in parsed record: {parsed}{RESET}")

                if entry_id:
                    metadata[entry_id] = parsed

            if batch_start % 250==0:
                print(f'{GREEN} Processing {dtype} from {batch_start}/{len(codes)}')

        entity_type = EntityType(dtype)
        modeled_entities = self.data_model_entities(metadata, entity_type)
        return modeled_entities


    def data_model_entities(self, metadata: dict, type_name: EntityType):

        handler = KEGGEntityFactory.REGISTRY[type_name]
        table = handler["table"]
        builder = handler["builder"]

        return {
            table: builder(list(metadata.values()))
        }


    def build_reaction_table(self, reaction_data: List[Reaction]):
        """
        Data table of reactions.
        """
        def fill_missing_fields(objects: List[Reaction], source_objects: List[Reaction], key_field: str)->List[Reaction]:
            source_lookup = {getattr(s, key_field): s for s in source_objects}
            filled = []
            for obj in objects:
                source = source_lookup.get(getattr(obj, key_field))
                if source is None:
                    filled.append(obj)
                    continue

                updates = {
                    field: getattr(source, field)
                    for field in obj._fields
                    if getattr(obj, field) is None and getattr(source, field, None) is not None
                }
                filled.append(obj._replace(**updates) if updates else obj)
            return filled
        
        codes = [reaction.reaction_id for reaction in reaction_data]
        reaction_from_flat = self.parse_kegg_txt(codes, 'reaction')

        reaction_list = reaction_from_flat["reactions"]

        return fill_missing_fields(reaction_list, reaction_data, key_field="reaction_id")



    def build_entity_tables(self, entity_data: List[Entity]):
        """
        List of data tables of genes, compounds, orthologs, and pathways.
        """
        from itertools import groupby
        from operator import attrgetter

        # Step 1: Sort by type
        objects_sorted = sorted(entity_data, key=attrgetter("type"))

        # Step 2: Group by type
        grouped = {
            type_key: list(group)
            for type_key, group in groupby(objects_sorted, key=attrgetter("type"))
        }

        all_tables = {}
        # Display grouped objects
        for type_name, entry_list in grouped.items():
            print(type_name, len(entry_list))

            if type_name in {'gene', 'compound', 'ortholog', 'pathways'}: #Just look at entries with data types that require annotation.
                codes = [entry.name for entry in entry_list] # Get all codes in a given type
                
                all_tables |= self.parse_kegg_txt(codes, type_name)

        return all_tables


