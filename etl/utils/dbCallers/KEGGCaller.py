from etl.utils.colored_text import RED, GREEN, RESET
from bs4 import BeautifulSoup
import requests
from typing import List, Tuple
from ..schema_def import PathwayIds, kgml_defaults, Entity, Interaction, Reaction, ReactionP, GeneData, CompoundData, OrthoData

class KEGGCaller:
    '''
    Fetching pathway data, this includes Pathway KGML, and every Entity file, works collaboratively with child parsers:
    1. Downloads current XML files from KEGG for every Pathway ID.
    2. Calls to get info on compounds, drugs, and enzymes.
    '''
    def __init__(self):
        self.base_url = "http://rest.kegg.jp"
    
    def fetch_pathway_ids(self):
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
    
    def parse_kgml_to_entry_map(self, xml_content):
        soup = BeautifulSoup(xml_content, "xml")

        entry_map = {}
        for entry in soup.find_all("entry"):
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


class KEGGSQLBuilder(KEGGCaller):

    def __init__(self):
        super().__init__()
        self.pathway_count = 0

    def parse_map_sql(self, entry_map, pathway_id):

        entity_table = []
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

                # Handle interactions
                for idx, targets in enumerate(relation_targets):
                    for target in targets:
                        interactions_table.append(Interaction(entity_code, clean_id(target), relation_types[idx], pathway_id))


                # Handle reaction participants
                for reaction_id_raw in reaction_ids or []:
                    reaction_id = clean_id(reaction_id_raw)

                    if entity_type == "ortholog":
                        reaction_ptable.append(ReactionP(reaction_id, entity_code, entity_type))
                        continue

                    for s in substrates:
                        reaction_ptable.append(ReactionP(reaction_id, clean_id(s), "substrate"))
                    for p in products:
                        reaction_ptable.append(ReactionP(reaction_id, clean_id(p), "product"))

                    if reaction_id not in seen_reactions:
                        reaction_table.append(Reaction(reaction_id, reaction_type, pathway_id))
                        seen_reactions.add(reaction_id)

        return entity_table, interactions_table, reaction_table, reaction_ptable
    
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
                elif code .startswith("D"):
                    return ""
                else:
                    raise ValueError(f"Unknown compound-like code: {code}")
            elif dtype == "pathway":
                return "path:"
            elif dtype == "gene":
                return "hsa:"  # Or make species dynamic if needed
            elif dtype == "ortholog":
                return "ko:"
            else:
                raise ValueError(f"Unsupported dtype: {dtype}")


        text_url = self.base_url + "/get/"
        metadata = {}

        # Compose batch URL with correct per-code prefix
        code_str = "+".join(f"{get_prefix(code, dtype)}{code}" for code in codes)
        send_url = text_url + code_str
        parsed_entries = preparse(send_url, code_str)

        # Map each parsed entry to its KEGG ID
        for parsed in parsed_entries:
            entry_id = parsed.get("ENTRY", "").split()[0]
            if entry_id=="":
                print(f"{RED}Missing ENTRY in parsed record: {parsed}{RESET}")

            if entry_id:
                metadata[entry_id] = parsed

        return metadata


    def build_sql_pathway_tables(self, pathway_data_batch: List[Tuple[str, str]]):
        '''
        Batch process data, uses the pathway ids sent in batches, iterates through each pathway,
        retrieves sql table formatted list from parse_map_sql, occupies batch_tables,
        if new pathways are discovered that have never been seen, then extend the original batched pathway codes,
        so that the new pathways are also explored.

        Then based on the entities, explore each entity based on its type.
        This part should be sent to another function.
        '''
        pathway_data = pathway_data_batch.copy()  # list for indexable, growing queue

        p_idx = 0
        entity_table, interactions_table, reaction_table, reaction_ptable = [], [], [], []
        while p_idx < len(pathway_data_batch):
                
            code = pathway_data_batch[p_idx].pathway_id
            xml = self.fetch_pathway_kgml(code)
            entry_map = self.parse_kgml_to_entry_map(xml)

            entity_list, intrx_list, rx_list, rx_pt_list = self.parse_map_sql(entry_map, code)
            entity_table += entity_list
            interactions_table += intrx_list
            reaction_table += rx_list
            reaction_ptable += rx_pt_list

            # Add new referenced pathways
            new_codes = [
                entity.name
                for entity in entity_list
                if entity.type == 'pathway' and entity.name not in self.seen_pathways
            ]
            
            if len(new_codes)>0:
                print(f"Added {len(new_codes)} new pathways")
                metadata = self.parse_kegg_txt(new_codes, 'pathway')
                pathway_data.extend([PathwayIds(code, metadata[code]["NAME"]) for code in new_codes if code in metadata])
        

            p_idx += 1
            self.pathway_count += 1
            print(f"Processed {self.pathway_count}/{len(self.seen_pathways)}: {code}")

        b_table = {
            'entities': entity_table,
            'interactions': interactions_table,
            'reactions': reaction_table,
            'reaction_participants': reaction_ptable,
            'PathwayIds': pathway_data
        }

        return b_table  # Or whatever data structure you’re building
    
    def build_entity_tables(self, entity_data):
        from itertools import groupby
        from operator import attrgetter

        # Step 1: Sort by type
        objects_sorted = sorted(entity_data, key=attrgetter("type"))

        # Step 2: Group by type
        grouped = {
            type_key: list(group)
            for type_key, group in groupby(objects_sorted, key=attrgetter("type"))
        }

        gene_table, compound_table, ortho_table = [], [], []
        # Display grouped objects
        for type_name, items in grouped.items():
            print(type_name, len(items))
            if type_name != "pathways" and type_name in {'gene', 'compound', 'ortholog'}: #already parsed pathways
                codes = [item.name for item in items]
                for batch_start in range(0, len(codes), 20):
                    batch_codes = codes[batch_start : batch_start+20]
                    metadata = self.parse_kegg_txt(batch_codes, type_name)
                    for code, data in metadata.items():
                        if type_name == "gene":
                            # Ensure SYMBOL exists
                            if 'SYMBOL' not in data:
                                data['SYMBOL'] = 'LOC' + data['ENTRY']

                            symbols = [s.strip() for s in data['SYMBOL'].split(',')]
                            main_symbol = symbols[0]
                            aliases = ', '.join(symbols[1:]) if len(symbols) > 1 else ''

                            gene_table.append(
                                GeneData(
                                    main_symbol,
                                    int(data['ENTRY'].split()[0]),
                                    data['NAME'].replace("(RefSeq)", "").strip(),
                                    aliases))
                            
                        elif type_name == "compound":
                            weight = 0
                            if 'MOL_WEIGHT' in data:
                                weight = round(float(data['MOL_WEIGHT']), 2)
                            elif 'MASS' in data:
                                weight = round(float(data['MASS'].split()[0]), 2)

                            formula = 'NA'
                            if 'FORMULA' in data:
                                formula = data['FORMULA']
                            elif 'COMPOSITION' in data:
                                formula = data['COMPOSITION']

                            main_name = 'NA'
                            synonyms = 'NA'
                            if 'NAME' in data:
                                name_data = data['NAME']
                                names = name_data.split(";")
                                if len(names)>1:
                                    synonyms = ';'.join(data['NAME'].split(';')[1:])
                                    main_name = names[0].strip()
                                else:
                                    main_name = name_data


                            compound_table.append(CompoundData(
                                                    data['ENTRY'].split()[0],
                                                    main_name,
                                                    formula,
                                                    synonyms,
                                                    weight))
                            
                        elif type_name == "ortholog":
                            ortho_table.append(OrthoData(data['ENTRY'].split()[0],
                                                        data['SYMBOL'].split(', ')[0],
                                                        data['NAME']))
                    if batch_start % 250==0:
                        print(f'{GREEN} Processing {type_name} from {batch_start}/{len(codes)}')
                        
        b_table = {
            'GeneData': gene_table,
            'CompoundData': compound_table,
            'OrthoData': ortho_table
        }
        return b_table

                        







        