from neo4j import GraphDatabase, Driver
import os
from utils.colored_text import RED, GREEN, RESET, YELLOW
from local_db.SQL.SQLCaller import SQL_ETL
from typing import Any
import pandas as pd

GRAPH_URI = os.getenv("NEO4J_URI")
GRAPH_AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

class OntoStateManager:
    def __init__(self, graph_driver: Driver):
        self.graph_driver = graph_driver

    @staticmethod
    def _upsert_nodes(tx, rows):
        tx.run(f"""
        UNWIND $rows as row
        MERGE (o:Ontology {{id: row.id}})
        SET o+=row.props
        """, rows=rows)

    @staticmethod
    def _upsert_edges(tx, rows):
        from itertools import groupby
        #sort by predicate
        rows_sorted = sorted(rows, key=lambda r: r["pred"])
        for pred, group in groupby(rows_sorted, key=lambda r: r["pred"]):
            tx.run(f"""
                UNWIND $rows as row
                MATCH (a:Ontology {{id: row.src}}), (b:Ontology {{id: row.obj}})
                MERGE (a)-[:`{pred}`]->(b)
            """, rows=list(group))

    @staticmethod
    def _upsert_annotation_edges(session, pred, rows):
        from itertools import groupby
        #sort by predicate
        session.run(f"""
            CALL apoc.periodic.iterate(
                "UNWIND $rows AS row RETURN row",
                "
                WITH row
                MATCH (o:Ontology {{id: row.go_id}})
                CALL {{
                    WITH row
                    MATCH (g:Gene {{gene_name: row.symbol}})
                    RETURN g
                    UNION
                    WITH row
                    MATCH (g:Gene)
                    WHERE row.uniprot_id IN g.uniprot_ids
                    RETURN g
                }}
                MERGE (g)-[r:`{pred}`]->(o)
                SET r.evidence = CASE
                    WHEN row.evidence IN coalesce(r.evidence, []) THEN r.evidence
                    ELSE coalesce(r.evidence, []) + row.evidence
                END
                ",
                {{batchSize: 2000, parallel: false, params: {{rows: $rows}}}}
            )
        """, rows=rows)

    @staticmethod
    def _detach_delete_deprecated(tx):
        tx.run(f"""
        MATCH (o:Ontology)
        WHERE o.deprecated
        DETACH DELETE o
        """)

    @staticmethod
    def flatten_go_props(node):
        import re

        def local_name(uri):
            return re.split(r'[/#]', uri)[-1]

        if node.get("lbl") is None:
            print(node)

        flat = {"name": node.get("lbl")}
        metadata = {k: v for k, v in node.get('meta', {}).items() if v is not None}
        if not metadata:
            print(node)

        if "definition" in metadata:
            flat["definition"] = metadata["definition"]["val"]
            flat["definition_xref"] = metadata["definition"].get("xrefs", [])
        if "synonyms" in metadata:
            flat["synonyms"] = [s["val"] for s in metadata["synonyms"]]
        if "basicPropertyValues" in metadata:
            for bpv in metadata['basicPropertyValues']:
                key = local_name(bpv["pred"])
                flat[key] = local_name(bpv["val"])
        
        flat['deprecated'] = metadata.get('deprecated', False)
        return flat

    def sync_ontology_annotations(self, annotations: pd.DataFrame, batch_size: int = 10000):
        """
        Syncs GO annotations to Neo4j.
        Annotations are expected to be a DataFrame with columns:
        ['gene_id', 'uniprot_id', 'go_id', 'evidence_code', 'reference']
        """
        def chunked(seq, size):
            for i in range(0, len(seq), size):
                yield seq[i:i + size]

        edge_payload = []

        for index, row in annotations.iterrows():
            uniprot_id = row['DB_Object_ID']
            symbol = row['DB_Object_Symbol']
            qualifier = row['Qualifier']
            evidence = row['Evidence_Code']
            go_id = row['GO_ID'].replace(":", "_")

            edge_payload.append({
                "uniprot_id": uniprot_id,
                "symbol": symbol,
                "qualifier": qualifier,
                "evidence": evidence,
                "go_id": go_id
            })

        rows_sorted = sorted(edge_payload, key=lambda r: r["qualifier"])
        with self.graph_driver.session() as session:
            # Upsert annotations
            for pred, group in groupby(rows_sorted, key=lambda r: r["qualifier"]):
                group_rows = list(group)  # materialize once
                total_chunks = (len(group_rows) + batch_size - 1) // batch_size
                for i, chunk in enumerate(chunked(group_rows, batch_size), start=1):
                    print(f"[sync_ontology_annotations] Upserting annotation batch {i}/{total_chunks} for predicate '{pred}' ({len(chunk)} edges)...")
                    self._upsert_annotation_edges(session, pred, chunk)  # note: chunk, not list(group) again

    def sync_ontology_structure(self, graph: dict, batch_size: int = 500):
        nodes = graph["nodes"]
        edges = graph["edges"]

        def clean(uri):
            return uri.split('/')[-1]

        def chunked(seq, size):
            for i in range(0, len(seq), size):
                yield seq[i:i + size]

        rare_annot = {"BFO_0000050": 'part_of', "RO_0002213": 'positively_regulates',
                    "RO_0002211": 'regulates', "RO_0002212": 'negatively_regulates'}

        print("[sync_ontology_structure] Building node payload...")
        node_payload = []
        for node in nodes:
            if node['type'] == 'CLASS' and node.get("lbl") is not None:
                id = clean(node['id'])
                node_payload.append({"id": id, "props": self.flatten_go_props(node)})
        print(f"[sync_ontology_structure] {len(node_payload)} nodes to upsert.")

        print("[sync_ontology_structure] Building edge payload...")
        edge_payload = [{
                'src': clean(e["sub"]),
                'pred': rare_annot.get(clean(e["pred"]), clean(e["pred"])),
                'obj': clean(e["obj"])
            }
            for e in edges]
        print(f"[sync_ontology_structure] {len(edge_payload)} edges to upsert.")

        with self.graph_driver.session() as session:
            # 1. Upsert all current nodes/props - safe no-op for unchanged terms
            total_node_batches = (len(node_payload) + batch_size - 1) // batch_size or 1
            for i, batch in enumerate(chunked(node_payload, batch_size), start=1):
                print(f"[sync_ontology_structure] Upserting node batch {i}/{total_node_batches} ({len(batch)} nodes)...")
                session.execute_write(self._upsert_nodes, batch)

            # 2. Link Ontology Edges.
            total_edge_batches = (len(edge_payload) + batch_size - 1) // batch_size or 1
            for i, batch in enumerate(chunked(edge_payload, batch_size), start=1):
                print(f"[sync_ontology_structure] Upserting edge batch {i}/{total_edge_batches} ({len(batch)} edges)...")
                session.execute_write(self._upsert_edges, batch)

            # 3. Delete and detach obsolete nodes.
            print("[sync_ontology_structure] Detaching and deleting deprecated nodes...")
            session.execute_write(self._detach_delete_deprecated)

        print("[sync_ontology_structure] Done.")
                               
from itertools import groupby

NODE_KEY = {
    'Gene': 'id',
    'Pathway': 'id',
    'Entity': 'id',
    'Reactions': 'id',
    'Compound': 'id',
    'Ortholog': 'id',
}

class Neo4jEdge_Sync:

    def __init__(self, graph_driver: Driver):
        self.graph_driver = graph_driver
    # ---- normalization: raw event -> common edge shape ----
    @staticmethod
    def _to_edge(event: dict, type_a: str, type_b: str, src_field: str, obj_field: str,
                 pred: str, extra_exclude: tuple = ()) -> dict:
        exclude = {'run_id', 'action', src_field, obj_field, *extra_exclude}
        attrs = {k: v for k, v in event.items() if k not in exclude}
        return {
            'action': event['action'],
            'type_a': type_a, 'type_b': type_b,
            'src': event[src_field], 'obj': event[obj_field],
            'pred': pred,
            'pathway_id': event.get('pathway_id'),
            'attrs': attrs,
        }

    @classmethod
    def _normalize_intx(cls, e: dict) -> dict:
        if e['source_id'] == 'undefined' or e['target_id'] == 'undefined':
            return
        
        if e['source_id'][0].isdigit():
            type_a = "Gene"
        elif e['source_id'].startswith('C'):
            type_a = "Compound"
        elif e['source_id'].startswith('K'):
            type_a = "Ortholog"
        else:
            return

        if e['target_id'][0].isdigit():
            type_b = "Gene"
        elif e['target_id'].startswith('C'):
            type_b = "Compound"
        elif e['target_id'].startswith('K'):
            type_b = "Ortholog"
        else:
            return
        
        return cls._to_edge(
            e, type_a, type_b, 'source_id', 'target_id',
            pred=e['relation_type'].upper(),
            extra_exclude=('relation_type',),
        )

    @classmethod
    def _normalize_reaction_part(cls, e: dict) -> dict:
        if e['entity_id'].startswith('C'):
            entity_type = "Compound"
        elif e['entity_id'].startswith('K'):
            entity_type = "Ortholog"
        elif e['entity_id'] == 'undefined':
            return
        else:
            return

        return cls._to_edge(
            e, 'Reactions', entity_type, 'reaction_id', 'entity_id',
            pred=e['role'].upper(),
            extra_exclude=('role',),
        ) if e['role'] == 'product' else cls._to_edge(
            e, entity_type, 'Reactions', 'entity_id', 'reaction_id',
            pred=e['role'].upper(),
            extra_exclude=('role', ),
        )

    @classmethod
    def _normalize_ent_path(cls, e: dict) -> dict:
        if e['entity_id'][0].isdigit():
            entity_type = "Gene"
        elif e['entity_id'].startswith('C'):
            entity_type = "Compound"
        elif e['entity_id'].startswith('K'):
            entity_type = "Ortholog"
        elif e['entity_id'] == 'undefined':
            return
        else:
            return

        return cls._to_edge(
            e, 'Pathway', entity_type, 'pathway_id', 'entity_id',
            pred='HAS_MEMBER',
        )

    # ---- grouped upsert ----

    def upsert_relationships(self, intx_events: list[dict], ent_path_events: list[dict],
                             reaction_part_events: list[dict]):
        edges = [
            e for e in (
            [self._normalize_intx(e) for e in intx_events]
            + [self._normalize_reaction_part(e) for e in reaction_part_events]
            + [self._normalize_ent_path(e) for e in ent_path_events]
            )
            if e is not None
        ]

        def group_key(edge):
            return (edge['action'], edge['type_a'], edge['type_b'], edge['pred'])

        edges_sorted = sorted(edges, key=group_key)

        with self.graph_driver.session() as session:
            for (action, type_a, type_b, pred), group in groupby(edges_sorted, key=group_key):
                rows = list(group)
                if action == 'DELETE':
                    session.execute_write(self._delete_edges, type_a, type_b, pred, rows)
                else:
                    session.execute_write(self._upsert_edges, type_a, type_b, pred, rows)

    #To do, revise Annotation step to include Pathway node creation. So that when it comes to this step all nodes that are to be linked are already present.
    #Also need to make sure Reactions are getting properly linked. Last fix was to properly label them, so now it should work but I should check.
    @staticmethod
    def _upsert_edges(tx, type_a: str, type_b: str, pred: str, rows: list[dict]):
        key_a, key_b = NODE_KEY[type_a], NODE_KEY[type_b]
        tx.run(f"""
            UNWIND $rows AS row
            MATCH (a:{type_a} {{{key_a}: row.src}})
            MATCH (b:{type_b} {{{key_b}: row.obj}})
            MERGE (a)-[p:`{pred}`]->(b)
            SET p += row.attrs
            SET p.pathway_ids = CASE
                WHEN row.pathway_id IS NULL THEN coalesce(p.pathway_ids, [])
                WHEN row.pathway_id IN coalesce(p.pathway_ids, []) THEN p.pathway_ids
                ELSE coalesce(p.pathway_ids, []) + row.pathway_id
            END
        """, rows=[{'src': r['src'], 'obj': r['obj'], 'attrs': r['attrs'], 'pathway_id': r['pathway_id']} for r in rows])

    @staticmethod
    def _delete_edges(tx, type_a: str, type_b: str, pred: str, rows: list[dict]):
        key_a, key_b = NODE_KEY[type_a], NODE_KEY[type_b]
        tx.run(f"""
            UNWIND $rows AS row
            MATCH (a:{type_a} {{{key_a}: row.src}})-[p:`{pred}`]->(b:{type_b} {{{key_b}: row.obj}})
            DELETE p
        """, rows=[{'src': r['src'], 'obj': r['obj']} for r in rows])


class Neo4j_ETL:
    def __init__(self):
        self._driver = None
        self._edge_sync = None
        self._ontology_manager = None

    def get_wsl_host_ip(self)->str:
        """
        Returns the windows host ip as seen from WSL2.
        Falls back to localhost if the lookup fails.
        """
        import subprocess

        try:
            result = subprocess.run(
                ["ip", "route", "show"],
                capture_output=True, text=True, check=True
            )
            for line in result.stdout.splitlines():
                if line.startswith("default"):
                    return line.split()[2]
        except (subprocess.CalledProcessError, IndexError, FileNotFoundError):
            pass
        return "localhost"

    @property
    def driver(self):
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                f"bolt://{self.get_wsl_host_ip()}:7687",
                auth=GRAPH_AUTH
            )
            self._driver.verify_connectivity()
        return self._driver

    @property
    def edge_sync(self):
        if self._edge_sync is None:
            self._edge_sync = Neo4jEdge_Sync(self.driver)
        return self._edge_sync

    @property
    def ontology_manager(self):
        if self._ontology_manager is None:
            self._ontology_manager = OntoStateManager(self.driver)
        return self._ontology_manager
    

    def build_cypher_merge_from_diff(self, update_content: dict[tuple[Any, Any, str], list[Any]]):
        from collections import defaultdict
        from decimal import Decimal

        def _sanitize_value(value):
            """
            Neo4j's Bolt protocol can't serialize Decimal. Recursively convert
            any Decimal values to float before sending as query parameters.
            """
            if isinstance(value, Decimal):
                return float(value)
            if isinstance(value, dict):
                return {k: _sanitize_value(v) for k, v in value.items()}
            if isinstance(value, list):
                return [_sanitize_value(v) for v in value]
            return value
        
        label_batches = defaultdict(list)

        for (action, entity_id, entity_type), payloads in update_content.items():
            label = entity_type.capitalize()
            for payload in payloads:
                # drop None so MERGE doesn't override existing props with null
                props = {k: v for k, v in payload.items() if v is not None}
                label_batches[label].append({"id": entity_id, "props": _sanitize_value(props)})
        
        queries = []
        for label, rows in label_batches.items():
            cypher = f"""
            UNWIND $rows as row
            MERGE (n:{label} {{id: row.id}})
            SET n+= row.props
            """
            queries.append((cypher, {"rows": rows}))

        return queries

    def update_functions(self, update_content: dict[tuple[Any, Any, str], dict]):
        """
        Will use fetched data from SQL to update into Neo4j.
        Input data is of format {(action, entity_id, uniprot_ac): {'function_text', 'embedding'}}
        """

        #Need to transform data into lists of entries where the uniprot_id, entrez_id are keys, and all else is props
        rows = [{"entrez_id": key[1],
                 "uniprot_id": key[2],
                 "props": content} for key, content in update_content.items()]

        def _run_merge(tx, cypher, rows):
            tx.run(cypher, rows=rows)

        function_query = f"""
        UNWIND $rows AS row
        MATCH (g:Gene {{id: row.entrez_id}})
        MERGE (f:Function {{uniprot_id: row.uniprot_id}})
        MERGE (g)-[:HAS_FUNCTION]->(f)
        SET f += row.props
        """
        with self.driver.session() as session:
            session.execute_write(_run_merge, function_query, rows)

    def update_gene_uniprot_ids(self, update_content: list[tuple]):
        """
        Will use fetched data from SQL to update into Neo4j.
        Input data is of format [(entrez_id, uniprot_id), ...]
        """
        from collections import defaultdict

        id_mapping = defaultdict(set, {entrez_id: set() for entrez_id, _ in update_content})
        for entrez_id, uniprot_id in update_content:
            id_mapping[entrez_id].add(uniprot_id)

        rows = [{"entrez_id": entrez_id, "uniprot_ids": sorted(uniprot_ids)} for entrez_id, uniprot_ids in id_mapping.items()]

        def _run_merge(tx, cypher, rows):
            tx.run(cypher, rows=rows)

        gene_uniprot_query = """
        UNWIND $rows AS row
        MATCH (g:Gene {id: row.entrez_id})
        SET g.uniprot_ids = row.uniprot_ids
        """
        with self.driver.session() as session:
            session.execute_write(_run_merge, gene_uniprot_query, rows)


            
    def update_entities(self, update_content: dict[tuple[Any, Any, str], list[Any]]):
        """
        Fetches all entities from SQL database diff tables and updates them in Neo4j.
        """
        def _run_merge(tx, cypher, rows):
            tx.run(cypher, rows=rows)

        with self.driver.session() as session:
            for cypher, params in self.build_cypher_merge_from_diff(update_content):
                session.execute_write(_run_merge, cypher, params["rows"])

    def get_existing_uniprot_accessions(self) -> set[str]:
        """
        Fetches all existing UniProt accessions from Neo4j.
        Returns a set of UniProt accessions.
        """
        query = "MATCH (f:Function) RETURN f.uniprot_id AS uniprot_id"
        with self.driver.session() as session:
            result = session.run(query)
            return {record["uniprot_id"] for record in result if record["uniprot_id"] is not None}





        

        

