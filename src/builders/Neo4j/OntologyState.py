from neo4j import Driver
import pandas as pd
from itertools import groupby


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