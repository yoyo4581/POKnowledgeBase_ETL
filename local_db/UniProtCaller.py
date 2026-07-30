import gzip
import requests
import tempfile
from pathlib import Path
from local_db.SQL.SQLCaller import SQL_ETL

IDMAPPING_URL = "https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/by_organism/HUMAN_9606_idmapping_selected.tab.gz"
UNIPROT_STREAM_URL = "https://rest.uniprot.org/uniprotkb/stream"


META_PATH = Path("data/id_mapping_selected.meta")

class UniProt_ETL:
    def __init__(self, sql_caller:SQL_ETL):
        self.sql_caller = sql_caller

    def sync_entrez_uniprot_map(self, url: str = IDMAPPING_URL, batch_size: int = 5000) -> bool:
        """
        Streams idmapping_selected.tab.gz, extracts (UniProt AC, EntrezGene ID) pairs,
        and upserts them into a SQL table -- without ever writing the decompressed
        file to disk. Uses conditional fetch (ETag/Last-Modified) to skip if unchanged.
        """
        headers = {}
        if META_PATH.exists():
            cached = META_PATH.read_text().strip().split("\n")
            meta = dict(line.split(": ", 1) for line in cached if ": " in line)
            if "etag" in meta:
                headers["If-None-Match"] = meta["etag"]
            if "last_modified" in meta:
                headers["If-Modified-Since"] = meta["last_modified"]

        response = requests.get(url, headers=headers, stream=True, timeout=300)

        if response.status_code == 304:
            print("idmapping unchanged, skipping.")
            return False

        response.raise_for_status()

        # Download the .gz to a temp file first (streaming), so we're not holding
        # an open HTTP connection for the entire multi-hour parse.
        with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                tmp.write(chunk)
            tmp_path = Path(tmp.name)

        try:
            batch = []
            rows_written = 0

            with gzip.open(tmp_path, "rt", encoding="utf-8") as fh:
                for line in fh:
                    fields = line.rstrip("\n").split("\t")
                    uniprot_ac = fields[0]
                    entrez_id = fields[2] if len(fields) > 2 else ""

                    if not entrez_id:
                        continue  # many rows have no Entrez mapping; skip

                    # idmapping allows multiple Entrez IDs semicolon/comma separated in some releases
                    for eid in entrez_id.replace(";", ",").split(","):
                        eid = eid.strip()
                        if eid:
                            batch.append((eid, uniprot_ac))

                    if len(batch) >= batch_size:
                        self.sql_caller.upsert_id_map_batch(batch)
                        rows_written += len(batch)
                        batch.clear()

            if batch:
                self.sql_caller.upsert_id_map_batch(batch)
                rows_written += len(batch)

            print(f"Wrote {rows_written} Entrez->UniProt pairs.")

        finally:
            tmp_path.unlink(missing_ok=True)  # always clean up, even on failure

        meta_lines = []
        if "ETag" in response.headers:
            meta_lines.append(f"etag: {response.headers['ETag']}")
        if "Last-Modified" in response.headers:
            meta_lines.append(f"last_modified: {response.headers['Last-Modified']}")
        META_PATH.write_text("\n".join(meta_lines))

        return True

    def fetch_functions_from_uniprot(self, uniprot_accessions: list[str], batch_size: int = 100) -> list[dict]:
        """
        Pulls function annotation (and a few useful adjacent fields) for a list
        of UniProt accessions via the stream endpoint, in TSV form.
        """
        fields = "accession,cc_function"
        results = []

        for i in range(0, len(uniprot_accessions), batch_size):
            batch = uniprot_accessions[i:i + batch_size]
            query = " OR ".join(f"accession:{acc}" for acc in batch)

            resp = requests.get(
                UNIPROT_STREAM_URL,
                params={"query": query, "fields": fields, "format": "tsv"},
                timeout=60,
            )
            if not resp.ok:
                print(f"Status: {resp.status_code}")
                print(f"Body: {resp.text}")
            resp.raise_for_status()

            lines = resp.text.strip().split("\n")
            header = lines[0].split("\t")
            for line in lines[1:]:
                row = dict(zip(header, line.split("\t")))
                results.append(row)

        return results
