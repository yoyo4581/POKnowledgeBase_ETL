import re
import requests
import shutil
import tempfile
from pathlib import Path
from typing import Iterator
import gzip

IDMAPPING_URL = "https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/by_organism/HUMAN_9606_idmapping_selected.tab.gz"
UNIPROT_STREAM_URL = "https://rest.uniprot.org/uniprotkb/stream"

LOCAL_PATH = Path("data/idmapping_selected.tab")
META_PATH = Path("data/idmapping_selected.tab.meta")

_EVIDENCE_TAG_RE = re.compile(r"\{[^}]*\}")
_FUNCTION_PREFIX_RE = re.compile(r"FUNCTION:\s*")


def clean_function_text(raw: str) -> str:
    """
    Strips UniProt's cc_function markup down to plain prose: the repeated
    'FUNCTION:' section prefix (one per isoform-specific note) and the
    trailing {ECO:...|PubMed:...} evidence tags, then collapses whitespace.
    Returns '' for entries with no function annotation at all.
    """
    if not raw:
        return ""
    text = _EVIDENCE_TAG_RE.sub("", raw)
    text = _FUNCTION_PREFIX_RE.sub("", text)
    return " ".join(text.split())


class UniProt_ETL:
    def _read_local_meta(self) -> dict:
        if not META_PATH.exists():
            return {}
        lines = META_PATH.read_text().strip().split("\n")
        return dict(line.split(": ", 1) for line in lines if ": " in line)

    def _write_local_meta(self, headers) -> None:
        meta_lines = []
        if "ETag" in headers:
            meta_lines.append(f"etag: {headers['ETag']}")
        if "Last-Modified" in headers:
            meta_lines.append(f"last_modified: {headers['Last-Modified']}")
        META_PATH.write_text("\n".join(meta_lines))

    def has_idmapping_changed(self, url: str = IDMAPPING_URL) -> bool:
        """
        Metadata-only screening: HEADs the remote file and compares its
        ETag/Last-Modified against what's saved locally from the last fetch.
        No body is ever transferred by this check (confirmed: UniProt's
        server returns proper ETag/Last-Modified on HEAD), so it costs
        nothing even when the answer is "unchanged" -- unlike relying on a
        conditional GET, which still depends on the server honoring
        If-None-Match with a bodyless 304.
        """
        local = self._read_local_meta()
        if not local:
            return True  # never fetched before

        response = requests.head(url, timeout=30, allow_redirects=True)
        response.raise_for_status()

        remote_etag = response.headers.get("ETag")
        remote_last_modified = response.headers.get("Last-Modified")

        if remote_etag:
            return local.get("etag") != remote_etag
        if remote_last_modified:
            return local.get("last_modified") != remote_last_modified
        return True  # server gave us nothing to compare against -- assume changed

    def fetch_latest_idmapping(self, url: str = IDMAPPING_URL) -> bool:
        """
        Downloads and decompresses idmapping_selected.tab.gz, only if
        has_idmapping_changed() says the remote file differs from what's
        recorded locally. Returns True if a new file is written, False if
        already up to date.
        """
        if not self.has_idmapping_changed(url):
            print(f"{str(LOCAL_PATH)} unchanged, skipping.")
            return False

        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)

        # Download the .gz to a temp file first (streaming), so we're not holding
        # an open HTTP connection for the entire decompress+write.
        with tempfile.NamedTemporaryFile(dir=LOCAL_PATH.parent, delete=False) as tmp:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                tmp.write(chunk)
            tmp_path = Path(tmp.name)

        decompressed_path = tmp_path.with_suffix(".decompressed")
        with gzip.open(tmp_path, "rb") as f_in, open(decompressed_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        tmp_path.unlink()
        decompressed_path.replace(LOCAL_PATH)

        self._write_local_meta(response.headers)

        print(f"Downloaded new {str(LOCAL_PATH)} ({LOCAL_PATH.stat().st_size / 1e6:.1f} MB)")
        return True

    def read_entrez_uniprot_map(self, batch_size: int = 5000) -> Iterator[list[tuple[str, str]]]:
        """
        Reads the decompressed idmapping file and yields batches of
        (entrez_id, uniprot_id) pairs. idmapping allows multiple Entrez IDs
        per row, semicolon/comma separated in some releases -- each gets its
        own pair. Column 2 (0-indexed) is GeneID per idmapping_selected's
        documented layout.
        """
        try:
            fh = open(LOCAL_PATH, "r", encoding="utf-8")
        except FileNotFoundError:
            print(f"{str(LOCAL_PATH)} not found — run fetch_latest_idmapping() first.")
            raise

        batch: list[tuple[str, str]] = []
        with fh:
            for line in fh:
                fields = line.rstrip("\n").split("\t")
                uniprot_ac = fields[0]
                entrez_field = fields[2] if len(fields) > 2 else ""

                if not entrez_field:
                    continue  # many rows have no Entrez mapping; skip

                for eid in entrez_field.replace(";", ",").split(","):
                    eid = eid.strip()
                    if eid:
                        batch.append((eid, uniprot_ac))

                if len(batch) >= batch_size:
                    yield batch
                    batch = []

        if batch:
            yield batch

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
