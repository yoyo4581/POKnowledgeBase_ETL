import requests
from pathlib import Path
import shutil
import tempfile
from utils.colored_text import RED, GREEN, RESET, YELLOW
from typing import Literal
import gzip


GO_JSON_URL = "https://purl.obolibrary.org/obo/go/go-basic.json"
GO_ANNOT_JSON_URL = "https://current.geneontology.org/annotations/gaf/HUMAN-uniprot.gaf.gz"


LOCAL_PATH = Path("data/go-basic.json")
LOCAL_ANNOT_PATH = Path("data/goa-human.gaf")

META_PATH = Path("data/go-basic.json.meta") # last modified tag
META_ANNOT_PATH = Path("data/goa-human.gaf.meta") # last modified tag

class GO_ETL:
    def __init__(self):
        self.graph = None

    def read_meta(self, file_type: Literal['go', 'goa']) -> dict:
        """Reads and parses the cached ETag/Last-Modified metadata for a file type."""
        meta_path = META_PATH if file_type == 'go' else META_ANNOT_PATH
        if not meta_path.exists():
            return {}
        lines = meta_path.read_text().strip().split("\n")
        return dict(line.split(": ", 1) for line in lines if ": " in line)

    def fetch_latest_go_file(self, file_type: Literal['go', 'goa']) -> bool:
        """
        Downloads go-basic.json only if its changed since last fetch.
        Returns True if a new file is written, False if already up to date.
        """
        headers = {}
        if file_type == 'go':
            url = GO_JSON_URL
            dest = LOCAL_PATH
            meta_path = META_PATH
        else:
            url = GO_ANNOT_JSON_URL
            dest = LOCAL_ANNOT_PATH
            meta_path = META_ANNOT_PATH

        cached = self.read_meta(file_type)
        if "etag" in cached:
            headers["If-None-Match"] = cached["etag"]
        if "last_modified" in cached:
            headers["If-Modified-Since"] = cached["last_modified"]

        response = requests.get(url, headers=headers, stream=True, timeout=60)

        if response.status_code == 304:
            print(f"{str(dest)} unchanged, skipping.")
            return False

        dest.parent.mkdir(parents=True, exist_ok=True)

        # write to a temp file first, then atomic-rename over old one.
        with tempfile.NamedTemporaryFile(dir=dest.parent, delete=False) as tmp:
            for chunk in response.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp_path = Path(tmp.name)

        if url.endswith(".gz"):
            decompressed_path = tmp_path.with_suffix(".decompressed")
            with gzip.open(tmp_path, "rb") as f_in, open(decompressed_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            tmp_path.unlink()
            tmp_path = decompressed_path

        tmp_path.replace(dest)

        meta_lines = []
        if "ETag" in response.headers:
            meta_lines.append(f"etag: {response.headers['ETag']}")
        if "Last-Modified" in response.headers:
            meta_lines.append(f"last_modified: {response.headers['Last-Modified']}")
        meta_path.write_text("\n".join(meta_lines))

        print(f"Downloaded new {str(dest)} ({dest.stat().st_size / 1e6:.1f} MB)")
        return True

    def read_to_graph(self):
        import json

        try:
            with open(LOCAL_PATH) as f:
                self.graph = json.load(f)["graphs"][0]

        except FileNotFoundError:
            print(f"{RED}{str(LOCAL_PATH)} not found — run fetch_latest_go_json() first.{RESET}")
            raise

        except json.JSONDecodeError as e:
            print(f"{RED}go-basic.json is corrupted or not valid JSON: {e}{RESET}")
            raise

        except (KeyError, IndexError):
            print(f"{RED}go-basic.json doesn't have the expected 'graphs' structure — "
                f"check whether the file format changed or download was incomplete.{RESET}")
            raise

        return self.graph

    def read_annotation(self):
        import pandas as pd
        
        GAF_COLUMNS = [
            "DB", "DB_Object_ID", "DB_Object_Symbol", "Qualifier", "GO_ID",
            "DB_Reference", "Evidence_Code", "With_From", "Aspect",
            "DB_Object_Name", "DB_Object_Synonym", "DB_Object_Type",
            "Taxon", "Date", "Assigned_By", "Annotation_Extension",
            "Gene_Product_Form_ID",
        ]
        try:
            self.annotation = pd.read_csv(
                LOCAL_ANNOT_PATH,
                sep="\t",
                comment="!",
                header=None,
                names=GAF_COLUMNS,
                dtype=str,
                keep_default_na=False,
            )
            self.annotation = self.annotation[self.annotation["DB"] == "UniProtKB"] 
        except FileNotFoundError:
            print(f"{RED}{str(LOCAL_ANNOT_PATH)} not found — run fetch_latest_go_json('goa') first.{RESET}")
            raise
        return self.annotation


