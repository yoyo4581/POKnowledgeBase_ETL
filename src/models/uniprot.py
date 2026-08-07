from dataclasses import dataclass
from typing import List, Dict
from src.models.base import BaseCDCObject


@dataclass
class UniProtRecord(BaseCDCObject):
    # Strong typing for your application domain
    protein_id: str
    sequence: str
    go_terms: List[str]
    kegg_pathways: List[str]
    status: str

    @classmethod
    def from_raw_api(cls, raw_data: Dict):
        """Parser-friendly constructor: Cleans up messy bioinformatics payloads."""
        return cls(
            protein_id=raw_data["accession"],
            sequence=raw_data["sequence"]["value"],
            # Extracts deeply nested data cleanly
            go_terms=[db["id"] for db in raw_data.get("dbReferences", []) if db["type"] == "GO"],
            kegg_pathways=[db["id"] for db in raw_data.get("dbReferences", []) if db["type"] == "KEGG"],
            status=raw_data.get("entryAudit", {}).get("status", "ACTIVE")
        )
