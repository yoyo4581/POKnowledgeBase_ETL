from dataclasses import dataclass
from typing import ClassVar, Optional, Any
from src.models.base import BaseSQLObject
from datetime import datetime


@dataclass(frozen=True)
class GOOntologyMeta(BaseSQLObject):
    """
    Snapshot of a fetched GO source file (go-basic.json, goa-human.gaf, ...)
    used to detect whether the ontology actually changed since the last run.
    """
    source: str
    etag: Optional[str]
    last_modified: Optional[str]
    node_count: int
    edge_count: int
    last_checked: datetime
    __table_name__: ClassVar[str] = "GOOntologyMeta"

    @classmethod
    def from_meta(cls, source: str, meta: dict, node_count: int, edge_count: int) -> "GOOntologyMeta":
        return cls(
            source=source,
            etag=meta.get("etag"),
            last_modified=meta.get("last_modified"),
            node_count=node_count,
            edge_count=edge_count,
            last_checked=datetime.now(),
        )


@dataclass
class GOOntologyRecord:
    """Intermediate record bundling the parsed OBO graph with the metadata snapshot built from it."""
    graph: dict[str, Any]
    metadata: GOOntologyMeta


@dataclass(frozen=True)
class EntrezUniprotMap(BaseSQLObject):
    entrez_id: int
    uniprot_id: str
    __table_name__: ClassVar[str] = "EntrezUniprotMap"
