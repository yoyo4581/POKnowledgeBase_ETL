from dataclasses import dataclass
from typing import ClassVar
from src.models.base import BaseSQLObject


@dataclass(frozen=True)
class FunctionData(BaseSQLObject):
    """UniProt free-text function annotation for one protein, keyed by uniprot_id."""
    uniprot_id: str
    function_text: str
    __table_name__: ClassVar[str] = "FunctionData"
