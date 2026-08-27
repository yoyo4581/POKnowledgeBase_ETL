from dataclasses import dataclass, asdict, fields
from datetime import datetime
import types
from typing import Dict, Any, ClassVar, get_type_hints, get_args, Union, get_origin, TypeVar
import logging
from src.builders.SQL.schema.definitions import table_schemas
import re

logger = logging.getLogger(__name__)
T = TypeVar("T", bound="BaseSQLObject")

@dataclass(frozen=True)
class BaseSQLObject:
    """
    Base class providing automatic, dynamic SQL flattening 
    for all biological data records.
    """
    __table_name__: ClassVar[str]

    def __post_init__(self):
        self._validate_types()
        self._validate_against_table_schema()

    def _validate_types(self)-> None:
        hints = get_type_hints(type(self))
        for f in fields(self):
            value = getattr(self, f.name)
            expected = hints[f.name]
            if not self._matches_type(value, expected):
                raise TypeError(
                    f"{type(self).__name__}.{f.name}: expected {expected}, "
                    f"got {type(value).__name__} ({value!r})"
                )

    @staticmethod
    def _matches_type(value, expected_type)->bool:
        origin = get_origin(expected_type)
        if origin is Union or origin is types.UnionType: #covers `str | None`
            return any(BaseSQLObject._matches_type(value, arg) for arg in get_args(expected_type))
        if expected_type is type(None):
            return value is None
        if origin is None:
            return isinstance(value, expected_type)
        return True

    def _validate_against_table_schema(self)->None:
        schema = table_schemas.get(self.__table_name__)
        if schema is None:
            return # no schema registered for the table

        for f in fields(self):
            column_def = schema.columns.get(f.name)
            if column_def is None:
                continue
            self._check_column_constraints(f.name, getattr(self, f.name), column_def)

    @staticmethod
    def _check_column_constraints(field_name: str, value, column_def: str)-> None:
        if "NOT NULL" in column_def.upper() and value is None:
            raise ValueError(f"{field_name} is NOT NULL in schema but got None")
        match = re.match(r"NVARCHAR\((\d+)\)", column_def.upper())
        if match and isinstance(value, str) and len(value)>int(match.group(1)):
            raise ValueError(f"{field_name}: length {len(value)} exceeds NVARCHAR({match.group(1)})")
        
            
    @classmethod
    def from_dict(cls: type[T], data: dict) -> T:
        """Builds a dataclass instance from a dict, keyed by field name (not position)."""
        field_names = {f.name for f in fields(cls)}
        missing = field_names - data.keys()
        if missing:
            raise ValueError(f"{cls.__name__}: missing required field(s) {missing} in {data!r}")

        extra = data.keys() - field_names
        filtered = {k: v for k, v in data.items() if k in field_names}
        if extra:
            logger.debug("%s.from_dict: ignoring unexpected key(s) %s", cls.__name__, extra)

        return cls(**filtered)

    def to_sql_staging_row(self) -> Dict[str, Any]:
        """
        Dynamically inspects the child class attributes and formats 
        them into a flat dictionary suitable for SQL staging inserts.
        """
        flat_dict = {}
        
        # 1. Use asdict to get all data, including inherited fields
        raw_dict = asdict(self)
        
        # 2. Iterate and dynamically resolve types (e.g., flatten lists into strings)
        for field_name, value in raw_dict.items():
            if isinstance(value, list):
                # Flatten lists of strings (like GO or KEGG IDs) into comma-separated text
                flat_dict[field_name] = ",".join(map(str, value)) if value else None
            elif isinstance(value, datetime):
                # Standardize datetime formatting for your SQL DB
                flat_dict[field_name] = value.isoformat()
            else:
                # Keep primitive types (strings, ints, floats, booleans) as-is
                flat_dict[field_name] = value
                
        return flat_dict
