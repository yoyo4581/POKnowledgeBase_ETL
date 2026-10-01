"""
Selects the active pathway source and exposes the one schema the rest of the
system reads.

There is exactly one `table_schemas` in this codebase -- table_registry
builds `table_managers` from it at import, SQLState provisions and clears
every table in it, and BaseSQLObject validates every model against it. So
sources cannot coexist: `pathway_source` picks kegg_definitions or
reactome_definitions, and the shared spine is merged into whichever wins.

The two sets redefine six table names with different columns, so the
selected source and `gene_database` must agree -- pointing Reactome code at
the KEGG database fails at upsert, not at import.
"""
import os
import re
from typing import Literal

from dotenv import load_dotenv

from .shared_definitions import shared_table_schemas
from .types import TableSchema, ForeignKey

load_dotenv()

PathwaySource = Literal["reactome", "kegg"]
pathway_source: PathwaySource = os.getenv("pathway_source", "reactome").strip().lower()

if pathway_source == "reactome":
    from .reactome_definitions import reactome_table_schemas as _source_schemas
    from .reactome_definitions import AnnotationTables
elif pathway_source == "kegg":
    from .kegg_definitions import kegg_table_schemas as _source_schemas
    from .kegg_definitions import AnnotationTables
else:
    raise ValueError(
        f"pathway_source={pathway_source!r} is not one of 'reactome', 'kegg'")

table_schemas: dict[str, TableSchema] = {**shared_table_schemas, **_source_schemas}



def _base_type(col_type: str) -> str:
    """Extract the core SQL type, stripping NULL/NOT NULL and whitespace variance."""
    return re.sub(r"\s+(NOT\s+)?NULL\b", "", col_type, flags=re.IGNORECASE).strip().upper()

def validate_schema(table_schemas: dict[str, TableSchema]) -> None:
    for table_name, schema in table_schemas.items():
        for c in schema.constraints:
            if isinstance(c, ForeignKey):
                if c.ref_table not in table_schemas:
                    raise ValueError(f"{table_name}.{c.name}: unknown table '{c.ref_table}'")
                ref_schema = table_schemas[c.ref_table]
                missing = set(c.ref_columns) - set(ref_schema.columns)
                if missing:
                    raise ValueError(f"{table_name}.{c.name}: unknown columns {missing} in '{c.ref_table}'")

                for local_col, ref_col in zip(c.columns, c.ref_columns):
                    local_type = _base_type(schema.columns[local_col])
                    ref_type = _base_type(ref_schema.columns[ref_col])
                    if local_type != ref_type:
                        raise ValueError(
                            f"{table_name}.{c.name}: type mismatch — "
                            f"{table_name}.{local_col} is {schema.columns[local_col]}, "
                            f"{c.ref_table}.{ref_col} is {ref_schema.columns[ref_col]}"
                        )

                    

validate_schema(table_schemas)
