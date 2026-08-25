import graphlib
from collections import defaultdict
from src.builders.SQL.schema.types import *
from src.builders.SQL.schema.definitions import table_schemas

import re


def creation_order(table_schemas: dict[str, TableSchema]) -> list[str]:
    graph = {name: set() for name in table_schemas}
    for name, schema in table_schemas.items():
        for c in schema.constraints:
            if isinstance(c, ForeignKey):
                graph[name].add(c.ref_table)
    return list(graphlib.TopologicalSorter(graph).static_order())


def resolve_staging_plan(schema: TableSchema)->list[ColumnPlan]:
    """
    Reconciles a schema's columns + constraints into a concrete staging plan.
    Any column governed by a ForeignKey with match_columns is deferred: instead of
    inserting the (unknown-at-insert-time) FK value, we stage the natural-key value
    and resolve the real FK afterward in a bulk UPDATE.
    """


    deffered_fk_by_col: dict[str, ForeignKey] = {}
    for c in schema.constraints:
        if isinstance(c, ForeignKey) and c.deferred:
            for local_col in c.columns:
                deffered_fk_by_col[local_col] = c

    plan = []
    for col, col_type in schema.columns.items():
        fk = deffered_fk_by_col.get(col)

        if schema.auto_id and col == schema.key:
            continue

        if fk is None:
            plan.append(ColumnPlan(col, col, col_type))
            continue
        
        if fk.deferred is None:
            continue

        idx = fk.columns.index(col)
        natural_col = fk.deferred.match_columns[idx]
        shadow_col = fk.deferred.staging_columns[idx]

        if shadow_col in schema.columns and shadow_col != col:
            plan.append(ColumnPlan(col, shadow_col, schema.columns[shadow_col], deffered_fk=fk, reused_column=True))
            continue

        ref_schema = table_schemas[fk.ref_table]
        shadow_type = _make_nullable(ref_schema.columns[natural_col]) # inherit type from the natural column

        plan.append(ColumnPlan(col, shadow_col, shadow_type, deffered_fk=fk))

    return plan

def _make_nullable(col_type: str) -> str:
    """Strip NOT NULL when a column type is reused for a staging/shadow column,
    since deferred-FK staging values may legitimately be absent even when the
    referenced natural-key column itself disallows NULL."""
    return re.sub(r"\s+NOT\s+NULL\b", "", col_type, flags=re.IGNORECASE).strip()


def resolve_clear_order(table_schemas: dict[str, "TableSchema"]) -> TableClearOrder:
    """
    Builds a dependency graph from each TableSchema's FK constraints and
    returns tables in child-before-parent order, safe for sequential clearing.
    Edge direction: child -> parent (child is a prerequisite for parent).
    """
    successors: dict[str, set[str]] = defaultdict(set)   # child -> {parents}
    in_degree: dict[str, int] = {name: 0 for name in table_schemas}
    self_refs: set[str] = set()

    for table_name, schema in table_schemas.items():
        for constraint in getattr(schema, "constraints", ()) or ():
            if not isinstance(constraint, ForeignKey):
                continue
            parent = constraint.ref_table
            if parent == table_name:
                self_refs.add(table_name)
                continue  # self-ref: not a linear-order dependency, handle separately
            if parent not in table_schemas:
                continue  # FK points outside this schema set, nothing to order against
            if parent not in successors[table_name]:
                successors[table_name].add(parent)
                in_degree[parent] += 1

    # Kahn's, batched by frontier so each level has no intra-level dependencies
    remaining = dict(in_degree)
    frontier = sorted(t for t, deg in remaining.items() if deg == 0)

    levels: list[list[str]] = []
    order: list[str] = []
    while frontier:
        levels.append(frontier)
        order.extend(frontier)
        next_frontier: list[str] = []
        for table_name in frontier:
            del remaining[table_name]
            for parent in successors.get(table_name, ()):
                if parent not in remaining:
                    continue
                remaining[parent] -= 1
                if remaining[parent] == 0:
                    next_frontier.append(parent)
        frontier = sorted(next_frontier)

    # Anything left in `remaining` never reached in_degree 0 -> part of a real cycle
    cycles = _find_cycles(set(remaining), successors) if remaining else []

    return TableClearOrder(order=order, levels=levels, cycles=cycles, self_refs=self_refs)


def _find_cycles(nodes: set[str], successors: dict[str, set[str]]) -> list[set[str]]:
    """Tarjan's SCC, restricted to unresolved `nodes`. Returns only components
    of size > 1 -- genuine multi-table circular FK chains."""
    index_counter = [0]
    stack: list[str] = []
    on_stack: set[str] = set()
    indices: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    result: list[set[str]] = []

    def strongconnect(v: str):
        indices[v] = lowlink[v] = index_counter[0]
        index_counter[0] += 1
        stack.append(v)
        on_stack.add(v)

        for w in successors.get(v, ()):
            if w not in nodes:
                continue
            if w not in indices:
                strongconnect(w)
                lowlink[v] = min(lowlink[v], lowlink[w])
            elif w in on_stack:
                lowlink[v] = min(lowlink[v], indices[w])

        if lowlink[v] == indices[v]:
            component = set()
            while True:
                w = stack.pop()
                on_stack.discard(w)
                component.add(w)
                if w == v:
                    break
            if len(component) > 1:
                result.append(component)

    for node in nodes:
        if node not in indices:
            strongconnect(node)

    return result
