"""
Dry-run harness for KEGG_ETL's KGML structure parsing. Parses a small sample
of already-downloaded KGML files (data/KGML/*.xml) the same way
kgml_structure_annotation.structure_resolve does, and stages the resulting
rows into the same table shape consume_kgml_structure_record would push to
SQL -- but writes them to JSON files instead of touching any database.

Sample pathways, chosen for structural variety:
  - hsa04080 (Neuroactive ligand-receptor interaction): pure signaling,
    gene/compound entries and relations only, no <reaction> tags.
  - hsa00670 (One carbon pool by folate): metabolic pathway exercising
    reactions, reaction_participants, and ortholog entries.
  - hsa01523 (Antifolate resistance): compound-typed entries whose name
    tokens carry the "dr:" (drug) prefix, exercising the drug branch of
    _parse_kegg_ref/KEGG_PREFIX_TO_ENTITY_TYPE.

Run with: python kegg_structure_tester.py   (from the repo root, so the
relative data/KGML/ path resolves and `src` is importable)
"""
import json
from collections import Counter
from pathlib import Path

from parsers.KEGG.KEGGCaller import KEGG_ETL
from src.models.kegg import Pathway, Entity, EntityPathMem, Interaction, ReactionP, PathwayKGMLRecord

SAMPLE_PATHWAYS = ["hsa04080", "hsa00670", "hsa01523"]
OUTPUT_DIR = Path("data/kgml_structure_test")

TABLE_EXTRACTORS = {
    Pathway.__table_name__: lambda r: [r.pathway],
    Entity.__table_name__: lambda r: r.entities,
    EntityPathMem.__table_name__: lambda r: r.entity_path_mem,
    Interaction.__table_name__: lambda r: r.relations,
    ReactionP.__table_name__: lambda r: r.reaction_participants,
}


def check_record(record: PathwayKGMLRecord) -> list[str]:
    """
    Referential-integrity checks mirroring what the SQL foreign keys on
    reaction_participants/EntityPathMem would enforce (entities.entity_id),
    plus a duplicate check -- entities are unioned from four separate
    sources in _parse_kgml_to_entry_map (kgml entries, the pathway itself,
    reaction-tag cross-refs, synthesized off-diagram compounds), so an
    overlap between any of them would double-stage the same entity.
    """
    issues = []

    entity_keys = Counter((e.entity_id, e.entity_type) for e in record.entities)
    dupes = {k: v for k, v in entity_keys.items() if v > 1}
    if dupes:
        issues.append(f"{len(dupes)} duplicate (entity_id, entity_type) pair(s) in entities: {list(dupes)[:5]}")

    entity_ids = {e.entity_id for e in record.entities}
    for rel in record.relations:
        if rel.source_id not in entity_ids:
            issues.append(f"interaction source_id {rel.source_id!r} has no matching entity")
        if rel.target_id not in entity_ids:
            issues.append(f"interaction target_id {rel.target_id!r} has no matching entity")

    for rp in record.reaction_participants:
        if rp.entity_id not in entity_ids:
            issues.append(f"reaction_participant entity_id {rp.entity_id!r} has no matching entity")

    for epm in record.entity_path_mem:
        if epm.entity_id not in entity_ids:
            issues.append(f"EntityPathMem entity_id {epm.entity_id!r} has no matching entity")

    long_ids = [e.entity_id for e in record.entities if len(e.entity_id) > 20]
    if long_ids:
        issues.append(f"{len(long_ids)} entity_id(s) exceed the entities.entity_id VARCHAR(20) column: {long_ids[:5]}")

    return issues


def main():
    kegg_caller = KEGG_ETL()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    summary = {}
    for pathway_id in SAMPLE_PATHWAYS:
        print(f"\n=== {pathway_id} ===")
        record = next(kegg_caller.parse_kgml_structure(pathway_id))

        tables = {
            table: [row.to_sql_staging_row() for row in extract(record)]
            for table, extract in TABLE_EXTRACTORS.items()
        }

        for table, rows in tables.items():
            print(f"  {table}: {len(rows)} row(s)")

        issues = check_record(record)
        if issues:
            print(f"  ISSUES ({len(issues)}):")
            for issue in issues:
                print(f"    - {issue}")
        else:
            print("  no consistency issues found")

        out_path = OUTPUT_DIR / f"{pathway_id}.json"
        out_path.write_text(json.dumps(tables, indent=2))
        print(f"  wrote {out_path}")

        summary[pathway_id] = {
            "title": record.pathway.description,
            "row_counts": {t: len(r) for t, r in tables.items()},
            "issues": issues,
        }

    summary_path = OUTPUT_DIR / "_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"\nSummary written to {summary_path}")


if __name__ == "__main__":
    main()
