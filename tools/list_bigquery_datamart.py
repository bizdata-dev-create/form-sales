import argparse
import json
from typing import Any, Dict, List, Optional

from google.cloud import bigquery


def list_datasets(client: bigquery.Client, location: Optional[str]) -> List[bigquery.DatasetListItem]:
    return list(client.list_datasets()) if location is None else [
        d for d in client.list_datasets() if getattr(d, "location", None) in {location, location.upper(), location.lower()}
    ]


def get_table_schema_dict(schema: Optional[List[bigquery.SchemaField]]) -> List[Dict[str, Any]]:
    if not schema:
        return []
    result: List[Dict[str, Any]] = []
    for field in schema:
        result.append(
            {
                "name": field.name,
                "field_type": field.field_type,
                "mode": field.mode,
                "description": field.description,
                "fields": get_table_schema_dict(field.fields) if field.fields else [],
            }
        )
    return result


def build_inventory(
    project_id: str,
    location: Optional[str] = None,
) -> List[Dict[str, Any]]:
    client = bigquery.Client(project=project_id, location=location)

    inventory: List[Dict[str, Any]] = []
    for ds_item in list_datasets(client, location):
        ds_ref = bigquery.DatasetReference(project_id, ds_item.dataset_id)
        try:
            ds = client.get_dataset(ds_ref)
        except Exception as exc:  # noqa: BLE001
            inventory.append(
                {
                    "dataset": ds_item.dataset_id,
                    "dataset_location": getattr(ds_item, "location", None),
                    "error": f"dataset_fetch_failed: {exc}",
                }
            )
            continue

        tables: List[Dict[str, Any]] = []
        for tbl_item in client.list_tables(ds):
            tbl_ref = ds_ref.table(tbl_item.table_id)
            try:
                tbl = client.get_table(tbl_ref)
                tables.append(
                    {
                        "table": tbl.table_id,
                        "type": tbl.table_type,
                        "num_rows": int(tbl.num_rows or 0),
                        "partitioning_type": getattr(tbl, "time_partitioning", None).type_ if getattr(tbl, "time_partitioning", None) else None,
                        "partitioning_field": getattr(getattr(tbl, "time_partitioning", None), "field", None),
                        "clustering_fields": getattr(tbl, "clustering_fields", None),
                        "schema": get_table_schema_dict(tbl.schema),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                tables.append(
                    {
                        "table": tbl_item.table_id,
                        "error": f"table_fetch_failed: {exc}",
                    }
                )

        inventory.append(
            {
                "dataset": ds.dataset_id,
                "dataset_full_id": ds.full_dataset_id,
                "dataset_location": ds.location,
                "labels": ds.labels,
                "tables": tables,
            }
        )

    return inventory


def main() -> None:
    parser = argparse.ArgumentParser(description="List BigQuery datasets/tables/schemas for a project")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--location", default=None, help="Region/location filter, e.g., asia-northeast1")
    parser.add_argument("--out-json", default=None, help="Output JSON file path")
    parser.add_argument("--out-csv", default=None, help="Output CSV file path (flattened)")
    args = parser.parse_args()

    inventory = build_inventory(project_id=args.project, location=args.location)

    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(inventory, f, ensure_ascii=False, indent=2)
        print(f"Wrote JSON: {args.out_json}")

    if args.out_csv:
        try:
            import pandas as pd  # local optional
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(
                f"pandas is required for CSV output (--out-csv). Install and retry. detail={exc}"
            )

        rows: List[Dict[str, Any]] = []
        for ds in inventory:
            ds_name = ds.get("dataset")
            ds_loc = ds.get("dataset_location")
            for tbl in ds.get("tables", []):
                rows.append(
                    {
                        "dataset": ds_name,
                        "dataset_location": ds_loc,
                        "table": tbl.get("table"),
                        "type": tbl.get("type"),
                        "num_rows": tbl.get("num_rows"),
                        "partitioning_type": tbl.get("partitioning_type"),
                        "partitioning_field": tbl.get("partitioning_field"),
                        "clustering_fields": ",".join(tbl.get("clustering_fields", []) or []),
                        "schema_json": json.dumps(tbl.get("schema", []), ensure_ascii=False),
                        "error": tbl.get("error"),
                    }
                )

        df = pd.DataFrame(rows)
        df.to_csv(args.out_csv, index=False, encoding="utf-8")
        print(f"Wrote CSV: {args.out_csv}")

    if not args.out_json and not args.out_csv:
        print(json.dumps(inventory, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()




