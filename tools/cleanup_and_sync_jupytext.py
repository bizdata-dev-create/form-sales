import json
import subprocess
import sys
from pathlib import Path


def remove_per_notebook_jupytext_metadata(notebooks_root: Path) -> int:
    removed_count = 0
    for ipynb_path in notebooks_root.rglob("*.ipynb"):
        try:
            with ipynb_path.open("r", encoding="utf-8") as f:
                nb = json.load(f)
            metadata = nb.get("metadata", {})
            if isinstance(metadata, dict) and "jupytext" in metadata:
                metadata.pop("jupytext", None)
                nb["metadata"] = metadata
                with ipynb_path.open("w", encoding="utf-8") as f:
                    json.dump(nb, f, ensure_ascii=False, indent=1)
                removed_count += 1
        except Exception as exc:
            print(f"[WARN] Skipped {ipynb_path} due to error: {exc}")
    return removed_count


def sync_with_jupytext(paths: list[Path]) -> tuple[int, int]:
    ok = 0
    failed = 0
    for p in paths:
        try:
            subprocess.run(["jupytext", "--sync", str(p)], check=True)
            ok += 1
        except subprocess.CalledProcessError as exc:
            print(f"[WARN] jupytext --sync failed for {p}: {exc}")
            failed += 1
    return ok, failed


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    notebooks_root = project_root / "notebooks" / "form_url_fetching_and_messege_writing"
    scripts_root = project_root / "src" / "form_url_fetch_msg_write_py"

    if not notebooks_root.exists():
        print(f"[ERROR] Notebooks root not found: {notebooks_root}")
        return 1

    # 1) Remove legacy per-notebook metadata that could override jupytext.toml
    removed = remove_per_notebook_jupytext_metadata(notebooks_root)
    print(f"Removed per-notebook jupytext metadata from {removed} notebook(s)")

    # 2) Sync notebooks -> scripts
    notebook_files = list(notebooks_root.rglob("*.ipynb"))
    n_ok, n_fail = sync_with_jupytext(notebook_files)
    print(f"Synced from notebooks: ok={n_ok}, failed={n_fail}")

    # 3) Sync scripts -> notebooks (if scripts root exists)
    s_ok = s_fail = 0
    if scripts_root.exists():
        script_files = list(scripts_root.rglob("*.py"))
        s_ok, s_fail = sync_with_jupytext(script_files)
        print(f"Synced from scripts: ok={s_ok}, failed={s_fail}")
    else:
        print(f"[INFO] Scripts root not found (skipped): {scripts_root}")

    return 0 if (n_fail == 0 and s_fail == 0) else 2


if __name__ == "__main__":
    sys.exit(main())



