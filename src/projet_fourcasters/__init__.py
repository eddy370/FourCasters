"""Commande du projet, à lancer depuis la racine du dépôt."""

from pathlib import Path
import runpy


def main() -> None:
    script = Path.cwd() / "run_pipeline.py"
    if not script.is_file():
        raise SystemExit("Lancer cette commande depuis la racine du dépôt FourCasters.")
    runpy.run_path(str(script), run_name="__main__")
