# run_model.py
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib.util
from nbclient import NotebookClient
from nbformat import read
import pandas as pd
from typing import Any, Dict, List


def _run_notebook(nb_path: Path) -> Dict[str, Any]:
    with nb_path.open() as f:
        nb = read(f, as_version=4)

    nb["metadata"]["kernelspec"] = {
        "display_name": "Python 3",
        "language": "python",
        "name": "python3",
    }

    NotebookClient(nb, timeout=600).execute()

    ns: dict = {}
    for cell in nb.cells:
        if cell.cell_type == "code":
            exec(cell.source, ns)

    return {"df": ns.get("df"), "flag": None}


def _run_py(py_path: Path) -> Dict[str, Any]:
    spec = importlib.util.spec_from_file_location(py_path.stem, py_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)          
    return {
        "df": getattr(mod, "df", None),
        "flag": getattr(mod, "up_indicator", None),
    }


def run_any(path: str) -> tuple[str, Dict[str, Any]]:
    p = Path(path)
    if p.suffix == ".ipynb":
        result = _run_notebook(p)
    elif p.suffix == ".py":
        result = _run_py(p)
    else:
        result = {"df": None, "flag": None}
    return p.name, result


def run_models_parallel(paths: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=len(paths)) as ex:
        for future in as_completed(ex.submit(run_any, p) for p in paths):
            name, data = future.result()
            out[name] = data
    return out
