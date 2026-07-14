from __future__ import annotations

import json
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_v2_server_import_does_not_load_legacy_runtime_graph() -> None:
    script = """
import json
import sys

import omd_server.v2.server

legacy = [
    name
    for name in ("omd_server.core", "omd_server.gitio", "omd_server.fsm")
    if name in sys.modules
]
print(json.dumps(legacy))
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(completed.stdout) == []


def test_built_wheel_contains_v2_runtime(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    shutil.copy2(ROOT / "pyproject.toml", source_dir / "pyproject.toml")
    shutil.copy2(ROOT / "README.md", source_dir / "README.md")
    shutil.copytree(
        ROOT / "omd_server",
        source_dir / "omd_server",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    wheel_dir = tmp_path / "wheel"
    wheel_dir.mkdir()
    script = """
import sys
from setuptools import build_meta

wheel_name = build_meta.build_wheel(sys.argv[1])
print(f"WHEEL_PATH={wheel_name}")
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(wheel_dir)],
        cwd=source_dir,
        check=True,
        capture_output=True,
        text=True,
    )
    marker = next(
        line for line in completed.stdout.splitlines() if line.startswith("WHEEL_PATH=")
    )
    wheel_path = wheel_dir / marker.removeprefix("WHEEL_PATH=")

    with zipfile.ZipFile(wheel_path) as wheel:
        names = set(wheel.namelist())
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        entry_points_name = next(
            name for name in names if name.endswith(".dist-info/entry_points.txt")
        )
        metadata = wheel.read(metadata_name).decode()
        entry_points = wheel.read(entry_points_name).decode()

    expected = {
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "omd_server" / "v2").glob("*.py")
    }
    assert expected
    assert expected <= names
    assert "Version: 0.1.0a1" in metadata
    assert "Description-Content-Type: text/markdown" in metadata
    assert "omd-v2-lease = omd_server.v2.server:main" in entry_points
