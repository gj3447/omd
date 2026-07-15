#!/usr/bin/env python3
"""CLI shim for :mod:`omd_server.overlap_pilot`."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from omd_server.overlap_pilot import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
