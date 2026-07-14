from __future__ import annotations

import time
from pathlib import Path

import pytest

from omd_server.v2.repo.errors import GitExecutionError, GitTimeoutError
from omd_server.v2.repo.git_process import GitResult, checked, run_bounded


@pytest.mark.parametrize(
    "script",
    [
        "sleep 2 &",
        "exec >/dev/null 2>&1; sleep 2",
    ],
)
def test_descendant_pipe_and_closed_pipe_cannot_bypass_timeout(
    tmp_path: Path, script: str
) -> None:
    started = time.monotonic()
    with pytest.raises(GitTimeoutError):
        run_bounded(
            ["/bin/sh", "-c", script],
            cwd=tmp_path,
            env={"PATH": "/usr/bin:/bin"},
            timeout_s=0.1,
            output_limit=1024,
        )

    assert time.monotonic() - started < 0.8


def test_git_error_diagnostics_escape_terminal_controls() -> None:
    result = GitResult(1, b"", b"bad\n\x1b[31mred\x1b[0m\tpath\n")

    with pytest.raises(GitExecutionError) as caught:
        checked(result, "fixed operation")

    detail = str(caught.value)
    assert "\\n" in detail and "\\x1b" in detail and "\\t" in detail
    assert "\n" not in detail and "\x1b" not in detail and "\t" not in detail
