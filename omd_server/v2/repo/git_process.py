"""POSIX bounded subprocess runner used only by fixed Git argv builders."""

from __future__ import annotations

import os
import selectors
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from .errors import GitExecutionError, GitOutputLimitError, GitTimeoutError


@dataclass(frozen=True, slots=True)
class GitResult:
    returncode: int
    stdout: bytes
    stderr: bytes


def base_env(home: Path) -> dict[str, str]:
    return {
        "HOME": os.fspath(home),
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": os.defpath,
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_SYSTEM": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_ATTR_NOSYSTEM": "1",
    }


def run_bounded(
    argv: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    timeout_s: float = 30.0,
    output_limit: int = 2 * 1024 * 1024,
) -> GitResult:
    """Run one fixed argv without inheriting ambient process configuration."""

    process = subprocess.Popen(
        argv,
        cwd=os.fspath(cwd),
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    assert process.stdout is not None and process.stderr is not None
    os.set_blocking(process.stdout.fileno(), False)
    os.set_blocking(process.stderr.fileno(), False)
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ, "stdout")
    selector.register(process.stderr, selectors.EVENT_READ, "stderr")
    chunks: dict[str, list[bytes]] = {"stdout": [], "stderr": []}
    total = 0
    deadline = time.monotonic() + timeout_s

    try:
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise GitTimeoutError("Git command timed out")
            events = selector.select(min(remaining, 0.25))
            for key, _ in events:
                try:
                    data = os.read(key.fileobj.fileno(), 65536)
                except BlockingIOError:
                    continue
                if not data:
                    selector.unregister(key.fileobj)
                    continue
                chunks[key.data].append(data)
                total += len(data)
                if total > output_limit:
                    raise GitOutputLimitError(
                        f"Git output exceeded {output_limit} bytes"
                    )
        try:
            returncode = process.wait(timeout=max(0.01, deadline - time.monotonic()))
        except subprocess.TimeoutExpired as exc:
            raise GitTimeoutError("Git command timed out") from exc
    except (GitTimeoutError, GitOutputLimitError):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        raise
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        raise
    finally:
        selector.close()

    return GitResult(
        returncode=returncode,
        stdout=b"".join(chunks["stdout"]),
        stderr=b"".join(chunks["stderr"]),
    )


def checked(result: GitResult, operation: str) -> bytes:
    if result.returncode != 0:
        detail = (
            result.stderr[-1000:]
            .decode("utf-8", "backslashreplace")
            .encode("unicode_escape")
            .decode("ascii")
            .strip()
        )
        raise GitExecutionError(f"{operation} failed ({result.returncode}): {detail}")
    return result.stdout
