"""Suite-wide isolation for the production default embedded lifecycle.

Most historical unit tests drive reconciliation deterministically through public
verbs and do not own Coordinator shutdown. Keep those tests inline-only so they
cannot leak hundreds of daemon writers or race a direct SQLite close. Dedicated
periodic-sweep tests explicitly restore a short default and bind the production
default-on contract.
"""

import pytest

from omd_server import core


@pytest.fixture(autouse=True)
def _isolate_embedded_background_default(monkeypatch):
    monkeypatch.setattr(core, "DEFAULT_EMBEDDED_SWEEP_INTERVAL", None)
