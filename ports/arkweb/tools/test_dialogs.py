# /// script
# requires-python = ">=3.9"
# dependencies = ["pytest>=7", "hdc-py>=0.2.0", "pillow>=10"]
# ///
"""JS dialogs (`alert`/`confirm`/`prompt`) for the Servo ArkWeb backend.

Servo surfaces these via `show_embedder_control(EmbedderControl::SimpleDialog)`; the shim
bridges them to ACE's `OnAlertDialogByJS`/`OnConfirmDialogByJS`/`OnPromptDialogByJS` and
routes the user's response back through an `NWebJSDialogResult`.

These tests exercise the round-trip with **no app dialog handler installed** (arkweb-test
sets none), which is the case where ACE reports the dialog unhandled and the shim resolves
it so the page's script is never blocked: `alert()` acknowledges and continues, `confirm()`
returns false, `prompt()` returns null. The page runs the dialog at load and encodes the
outcome as its background colour (OCR-free pixel check). This verifies the full
Servo -> shim -> ACE -> shim -> Servo path is wired and non-blocking.

NOTE: a *handled* dialog (an app onConfirm/onAlert/onPrompt that shows UI and calls the
JsResult) exercises the ACE->result->Servo callback returning a real user choice; verifying
that needs dialog handlers added to the test app (and a HAP rebuild), tracked separately.
See conftest.py for the harness/provisioning notes.
"""

import time

import pytest

from conftest import data_url, is_green, is_red, sample


def _page(script: str) -> str:
    return (
        '<html><head><meta name=viewport content="width=device-width,initial-scale=1"></head>'
        '<body id=b style="margin:0;background:#bbbbdd">'
        f"<script>{script}</script></body></html>"
    )


# alert() has no return value: if it resolves, the script continues and paints green.
ALERT = _page("alert('a'); document.getElementById('b').style.background='#00cc00';")
# confirm() with no handler resolves to false -> red.
CONFIRM = _page(
    "document.getElementById('b').style.background = confirm('c') ? '#00cc00' : '#cc0000';"
)
# prompt() with no handler resolves to null -> green (null === null).
PROMPT = _page(
    "document.getElementById('b').style.background = (prompt('p','d') === null) ? '#00cc00' : '#cc0000';"
)

PROBE = (360, 600)


def test_alert_resolves_and_continues(launch, cap):
    launch(url=data_url(ALERT))
    time.sleep(3.0)
    assert is_green(sample(cap(), PROBE)), "alert() did not resolve -- the page's script stayed blocked"


def test_confirm_returns_result(launch, cap):
    launch(url=data_url(CONFIRM))
    time.sleep(3.0)
    assert is_red(sample(cap(), PROBE)), "confirm() (no handler) did not return false"


def test_prompt_returns_result(launch, cap):
    launch(url=data_url(PROMPT))
    time.sleep(3.0)
    assert is_green(sample(cap(), PROBE)), "prompt() (no handler) did not return null"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
