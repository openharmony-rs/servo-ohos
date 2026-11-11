import contextlib
import io
import os
import sys
import tempfile
import textwrap
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import api_inventory


STUB_SOURCE = r'''
import re

FN = re.compile(
    r"(?s)^(?P<attrs>.*?)(?P<vis>pub )fn (?P<name>\w+)\s*\((?P<params>.*)\)"
    r"(?P<ret>\s*->\s*.*?)?;\s*$"
)


def skip_string(text, i):
    i += 1
    while text[i] != '"':
        i += 2 if text[i] == "\\" else 1
    return i + 1


def find_block_end(text, start):
    depth = 0
    i = start
    while True:
        char = text[i]
        if char == '"':
            i = skip_string(text, i)
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return i
        i += 1


def split_items(body):
    items = []
    depth = 0
    current = []
    i = 0
    while i < len(body):
        char = body[i]
        if char == '"':
            end = skip_string(body, i)
            current.append(body[i:end])
            i = end
            continue
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
        current.append(char)
        if char == ";" and depth == 0:
            items.append("".join(current))
            current = []
        i += 1
    assert not "".join(current).strip()
    return items


def param_names(params):
    names = []
    depth = 0
    current = ""
    for char in params + ",":
        if char in "([<{":
            depth += 1
        elif char in ")]>}":
            depth -= 1
        if char == "," and depth == 0:
            if current.strip():
                names.append(current.split(":", 1)[0].strip())
            current = ""
        else:
            current += char
    return names
'''


class ManifestVerifyTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = self.temporary.name
        self.bindgen = os.path.join(self.root, "mozjs", "upstream-bindgen")
        self.engine = os.path.join(self.root, "mozjs", "src", "engine")
        self.stub = os.path.join(self.root, "mozjs", "stub_bindings.py")
        self.manifest = os.path.join(self.root, "api-manifest.toml")
        self.write(self.stub, STUB_SOURCE)
        self.write(
            os.path.join(self.bindgen, "jsapi.rs"),
            """
            unsafe extern "C" {
                pub fn Existing(x: i32) -> i32;
            }
            """,
        )
        self.write(os.path.join(self.bindgen, "gluebindings.rs"), "")
        self.write_engine(2)

    def tearDown(self):
        self.temporary.cleanup()

    def write(self, path, source):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as file:
            file.write(textwrap.dedent(source))

    def write_engine(self, blank_lines, body="x"):
        self.write(
            os.path.join(self.engine, "object.rs"),
            "\n" * blank_lines
            + textwrap.dedent(
                f"""
                pub(crate) unsafe fn Existing(x: i32) -> i32 {{
                    {body}
                }}
                """
            ),
        )

    def generate(self):
        with contextlib.redirect_stdout(io.StringIO()):
            api_inventory.generate(
                types.SimpleNamespace(
                    bindgen_dir=self.bindgen,
                    engine_src=self.engine,
                    stub_bindings=self.stub,
                    output=self.manifest,
                    mozjs_version="0.26.14",
                    quickjs_revision="test",
                )
            )

    def verify(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            result = api_inventory.verify(
                types.SimpleNamespace(
                    manifest=self.manifest,
                    bindgen_dir=self.bindgen,
                    engine_src=self.engine,
                    stub_bindings=self.stub,
                )
            )
        return result, output.getvalue()

    def test_verify_accepts_a_fresh_manifest(self):
        self.generate()
        result, output = self.verify()
        self.assertEqual(result, 0)
        self.assertIn("manifest ok: 1 entries", output)

    def test_verify_ignores_moved_shim_lines(self):
        self.generate()
        self.write_engine(20)
        result, output = self.verify()
        self.assertEqual(result, 0, output)

    def test_verify_reports_a_new_bindgen_entry(self):
        self.generate()
        self.write(
            os.path.join(self.bindgen, "jsapi.rs"),
            """
            unsafe extern "C" {
                pub fn Existing(x: i32) -> i32;
                pub fn NewEntry(y: i32) -> i32;
            }
            """,
        )
        result, output = self.verify()
        self.assertEqual(result, 1)
        self.assertIn("+ NewEntry(1) [jsapi]: todo", output)

    def test_verify_reports_a_changed_status(self):
        self.generate()
        self.write_engine(2, body="")
        result, output = self.verify()
        self.assertEqual(result, 1)
        self.assertIn("~ Existing(1) [jsapi]: status implemented -> empty", output)

    def test_verify_ignores_renamed_parameters(self):
        self.generate()
        self.write(
            os.path.join(self.bindgen, "jsapi.rs"),
            """
            unsafe extern "C" {
                pub fn Existing(renamed: i32) -> i32;
            }
            """,
        )
        result, output = self.verify()
        self.assertEqual(result, 0, output)

    def test_verify_reports_a_changed_parameter_type(self):
        self.generate()
        self.write(
            os.path.join(self.bindgen, "jsapi.rs"),
            """
            unsafe extern "C" {
                pub fn Existing(x: u64) -> i32;
            }
            """,
        )
        result, output = self.verify()
        self.assertEqual(result, 1)
        self.assertIn("~ Existing(1) [jsapi]: signature", output)
        self.assertIn("recorded:  (i32) -> i32", output)
        self.assertIn("generated: (u64) -> i32", output)

    def test_verify_reports_a_changed_return_type(self):
        self.generate()
        self.write(
            os.path.join(self.bindgen, "jsapi.rs"),
            """
            unsafe extern "C" {
                pub fn Existing(x: i32) -> u64;
            }
            """,
        )
        result, output = self.verify()
        self.assertEqual(result, 1)
        self.assertIn("signature", output)
        self.assertIn("recorded:  (i32) -> i32", output)
        self.assertIn("generated: (i32) -> u64", output)

    def test_verify_reports_reordered_parameters_with_the_same_types(self):
        self.generate()
        self.write(
            os.path.join(self.bindgen, "jsapi.rs"),
            """
            unsafe extern "C" {
                pub fn Existing(x: i32, y: i32) -> i32;
            }
            """,
        )
        self.generate()
        self.write(
            os.path.join(self.bindgen, "jsapi.rs"),
            """
            unsafe extern "C" {
                pub fn Existing(x: i32, y: u64) -> i32;
            }
            """,
        )
        result, output = self.verify()
        self.assertEqual(result, 1)
        self.assertIn("recorded:  (i32, i32) -> i32", output)
        self.assertIn("generated: (i32, u64) -> i32", output)


if __name__ == "__main__":
    unittest.main()
