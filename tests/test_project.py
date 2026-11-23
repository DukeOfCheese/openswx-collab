"""The repository itself: versions that must agree, labels the forms use, files a public project needs."""
import re
import tomllib
from pathlib import Path

import openswx_collab_protocol
import openswx_collab_server
from openswx_collab_protocol import messages

ROOT = Path(__file__).resolve().parent.parent


def test_one_version_everywhere():
    versions = {name: tomllib.loads((ROOT / name / "pyproject.toml").read_text(encoding="utf-8"))["project"]
                for name in ("protocol", "server")}
    v = versions["server"]["version"]
    assert {p["version"] for p in versions.values()} == {v}
    assert openswx_collab_server.__version__ == v == openswx_collab_protocol.__version__
    # the server pins the exact protocol version it was tested with
    assert f"openswx-collab-protocol=={v}" in versions["server"]["dependencies"]
    assert openswx_collab_protocol.PROTOCOL_VERSION == messages.PROTOCOL_VERSION


def test_issue_forms_only_use_labels_that_exist():
    labels = set(re.findall(r"\{name: \"?([^,\"]+)\"?,", (ROOT / ".github/labels.yml").read_text(encoding="utf-8")))
    for form in (ROOT / ".github/ISSUE_TEMPLATE").glob("[0-9]-*.yml"):
        used = re.search(r"^labels: \[(.*)\]$", form.read_text(encoding="utf-8"), re.M)
        for label in re.findall(r'"([^"]+)"', used.group(1)):
            assert label in labels, (form.name, label)


def test_everything_here_is_apache_and_the_public_files_exist():
    for name in ("protocol", "server"):  # what openswx imports or talks to must stay permissive
        assert tomllib.loads((ROOT / name / "pyproject.toml").read_text(encoding="utf-8"))["project"]["license"] == "Apache-2.0"
    assert not (ROOT / "ee").exists()  # the commercial layer lives in its own, private repository
    for needed in ("LICENSE", "NOTICE", "README.md", "CONTRIBUTING.md", "SECURITY.md", "CODE_OF_CONDUCT.md",
                   "CHANGELOG.md", "AGENTS.md"):
        assert (ROOT / needed).is_file(), needed


def test_the_protocol_package_depends_on_nothing_but_pydantic():
    deps = tomllib.loads((ROOT / "protocol/pyproject.toml").read_text(encoding="utf-8"))["project"]["dependencies"]
    assert [re.split(r"[<>=!~ ]", d)[0] for d in deps] == ["pydantic"]


def test_the_release_notes_come_from_the_changelog():
    import importlib.util
    spec = importlib.util.spec_from_file_location("release_notes", ROOT / "tools" / "release_notes.py")
    rn = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(rn)
    v = openswx_collab_server.__version__
    text = rn.notes(f"v{v}")  # (every released version has its own CHANGELOG section)
    assert "## Run it" in text and f"openswx-collab-server:{v}" in text and "Unreleased" not in text
