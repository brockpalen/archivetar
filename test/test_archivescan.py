"""Tests for the standalone bin/archivescan script.

archivescan parses its arguments at import time, so it is exercised as a
subprocess run inside a temporary directory tree.
"""

import json
import re
import subprocess  # nosec B404 - runs our own script with fixed arguments
import sys
from pathlib import Path

import pytest

ARCHIVESCAN = Path(__file__).resolve().parent.parent / "bin" / "archivescan"


def run_archivescan(cwd, *args):
    """Run archivescan in cwd and return its stdout.

    Args:
        cwd: Directory to scan (archivescan scans its working directory).
        *args: Extra command line arguments.

    Returns:
        The captured standard output as text.
    """
    result = subprocess.run(  # nosec B603 - fixed interpreter and script path
        [sys.executable, str(ARCHIVESCAN), *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def parse_sections(output):
    """Split archivescan output into {section: (files, size_text)}.

    Args:
        output: Text printed by archivescan.

    Returns:
        Dict keyed by section heading with (file count, size string) values.
    """
    sections = {}
    pattern = re.compile(r"^(.+):\nFiles: (\d+)\nSize: (\S+)$", re.M)
    for heading, files, size in pattern.findall(output):
        sections[heading] = (int(files), size)
    return sections


@pytest.fixture
def scan_tree(tmp_path):
    """Sparse files of 1 KiB, 2 MiB and 20 MiB.

    Args:
        tmp_path: pytest temporary directory.

    Returns:
        Path to the populated directory.
    """
    for name, size in (("small", 1024), ("medium", 2 * 1024**2), ("big", 20 * 1024**2)):
        with open(tmp_path / name, "wb") as handle:
            handle.truncate(size)
    return tmp_path


@pytest.mark.parametrize(
    "size_arg,expected_dataden,expected_cache",
    [
        ("1M", 2, 1),
        ("1MiB", 2, 1),
        ("10M", 1, 2),
        ("0.5G", 0, 3),
        ("2000", 2, 1),  # bare number is bytes
    ],
)
def test_size_option(scan_tree, size_arg, expected_dataden, expected_cache):
    """--size sets the Data Den / cache split."""
    sections = parse_sections(run_archivescan(scan_tree, "-s", size_arg))
    assert sections["Data Den Candidates"][0] == expected_dataden
    assert sections["Cache (Locker) Candidates"][0] == expected_cache


def test_migratesize_env_default(scan_tree, monkeypatch):
    """Without --size the MIGRATESIZE environment value (MB) is used."""
    monkeypatch.setenv("MIGRATESIZE", "1")
    sections = parse_sections(run_archivescan(scan_tree))
    assert sections["Data Den Candidates"][0] == 2


def test_size_overrides_env(scan_tree, monkeypatch):
    """--size wins over MIGRATESIZE."""
    monkeypatch.setenv("MIGRATESIZE", "1")
    output = run_archivescan(scan_tree, "--size", "10M")
    assert parse_sections(output)["Data Den Candidates"][0] == 1


def test_totals(scan_tree):
    """Total section sums both candidate groups."""
    sections = parse_sections(run_archivescan(scan_tree, "-s", "1M"))
    assert sections["Total"] == (3, "22.0MiB")


def test_totals_empty_tree(tmp_path):
    """An empty tree reports zero totals instead of dividing by zero."""
    sections = parse_sections(run_archivescan(tmp_path))
    assert sections["Total"] == (0, "0.0B")


def test_invalid_size(tmp_path):
    """A malformed size is rejected by argparse."""
    with pytest.raises(subprocess.CalledProcessError):
        run_archivescan(tmp_path, "-s", "lots")


def test_json_output(scan_tree):
    """--json prints a machine-readable summary with raw byte counts."""
    results = json.loads(run_archivescan(scan_tree, "--json", "-s", "1M"))
    assert results["threshold_bytes"] == 1024**2
    assert results["dataden"]["files"] == 2
    assert results["dataden"]["bytes"] == 22 * 1024**2
    assert results["cache"] == {"files": 1, "bytes": 1024, "size": "1.0KiB"}
    assert results["total"]["files"] == 3
    assert results["total"]["bytes"] == 22 * 1024**2 + 1024
    assert results["current_state"] is False


def test_json_empty_tree(tmp_path):
    """--json on an empty tree reports zeros."""
    results = json.loads(run_archivescan(tmp_path, "--json"))
    assert results["total"]["files"] == 0
    assert results["fraction_offline_percent"] == 0.0


@pytest.mark.parametrize("other_flag", ["--quiet", "--print", "--print-offline"])
def test_json_rejects_stdout_flags(tmp_path, other_flag):
    """--json refuses flags that would mix other text into stdout."""
    with pytest.raises(subprocess.CalledProcessError):
        run_archivescan(tmp_path, "--json", other_flag)
