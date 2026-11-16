"""Tests for the standalone bin/archivescan script.

archivescan parses its arguments at import time, so it is exercised as a
subprocess run inside a temporary directory tree.
"""

import csv
import io
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
    pattern = re.compile(r"^(.+):\nFiles: (\d+)\nSize: (.+)$", re.M)
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
    assert sections["Total"] == (3, "22 MiB")


def test_totals_empty_tree(tmp_path):
    """An empty tree reports zero totals instead of dividing by zero."""
    output = run_archivescan(tmp_path)
    assert parse_sections(output)["Total"] == (0, "0 bytes")
    assert "Fraction Offline: n/a (no files)" in output


def test_invalid_size(tmp_path):
    """A malformed size is rejected by argparse."""
    with pytest.raises(subprocess.CalledProcessError):
        run_archivescan(tmp_path, "-s", "lots")


def test_table_threshold_line(scan_tree):
    """The table names the threshold as globus-fs archivescan does."""
    output = run_archivescan(scan_tree, "-s", "1.5G")
    assert "Threshold: files larger than 1.5 GiB" in output


@pytest.mark.parametrize("num_bytes", [0, 1, 1023, 1024, 1536, 22 * 1024**2 + 1024])
def test_sizes_match_humanfriendly(tmp_path, num_bytes):
    """Sizes are formatted exactly like humanfriendly, as globus-fs prints them."""
    humanfriendly = pytest.importorskip("humanfriendly")
    with open(tmp_path / "file", "wb") as handle:
        handle.truncate(num_bytes)
    sections = parse_sections(run_archivescan(tmp_path))
    assert sections["Total"][1] == humanfriendly.format_size(num_bytes, binary=True)


EXPECTED_KEYS = [
    "path",
    "threshold_bytes",
    "dataden_files",
    "dataden_bytes",
    "cache_files",
    "cache_bytes",
    "total_files",
    "total_bytes",
    "fraction_offline",
    "seconds",
]


def test_json_output(scan_tree):
    """--format json prints one flat object with the globus-fs keys."""
    results = json.loads(run_archivescan(scan_tree, "--format", "json", "-s", "1M"))
    assert list(results) == EXPECTED_KEYS
    assert results["path"] == str(scan_tree)
    assert results["threshold_bytes"] == 1024**2
    assert (results["dataden_files"], results["dataden_bytes"]) == (2, 22 * 1024**2)
    assert (results["cache_files"], results["cache_bytes"]) == (1, 1024)
    assert (results["total_files"], results["total_bytes"]) == (3, 22 * 1024**2 + 1024)
    assert results["fraction_offline"] == round(100.0 * 22 * 1024 / (22 * 1024 + 1), 5)


def test_json_empty_tree(tmp_path):
    """An empty tree has no fraction offline."""
    results = json.loads(run_archivescan(tmp_path, "--format", "json"))
    assert results["total_files"] == 0
    assert results["fraction_offline"] is None


@pytest.mark.parametrize("output_format,delimiter", [("tsv", "\t"), ("csv", ",")])
def test_delimited_output(scan_tree, output_format, delimiter):
    """tsv and csv print an upper-case header and one row."""
    output = run_archivescan(scan_tree, "--format", output_format, "-s", "1M")
    rows = list(csv.reader(io.StringIO(output), delimiter=delimiter))
    assert rows[0] == [key.upper() for key in EXPECTED_KEYS]
    assert len(rows) == 2
    row = dict(zip(EXPECTED_KEYS, rows[1]))
    assert row["dataden_files"] == "2"
    assert row["total_bytes"] == str(22 * 1024**2 + 1024)


def test_delimited_no_header(scan_tree):
    """--no-header leaves only the data row, for loops over many directories."""
    output = run_archivescan(scan_tree, "--format", "tsv", "--no-header")
    assert len(output.splitlines()) == 1
    assert output.split("\t")[0] == str(scan_tree)


def test_delimited_empty_fraction(tmp_path):
    """No fraction offline is an empty field, not "None"."""
    output = run_archivescan(tmp_path, "--format", "csv", "--no-header")
    assert (
        next(csv.reader(io.StringIO(output)))[EXPECTED_KEYS.index("fraction_offline")]
        == ""
    )


@pytest.mark.parametrize("output_format", ["tsv", "csv", "json"])
@pytest.mark.parametrize("other_flag", ["--quiet", "--print", "--print-offline"])
def test_machine_formats_reject_stdout_flags(tmp_path, output_format, other_flag):
    """Machine formats refuse flags that would mix other text into stdout."""
    with pytest.raises(subprocess.CalledProcessError):
        run_archivescan(tmp_path, "--format", output_format, other_flag)


def test_quiet_table(scan_tree):
    """--quiet still silences the default table."""
    assert run_archivescan(scan_tree, "--quiet") == ""
