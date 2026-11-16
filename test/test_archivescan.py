"""Tests for the standalone bin/archivescan script.

archivescan parses its arguments at import time, so it is exercised as a
subprocess run inside a temporary directory tree.
"""

import csv
import io
import json
import os
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


@pytest.fixture
def depth_tree(tmp_path):
    """A tree with subdirectories, a nested directory and a snapshot.

    Layout (sparse files)::

        top.txt                1 KiB
        raw/run.tar          300 MiB
        raw/deep/part.tar    200 MiB
        notes/a.txt, b.txt     4 KiB each
        empty/
        .snapshot/copy.tar     1 GiB  (never counted)

    Args:
        tmp_path: pytest temporary directory.

    Returns:
        Path to the populated directory.
    """
    files = {
        "top.txt": 1024,
        "raw/run.tar": 300 * 1024**2,
        "raw/deep/part.tar": 200 * 1024**2,
        "notes/a.txt": 4096,
        "notes/b.txt": 4096,
        ".snapshot/copy.tar": 1024**3,
    }
    for name, size in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as handle:
            handle.truncate(size)
    (tmp_path / "empty").mkdir()
    return tmp_path


def depth_rows(cwd, *args):
    """Run archivescan with --format json and return rows keyed by path.

    Args:
        cwd: Directory to scan.
        *args: Extra command line arguments.

    Returns:
        List of (path relative to cwd, row) in output order.
    """
    rows = json.loads(run_archivescan(cwd, "--format", "json", *args))
    return [(os.path.relpath(row["path"], cwd), row) for row in rows]


def test_max_depth_rows_roll_up(depth_tree):
    """-d 1 gives one row per top directory, each counting everything below."""
    rows = dict(depth_rows(depth_tree, "-d", "1"))
    assert set(rows) == {".", "raw", "notes", "empty"}
    assert rows["raw"]["dataden_files"] == 2
    assert rows["raw"]["dataden_bytes"] == 500 * 1024**2
    assert (rows["notes"]["cache_files"], rows["notes"]["cache_bytes"]) == (2, 8192)
    assert rows["empty"]["total_files"] == 0
    assert rows["empty"]["fraction_offline"] is None
    # the whole tree, including files directly in it, but not .snapshot
    assert rows["."]["total_files"] == 5
    assert rows["."]["total_bytes"] == 500 * 1024**2 + 1024 + 8192


def test_max_depth_two_includes_nested(depth_tree):
    """-d 2 adds the nested directory as its own row."""
    rows = dict(depth_rows(depth_tree, "-d", "2"))
    assert rows[os.path.join("raw", "deep")]["dataden_bytes"] == 200 * 1024**2
    assert rows["raw"]["dataden_bytes"] == 500 * 1024**2


@pytest.mark.parametrize(
    "sort,expected",
    [
        ("size", ["raw", "notes", "empty"]),
        ("files", ["notes", "raw", "empty"]),  # 2 files each: ties sort by name
        ("cache", ["notes", "empty", "raw"]),
        ("offline", ["raw", "notes", "empty"]),
        ("name", ["empty", "notes", "raw"]),
    ],
)
def test_max_depth_sort(depth_tree, sort, expected):
    """--sort orders the directory rows and the "." total stays last."""
    paths = [path for path, _ in depth_rows(depth_tree, "-d", "1", "--sort", sort)]
    assert paths == expected + ["."]


def test_max_depth_table(depth_tree):
    """The table uses du -h sizes, right-aligned, with the path last."""
    lines = run_archivescan(depth_tree, "-d", "1").splitlines()
    assert lines[0].split() == [
        "DATADEN_FILES",
        "DATADEN_SIZE",
        "CACHE_FILES",
        "CACHE_SIZE",
        "TOTAL_FILES",
        "TOTAL_SIZE",
        "OFFLINE%",
        "PATH",
    ]
    assert lines[1].split() == ["2", "500M", "0", "0", "2", "500M", "100.00", "./raw"]
    assert lines[3].split() == ["0", "0", "0", "0", "0", "0", "-", "./empty"]
    assert lines[-1].split()[-1] == "."
    # every column lines up on the right
    assert len({len(line) - len(line.split()[-1]) for line in lines}) == 1


def test_max_depth_table_no_header(depth_tree):
    """--no-header drops the header line of the directory table too."""
    lines = run_archivescan(depth_tree, "-d", "1", "--no-header").splitlines()
    assert len(lines) == 4
    assert "PATH" not in lines[0]


@pytest.mark.parametrize("output_format,delimiter", [("tsv", "\t"), ("csv", ",")])
def test_max_depth_delimited_bytes(depth_tree, output_format, delimiter):
    """Machine formats give exact bytes per directory, never du -h units."""
    output = run_archivescan(depth_tree, "-d", "1", "--format", output_format)
    rows = list(csv.reader(io.StringIO(output), delimiter=delimiter))
    assert rows[0] == [key.upper() for key in EXPECTED_KEYS]
    by_path = {os.path.relpath(row[0], depth_tree): row for row in rows[1:]}
    raw = dict(zip(EXPECTED_KEYS, by_path["raw"]))
    assert raw["dataden_bytes"] == str(500 * 1024**2)
    assert raw["total_bytes"] == str(500 * 1024**2)


def test_max_depth_zero_matches_summary(depth_tree):
    """-d 0 in tsv is the same row as the plain summary."""

    def without_seconds(output):
        return [line.rsplit("\t", 1)[0] for line in output.splitlines()]

    plain = run_archivescan(depth_tree, "--format", "tsv")
    depth_zero = run_archivescan(depth_tree, "--format", "tsv", "-d", "0")
    assert without_seconds(plain) == without_seconds(depth_zero)


def test_max_depth_negative(tmp_path):
    """A negative depth is rejected."""
    with pytest.raises(subprocess.CalledProcessError):
        run_archivescan(tmp_path, "-d", "-1")


@pytest.mark.parametrize(
    "num_bytes,expected",
    [
        (512, "512"),
        (1024, "1.0K"),
        (1536, "1.5K"),
        (10 * 1024 - 1, "10K"),
        (234 * 1024**2, "234M"),
        (1024**3 + 1, "1.1G"),
    ],
)
def test_human_size_like_du(tmp_path, num_bytes, expected):
    """Directory table sizes follow GNU du -h, rounded up."""
    (tmp_path / "dir").mkdir()
    with open(tmp_path / "dir" / "file", "wb") as handle:
        handle.truncate(num_bytes)
    lines = run_archivescan(tmp_path, "-d", "1", "--no-header").splitlines()
    assert lines[0].split()[5] == expected
