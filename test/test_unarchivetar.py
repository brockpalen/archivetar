import os
import subprocess
from pathlib import Path

import pytest

import archivetar.unarchivetar
from archivetar.unarchivetar import find_prefix_files


@pytest.mark.parametrize(
    "prefix,suffix,args",
    [
        ("prefix", "tar", {}),
        ("prefix", "tar.bz2", {}),
        ("prefix-1234", "tar", {}),
        ("1234-1234", "tar", {}),
        ("1234-1234", "tar.gz", {}),
        ("1234-1234", "tar.lz4", {}),
        ("1234-1234", "tar.xz", {}),
        ("1234-1234", "tar.lzma", {}),
        ("1234-1234", "index.txt", {"suffix": "index.txt"}),
        ("1234-1234", "DONT_DELETE.txt", {"suffix": "DONT_DELETE.txt"}),
    ],
)
def test_find_prefix_files(tmp_path, prefix, suffix, args):
    """
    Test find_prefix_files().

    Several archives with same prefix
    Count number found in array

    Takes <prefix> finds all tars
    """
    # need to start in tmp_dir to matchin real usecases
    os.chdir(tmp_path)

    # create a few files
    a1 = Path(f"{prefix}-1.{suffix}")
    a10 = Path(f"{prefix}-10.{suffix}")
    a2 = Path(f"{prefix}-2.{suffix}")
    a33 = Path(f"{prefix}-33.{suffix}")
    a1.touch()
    a10.touch()
    a2.touch()
    a33.touch()

    tars = find_prefix_files(prefix, **args)

    assert len(tars) == 4


def test_archive_dir_extracts_into_cwd(tmp_path, monkeypatch):
    """--archive-dir finds tars elsewhere but extracts into the cwd."""
    src, tars, out = tmp_path / "src", tmp_path / "tars", tmp_path / "out"
    for d in (src, tars, out):
        d.mkdir()
    (src / "f.txt").write_text("hello")
    subprocess.run(["tar", "-cf", str(tars / "p-1.tar"), "f.txt"], cwd=src, check=True)

    monkeypatch.chdir(out)
    archivetar.unarchivetar.main(
        [
            "unarchivetar",
            "--prefix",
            "p",
            "--archive-dir",
            str(tars),
            "--tar-processes",
            "1",
        ]
    )
    assert (out / "f.txt").read_text() == "hello"
