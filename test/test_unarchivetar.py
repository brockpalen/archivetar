import os
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
