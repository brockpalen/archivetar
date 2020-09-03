import os
from pathlib import Path

import pytest

import archivetar.unarchivetar
from archivetar.unarchivetar import find_archives


@pytest.mark.parametrize(
    "prefix,suffix",
    [
        ("prefix", ""),
        ("prefix", ".bz2"),
        ("prefix-1234", ""),
        ("1234-1234", ""),
        ("1234-1234", ".gz"),
    ],
)
def test_find_archives(tmp_path, prefix, suffix):
    """
    Test find_archives().

    Several archives with same prefix
    Count number found in array

    Takes <prefix> finds all tars
    """
    # need to start in tmp_dir to matchin real usecases
    os.chdir(tmp_path)

    # create a few files
    a1 = Path(f"{prefix}-1.tar{suffix}")
    a10 = Path(f"{prefix}-10.tar{suffix}")
    a2 = Path(f"{prefix}-2.tar{suffix}")
    a33 = Path(f"{prefix}-33.tar{suffix}")
    a1.touch()
    a10.touch()
    a2.touch()
    a33.touch()

    tars = find_archives(prefix)

    assert len(tars) == 4
