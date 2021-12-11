import os
import pathlib
from contextlib import ExitStack as does_not_raise
from unittest.mock import MagicMock

import pytest

import archivetar
from archivetar import build_list, validate_prefix
from archivetar.archive_args import file_check, stat_check, unix_check
from archivetar.exceptions import ArchivePrefixConflict
from mpiFileUtils import DWalk


@pytest.mark.parametrize(
    "string,exception",
    [
        ("1", does_not_raise()),
        ("-1", does_not_raise()),
        ("+1", does_not_raise()),
        ("9999999", does_not_raise()),
        ("+9999999", does_not_raise()),
        ("-9999999", does_not_raise()),
        ("abc", pytest.raises(ValueError)),
        ("+ 1", pytest.raises(ValueError)),
        ("1 ", pytest.raises(ValueError)),
        (" 1 ", pytest.raises(ValueError)),
        (" 1", pytest.raises(ValueError)),
        (" +1", pytest.raises(ValueError)),
        ("1 2", pytest.raises(ValueError)),
        ("1.2", pytest.raises(ValueError)),
        ("+1.2", pytest.raises(ValueError)),
        ("a2", pytest.raises(ValueError)),
        ("$1", pytest.raises(ValueError)),
    ],
)
def test_stat_check(string, exception):
    """Test stat_check parse function for valid entries."""
    with exception:
        result = stat_check(string)
        print(result)


@pytest.mark.parametrize(
    "string,exception",
    [
        ("brockp", does_not_raise()),
        ("coe-brockp-turbo", does_not_raise()),
        ("%", pytest.raises(ValueError)),
        ("brockp%", pytest.raises(ValueError)),
        ("bro ckp", pytest.raises(ValueError)),
        ("brockp ", pytest.raises(ValueError)),
        (" brockp", pytest.raises(ValueError)),
    ],
)
def test_unix_check(string, exception):
    """Test validation of usernames and groupnames."""
    with exception:
        result = unix_check(string)
        print(result)


def test_file_check(tmp_path):
    """Make sure file check throw correct errors."""
    # bogus file
    f = tmp_path / "testfile.cache"

    # test it doesn't exist
    with pytest.raises(ValueError):
        file_check(f)

    # test it does exist
    f.touch()
    a = file_check(f)
    assert a == f  # nosec


@pytest.mark.parametrize(
    "kwargs,outcache",
    [
        ({"path": ".", "prefix": "brockp"}, "/tmp/brockp-2017-05-21-00-00-00.cache"),
        (
            {"path": ".", "prefix": "brockp", "savecache": "True"},
            "hello/brockp-2017-05-21-00-00-00.cache",
        ),
    ],
)
@pytest.mark.freeze_time("2017-05-21")
def test_build_list(kwargs, outcache, monkeypatch):
    """test build_list function inputs/output expected"""
    # fake dwalk
    mock_dwalk = MagicMock(spec=DWalk)
    monkeypatch.setattr(archivetar, "DWalk", mock_dwalk)

    mock_cwd = MagicMock()
    mock_cwd.return_value = pathlib.Path("hello")
    monkeypatch.setattr(archivetar.pathlib.Path, "cwd", mock_cwd)

    # doesn't work because you cant patch internals that are in C
    # use https://pypi.org/project/pytest-freezegun/
    # mock_datestr = MagicMock()
    # mock_datestr.return_value = 'my-fake-string'
    # monkeypatch.setattr(archivetar.datetime.datetime, "strftime", mock_datestr)

    path = build_list(**kwargs)
    print(mock_dwalk.call_args)
    print(path)
    assert str(path) == outcache


@pytest.mark.parametrize(
    "prefix,tarname,exexception",
    [
        ("myprefix", "box-archive-1.tar", does_not_raise()),
        ("myprefix", "myprefix-1.tar", pytest.raises(ArchivePrefixConflict)),
        ("myprefix", "myprefix-1.tar.gz", pytest.raises(ArchivePrefixConflict)),
        ("myprefix", "myprefix-1.tar.lz4", pytest.raises(ArchivePrefixConflict)),
        ("myprefix", "myprefix-100.tar", pytest.raises(ArchivePrefixConflict)),
    ],
)
def test_validate_prefix(tmp_path, prefix, tarname, exexception):
    """
    validate_prefix(prefix) protects against selected prefix conflicting

    eg  existing myprefix-1.tar  and would be selected by unarchivetar
    """

    os.chdir(tmp_path)
    tar = tmp_path / tarname
    tar.touch()

    with exexception:
        validate_prefix(prefix)
