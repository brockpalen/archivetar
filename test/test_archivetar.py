import os
import pathlib
from contextlib import ExitStack as does_not_raise
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

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
@freeze_time("2017-05-21")
def test_build_list(kwargs, outcache, monkeypatch):
    """test build_list function inputs/output expected"""
    # fake dwalk
    mock_dwalk = MagicMock(spec=DWalk)
    monkeypatch.setattr(archivetar, "DWalk", mock_dwalk)

    mock_cwd = MagicMock()
    mock_cwd.return_value = pathlib.Path("hello")
    monkeypatch.setattr(archivetar.Path, "cwd", mock_cwd)

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


@pytest.fixture
def checksum_tree(tmp_path, monkeypatch):
    """A tar list with a file, links, an odd name and a vanished file."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data.txt").write_text("hello")
    (tmp_path / "trail.txt ").write_text("spaces")
    os.symlink("data.txt", "good-link")
    os.symlink("doesntexist", "dead-link")
    tar_list = tmp_path / "p-1.DONT_DELETE.txt"
    tar_list.write_text("data.txt\ntrail.txt \ngood-link\ndead-link\n")
    return tar_list


def manifest(path):
    return [line.split(" ", 1)[1] for line in path.read_text().splitlines()]


def test_checksum_skips_symlinks_including_dangling(checksum_tree):
    """tar stores links as links; a dead link must not fail the tar."""
    sha = archivetar.create_sha1_manifest_from_file(checksum_tree)
    assert manifest(sha) == ["data.txt", "trail.txt "]
    assert sha.read_text().startswith(
        "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d data.txt\n"
    )


def test_checksum_with_dereference_follows_good_links(checksum_tree):
    checksum_tree.write_text("data.txt\ngood-link\n")
    sha = archivetar.create_sha1_manifest_from_file(checksum_tree, dereference=True)
    assert manifest(sha) == ["data.txt", "good-link"]


def test_checksum_vanished_file_fails_unless_ignore_failed_read(checksum_tree):
    checksum_tree.write_text("data.txt\ngone.txt\n")
    with pytest.raises(FileNotFoundError):
        archivetar.create_sha1_manifest_from_file(checksum_tree)
    sha = archivetar.create_sha1_manifest_from_file(checksum_tree, skip_unreadable=True)
    assert manifest(sha) == ["data.txt"]
