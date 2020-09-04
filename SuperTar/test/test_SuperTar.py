import logging
import os
import shutil
import subprocess
import tarfile
from contextlib import ExitStack as does_not_raise
from pathlib import Path
from pprint import pprint as pp
from unittest.mock import Mock

import pytest
from conftest import count_files_dir

from SuperTar import SuperTar, what_comp
from SuperTar.exceptions import SuperTarMissmatchedOptions


@pytest.mark.parametrize(
    "kwargs,expex",
    [
        ({"filename": "mytar.tar"}, does_not_raise()),
        ({}, pytest.raises(BaseException)),  # missing required filename= kwarg
    ],
)
def test_SuperTar(kwargs, expex):
    with expex:
        tar = SuperTar(**kwargs)
        pp(tar._flags)


@pytest.mark.parametrize(
    "kwargs,kresult,kwresult,expex",
    [
        (
            {"verbose": True, "filename": "mytar.tar"},
            ["tar", "--sparse", "--create", "--file", "mytar.tar", "--verbose"],
            {"check": True},
            does_not_raise(),
        ),
        (
            {"filename": "mytar.tar"},
            ["tar", "--sparse", "--create", "--file", "mytar.tar"],
            {"check": True},
            does_not_raise(),
        ),
    ],
)
@pytest.mark.xfail
def test_SuperTar_opts_addfromfile(monkeypatch, kwargs, kresult, kwresult, expex):
    mock = Mock(spec=subprocess)
    mock.return_value = 0

    monkeypatch.setattr(subprocess, "run", mock)

    # actual test code
    tar = SuperTar(**kwargs)
    tar.addfromfile("/does/not/exist")
    tar.archive()
    kresult.append("--files-from=/does/not/exist")  # added from .addfromfile()
    mock.assert_called_once_with(kresult, **kwresult)


@pytest.mark.parametrize(
    "kwargs,mreturn,expex",
    [
        ({"compress": "GZIP"}, False, BaseException),  # GZIP requested but none found
        ({"compress": "NOT REAL"}, "/usr/bin/gzip", BaseException),  # No real parser
    ],
)
def test_SuperTar_ops_comp(monkeypatch, kwargs, mreturn, expex):
    """check how compressions handlers behave when not found or not exist"""

    mock = Mock(spec=shutil)
    mock.return_value = mreturn
    monkeypatch.setattr(shutil, "which", mock)

    with pytest.raises(expex):
        SuperTar(**kwargs)


@pytest.mark.parametrize(
    "infile,expcomp",
    [
        ("testfile.tar.gz", "GZIP"),
        ("testfile.tar.GZ", "GZIP"),  # Check mixed case
        ("testfile.tar.tgz", "GZIP"),
        ("testfile.tar.bz2", "BZ2"),
        ("testfile.tar.xz", "XZ"),
        ("testfile.tar.lzma", "XZ"),
        ("testfile.tar.lz4", "LZ4"),
        ("testfile.tar", None),
    ],
)
def test_what_comp(tmp_path, infile, expcomp):
    """Check type of compressoin a given file uses"""
    filename = tmp_path / infile

    # create actual tar, only needed for last test
    tar = tarfile.open(filename, "w")
    tar.close()
    comptype = what_comp(filename)

    assert comptype == expcomp


def test_what_comp_not_tar(tmp_path):
    """Check we get exception if a nontar file is passed to what_comp()"""
    filename = tmp_path / "junkfile.tar"
    with open(filename, "a") as f:
        f.write("test data but not a real tar!")

    with pytest.raises(BaseException, match=r"has unknown compression or not tar file"):
        what_comp(filename)


@pytest.fixture
def junk_tar(tmp_path):
    """Extract a tar file"""

    # set CWD
    os.chdir(tmp_path)
    filename = Path("junktar.tar.gz")
    tar = tarfile.open(filename, "w:gz")

    # add some fake files
    a = tmp_path / "a"
    a.touch()
    tar.add("a")
    b = tmp_path / "b"
    b.touch()
    tar.add("b")

    # create test tar w 2 files
    tar.close()

    # remove files
    a.unlink()
    b.unlink()

    return filename.resolve()


def test_SuperTar_extract(tmp_path, junk_tar):
    """
    Basic tar extraction test nothing fancy just untar the example and count number of files after
    """
    os.chdir(tmp_path)
    # Try to extract
    st = SuperTar(filename=junk_tar, verbose=True)
    st.extract()

    num_files = count_files_dir(tmp_path)
    assert num_files == 3  # two files in tar + tar


@pytest.mark.parametrize(
    "keyword,cli,expex",
    [
        ({"keep_old_files": True}, "--keep-old-files", does_not_raise()),
        ({"skip_old_files": True}, "--skip-old-files", does_not_raise()),
        ({"keep_newer_files": True}, "--keep-newer-files", does_not_raise()),
    ],
)
def test_SuperTar_extract_preserve(tmp_path, junk_tar, caplog, keyword, cli, expex):
    """
    Tar extraction with multiple extract only options

    SuperTar.extract()  will log.DEBUG the flags check it's included
    """
    os.chdir(tmp_path)

    with caplog.at_level(logging.DEBUG):
        # Try to extract
        st = SuperTar(filename=junk_tar, verbose=True)

        with expex:
            st.extract(**keyword)

        # check option eg --keep-old-files is in log text
        assert cli in caplog.text

        num_files = count_files_dir(tmp_path)
        assert num_files == 3  # two files in tar + tar


@pytest.mark.parametrize(
    "keyword,cli,expex",
    [
        (
            {"keep_newer_files": True, "skip_old_files": True},
            "--keep-newer-files",
            pytest.raises(SuperTarMissmatchedOptions),
        )
    ],
)
def test_SuperTar_extract_preserve_errors(
    tmp_path, junk_tar, caplog, keyword, cli, expex
):
    """
    Tar extraction with multiple extract only options

    SuperTar.extract()  will log.DEBUG the flags check it's included
    """
    os.chdir(tmp_path)

    with caplog.at_level(logging.DEBUG):
        # Try to extract
        st = SuperTar(filename=junk_tar, verbose=True)

        with expex:
            st.extract(**keyword)

        # check option eg --keep-old-files is in log text
        #  assert cli in caplog.text  #  can't use as exception thrown

        num_files = count_files_dir(tmp_path)
        assert num_files == 1  # no untar, origonal only
