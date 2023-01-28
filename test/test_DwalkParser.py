import os
import pathlib
from contextlib import ExitStack as does_not_raise
from unittest.mock import MagicMock

import pytest
from conftest import count_files_dir, count_lines_dir

from archivetar import DwalkLine, DwalkParser


@pytest.fixture
def example_data():
    """Example data setup"""
    tdata = (
        pathlib.Path(__file__).parent.absolute() / "data" / "ident-example-support.txt"
    )
    return tdata


@pytest.fixture
def parser(example_data):
    """test constructor with default options"""
    parser = DwalkParser(path=example_data)

    yield parser


@pytest.mark.parametrize("kwargs", ["", {"path": "/tmp/absolutegarbage.txta"}])
def test_DwalkParser_doesntexist(kwargs):
    """test invalid paths"""
    with pytest.raises(BaseException):
        DwalkParser(**kwargs)


@pytest.mark.parametrize(
    "kwargs,result,expex",
    [
        ({"minsize": 1e9}, 6, does_not_raise()),
        ({}, 2, does_not_raise()),  # default arg 10 GByte
        (
            {"minsize": 1e12},
            2,
            does_not_raise(),
        ),  # PB should only create 2 files (index + tar)
    ],
)
def test_DwalkParser_tarlist(parser, tmp_path, kwargs, result, expex):
    """test DwalkParser.tarlist()"""
    os.chdir(tmp_path)
    print(tmp_path)
    for count, index, tarlist in parser.tarlist(**kwargs):
        print(f"Index -> {index}")
        print(f"tarlist -> {tarlist}")

    assert result == count_files_dir(tmp_path)
    assert 69 * 2 == count_lines_dir(
        tmp_path
    )  # sample data has 69 lines * 2 (index + tar)


def test_DwalkParser_getpath(parser):
    """Test getting a path."""
    path = parser.getpath()
    path = next(path)  # advance generator
    print(path)
    assert (
        path
        == b"/scratch/support_root/support/bennet/haoransh/DDA_2D_60x70_kulow_1.batch\n"
    )  # nosec


@pytest.fixture
def test_DwalkLine(monkeypatch):

    s = b"-rwxr-xr-x mmiranda support   1.220 GB Mar  4 2020 15:58 /scratch/support_root/support/mmiranda/ouser/dmontiel/mg1/lib/libdeal_II.g.so"

    # patch out os.getcwd() to use that expected by test data
    mock_os_getcwd = MagicMock(spec=os.path)
    mock_os_getcwd.return_value = "/scratch/support_root/support"
    monkeypatch.setattr(os, "getcwd", mock_os_getcwd)

    line = DwalkLine(line=s)
    assert line.size == 1.220 * 1e9
    assert line.path == b"mmiranda/ouser/dmontiel/mg1/lib/libdeal_II.g.so"
    yield line


@pytest.mark.parametrize(
    "line,result,size",
    [
        (
            b"-rwxr-xr-x mmiranda support   1.220 GB Mar  4 2020 15:58 /scratch/support_root/support/mmiranda/ouser/dmontiel/mg1/lib/libdeal_II.g.so",
            b"mmiranda/ouser/dmontiel/mg1/lib/libdeal_II.g.so",
            1.220 * 1e9,
        ),
        (
            b"-rw-r--r-- joegrego okoues_root 875.000  B Jul 15 2020 12:55 /scratch/support_root/support/joegrego/CoreSequence/Data/0-9999/9000-9999/9800-9899/9810-9819/9814_Jm_2004-11-05_1/.AppleDouble/Icon\n",
            b"joegrego/CoreSequence/Data/0-9999/9000-9999/9800-9899/9810-9819/9814_Jm_2004-11-05_1/.AppleDouble/Icon\n",
            875,
        ),
    ],
)
def test_DwalkLine_parse(monkeypatch, line, result, size):

    s = line

    # patch out os.getcwd() to use that expected by test data
    mock_os_getcwd = MagicMock(spec=os.path)
    mock_os_getcwd.return_value = "/scratch/support_root/support"
    monkeypatch.setattr(os, "getcwd", mock_os_getcwd)

    line = DwalkLine(line=s)
    assert line.size == size
    assert line.path == result


@pytest.mark.parametrize(
    "kwargs,result,expex",
    [
        ({"units": b"B", "count": 909}, 909, does_not_raise()),
        ({"units": b"KB", "count": 1}, 1000, does_not_raise()),
        ({"units": b"MB", "count": 1}, 1000000, does_not_raise()),
        ({"units": b"GB", "count": 1}, 1e9, does_not_raise()),
        ({"units": b"TB", "count": 1}, 1e12, does_not_raise()),
        ({"units": b"KB", "count": 321.310}, 321310, does_not_raise()),  # fractional
        (
            {"units": "mB", "count": 1},
            1000000,
            pytest.raises(BaseException),
        ),  # case maters
    ],
)
def test_DwalkLine_normilizeunits(test_DwalkLine, kwargs, result, expex):
    with expex:
        count = test_DwalkLine._normilizeunits(**kwargs)
        assert count == result
