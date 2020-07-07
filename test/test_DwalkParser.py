import os
import pathlib
import sys
from contextlib import ExitStack as does_not_raise
from pprint import PrettyPrinter as pp
from unittest.mock import MagicMock

import pytest
from conftest import count_files_dir, count_lines_dir

sys.path.append(os.path.abspath('./'))

from archivetar import DwalkLine, DwalkParser


@pytest.fixture
def example_data():
    """Example data setup"""
    tdata = pathlib.Path(__file__).parent.absolute() / "data" / "ident-example-support.txt"
    return tdata

@pytest.fixture
def parser(example_data):
    """test constructor with default options"""
    parser = DwalkParser(path=example_data)

    yield parser

@pytest.mark.parametrize("kwargs", ["", { 'path': '/tmp/absolutegarbage.txta'}])
def test_DwalkParser_doesntexist(kwargs):
    """test invalid paths"""   
    with pytest.raises(BaseException):
        parser = DwalkParser(**kwargs)


@pytest.mark.parametrize("kwargs,result,expex", [
           ({'minsize': 1e9}, 6, does_not_raise()),
           ({}, 2, does_not_raise()),                  # default arg 10 GByte
           ({'minsize': 1e12}, 2, does_not_raise()),   # PB should only create 2 files (index + tar)
           ])
def test_DwalkParser_tarlist(parser, tmp_path, kwargs, result, expex):
    """test DwalkParser.tarlist() """
    os.chdir(tmp_path)
    print(tmp_path)
    for index, tarlist in parser.tarlist(**kwargs):
        print(f"Index -> {index}")
        print(f"tarlist -> {tarlist}")

    assert ( result == count_files_dir(tmp_path) )
    assert ( 69*2 == count_lines_dir(tmp_path) )  # sample data has 69 lines * 2 (index + tar)
        

@pytest.fixture
def test_DwalkLine(monkeypatch):
 
    s = r"-rwxr-xr-x mmiranda support   1.220 GB Mar  4 2020 15:58 /scratch/support_root/support/mmiranda/ouser/dmontiel/mg1/lib/libdeal_II.g.so"
 
    # patch out os.getcwd() to use that expected by test data
    mock_os_getcwd = MagicMock(spec=os.path)
    mock_os_getcwd.return_value = '/scratch/support_root/support'
    monkeypatch.setattr(os, 'getcwd', mock_os_getcwd)
 
 
    line = DwalkLine(line=s)
    assert line.size == 1.220*1e9
    assert line.path == 'mmiranda/ouser/dmontiel/mg1/lib/libdeal_II.g.so'
    yield line

@pytest.mark.parametrize("kwargs,result,expex", [
           ({'units': 'B', 'count': 909}, 909, does_not_raise()),
           ({'units': 'KB', 'count': 1}, 1000, does_not_raise()),
           ({'units': 'MB', 'count': 1}, 1000000, does_not_raise()),
           ({'units': 'GB', 'count': 1}, 1e9, does_not_raise()),
           ({'units': 'TB', 'count': 1}, 1e12, does_not_raise()),
           ({'units': 'KB', 'count': 321.310}, 321310, does_not_raise()),    # fractional
           ({'units': 'mB', 'count': 1}, 1000000, pytest.raises(BaseException)) # case maters
           ])
def test_DwalkLine_normilizeunits(test_DwalkLine, kwargs, result, expex):
   with expex:
       count = test_DwalkLine._normilizeunits(**kwargs)
       assert count == result
