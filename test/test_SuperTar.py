import os
import pathlib
import shutil
import subprocess
import sys
from contextlib import ExitStack as does_not_raise
from pprint import pprint as pp
from unittest.mock import Mock

import pytest
from conftest import count_files_dir, count_lines_dir

sys.path.append(os.path.abspath('./'))

from archivetar import SuperTar


@pytest.mark.parametrize("kwargs,expex", [
           (
              {'filename': 'mytar.tar'},
               does_not_raise()
           ),
           (
              {},   # missing required filename= kwarg
               pytest.raises(BaseException)
           )
           ])
def test_SuperTar(kwargs, expex):
    with expex:
        tar = SuperTar(**kwargs)
        pp(tar._flags)

@pytest.mark.parametrize("kwargs,kresult,kwresult,expex", [
           (
              {'verbose': True, 'filename': 'mytar.tar'},
              ['tar', '--sparse', '--create', '--file', 'mytar.tar', '--verbose'],
              {'check': True},
               does_not_raise()
           ),
           (
              {'filename': 'mytar.tar'},
              ['tar', '--sparse', '--create','--file', 'mytar.tar'],
              {'check': True},
               does_not_raise())
           ])
@pytest.mark.xfail
def test_SuperTar_opts_addfromfile(monkeypatch, kwargs, kresult, kwresult, expex):
    mock = Mock(spec=subprocess)
    mock.return_value = 0

    monkeypatch.setattr(subprocess, "run", mock)

    # actual test code
    tar = SuperTar(**kwargs)
    tar.addfromfile('/does/not/exist')
    tar.invoke()
    kresult.append('--files-from=/does/not/exist')  # added from .addfromfile()
    mock.assert_called_once_with(kresult, **kwresult)


@pytest.mark.parametrize("kwargs,mreturn,expex", [
           ({'compress': 'GZIP'}, False, BaseException),  # GZIP requested but none found
           ({'compress': 'NOT REAL'}, '/usr/bin/gzip', BaseException),     # No real parser
           ])
def test_SuperTar_ops_comp(monkeypatch, kwargs, mreturn, expex):
    """check how compressions handlers behave when not found or not exist"""

    mock = Mock(spec=shutil)
    mock.return_value=mreturn
    monkeypatch.setattr(shutil, "which", mock)

    with pytest.raises(expex):
        tar = SuperTar(**kwargs)
