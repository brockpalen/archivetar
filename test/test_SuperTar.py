import pytest, shutil, pathlib, sys, os, subprocess
from pprint import pprint as pp
from unittest.mock import Mock
from contextlib import ExitStack as does_not_raise
from conftest import count_files_dir, count_lines_dir
sys.path.append(os.path.abspath('./'))

from archivetar import SuperTar


def test_SuperTar():
    tar = SuperTar()
    pp(tar._flags)

@pytest.mark.parametrize("kwargs,kresult,kwresult,expex", [
           ({'verbose': True}, ['tar', '--sparse', '--verbose'], {'check': True}, does_not_raise()),
           ({}, ['tar', '--sparse'], {'check': True}, does_not_raise())
           ])
def test_SuperTar_opts_addfromfile(monkeypatch, kwargs, kresult, kwresult, expex):
    mock = Mock(spec=subprocess)
    mock.return_value = 0

    monkeypatch.setattr(subprocess, "run", mock)

    # actual test code
    tar = SuperTar(**kwargs)
    tar.addfromfile('/does/not/exist')
    kresult.append('--files-from=/does/not/exist')
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
