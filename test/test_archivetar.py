import os
import pathlib
import sys
from unittest.mock import MagicMock

import pytest

sys.path.append(os.path.abspath('./'))

import archivetar
from archivetar import build_list
from mpiFileUtils import DWalk


@pytest.mark.parametrize("kwargs,outcache", [
    ({'path': '.', 'prefix': 'brockp'}, '/tmp/brockp-2017-05-21-00-00-00.cache'),
    ({'path': '.', 'prefix': 'brockp', 'savecache': 'True'}, 'hello/brockp-2017-05-21-00-00-00.cache'),
])
@pytest.mark.freeze_time('2017-05-21')
def test_build_list(kwargs, outcache, monkeypatch):
    """test build_list function inputs/output expected"""

    # fake dwalk
    mock_dwalk = MagicMock(spec=DWalk)
    monkeypatch.setattr(archivetar, "DWalk", mock_dwalk)

    mock_cwd = MagicMock()
    mock_cwd.return_value = pathlib.Path('hello')
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
