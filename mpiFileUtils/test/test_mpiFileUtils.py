import pathlib
import subprocess
import sys
from contextlib import ExitStack as does_not_raise

import pytest

sys.path.append(pathlib.Path(__file__).parent.parent)

from mpiFileUtils import DWalk, mpiFileUtils, mpirunError


@pytest.mark.parametrize(
    "kwargs,expex",
    [
        ({}, pytest.raises(mpirunError)),
        ({"mpirun": "/does/not/mpirun", "inst": "/my/install"}, does_not_raise()),
    ],
)
def test_mpiFileUtils(kwargs, expex):
    with expex:
        mpiFileUtils(**kwargs)


@pytest.mark.parametrize(
    "kwargs,expex",
    [
        ({}, pytest.raises(mpirunError)),
        ({"mpirun": "/does/not/mpirun", "inst": "/my/install"}, does_not_raise()),
    ],
)
def test_DWalk(kwargs, expex, monkeypatch, mock_subprocess):
    monkeypatch.setattr(subprocess, "run", mock_subprocess)
    with expex:
        dwalk = DWalk(**kwargs)
        dwalk.scanpath(path="/tmp", textout="/tmp/output.txt")
        args, kwargs = mock_subprocess.call_args
        print(f"org: {mock_subprocess.call_args}")
        print(f"args: {args}")
        print(f"kwargs: {kwargs}")

        # check that defaults are set
        assert "--oversubscribe" in mock_subprocess.call_args[0][0]
        assert "-np" in mock_subprocess.call_args[0][0]
        assert str(12) in mock_subprocess.call_args[0][0]
