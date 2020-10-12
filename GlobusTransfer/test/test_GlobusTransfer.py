import os
import pathlib
import sys

import pytest

sys.path.append(pathlib.Path(__file__).parent.parent)


from GlobusTransfer import GlobusTransfer

pytestmark = pytest.mark.globus


@pytest.fixture(scope="module")
def globus():
    """simple globus constructor"""
    # DEST_EP = 'fa67b5dc-1b2d-11e9-9835-0262a1f2f698'  # conflux
    SOURCE_EP = "e0370902-9f48-11e9-821b-02b7a92d8e58"  # greatlakes
    DEST_EP = SOURCE_EP
    globus = GlobusTransfer(SOURCE_EP, DEST_EP, "~")

    yield globus


@pytest.mark.skip
def test_ls(globus):
    globus.ls_endpoint()


def test_transfer(globus):
    """Create file in tmp_path and transfer it"""
    # save cwd to switch back
    cwd = os.getcwd()

    # setup test data
    test_file = pathlib.Path.home() / "tmp" / "test_file.txt"
    test_file.touch()

    # change to testing location that's global
    os.chdir(pathlib.Path.home())
    globus.add_item(test_file)
    globus.submit_pending_transfer()

    # change back
    os.chdir(cwd)
