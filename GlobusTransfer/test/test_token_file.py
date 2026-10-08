"""Token file permission handling (issue #49).

These do not talk to Globus, so they are not marked `globus` and always run.
"""

import json
import os
import stat

import pytest

from GlobusTransfer import check_token_file, write_token_file


@pytest.fixture
def shared_globus_dir(tmp_path):
    """~/.globus as the Globus CLI leaves it: group writable 775."""
    d = tmp_path / ".globus"
    d.mkdir()
    d.chmod(0o775)
    return d


def test_write_creates_user_only_file(shared_globus_dir):
    token_file = shared_globus_dir / "tokens.json"
    old_umask = os.umask(0o002)  # common HPC umask; must not widen the file
    try:
        write_token_file(token_file, {"refresh_token": "abc"})
    finally:
        os.umask(old_umask)

    assert stat.S_IMODE(token_file.stat().st_mode) == 0o600
    assert json.loads(token_file.read_text()) == {"refresh_token": "abc"}


def test_check_accepts_shared_dir_with_private_file(shared_globus_dir):
    """The bug: a 775 ~/.globus must not stop archivetar."""
    token_file = shared_globus_dir / "tokens.json"
    write_token_file(token_file, {})

    check_token_file(token_file)  # no exception


@pytest.mark.parametrize("perms", [0o644, 0o664, 0o640, 0o606])
def test_check_tightens_permissive_file(shared_globus_dir, perms, caplog):
    """Files written by older versions with the umask get fixed, with a warning."""
    token_file = shared_globus_dir / "tokens.json"
    token_file.write_text("{}")
    token_file.chmod(perms)

    check_token_file(token_file)

    assert stat.S_IMODE(token_file.stat().st_mode) == 0o600
    assert "too open" in caplog.text


def test_check_missing_file_raises_filenotfound(shared_globus_dir):
    """Caller relies on FileNotFoundError to start a fresh login."""
    with pytest.raises(FileNotFoundError):
        check_token_file(shared_globus_dir / "tokens.json")
