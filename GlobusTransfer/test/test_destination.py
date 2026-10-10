"""Up-front checks of the source and destination paths.

These do not talk to Globus, so they are not marked `globus` and always run.
"""

import os
from types import SimpleNamespace

import globus_sdk
import pytest

from GlobusTransfer import GlobusTransfer
from GlobusTransfer.exceptions import (
    GlobusDestinationError,
    GlobusSourceError,
    ScopeOrSingleDomainError,
)


class FakeError(globus_sdk.TransferAPIError):
    """TransferAPIError without an HTTP response behind it."""

    info = None
    message = None

    def __init__(self, http_status, code, consent=None, domain=None):
        self.http_status = http_status
        self.code = code
        self.message = code
        self.info = SimpleNamespace(
            consent_required=consent, authorization_parameters=domain
        )


NOT_FOUND = (404, "ClientError.NotFound")
EXISTS = (502, "ExternalError.MkdirFailed.Exists")
DENIED = (403, "ExternalError.MkdirFailed.PermissionDenied")


class FakeTC:
    """operation_ls / operation_mkdir over a set of existing paths."""

    def __init__(self, existing, mkdir_error=None):
        self.existing = set(existing)
        self.mkdir_error = mkdir_error
        self.made = []

    def operation_ls(self, ep, path):
        if path not in self.existing:
            raise FakeError(*NOT_FOUND)
        return []

    def operation_mkdir(self, ep, path):
        if self.mkdir_error:
            raise FakeError(*self.mkdir_error)
        if str(os.path.dirname(path)) not in self.existing:
            raise FakeError(*NOT_FOUND)  # Globus mkdir is not recursive
        if path in self.existing:
            raise FakeError(*EXISTS)
        self.existing.add(path)
        self.made.append(path)


def globus(tc, dest="/archive/me/new/dir"):
    """GlobusTransfer without logging in."""
    g = GlobusTransfer.__new__(GlobusTransfer)
    g.ep_source, g.ep_dest, g.path_dest = "SRC", "DST", dest
    g.required_scopes, g.session_required_single_domain = [], None
    g.tc = tc
    return g


def test_existing_destination_is_left_alone():
    tc = FakeTC(
        {"/", "/archive", "/archive/me", "/archive/me/new", "/archive/me/new/dir"}
    )
    globus(tc).ensure_destination()
    assert tc.made == []


def test_missing_destination_is_created_with_parents():
    tc = FakeTC({"/", "/archive", "/archive/me"})
    globus(tc).ensure_destination()
    assert tc.made == ["/archive/me/new", "/archive/me/new/dir"]


def test_destination_created_meanwhile_is_fine():
    tc = FakeTC({"/", "/archive"}, mkdir_error=EXISTS)
    globus(tc).ensure_destination()  # no exception


def test_no_permission_fails_early_and_clearly():
    tc = FakeTC({"/", "/archive"}, mkdir_error=DENIED)
    with pytest.raises(GlobusDestinationError, match="No permission to create"):
        globus(tc).ensure_destination()


def test_other_mkdir_errors_are_reported():
    tc = FakeTC({"/"}, mkdir_error=(502, "EndpointError"))
    with pytest.raises(GlobusDestinationError, match="Could not create"):
        globus(tc).ensure_destination()


def test_source_not_visible_fails_early(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(GlobusSourceError, match="not found on the source"):
        globus(FakeTC(set())).check_source()


def test_source_visible(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    globus(FakeTC({str(tmp_path)})).check_source()


def test_consent_check_ignores_missing_path():
    """A missing destination is not an auth problem; no noisy error."""
    globus(FakeTC(set())).check_for_concent_required("DST", "/nope")


def test_consent_check_still_asks_for_consent():
    class TC:
        def operation_ls(self, ep, path):
            consent = SimpleNamespace(required_scopes=["scope-a"])
            raise FakeError(403, "ConsentRequired", consent=consent)

    g = globus(TC())
    with pytest.raises(ScopeOrSingleDomainError):
        g.check_for_concent_required("DST", "/archive")
    assert g.required_scopes == ["scope-a"]


def test_consent_check_raises_other_errors():
    class TC:
        def operation_ls(self, ep, path):
            raise FakeError(502, "EndpointError")

    with pytest.raises(globus_sdk.TransferAPIError):
        globus(TC()).check_for_concent_required("DST", "/archive")
