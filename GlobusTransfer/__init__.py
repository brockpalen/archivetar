import json
import logging
import os
import stat
from pathlib import Path, PurePosixPath

import globus_sdk
from globus_sdk.scopes import TransferScopes
from humanfriendly import format_size

from .exceptions import (
    GlobusDestinationError,
    GlobusFailedTransfer,
    GlobusSourceError,
    GlobusTransferConflict,
    ScopeOrSingleDomainError,
)

logging.getLogger(__name__).addHandler(logging.NullHandler)


def check_token_file(path):
    """Make sure the token file is user-only (600), fixing it if needed.

    Older archivetar versions wrote it with the user's umask (often 664 on
    HPC systems), so tighten it with a warning rather than refusing to run.
    Raises FileNotFoundError if it does not exist (caller starts a new login).
    """
    st = os.stat(path)
    if st.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        logging.warning(
            f"Permissions {stat.filemode(st.st_mode)} for {path} were too open, "
            "changed to -rw-------"
        )
        os.chmod(path, 0o600)


def write_token_file(path, tokens):
    """Write tokens as JSON readable only by the owner (600)."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(tokens, f)


class GlobusTransfer:
    """
    object of where / how to transfer data
    """

    def __init__(
        self,
        ep_source,
        ep_dest,
        path_dest,
        notify_on_succeeded=True,
        notify_on_failed=True,
        notify_on_inactive=True,
        fail_on_quota_errors=False,
        skip_source_errors=False,
        preserve_timestamp=False,
    ):
        """
        ep_source  Globus Collection/Endpoint Source Name
        ep_dest    Globus Collection/Endpoint Destination Name
        path_dest   Path on destination endpoint

        Other options see: https://globus-sdk-python.readthedocs.io/en/stable/services/transfer.html#globus_sdk.TransferData
        """

        self._CLIENT_ID = "8359fb34-39cf-410d-bd93-e8502aa68c46"
        self.ep_source = ep_source
        self.ep_dest = ep_dest
        self.path_dest = path_dest
        self.notify_on_succeeded = notify_on_succeeded
        self.notify_on_failed = notify_on_failed
        self.notify_on_inactive = notify_on_inactive
        # CANT USE multiple jobs will cause other files to be wiped out
        # self.delete_destination_extra = delete_destination_extra
        self.fail_on_quota_errors = fail_on_quota_errors
        self.skip_source_errors = skip_source_errors
        self.preserve_timestamp = preserve_timestamp
        self.session_required_single_domain = None  # used with HA collections
        self.TransferData = None  # start empty created as needed
        self.transfers = []

        """Create an authorizer to use with Globus Service Clients."""
        """
        Get globus tokens data.

        Create ~/.globus (700) if missing.  The directory may be shared with
        other tools (e.g. the Globus CLI creates it 775), so its permissions
        are not enforced; the token file itself must be user-only (600),
        the same rule ssh applies to private keys.  See issue #49.
        Try to load tokens
        Else start authorization
        """

        self.client = globus_sdk.NativeAppAuthClient(self._CLIENT_ID)
        self.required_scopes = []  # list of scopes for GCS5 collections

        save_path = Path.home() / ".globus"
        self.token_file = save_path / "tokens.json"

        if save_path.is_dir():  # exists and directory
            st = os.stat(save_path)
            logging.debug(
                f"{str(save_path)} exists permissions {stat.filemode(st.st_mode)}"
            )
        else:  # create ~/.globus
            logging.debug(f"Creating {str(save_path)}")
            save_path.mkdir(mode=0o700)

        try:  # try and read tokens from file else create and save
            check_token_file(self.token_file)
            with self.token_file.open() as f:
                tokens = json.load(f)

            authorizer = globus_sdk.RefreshTokenAuthorizer(
                tokens["refresh_token"],
                self.client,
                access_token=tokens["access_token"],
                expires_at=tokens["expires_at_seconds"],
                on_refresh=self._save_tokens,
            )
            self.tc = globus_sdk.TransferClient(authorizer=authorizer)
        except FileNotFoundError:
            self.tc = self.do_native_app_authentication()

        # keep checking until no exceptions
        clean = False
        while clean is False:
            try:
                # check our concent situation for GCS5 systems
                self.check_for_concent_required(self.ep_source, os.getcwd())
                self.check_for_concent_required(self.ep_dest, self.path_dest)
            except ScopeOrSingleDomainError as e:
                print(e)
                if self.required_scopes:
                    # we need to auth again asking for these scopes
                    print(
                        "\n"
                        "One of your endpoints requires consent in order to be used.\n"
                        "You must login a second time to grant consents.\n\n"
                    )
                    self.tc = self.do_native_app_authentication(
                        scopes=self.required_scopes
                    )

                if self.session_required_single_domain:
                    # we need to auth again asking for these scopes
                    print(
                        "\n"
                        "One of your endpoints requires domain constraints in order to be used.\n"
                        "You must login a second time to grant consents.\n\n"
                    )
                    self.tc = self.do_native_app_authentication(
                        session_required_single_domain=self.session_required_single_domain
                    )
            else:
                clean = True

        # find out now, not hours later when the first tar is ready, if the
        # source is visible and the destination is usable
        self.check_source()
        self.ensure_destination()

    def _save_tokens(self, tokens):
        """Save Globus auth tokens as required.

        Expects OAuthTokenResponse
        https://globus-sdk-python.readthedocs.io/en/stable/authorization.html#globus_sdk.RefreshTokenAuthorizer
        """

        # we only want transfer tokens
        tokens = tokens.by_resource_server["transfer.api.globus.org"]
        logging.debug(f"Saving tokens to {str(self.token_file)}")
        write_token_file(self.token_file, tokens)

    def do_native_app_authentication(
        self, scopes=TransferScopes.all, session_required_single_domain=None
    ):
        """
        Does Native App Authentication Flow and returns a transfer client.
        """

        self.client.oauth2_start_flow(refresh_tokens=True, requested_scopes=scopes)

        kwargs = {}
        # only pass session_required_single_domain if it's requested by the collection
        if self.session_required_single_domain:
            kwargs["session_required_single_domain"] = (
                self.session_required_single_domain
            )

        authorize_url = self.client.oauth2_get_authorize_url(**kwargs)
        print("\nPlease go to this URL and login: \n{0}".format(authorize_url))

        auth_code = input("\nPlease enter the code you get after login here: ").strip()
        tokens = self.client.oauth2_exchange_code_for_tokens(auth_code)
        self._save_tokens(tokens)
        tokens = tokens.by_resource_server["transfer.api.globus.org"]
        authorizer = globus_sdk.RefreshTokenAuthorizer(
            tokens["refresh_token"],
            self.client,
            access_token=tokens["access_token"],
            expires_at=tokens["expires_at_seconds"],
            on_refresh=self._save_tokens,
        )
        return globus_sdk.TransferClient(authorizer=authorizer)

    def check_for_concent_required(self, target, path):
        """
        Make sure our tokens have access before doing anything by listing path.

        target : UUID of collection / endpoint
        path : path to list

        GCS5 collections can require extra consent, and HA collections can
        require a single-domain session; both show up as errors on this ls.
        Those are recorded and ScopeOrSingleDomainError is raised so the
        caller logs in again with them, looping until none remain.

        A path that does not exist is not an access problem: it is ignored
        here and handled by check_source() and ensure_destination().  Any
        other error is raised.
        """
        try:
            self.tc.operation_ls(target, path=path)
        except globus_sdk.TransferAPIError as err:
            if err.info.consent_required:
                self.required_scopes.extend(err.info.consent_required.required_scopes)
                raise ScopeOrSingleDomainError("adding missing consent")
            if err.info.authorization_parameters:
                self.session_required_single_domain = (
                    err.info.authorization_parameters.session_required_single_domain
                )
                raise ScopeOrSingleDomainError("adding missing domain")
            if err.http_status == 404:
                return
            raise

    def check_source(self):
        """The current directory must be visible on the source collection.

        archivetar sends files by their absolute path, so if the source
        collection does not show this directory at the same path every
        transfer would fail.
        """
        path = os.getcwd()
        try:
            self.tc.operation_ls(self.ep_source, path=path)
        except globus_sdk.TransferAPIError as err:
            if err.http_status == 404:
                raise GlobusSourceError(
                    f"The current directory {path} was not found on the source "
                    f"collection {self.ep_source}. Check --source; archivetar "
                    "needs a collection that shows this directory at the same "
                    "path."
                ) from err
            raise

    def ensure_destination(self):
        """Create --destination-dir if it does not exist yet.

        Raises GlobusDestinationError right away if it cannot be created (for
        example no permission), instead of failing after the tars are built.
        """
        try:
            self.tc.operation_ls(self.ep_dest, path=str(self.path_dest))
            logging.debug(f"Destination {self.path_dest} exists")
            return
        except globus_sdk.TransferAPIError as err:
            if err.http_status != 404:
                raise
        logging.info(f"Destination {self.path_dest} does not exist, creating it")
        self._mkdir_parents(PurePosixPath(self.path_dest))

    def _mkdir_parents(self, path):
        """mkdir -p on the destination (Globus mkdir is not recursive)."""
        try:
            self.tc.operation_mkdir(self.ep_dest, path=str(path))
        except globus_sdk.TransferAPIError as err:
            if err.code == "ExternalError.MkdirFailed.Exists":
                return  # created by someone else meanwhile
            if err.http_status == 404 and path.parent != path:
                self._mkdir_parents(path.parent)  # parent missing too
                self._mkdir_parents(path)
                return
            if err.http_status == 403:
                raise GlobusDestinationError(
                    f"No permission to create {path} on the destination "
                    f"collection {self.ep_dest}. Check --destination-dir and "
                    "that you can write there."
                ) from err
            raise GlobusDestinationError(
                f"Could not create {path} on the destination collection "
                f"{self.ep_dest}: {err.message}"
            ) from err

    def ls_endpoint(self):
        """Just here for debug that globus is working."""
        for entry in self.tc.operation_ls(self.ep_source, path=self.path_source):
            print(entry["name"] + ("/" if entry["type"] == "dir" else ""))

    def task_wait(self, task_id, timeout=60, polling_interval=30):
        """Wait for task to finish."""
        while not self.tc.task_wait(
            task_id, timeout=timeout, polling_interval=polling_interval
        ):
            status = self.tc.get_task(task_id)
            print(
                f"Status: {status['status']} Task: {status['label']} TX: {format_size(status['bytes_transferred'])} Speed: {format_size(status['effective_bytes_per_second'])}/s TaskID: {task_id}"
            )

        status = self.tc.get_task(task_id)
        print(
            f"Status: {status['status']} Task: {status['label']} TX: {format_size(status['bytes_transferred'])} Speed: {format_size(status['effective_bytes_per_second'])}/s TaskID: {task_id}"
        )
        # if status is FAILED raise an exception
        if status["status"] == "FAILED":
            logging.debug(f"Failed Transfer status object: {status}")
            raise GlobusFailedTransfer(status)

    def add_item(self, source_path, label="PY", in_root=False):
        """Add an item to send as part of the current bundle."""
        if not self.TransferData:
            # no prior TransferData object create a new one
            logging.debug("No prior TransferData object found creating")

            # labels can only be letters, numbers, spaces, dashes, and underscores
            label = label.replace(".", "-")
            self.TransferData = globus_sdk.TransferData(
                self.ep_source,
                self.ep_dest,
                verify_checksum=True,
                label=f"archivetar {label}",
                notify_on_succeeded=self.notify_on_succeeded,
                notify_on_failed=self.notify_on_failed,
                notify_on_inactive=self.notify_on_inactive,
                fail_on_quota_errors=self.fail_on_quota_errors,
                skip_source_errors=self.skip_source_errors,
                preserve_timestamp=self.preserve_timestamp,
            )

        # add item
        logging.debug(f"Source Path: {source_path}")

        # pathlib comes though as absolute we need just the relative string
        # then append that to the destimations path  eg:

        # cwd  /home/brockp
        # pathlib  /home/brockp/dir1/data.txt
        # result dir1/data.txt
        # Final Dest path: path_dest/dir1/data.txt

        # UNLESS in_root=True then stick the file right in the root of destination
        if in_root:
            path_dest = Path(self.path_dest) / source_path.name
        else:
            relative_paths = os.path.relpath(source_path, os.getcwd())
            path_dest = Path(self.path_dest) / relative_paths

        logging.debug(f"Dest Path: {path_dest}")

        # convert PosixPath to string to avoid JSON serlizer issues
        self.TransferData.add_item(str(source_path), str(path_dest))

        # TODO check if threshold hit

    def submit_pending_transfer(self):
        """Submit actual transfer, could be called automatically or manually"""
        if not self.TransferData:
            # no current transfer queued up do nothing
            logging.debug("No current TransferData queued found")
            return None

        try:
            transfer = self.tc.submit_transfer(self.TransferData)
        except globus_sdk.TransferAPIError as err:
            details = str(err).lower()
            is_conflict = (
                getattr(err, "http_status", None) == 409
                or getattr(err, "code", None) == "Conflict"
                or ("409" in details and "conflict" in details)
            )
            if is_conflict and "identical paths" in details:
                raise GlobusTransferConflict(err) from err
            raise
        logging.debug(f"Submitted Transfer: {transfer['task_id']}")
        self.transfers.append(transfer)
        self.TransferData = None
        return transfer["task_id"]

    def task_successful_transfers(self, task_id):
        """
        Get data about each file transfered in the task.

        Paramter:
            task_id (str): Globus transfer ID to check on
        """

        for entry in self.tc.task_successful_transfers(task_id):
            yield entry
