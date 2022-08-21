import json
import logging
import os
import stat
from pathlib import Path

import globus_sdk
from humanfriendly import format_size

from .exceptions import GlobusFailedTransfer

logging.getLogger(__name__).addHandler(logging.NullHandler)


class GlobusTransfer:
    """
    object of where / how to transfer data
    """

    def __init__(self, ep_source, ep_dest, path_dest):
        """
        ep_source  Globus Collection/Endpoint Source Name
        ep_dest    Globus Collection/Endpoint Destination Name
        path_dest   Path on destination endpoint
        """

        self._CLIENT_ID = "8359fb34-39cf-410d-bd93-e8502aa68c46"
        self.ep_source = ep_source
        self.ep_dest = ep_dest
        self.path_dest = path_dest
        self.TransferData = None  # start empty created as needed
        self.transfers = []

        authorizer = self.get_authorizer()

        self.tc = globus_sdk.TransferClient(authorizer=authorizer)

        #  attempt to auto activate each endpoint so to not stop later in the flow
        self.endpoint_autoactivate(self.ep_source)
        self.endpoint_autoactivate(self.ep_dest)

    def get_authorizer(self):
        """Create an authorizer to use with Globus Service Clients."""
        client, tokens = self.get_tokens()
        # most specifically, you want these tokens as strings
        refresh_token = tokens["refresh_token"]

        authorizer = globus_sdk.RefreshTokenAuthorizer(refresh_token, client)
        return authorizer

    def get_tokens(self):
        """
        Get globus tokens data.

        Check if  ~/.globus exists else create
        If it exists check permissions are user only
        If overly permissive bail
        Try to load tokens
        Else start authorization
        """
        client = globus_sdk.NativeAppAuthClient(self._CLIENT_ID)
        client.oauth2_start_flow(refresh_tokens=True)

        save_path = Path.home() / ".globus"
        token_file = save_path / "tokens.json"

        if save_path.is_dir():  # exists and directory
            st = os.stat(save_path)
            logging.debug(f"{str(save_path)} exists permissions {st.st_mode}")
            if bool(st.st_mode & stat.S_IRWXO):
                raise Exception("~/.globus is world readable and to permissive set 700")
            if bool(st.st_mode & stat.S_IRWXG):
                raise Exception("~/.globus is group readable and to permissive set 700")
        else:  # create ~/.globus
            logging.debug(f"Creating {str(save_path)}")
            save_path.mkdir(mode=0o700)

        try:  # try and read tokens from file else create and save
            with token_file.open() as f:
                return client, json.load(f)
        except FileNotFoundError:
            tokens = self.do_native_app_authentication(client)
            tokens = tokens["transfer.api.globus.org"]
            with token_file.open("w") as f:
                logging.debug("Saving tokens to {str(token_file)}")
                json.dump(tokens, f)
                return client, tokens

    def do_native_app_authentication(self, client):
        """Does Native App Authentication Flow and returns tokens."""
        authorize_url = client.oauth2_get_authorize_url()
        print("Please go to this URL and login: \n{0}".format(authorize_url))

        auth_code = input("Please enter the code you get after login here: ").strip()
        token_response = client.oauth2_exchange_code_for_tokens(auth_code)

        return token_response.by_resource_server

    def endpoint_autoactivate(self, endpoint, if_expires_in=3600):
        """Use TransferClient.endpoint_autoactivate() to make sure the endpoint is question is active."""
        # attempt to auto activate if fail prompt to activate
        r = self.tc.endpoint_autoactivate(endpoint, if_expires_in=if_expires_in)
        while r["code"] == "AutoActivationFailed":
            print(
                "Endpoint requires manual activation, please open "
                "the following URL in a browser to activate the "
                "endpoint:"
            )
            print(f"https://app.globus.org/file-manager?origin_id={endpoint}")
            input("Press ENTER after activating the endpoint:")
            r = self.tc.endpoint_autoactivate(endpoint, if_expires_in=3600)

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
                self.tc,
                self.ep_source,
                self.ep_dest,
                verify_checksum=True,
                label=f"archivetar {label}",
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

        transfer = self.tc.submit_transfer(self.TransferData)
        logging.debug(f"Submitted Transfer: {transfer['task_id']}")
        self.transfers.append(transfer)
        return transfer["task_id"]
