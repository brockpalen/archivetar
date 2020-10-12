import logging
import os

import globus_sdk

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

        # Do OAuth flow and get tokens
        tokens = self.do_native_app_authentication()

        # most specifically, you want these tokens as strings
        TRANSFER_TOKEN = tokens["transfer.api.globus.org"]["access_token"]

        authorizer = globus_sdk.AccessTokenAuthorizer(TRANSFER_TOKEN)
        self.tc = globus_sdk.TransferClient(authorizer=authorizer)

        #  attempt to auto activate each endpoint so to not stop later in the flow
        self.endpoint_autoactivate(self.ep_source)
        self.endpoint_autoactivate(self.ep_dest)

    def do_native_app_authentication(self):
        """
        Does Native App Authentication Flow and returns tokens.
        """
        client = globus_sdk.NativeAppAuthClient(self._CLIENT_ID)
        client.oauth2_start_flow()

        authorize_url = client.oauth2_get_authorize_url()
        logging.info("Please go to this URL and login: \n{0}".format(authorize_url))

        auth_code = input("Please enter the code you get after login here: ").strip()
        token_response = client.oauth2_exchange_code_for_tokens(auth_code)

        return token_response.by_resource_server

    def endpoint_autoactivate(self, endpoint, if_expires_in=3600):
        """Use TransferClient.endpoint_autoactivate() to make sure the endpoint is question is active"""
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

    def add_item(self, source_path):
        """Add an item to send as part of the current bundle."""
        if not self.TransferData:
            # no prior TransferData object create a new one
            logging.info("No prior TransferData object found creating")
            self.TransferData = globus_sdk.TransferData(
                self.tc,
                self.ep_source,
                self.ep_dest,
                verify_checksum=True,
                label="GlobusTransfer-PY",
            )

        # add item
        logging.info(f"Source Path: {source_path}")

        # pathlib comes though as absolute we need just the relative string
        # then append that to the destimations path  eg:

        # cwd  /home/brockp
        # pathlib  /home/brockp/dir1/data.txt
        # result dir1/data.txt
        # Final Dest path: path_dest/dir1/data.txt
        relative_paths = os.path.relpath(source_path, os.getcwd())
        path_dest = f"{self.path_dest}/{str(relative_paths)}"
        logging.info(f"Dest Path: {path_dest}")

        self.TransferData.add_item(source_path, path_dest)

        # TODO check if threshold hit

    def submit_pending_transfer(self):
        """Submit actual transfer, could be called automatically or manually"""
        if not self.TransferData:
            # no current transfer queued up do nothing
            logging.debug("No current TransferData queued found")
            return None

        transfer = self.tc.submit_transfer(self.TransferData)
        logging.info(f"Submitted Transfer: {transfer['task_id']}")
        self.transfers.append(transfer)
