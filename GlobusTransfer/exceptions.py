class GlobusError(Exception):
    """Globus base exception class."""

    pass


class GlobusTransferConflict(GlobusError):
    """Transfer conflicts with an existing incomplete Globus transfer."""

    def __init__(self, err):
        self.original = err
        message = (
            "Globus refused to submit this transfer because an identical "
            "source/destination path set has not completed yet. Wait for the "
            "existing Globus task to finish or cancel it before rerunning "
            "archivetar."
        )
        super().__init__(message)


class GlobusFailedTransfer(GlobusError):
    """Transfer failed or was canceled."""

    def __init__(self, status):
        """
        Messy hack, picling the exception and re-raising it causes error,
        Checking if already a string and pass rather than building from results dict.
        """
        if isinstance(status, str):
            super().__init__(status)
        else:
            self.message = f"Task: {status['label']} with id: {status['task_id']}"
            super().__init__(self.message)


class ScopeOrSingleDomainError(GlobusError):
    """Auth found missing scope or single_domain requirement"""

    pass
