class GlobusError(BaseException):
    """Globus base exception class."""

    pass


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
