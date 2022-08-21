class ArchiveTarException(Exception):
    """ArchiveTar base exception class."""

    pass


class ArchivePrefixConflict(ArchiveTarException):
    """Selected prefix conflicts with existing files."""

    pass


class ArchiveTarArchiveError(ArchiveTarException):
    """Errors related to the archiving process, tar, Globus, etc."""

    pass


class TarError(ArchiveTarException):
    """
    Error during Tar Process
    """

    pass
