class ArchiveTarException(Exception):
    """
    ArchiveTar base exception class.
    """

    pass


class ArchivePrefixConflict(ArchiveTarException):
    """
    Selected prefix conflicts with existing files
    """

    pass
