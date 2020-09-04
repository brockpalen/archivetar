class SuperTarException(Exception):
    """
    SuperTar base exception class.
    """

    pass


class SuperTarMissmatchedOptions(SuperTarException):
    """
    Exception for user selected optoins that cannot be used together.
    """

    pass
