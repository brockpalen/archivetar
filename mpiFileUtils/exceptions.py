
class mpiFileUtilsError(BaseException):
    """Base Exception Class for Module"""
    def __init__(self, *kargs, **kwargs):
        super().__init__(*kargs, **kwargs)


class mpirunError(mpiFileUtilsError):
    """problem with mpirun option given"""
    pass
