import logging
import shutil
import subprocess  # nosec

logging.getLogger(__name__).addHandler(logging.NullHandler)


def find_gzip():
    """find pigz if installed in PATH otherwise return gzip"""
    pigz = shutil.which("pigz")
    gzip = shutil.which("gzip")
    if pigz:
        return pigz
    elif gzip:
        return gzip
    else:
        raise Exception("gzip compression but no gzip or pigz found in PATH")


def find_bzip():
    """find pigz if installed in PATH otherwise return gzip"""
    lbzip2 = shutil.which("lbzip2")
    pbzip2 = shutil.which("pbzip2")
    bzip2 = shutil.which("bzip2")
    if lbzip2:
        return lbzip2
    elif pbzip2:
        return pbzip2
    elif bzip2:
        return bzip2
    else:
        raise Exception("gzip compression but no gzip or pigz found in PATH")


def find_lz4():
    """find pixz if installed in PATH otherwise return xz"""
    lz4 = shutil.which("lz4")
    if lz4:
        return lz4
    else:
        raise Exception("lzma/xz compression but no pixz or xz found in PATH")


def find_xz():
    """find pixz if installed in PATH otherwise return xz"""
    pixz = shutil.which("pixz")
    xz = shutil.which("xz")
    if pixz:
        return pixz
    elif xz:
        return xz
    else:
        raise Exception("lzma/xz compression but no pixz or xz found in PATH")


def find_lzma():
    """alias for find_xz()"""
    return find_xz()


def what_comp(filename):
    """
    Return what compression type based on file suffix passed.

    Currently based on suffix could be updated to be based on FileMagic
    Currently assumes input is a pathlib

    Current known versions:
    GZIP  .gz or .tgz
    BZ2   .bz2
    XZ    .xz or .lzma
    LZ4   .lz4
    None  None
    """

    # Grab current suffix, force lower case
    suffix = filename.suffix
    suffix = suffix.lower()

    # GZIP
    if suffix in [".gz", ".tgz"]:
        return "GZIP"
    elif suffix in [".bz2"]:
        return "BZ2"
    elif suffix in [".xz", ".lzma"]:
        return "XZ"
    elif suffix in [".lz4"]:
        return "LZ4"
    else:
        return None


class SuperTar:
    """ tar wrapper class for high speed """

    # requires gnu tar
    def __init__(
        self,
        filename=False,  # path to file eg output.tar
        compress=False,  # compress or not False | GZIP | BZ2 | LZ4
        verbose=False,  # print extra information when arching
        purge=False,  # pass --remove-files
        extract=False,  # pass -x default is create
    ):

        if not filename:  # filename needed  eg tar --file <filename>
            raise Exception("no filename given for tar")

        self.filename = filename

        if extract:
            # we are extracting an existing tar
            self._flags = ["tar", "--sparse", "--extract"]

        else:
            # we are creating a tar
            self._flags = ["tar", "--sparse", "--create"]

            # are we deleting as we go?
            if purge:
                self._flags.append("--remove-files")

        # if a compression option is given set the suffix (unused in extraction)
        # Set the compression program
        self.compsuffix = None
        if compress == "GZIP":
            self._flags.append(f"--use-compress-program={find_gzip()}")
            self.compsuffix = ".gz"
        elif compress == "BZ2":
            self._flags.append(f"--use-compress-program={find_bzip()}")
            self.compsuffix = ".bz2"
        elif compress == "XZ":
            self._flags.append(f"--use-compress-program={find_xz()}")
            self.compsuffix = ".xz"
        elif compress == "LZ4":
            self._flags.append(f"--use-compress-program={find_lz4()}")
            self.compsuffix = ".lz4"
        elif compress:
            raise Exception("Invalid Compressor {compress}")

        if verbose:
            self._flags.append("--verbose")
            self._verbose = True

    def addfromfile(self, path):
        """load list of files from file eg tar -cvf output.tar --files-from=<file>"""
        self._flags.append(f"--files-from={path}")

    def addfrompath(self, path):
        """load from fs path eg tar -cvf output.tar /path/to/tar"""
        pass

    def archive(self):
        """"actually kick off the tar"""
        if self.compsuffix:
            self.filename = f"{self.filename}{self.compsuffix}"

        self._flags += ["--file", self.filename]
        logging.debug(f"Tar invoked with: {self._flags}")
        subprocess.run(self._flags, check=True)  # nosec
