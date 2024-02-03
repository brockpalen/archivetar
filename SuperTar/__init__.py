import logging
import shutil
import subprocess  # nosec
import tarfile

from SuperTar.exceptions import SuperTarMissmatchedOptions

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


def find_zstd():
    """find zstd if installed"""
    zstd = shutil.which("zstd")
    if zstd:
        return zstd
    else:
        raise Exception("zstd/zst compression but no zstd found in PATH")


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
    elif suffix in [".zst"]:
        return "ZSTD"

    # check that it's an actual tar file
    elif tarfile.is_tarfile(filename):
        # is a tar just without compression so continue
        return None
    else:
        # we don't know what this file is throw
        raise Exception(f"{filename} has unknown compression or not tar file")


class SuperTar:
    """tar wrapper class for high speed"""

    # requires gnu tar
    def __init__(
        self,
        filename=False,  # path to file eg output.tar
        compress=False,  # compress or not False | GZIP | BZ2 | LZ4
        verbose=False,  # print extra information when arching
        purge=False,  # pass --remove-files
        ignore_failed_read=False,  # pass --ignore-failed-read when creating files, does nothing on extract
        dereference=False,  # pass --dereference when creating files, does nothing on extract
        path=None,  # path to extract TODO: (not currently used for compress)
    ):

        if not filename:  # filename needed  eg tar --file <filename>
            raise Exception("no filename given for tar")

        self.filename = filename
        self._purge = purge
        self._compress = compress
        self._ignore_failed_read = ignore_failed_read
        self._dereference = dereference
        self._path = path

        # set inital tar options,
        self._flags = ["tar"]

        if verbose:
            self._flags.append("--verbose")
            self._verbose = True

    def _setComp(self, compress):
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
        elif compress == "ZSTD":
            self._flags.append(f"--use-compress-program={find_zstd()}")
            self.compsuffix = ".zst"
        elif compress:
            raise Exception("Invalid Compressor {compress}")

    def addfromfile(self, path):
        """Load list of files from file eg tar -cvf output.tar --files-from=<file>."""
        # check that were not told to use a path
        if self._path:
            raise Exception("cannot provide a path and use addfromfile()")

        self._flags.append(f"--files-from={path}")

    def addfrompath(self, path):
        """load from fs path eg tar -cvf output.tar /path/to/tar"""
        pass

    def archive(self):
        """actually kick off the tar"""
        # we are creating a tar
        self._flags += ["--create"]

        # set compression options suffix and program if set
        self._setComp(self._compress)

        # are we deleting as we go?
        if self._purge:
            self._flags.append("--remove-files")
        if self.compsuffix:
            self.filename = f"{self.filename}{self.compsuffix}"

        # are we ignoring files that are deleted before we run?
        if self._ignore_failed_read:
            self._flags.append("--ignore-failed-read")

        # grab what symlinks point to and not the links themselves
        if self._dereference:
            self._flags.append("--dereference")

        self._flags += ["--file", self.filename]
        logging.debug(f"Tar invoked with: {self._flags}")
        subprocess.run(self._flags, check=True)  # nosec

    def extract(
        self, skip_old_files=False, keep_old_files=False, keep_newer_files=False
    ):
        """Extract the tar listed"""
        # we are extracting an existing tar
        self._flags += ["--extract"]
        self._flags += ["--file", str(self.filename)]

        # These options must be exclusive, if more than one is given outcome is ambigous
        preserve_ops = 0
        if skip_old_files:  # --skip-old-files don't replace files that already exist
            self._flags += ["--skip-old-files"]
            preserve_ops += 1
        if (
            keep_old_files
        ):  # --keep-old-files don't replace files that already exist and error
            self._flags += ["--keep-old-files"]
            preserve_ops += 1
        if (
            keep_newer_files
        ):  # --keep-newer-files don't replace files that are newer than archive
            self._flags += ["--keep-newer-files"]
            preserve_ops += 1

        if preserve_ops > 1:
            raise SuperTarMissmatchedOptions(
                "skip_old_files keep_old_files keep_newer_files are exclusive options and cannot be combined"
            )

        # set compress program
        self._setComp(what_comp(self.filename))

        # add path last if set
        if self._path:
            self._flags.append(str(self._path))

        logging.debug(f"Tar invoked with: {self._flags}")

        try:
            subprocess.run(self._flags, check=True)  # nosec
        except Exception as e:
            logging.error(f"{e}")
