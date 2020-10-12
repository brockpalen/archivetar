import logging
import os
import subprocess  # nosec

from mpiFileUtils.exceptions import mpiFileUtilsError, mpirunError

logging.getLogger(__name__).addHandler(logging.NullHandler)


class mpiFileUtils:
    """wrapper class for github.io/hpc/mpifileutils"""

    def __init__(
        self,
        np=int(12),  # MPI ranks to start
        inst=False,  # path to mpiFileUtils install
        mpirun=False,
        umask=False,
        verbose=False,
    ):

        self.kwargs = {}

        if not mpirun:
            raise mpirunError("mpirun required")
        else:
            self.args = [mpirun]

        if umask:
            # set umask for call to subprocess
            self.kwargs["preexec_fn"] = lambda: os.umask(umask)

        self.args.append("--oversubscribe")
        self.args += ["-np", f"{np}"]

        self.inst = inst
        self.verbose = verbose  # save verbose for apply

    def apply(self):
        """execute wrapped application"""
        if self.verbose:
            self.args.append("--verbose")
        logging.debug(f"BLANK invoked as {self.args}")
        try:
            subprocess.run(self.args, check=True, **self.kwargs)  # nosec
        except Exception as e:
            logging.exception(f"Problem running: {self.args} and {e}")
            raise mpiFileUtilsError(f"Problems {e}")


class DRm(mpiFileUtils):
    """
    Wrapper for drm.
    
    progress int  seconds to print progress
    exe      str  alternative executable name
    dryrun   bool Don't run just print
    """

    def __init__(self, progress=False, exe="drm", dryrun=False, *kargs, **kwargs):
        super().__init__(*kargs, **kwargs)

        # add exeutable  before options
        # BaseClass ( mpirun -np ... ) SubClass (exe { exe options } )
        self.args.append(f"{self.inst}/bin/{exe}")

        if progress:
            self.args += ["--progress", str(progress)]

        if dryrun:
            self.args += ["--dryrun"]

    def scancache(self, cachein=False):
        """
        Pass in .cache file as input list to use in purge.

        cachein str/pathlib
        """
        if not cachein:
            logging.error("cachein required")
            raise mpiFileUtilsError("cache in required")
        else:
            self.args += ["--input", str(cachein)]

        self.apply()


class DWalk(mpiFileUtils):
    """wrapper for dwalk"""

    def __init__(
        self, sort=False, filter=False, progress=False, exe="dwalk", *kargs, **kwargs
    ):
        super().__init__(*kargs, **kwargs)

        # add exeutable  before options
        # BaseClass ( mpirun -np ... ) SubClass (exe { exe options } )
        self.args.append(f"{self.inst}/bin/{exe}")

        if sort:
            self.args += ["--sort", str(sort)]

        if filter:
            # can be many options -atime +60 -user user etc
            self.args += filter

        if progress:
            self.args += ["--progress", str(progress)]

    def scanpath(self, path=False, textout=False, cacheout=False):
        """walk a path on filesystem"""

        self._setoutput(textout=textout, cacheout=cacheout)

        if not path:
            logging.error(f"path: {path} not set/exist")
            raise mpiFileUtilsError(f"path: {path} not set/exist")
        else:
            self.args.append(path)

        # actually run it
        self.apply()

    def scancache(self, cachein=False, textout=False, cacheout=False):
        """pass cache file from prior scan"""
        if not cachein:
            logging.error("cachein required")
            raise mpiFileUtilsError("cache in required")
        else:
            self.args += ["--input", str(cachein)]

        self._setoutput(textout=textout, cacheout=cacheout)
        # actually run it
        self.apply()

    def _setoutput(self, textout=False, cacheout=False):
        """ stay DRY """
        if textout:
            self.args += ["--text-output", f"{textout}"]
            self.textout = textout
        else:
            self.textout = None

        if cacheout:
            self.args += ["--output", f"{cacheout}"]
            self.cacheout = cacheout
        else:
            self.cacheout = None
