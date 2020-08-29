# Brock Palen
# brockp@umich.edu
# 7/2020
#
#  prep a directory for placement in dataden
#  process:
#    1. run mpiFileUtils / dwalk  (deafault sort in name / path order) all files < minsize
#    2. Take resulting list build tar lists by summing size until > tarsize (before compression)
#    3. Tar each list:  OR --dryrun create list with est size
#       a. Create Index file of contents
#       b. Optionally compress -z / -j  with gzip/pigz bzip/lbzip2 if installed
#       c. Optionally purge
#    4. (?) Kick out optimized untar script (pigz / lbzip2)

## TODO
# * filter and produce list to feed to scp/globus?
# * allow direct handoff to Globus CLI
# * mpibzip2

import argparse
import datetime
import logging
import multiprocessing as mp
import os
import pathlib
import re
import sys
import tempfile

import humanfriendly
from dotenv import find_dotenv, load_dotenv

from mpiFileUtils import DWalk
from SuperTar import SuperTar


class DwalkLine:
    def __init__(self, line=False, relativeto=False):
        """parse dwalk output line"""
        # -rw-r--r-- bennet support 578.000  B Oct 22 2019 09:35 /scratch/support_root/support/bennet/haoransh/DDA_2D_60x70_kulow_1.batch
        match = re.match(
            rb"\S+\s+\S+\s+\S+\s+(\d+\.\d+)\s+(\S+)\s+.+\s(/.+)", line, re.DOTALL
        )  # use re.DOTALL to match newlines in filenames
        if relativeto:
            self.relativeto = relativeto
        else:
            self.relativeto = os.getcwd()

        self.size = self._normilizeunits(
            units=match[2], count=float(match[1])
        )  # size in bytes
        self.path = self._stripcwd(match[3])

    def _normilizeunits(self, units=False, count=False):
        """convert size by SI units to Bytes"""
        units = units.decode()  # convert binary data to string type
        if units == "B":
            return count
        elif units == "KB":
            return 1000 * count
        elif units == "MB":
            return 1000 * 1000 * count
        elif units == "GB":
            return 1000 * 1000 * 1000 * count
        elif units == "TB":
            return 1000 * 1000 * 1000 * 1000 * count
        elif units == "PB":
            return 1000 * 1000 * 1000 * 1000 * count
        else:
            raise Exception(f"{units} is not a known SI unit")

    def _stripcwd(self, path):
        """dwalk print absolute paths, we need relative"""
        return os.path.relpath(path, self.relativeto.encode())


class DwalkParser:
    def __init__(self, path=False):
        # check that path exists
        path = pathlib.Path(path)
        self.indexcount = 1
        if path.is_file():
            logging.debug(f"using {path} as input for DwalkParser")
            self.path = path.open("br")
        else:
            raise Exception(f"{self.path} doesn't exist")

    def tarlist(
        self, prefix="archivetar", minsize=1e9 * 100  # prefix for files
    ):  # min size sum of all files in list
        # OUT tar list suitable for gnutar
        # OUT index list
        """takes dwalk output walks though until sum(size) >= minsize"""

        logging.debug(f"minsize is set to {minsize} B")

        tartmp_p = (
            pathlib.Path.cwd() / f"{prefix}-{self.indexcount}.DONT_DELETE.txt"
        )  # list of files suitable for gnutar
        index_p = pathlib.Path.cwd() / f"{prefix}-{self.indexcount}.index.txt"
        sizesum = 0  # size in bytes thus far
        index = index_p.open("wb")
        tartmp = tartmp_p.open("wb")
        for line in self.path:
            pl = DwalkLine(line=line)
            sizesum += pl.size
            index.write(line)  # already has newline
            tartmp.write(pl.path)  # already has newline (binary)
            if sizesum >= minsize:
                # max size in tar reached
                tartmp.close()
                index.close()
                print(
                    f"Minimum Archive Size {humanfriendly.format_size(minsize)} reached, Expected size: {humanfriendly.format_size(sizesum)}"
                )
                yield self.indexcount, index_p, tartmp_p
                self.indexcount += 1
                # continue after yeilding file paths back to program
                sizesum = 0
                tartmp_p = (
                    pathlib.Path.cwd() / f"{prefix}-{self.indexcount}.DONT_DELETE.txt"
                )  # list of files suitable for gnutar
                index_p = pathlib.Path.cwd() / f"{prefix}-{self.indexcount}.index.txt"
                index = index_p.open("wb")
                tartmp = tartmp_p.open("wb")
        index.close()  # close and return for final round
        tartmp.close()
        yield self.indexcount, index_p, tartmp_p


#############  MAIN  ################


def parse_args(args):
    """ CLI options"""
    parser = argparse.ArgumentParser(
        description="Prepare a directory for arching",
        epilog="Brock Palen brockp@umich.edu",
    )
    parser.add_argument(
        "--dryrun",
        help="Print what would do but dont do it, aditional --dryrun increases how far the script runs\n 1 = Walk Filesystem and stop, 2 = Filter and create sublists",
        action="count",
        default=0,
    )
    parser.add_argument(
        "-p",
        "--prefix",
        help="prefix for tar eg prefix-1.tar prefix-2.tar etc",
        type=str,
        required=True,
    )

    parser.add_argument(
        "-s",
        "--size",
        help="Cutoff size for files include (eg. 10G 100M) Default 20G",
        type=str,
        default="20G",
    )
    parser.add_argument(
        "-t",
        "--tar-size",
        help="Target tar size before options (eg. 10G 1T) Default 100G",
        type=str,
        default="100G",
    )
    num_cores = round(mp.cpu_count() / 4)
    parser.add_argument(
        "--tar-processes",
        help=f"Number of parallel tars to invoke a once. Default {num_cores} is dynamic.  Increase for iop bound not using compression",
        type=int,
        default=num_cores,
    )

    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument(
        "-v",
        "--verbose",
        help="Increase messages, including files as added",
        action="store_true",
    )
    verbosity.add_argument(
        "-q", "--quiet", help="Decrease messages", action="store_true"
    )

    tar_opts = parser.add_argument_group(
        title="Tar Options", description="Options to pass to underlying tar commands"
    )
    tar_opts.add_argument(
        "--tar-verbose",
        help="Pass -v to tar (print files as tar'd)",
        action="store_true",
    )
    tar_opts.add_argument(
        "--remove-files",
        help="Pass --remove-files to tar, Delete files as/when added to archive (CAREFUL)",
        action="store_true",
    )

    compression = parser.add_mutually_exclusive_group()
    compression.add_argument(
        "-z", "--gzip", help="Compress tar with GZIP", action="store_true"
    )
    compression.add_argument(
        "-j", "--bzip", "--bzip2", help="Compress tar with BZIP", action="store_true"
    )
    compression.add_argument("--lz4", help="Compress tar with lz4", action="store_true")
    compression.add_argument(
        "--xz", "--lzma", help="Compress tar with xz/lzma", action="store_true"
    )

    args = parser.parse_args(args)
    return args


def build_list(path=False, prefix=False, savecache=False):
    """
    scan filelist and return path to results

    Parameters:
        path (str/pathlib) Path to scan
        prefix (str) Prefix for scan file eg. prefix-{date}.cache
        savecache (bool) Save cache file in cwd or only in TMPDIR

    Returns:
        cache (pathlib) Path to cache file
    """

    # configure DWalk
    dwalk = DWalk(
        inst=os.getenv("AT_MPIFILEUTILS", default="/home/brockp/mpifileutils/install"),
        mpirun=os.getenv(
            "AT_MPIRUN",
            default="/sw/arcts/centos7/stacks/gcc/8.2.0/openmpi/4.0.3/bin/mpirun",
        ),
        sort="name",
        filter=["--distribution", "size:0,1K,1M,10M,100M,1G,10G,100G,1T"],
        progress="10",
        umask=0o077,  # set premissions to only the user invoking
    )

    # generate timestamp name
    today = datetime.datetime.today()
    datestr = today.strftime("%Y-%m-%d-%H-%M-%S")

    # put into cwd or TMPDIR ?
    c_path = pathlib.Path.cwd() if savecache else pathlib.Path(tempfile.gettempdir())
    cache = c_path / f"{prefix}-{datestr}.cache"
    logging.debug(f"Scan saved to {cache}")

    # start the actual scan
    dwalk.scanpath(path=path, cacheout=cache)

    return cache


def filter_list(path=False, size=False, prefix=False):
    """
    Take cache list and filter it into two lists
    Files greater than size and those less than

    Prameters:
        path (pathlib) Path to existing cache file
        size (int) size in bytes to filter on
        prefix (str) Prefix for scanfiles

    Returns:
        oversize (pathlib) Path to files over size
        undersize (pathlib) Path to files under size
    """

    # configure DWalk
    under_dwalk = DWalk(
        inst=os.getenv("AT_MPIFILEUTILS", default="/home/brockp/mpifileutils/install"),
        mpirun=os.getenv(
            "AT_MPIRUN",
            default="/sw/arcts/centos7/stacks/gcc/8.2.0/openmpi/4.0.3/bin/mpirun",
        ),
        sort="name",
        progress="10",
        filter=["--type", "f", "--size", f"-{size}"],
        umask=0o077,  # set premissions to only the user invoking
    )

    c_path = pathlib.Path(tempfile.gettempdir())
    textout = c_path / f"{prefix}.under.txt"

    # start the actual scan
    under_dwalk.scancache(cachein=path, textout=textout)

    return textout


def process(q, iolock):
    while True:
        args = q.get()  # tuple (t_args, tar_list)
        if args is None:
            break
        t_args, tar_list = args
        with iolock:
            tar = SuperTar(**t_args)  # call inside the lock to keep stdout pretty
            tar.addfromfile(tar_list)
        tar.invoke()  # this is the long running portion so let run outside the lock it prints nothing anyway
        filesize = pathlib.Path(tar.filename).stat().st_size
        with iolock:
            logging.info(
                f"Complete {tar.filename} Size: {humanfriendly.format_size(filesize)}"
            )


def main(argv):
    args = parse_args(argv[1:])
    if args.quiet:
        logging.basicConfig(level=logging.WARNING)
    elif args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # load in config from .env
    load_dotenv(find_dotenv(), verbose=args.verbose)

    # scan entire filesystem
    logging.info("----> [Phase 1] Build Global List of Files")
    b_args = {"path": ".", "prefix": args.prefix}
    cache = build_list(**b_args)
    logging.debug(f"Results of full path scan saved at {cache}")

    # filter for files under size
    if (not args.dryrun) or (args.dryrun == 2):
        logging.info(f"----> [Phase 1.5] Filter out files greater than {args.size}")
        littlelist = filter_list(
            path=cache, size=humanfriendly.parse_size(args.size), prefix=cache.stem
        )
        # list parser
        logging.info(
            f"----> [Phase 2] Parse fileted list into sublists of size {args.tar_size}"
        )
        parser = DwalkParser(path=littlelist)

        # start parallel pool
        q = mp.Queue()
        iolock = mp.Lock()
        pool = mp.Pool(args.tar_processes, initializer=process, initargs=(q, iolock))
        for index, index_p, tar_list in parser.tarlist(
            prefix=args.prefix, minsize=humanfriendly.parse_size(args.tar_size)
        ):
            logging.info(f"    Index: {index_p}")
            logging.info(f"    tar: {tar_list}")

            # actauly tar them up
            if not args.dryrun:
                # if compression
                # if remove
                t_args = {"filename": f"{args.prefix}-{index}.tar"}
                if args.remove_files:
                    t_args["purge"] = True
                if args.tar_verbose:
                    t_args["verbose"] = True

                # compression options
                if args.gzip:
                    t_args["compress"] = "GZIP"
                if args.bzip:
                    t_args["compress"] = "BZ2"
                if args.lz4:
                    t_args["compress"] = "LZ4"
                if args.xz:
                    t_args["compress"] = "XZ"

                q.put((t_args, tar_list))  # put work on the queue

        for _ in range(args.tar_processes):  # tell workers we're done
            q.put(None)

        pool.close()
        pool.join()

    # bail if --dryrun requested
    if args.dryrun:
        logging.info("--dryrun requested exiting")
        sys.exit(0)
    # for list in tarlist()
    #     SuperTar

    # if globus(dest)
    #     globus put
