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
from environs import Env

from archivetar.exceptions import ArchivePrefixConflict
from archivetar.unarchivetar import find_archives
from GlobusTransfer import GlobusTransfer
from mpiFileUtils import DWalk
from SuperTar import SuperTar

# load in config from .env
env = Env()
env.read_env()  # read .env file, if it exists


class DwalkLine:
    def __init__(self, line=False, relativeto=False, stripcwd=True):
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
        if stripcwd:
            self.path = self._stripcwd(match[3])
        else:
            self.path = match[3]

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

    def getpath(self):
        """Get path one line at a time."""
        for line in self.path:
            pl = DwalkLine(line=line, stripcwd=False)
            yield pl.path

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
                logging.info(
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
        description="Prepare a directory for archive",
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
        help="Cutoff size for files include (eg. 10G 100M)",
        type=str,
        default=None,
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
    parser.add_argument(
        "--save-purge-list",
        help="Save an mpiFileUtils purge list <prefix>-<timestamp>.under.cache for files saved in tars, used to delete files under --size after archive process.  Use as alternative to --remove-files",
        action="store_true",
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
        help="Pass --remove-files to tar, Delete files as/when added to archive (CAREFUL). --save-purge-list is safer but requires more storage space.",
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
        "--xz",
        "--lzma",
        help='Compress tar with xz/lzma\n If using xz to enable multi-threaded  set XZ_OPT="-T0 -9"',
        action="store_true",
    )

    globus = parser.add_argument_group(
        title="Globus Transfer Options",
        description="Options to setup transfer of data to archive",
    )
    source_default = env.str("AT_SOURCE", default="umich#greatlakes")
    globus.add_argument(
        "--source",
        help=f"Source endpoint/collection Default: {source_default}",
        default=source_default,
    )

    dest_default = env.str("AT_DESTINATION", default="umich#flux")
    globus.add_argument(
        "--destination",
        help=f"Destination endpoint/collection Default: {dest_default}",
        default=dest_default,
    )
    globus.add_argument("--destination-dir", help="Directory on Destination server")
    globus.add_argument(
        "--globus-verbose", help="Globus Verbose Logging", action="store_true"
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
        inst=env.str("AT_MPIFILEUTILS", default="/home/brockp/mpifileutils/install"),
        mpirun=env.str(
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


def filter_list(path=False, size=False, prefix=False, purgelist=False):
    """
    Take cache list and filter it into two lists
    Files greater than size and those less than

    Prameters:
        path (pathlib) Path to existing cache file
        size (int) size in bytes to filter on
        prefix (str) Prefix for scanfiles
        purgelist (bool) Save the undersize  cache in CWD for purges

    Returns:
        TODO o_textout (pathlib) Path to files over or equal size text format
        TODO o_cacheout (pathlib) Path to files over or equal size mpifileutils bin format
        u_textout (pathlib) Path to files under size text format
        u_cacheout (pathlib) Path to files under size mpifileutils bin format
    """

    # configure DWalk
    under_dwalk = DWalk(
        inst=env.str("AT_MPIFILEUTILS", default="/home/brockp/mpifileutils/install"),
        mpirun=env.str(
            "AT_MPIRUN",
            default="/sw/arcts/centos7/stacks/gcc/8.2.0/openmpi/4.0.3/bin/mpirun",
        ),
        sort="name",
        progress="10",
        filter=["--type", "f", "--size", f"-{size}"],
        umask=0o077,  # set premissions to only the user invoking
    )

    ut_path = pathlib.Path(tempfile.gettempdir())
    u_textout = ut_path / f"{prefix}.under.txt"
    uc_path = pathlib.Path.cwd() if purgelist else pathlib.Path(tempfile.gettempdir())
    u_cacheout = uc_path / f"{prefix}.under.cache"

    # start the actual scan
    under_dwalk.scancache(cachein=path, textout=u_textout, cacheout=u_cacheout)

    # get the list of files larger than
    over_dwalk = DWalk(
        inst=env.str("AT_MPIFILEUTILS", default="/home/brockp/mpifileutils/install"),
        mpirun=env.str(
            "AT_MPIRUN",
            default="/sw/arcts/centos7/stacks/gcc/8.2.0/openmpi/4.0.3/bin/mpirun",
        ),
        sort="name",
        progress="10",
        filter=["--type", "f", "--size", f"+{size}"],
        umask=0o077,  # set premissions to only the user invoking
    )

    ot_path = pathlib.Path(tempfile.gettempdir())
    o_textout = ot_path / f"{prefix}.over.txt"

    # start the actual scan
    over_dwalk.scancache(cachein=path, textout=o_textout)

    # get the list of files exactly equal to
    at_dwalk = DWalk(
        inst=env.str("AT_MPIFILEUTILS", default="/home/brockp/mpifileutils/install"),
        mpirun=env.str(
            "AT_MPIRUN",
            default="/sw/arcts/centos7/stacks/gcc/8.2.0/openmpi/4.0.3/bin/mpirun",
        ),
        sort="name",
        progress="10",
        filter=["--type", "f", "--size", f"{size}"],
        umask=0o077,  # set premissions to only the user invoking
    )

    at_path = pathlib.Path(tempfile.gettempdir())
    a_textout = at_path / f"{prefix}.at.txt"

    # start the actual scan
    at_dwalk.scancache(cachein=path, textout=a_textout)

    # append a_textout to o_textout
    with o_textout.open("a+") as o:
        with a_textout.open() as a:
            o.write(a.read())

    return u_textout, u_cacheout, o_textout


def process(q, iolock, args):
    while True:
        q_args = q.get()  # tuple (t_args, tar_list, index)
        if q_args is None:
            break
        t_args, tar_list, index = q_args
        with iolock:
            tar = SuperTar(**t_args)  # call inside the lock to keep stdout pretty
            tar.addfromfile(tar_list)
        tar.archive()  # this is the long running portion so let run outside the lock it prints nothing anyway
        filesize = pathlib.Path(tar.filename).stat().st_size
        with iolock:
            logging.info(
                f"Complete {tar.filename} Size: {humanfriendly.format_size(filesize)}"
            )
            if args.destination_dir:  # if globus destination is set upload
                globus = GlobusTransfer(
                    args.source, args.destination, args.destination_dir
                )
                path = pathlib.Path(tar.filename).resolve()
                logging.debug(f"Adding file {path} to Globus Transfer")
                globus.add_item(path, label=f"{path.name}")
                tar_list = pathlib.Path(tar_list).resolve()
                logging.debug(f"Adding file {tar_list} to Globus Transfer")
                globus.add_item(tar_list, label=f"{path.name}")
                index_p = pathlib.Path(index).resolve()
                logging.debug(f"Adding file {index_p} to Globus Transfer")
                globus.add_item(index_p, label=f"{path.name}")
                taskid = globus.submit_pending_transfer()
                logging.info(
                    f"Globus Transfer of Small file tar {path.name} : {taskid}"
                )


def validate_prefix(prefix):
    """Check that the prefix selected won't conflict with current files"""

    # use find_archives from unarchivetar to use the same match
    tars = find_archives(prefix)

    if len(tars) != 0:
        logging.critical(f"Prefix {prefix} conflicts with current files {tars}")
        raise ArchivePrefixConflict(
            f"Prefix {prefix} conflicts with current files {tars}"
        )
    else:
        return True


def main(argv):
    args = parse_args(argv[1:])
    if args.quiet:
        logging.basicConfig(level=logging.WARNING)
    elif args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # globus built in logger is very verbose adjust lower unless verbose
    globus_logger = logging.getLogger("globus_sdk")
    urllib_logger = logging.getLogger("urllib3")
    if not args.globus_verbose:
        globus_logger.setLevel(logging.WARNING)
        urllib_logger.setLevel(logging.WARNING)

    # check that selected prefix is usable
    validate_prefix(args.prefix)

    # if using globus, init to prompt for endpoiont activation etc
    if args.destination_dir:
        globus = GlobusTransfer(args.source, args.destination, args.destination_dir)

    # scan entire filesystem
    logging.info("----> [Phase 1] Build Global List of Files")
    b_args = {"path": ".", "prefix": args.prefix}
    cache = build_list(**b_args)
    logging.debug(f"Results of full path scan saved at {cache}")

    # filter for files under size
    if (not args.dryrun) or (args.dryrun == 2):

        # Set --size filter to 1ExaByte if not set
        filtersize = args.size if args.size else "1EB"
        logging.info(
            f"----> [Phase 1.5] Filter out files greater than {filtersize} if --size given"
        )

        # IN: List of files
        # OUT: pathlib: undersize_text, undersize_cache, oversize_text, atsize_text
        under_t, under_c, over_t = filter_list(
            path=cache,
            size=humanfriendly.parse_size(filtersize),
            prefix=cache.stem,
            purgelist=args.save_purge_list,
        )

        # if globus get transfer the large files
        if args.destination_dir and not args.dryrun:
            # transfer = upload_overlist(over_t, globus)
            over_p = DwalkParser(path=over_t)
            for path in over_p.getpath():
                path = path.rstrip(b"\n")  # strip trailing newline
                path = path.decode("utf-8")  # convert byte array to string
                path = pathlib.Path(path)
                logging.debug(f"Adding file {path} to Globus Transfer")
                globus.add_item(path, label=f"Large File List {args.prefix}")

            taskid = globus.submit_pending_transfer()
            logging.info(f"Globus Transfer of Oversize files: {taskid}")

        # Dwalk list parser
        logging.info(
            f"----> [Phase 2] Parse fileted list into sublists of size {args.tar_size}"
        )
        parser = DwalkParser(path=under_t)

        # start parallel pool
        q = mp.Queue()
        iolock = mp.Lock()
        pool = mp.Pool(
            args.tar_processes, initializer=process, initargs=(q, iolock, args)
        )
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

                q.put((t_args, tar_list, index_p))  # put work on the queue

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
