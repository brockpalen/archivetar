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

import datetime
import hashlib
import logging
import multiprocessing as mp
import os
import re
import sys
import tempfile
from pathlib import Path
from subprocess import CalledProcessError  # nosec

import humanfriendly
from environs import Env

from archivetar.archive_args import parse_args
from archivetar.exceptions import ArchivePrefixConflict, TarError
from archivetar.unarchivetar import find_prefix_files
from GlobusTransfer import GlobusTransfer
from GlobusTransfer.exceptions import GlobusError, GlobusFailedTransfer
from mpiFileUtils import DWalk
from SuperTar import SuperTar

# load in config from .env
env = Env()

# can't load breaks singularity
# env.read_env()  # read .env file, if it exists


# defaults used for development
# overridden with AT_MPIRUN and AT_MPIFILEUTILS
fileutils = "/sw/pkgs/arc/archivetar/0.17.0/install"
mpirun = "/sw/pkgs/arc/stacks/gcc/10.3.0/openmpi/4.1.6/bin/mpirun"


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

        self.size = self._normalizeunits(
            units=match[2], count=float(match[1])
        )  # size in bytes
        if stripcwd:
            self.path = self._stripcwd(match[3])
        else:
            self.path = match[3]

    def _normalizeunits(self, units=False, count=False):
        """convert size by SI units to Bytes"""
        units = units.decode()  # convert binary data to string type
        # SI powers, e.g., 1 KB = 10**3 bytes
        SI_powers = dict(B=0, KB=3, MB=6, GB=9, TB=12, PB=15)
        try:
            num_bytes = count * 10 ** SI_powers[units]
        except KeyError as ex:
            raise Exception(f"{units} is not a known SI unit")
        return num_bytes

    def _stripcwd(self, path):
        """dwalk print absolute paths, we need relative"""
        return os.path.relpath(path, self.relativeto.encode())


class DwalkParser:
    def __init__(self, path=False):
        # check that path exists
        path = Path(path)
        self.indexcount = 1
        if path.is_file():
            logging.debug(f"using {path} as input for DwalkParser")
            self.path = path.open("br")
        else:
            raise Exception(f"{self.path} doesn't exist")

    def getpath(self, stripcwd=False):
        """Get path one line at a time."""
        for line in self.path:
            pl = DwalkLine(line=line, stripcwd=stripcwd)
            yield pl.path

    def tarlist(
        self, prefix="archivetar", minsize=1e9 * 100, bundle_path=None
    ):  # prefix for files
        # min size sum of all files in list
        # bundle_path where should indexes and files be created
        # OUT tar list suitable for gnutar
        # OUT index list
        """takes dwalk output walks though until sum(size) >= minsize"""

        logging.debug(f"minsize is set to {minsize} B")

        if bundle_path:
            # set outpath to this location
            outpath = Path(bundle_path)
        else:
            # set to cwd
            outpath = Path.cwd()

        logging.debug(f"Indexes and lists will be written to: {outpath}")

        tartmp_p = (
            outpath / f"{prefix}-{self.indexcount}.DONT_DELETE.txt"
        )  # list of files suitable for gnutar
        index_p = outpath / f"{prefix}-{self.indexcount}.index.txt"
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
                    outpath / f"{prefix}-{self.indexcount}.DONT_DELETE.txt"
                )  # list of files suitable for gnutar
                index_p = outpath / f"{prefix}-{self.indexcount}.index.txt"
                index = index_p.open("wb")
                tartmp = tartmp_p.open("wb")
        index.close()  # close and return for final round
        tartmp.close()
        yield self.indexcount, index_p, tartmp_p


#############  MAIN  ################


def get_relative_path(filepath, cwd=None):
    """Convert absolute path to relative path.

    This is useful for Globus logs that return absolute path but we want just relative to some path.
    /gpfs/accounts/support_root/support/brockp/box-copy/ARC Projects/P874443 - ARC Data Center Upgrade/874443 Data Center Upgrade P874443 Project Summary Mar-18.xlsx
    Becomes
    P874443 - ARC Data Center Upgrade/874443 Data Center Upgrade P874443 Project Summary Mar-18.xlsx
    If cwd is
    /gpfs/accounts/support_root/support/brockp/box-copy/ARC Projects/

    Parameters:
        filepath (path): Full absolute path with leaading /
        cwd (path): Leading path to strip Defeault cwd
    """
    if cwd is None:
        cwd = Path.cwd()
    else:
        cwd = Path(cwd)

    # Resolve symlinks and normalize both paths
    filepath_resolved = Path(filepath).resolve()
    cwd_resolved = cwd.resolve()

    try:
        # Try to get the relative path
        rel = filepath_resolved.relative_to(cwd_resolved)
    except ValueError:
        # If the file isn't under cwd, just return basename
        rel = filepath_resolved.name
    return str(rel)


def globus_transfer_singleton(args, path, label="Globus Singleton"):
    """
    Transfer a single file using globus with default options.

    args (argparse): Arguments struct
    path (path): Files to upload
    label (str): Label for globus transfer

    returns:
        taskid (str): Globus task id
    """
    globus = GlobusTransfer(
        args.source,
        args.destination,
        args.destination_dir,
        # note notify are the reverse of the SDK
        notify_on_succeeded=args.no_notify_on_succeeded,
        notify_on_failed=args.no_notify_on_failed,
        notify_on_inactive=args.no_notify_on_inactive,
        fail_on_quota_errors=args.fail_on_quota_errors,
        skip_source_errors=args.skip_source_errors,
        preserve_timestamp=args.preserve_timestamp,
    )
    globus.add_item(Path(path).resolve(), label=f"{label}: {args.prefix}")
    taskid = globus.submit_pending_transfer()
    logging.info(f"Globus Transfer: {label} taskid: {taskid}")
    return taskid


def create_sha1_manifest_from_file(path):
    """
    Create a manifest file suitable for sha1sum -c from a file containing lists of files.

    path (str): Path to file with list of files to checksum
    """
    file_list = Path(path)
    sha_list = file_list.with_suffix(".sha1")
    with sha_list.open("w") as f:
        for line in file_list.read_text().splitlines():
            path = line.strip()
            f.write(f"{sha1_of(path)} {line}\n")

    return sha_list


def sha1_of(path, bufsize=1 << 20):
    """
    Calculate a sha1 hash of a given file.

    Globus currently uses sha1 for checksums

    Parameters:
        path (str/pathlib) Path to file
        bufsize (int) Size of buffer to read at a time
    """
    h = hashlib.sha1(usedforsecurity=False)
    logging.debug(f"Calculating Checksum for {path}")
    with Path(path).open("rb") as f:
        while chunk := f.read(bufsize):
            h.update(chunk)
    return h.hexdigest()


def create_sha256_manifest_from_file(path):
    """
    Create a manifest file suitable for sha256sum -c from a file containing lists of files
    """
    file_list = Path(path)
    sha_list = file_list.with_suffix(".sha256")
    with sha_list.open("w") as f:
        for line in file_list.read_text().splitlines():
            path = line.strip()
            f.write(f"{sha256_of(path)} {line}\n")

    return sha_list


def sha256_of(path, bufsize=1 << 20):
    """
    Calculate a shaw256 hash of a given file.

    Parameters:
        path (str/pathlib) Path to file
        bufsize (int) Size of buffer to read at a time
    """
    h = hashlib.sha256(usedforsecurity=False)
    logging.debug(f"Calculating Checksum for {path}")
    with Path(path).open("rb") as f:
        while chunk := f.read(bufsize):
            h.update(chunk)
    return h.hexdigest()


def build_list(path=False, prefix=False, savecache=False, filters=None):
    """
    scan filelist and return path to results

    Parameters:
        path (str/pathlib) Path to scan
        prefix (str) Prefix for scan file eg. prefix-{date}.cache
        savecache (bool) Save cache file in cwd or only in TMPDIR
        filters (args) HACK pass in argparser for passing filter options eg --atime

    Returns:
        cache (pathlib) Path to cache file
    """

    # build filter list
    filter = ["--distribution", "size:0,1K,1M,10M,100M,1G,10G,100G,1T"]
    if filters:
        if filters.atime:
            filter.extend(["--atime", filters.atime])
        if filters.mtime:
            filter.extend(["--mtime", filters.mtime])
        if filters.ctime:
            filter.extend(["--ctime", filters.ctime])
        if filters.user:
            filter.extend(["--user", filters.user])
        if filters.group:
            filter.extend(["--group", filters.group])

    logging.debug(f"build_list filter options: {filter}")

    # configure DWalk
    dwalk = DWalk(
        inst=env.str("AT_MPIFILEUTILS", default=fileutils),
        mpirun=env.str("AT_MPIRUN", default=mpirun),
        sort="name",
        filter=filter,
        progress="10",
        umask=0o077,  # set premissions to only the user invoking
    )

    # generate timestamp name
    today = datetime.datetime.today()
    datestr = today.strftime("%Y-%m-%d-%H-%M-%S")

    # put into cwd or TMPDIR ?
    c_path = Path.cwd() if savecache else Path(tempfile.gettempdir())
    cache = c_path / f"{prefix}-{datestr}.cache"
    print(f"Scan saved to {cache}")

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
        inst=env.str("AT_MPIFILEUTILS", default=fileutils),
        mpirun=env.str("AT_MPIRUN", default=mpirun),
        sort="name",
        progress="10",
        filter=["--type", "f", "--size", f"-{size}"],
        umask=0o077,  # set premissions to only the user invoking
    )

    ut_path = Path(tempfile.gettempdir())
    u_textout = ut_path / f"{prefix}.under.txt"
    uc_path = Path.cwd() if purgelist else Path(tempfile.gettempdir())
    u_cacheout = uc_path / f"{prefix}.under.cache"

    # start the actual scan
    under_dwalk.scancache(cachein=path, textout=u_textout, cacheout=u_cacheout)

    # get the list of all symlinks
    symlink_dwalk = DWalk(
        inst=env.str("AT_MPIFILEUTILS", default=fileutils),
        mpirun=env.str("AT_MPIRUN", default=mpirun),
        sort="name",
        progress="10",
        filter=["--type", "l"],  # don't set size so even --size 0B works
        umask=0o077,  # set premissions to only the user invoking
    )

    symlink_path = Path(tempfile.gettempdir())
    symlink_textout = symlink_path / f"{prefix}.symlink.txt"

    # start the actual scan
    symlink_dwalk.scancache(cachein=path, textout=symlink_textout)

    # append symlink_textout to u_textout to add to tars
    with u_textout.open("a+") as u:
        with symlink_textout.open() as links:
            u.write(links.read())

    # get the list of files larger than
    over_dwalk = DWalk(
        inst=env.str("AT_MPIFILEUTILS", default=fileutils),
        mpirun=env.str("AT_MPIRUN", default=mpirun),
        sort="name",
        progress="10",
        filter=["--type", "f", "--size", f"+{size}"],
        umask=0o077,  # set premissions to only the user invoking
    )

    ot_path = Path(tempfile.gettempdir())
    o_textout = ot_path / f"{prefix}.over.txt"

    # start the actual scan
    over_dwalk.scancache(cachein=path, textout=o_textout)

    # get the list of files exactly equal to
    at_dwalk = DWalk(
        inst=env.str("AT_MPIFILEUTILS", default=fileutils),
        mpirun=env.str("AT_MPIRUN", default=mpirun),
        sort="name",
        progress="10",
        filter=["--type", "f", "--size", f"{size}"],
        umask=0o077,  # set premissions to only the user invoking
    )

    at_path = Path(tempfile.gettempdir())
    a_textout = at_path / f"{prefix}.at.txt"

    # start the actual scan
    at_dwalk.scancache(cachein=path, textout=a_textout)

    # append a_textout to o_textout
    with o_textout.open("a+") as o:
        with a_textout.open() as a:
            o.write(a.read())

    return u_textout, u_cacheout, o_textout


def process(q, out_q, iolock, args):
    while True:
        q_args = q.get()  # tuple (t_args, tar_list, index)
        if q_args is None:
            break
        try:
            t_args, tar_list, index = q_args
            with iolock:
                tar = SuperTar(**t_args)  # call inside the lock to keep stdout pretty
                tar.addfromfile(tar_list)
            tar.archive()  # this is the long running portion so let run outside the lock it prints nothing anyway
            filesize = Path(tar.filename).stat().st_size

            # create checksums for tared files
            if args.checksum:
                logging.debug(f"Checksums requested making for files in tar {tar_list}")
                checksum_manifest = create_sha1_manifest_from_file(tar_list)

            with iolock:
                logging.info(
                    f"Complete {tar.filename} Size: {humanfriendly.format_size(filesize)}"
                )
                if args.destination_dir:  # if globus destination is set upload
                    globus = GlobusTransfer(
                        args.source,
                        args.destination,
                        args.destination_dir,
                        # note notify are the reverse of the SDK
                        notify_on_succeeded=args.no_notify_on_succeeded,
                        notify_on_failed=args.no_notify_on_failed,
                        notify_on_inactive=args.no_notify_on_inactive,
                        fail_on_quota_errors=args.fail_on_quota_errors,
                        skip_source_errors=args.skip_source_errors,
                        preserve_timestamp=args.preserve_timestamp,
                    )
                    path = Path(tar.filename).resolve()
                    logging.debug(f"Adding file {path} to Globus Transfer")
                    globus.add_item(path, label=f"{path.name}", in_root=True)
                    tar_list = Path(tar_list).resolve()
                    logging.debug(f"Adding file {tar_list} to Globus Transfer")
                    globus.add_item(tar_list, label=f"{path.name}", in_root=True)
                    index_p = Path(index).resolve()
                    logging.debug(f"Adding file {index_p} to Globus Transfer")
                    globus.add_item(index_p, label=f"{path.name}", in_root=True)

                    # only add checksums if they exist
                    checksum_p = checksum_manifest.resolve()
                    if checksum_p.is_file():
                        logging.debug(f"Adding file {checksum_p} to Globus Transfer")
                        globus.add_item(checksum_p, label=f"{path.name}", in_root=True)
                    else:
                        logging.info(
                            f"Skipping checksum for {path.name}: file does not exist"
                        )

                    taskid = globus.submit_pending_transfer()
                    logging.info(
                        f"Globus Transfer of Small file tar {path.name} : {taskid}"
                    )

            if (
                args.wait or args.rm_at_files
            ):  # wait for globus transfers to finish, in own block to avoid iolock
                globus.task_wait(taskid)
                if args.rm_at_files:  # delete the AT created files tar, index, etc
                    logging.info(f"Deleting {path}")
                    path.unlink()
                    logging.info(f"Deleting {tar_list}")
                    tar_list.unlink()
                    logging.info(f"Deleting {index_p}")
                    index_p.unlink()
                    if checksum_p.is_file():
                        logging.info(f"Deleting {checksum_p}")
                        checksum_p.unlink()
        except GlobusFailedTransfer as e:
            logging.error(f"error with globus transfer of: {tar.filename}")
            out_q.put((-1, tar.filename, e))
            raise e
        except CalledProcessError as e:
            logging.error(f"error with external tar process: {tar.filename}")
            out_q.put((-1, tar.filename, e))
            raise e
        except Exception as e:
            # something bad happened put it on the out_q for return code
            logging.error(f"Unknown error in worker process for: {tar.filename}")
            out_q.put((-1, tar.filename, e))
            raise e
        else:
            # no issues put on were ok
            out_q.put((0, tar.filename, None))


def validate_prefix(prefix, path=None):
    """Check that the prefix selected won't conflict with current files"""

    # use find_prefix_files from unarchivetar to use the same match
    tars = find_prefix_files(prefix, path)
    tars.extend(find_prefix_files(prefix, path, suffix="index.txt"))
    tars.extend(find_prefix_files(prefix, path, suffix="DONT_DELETE.txt"))
    tars.extend(find_prefix_files(prefix, path, suffix="DONT_DELETE.sha1"))

    if len(tars) != 0:
        logging.critical(f"Prefix {prefix} conflicts with current files {tars}")
        print("\n")
        print(
            "Conflicting filex for selected prefix stopping to avoid unexpected behavior"
        )
        for item in tars:
            print(f"\t{item}")

        print("\n")
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

    # initialize locals
    large_taskid = None

    # check that selected prefix is usable
    validate_prefix(args.prefix, path=args.bundle_dir)

    # if using globus, init to prompt for endpoiont activation etc
    if args.destination_dir:
        globus = GlobusTransfer(
            args.source,
            args.destination,
            args.destination_dir,
            # note notify are the reverse of the SDK
            notify_on_succeeded=args.no_notify_on_succeeded,
            notify_on_failed=args.no_notify_on_failed,
            notify_on_inactive=args.no_notify_on_inactive,
            fail_on_quota_errors=args.fail_on_quota_errors,
            skip_source_errors=args.skip_source_errors,
            preserve_timestamp=args.preserve_timestamp,
        )

    # do we have a user provided list?
    if args.list:
        logging.info("---> [Phase 1] Found User Provided File List")
        cache = args.list
    else:
        # scan entire filesystem
        logging.info("----> [Phase 1] Build Global List of Files")
        b_args = {
            "path": ".",
            "prefix": args.prefix,
            "savecache": args.save_list,
            "filters": args,
        }
        cache = build_list(**b_args)
        logging.debug(f"Results of full path scan saved at {cache}")

    # bail if --dryrun requested
    if args.dryrun == 1:
        logging.info("--dryrun requested exiting")
        sys.exit(0)

    # filter for files under size
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
            path = Path(path)
            logging.debug(f"Adding file {path} to Globus Transfer")
            globus.add_item(path, label=f"Large File List {args.prefix}")

        large_taskid = globus.submit_pending_transfer()
        logging.info(f"Globus Transfer of Oversize files: {large_taskid}")

    # this may look less efficent to do checksums after transfer,
    # it's not though globus transfers are async and isn't checked until the end
    # so lets calculate checksums while the transfers happen
    if not args.dryrun and args.checksum:
        # we only calculate checksums locally for large files if Globus is not available
        # we calculate checksums if requested to by --force-local-checksum
        if not args.destination_dir or args.force_local_checksum:
            large_checksum_wrote_anything = (
                False  # used to track if antyhing is written
            )
            logging.info("----> Calculating Checksums for large files locally")
            bundle_dir = Path(args.bundle_dir or Path.cwd())
            sha_file = bundle_dir / f"{args.prefix}-large.DONT_DELETE.sha1"
            logging.debug(f"Large File checksum  manifest is {sha_file}")
            with sha_file.open("w") as f:
                over_p = DwalkParser(path=over_t)
                for path in over_p.getpath(stripcwd=True):
                    stripped = path.decode("utf-8").strip()
                    sha1 = sha1_of(stripped)
                    f.write(f"{sha1} {stripped}\n")
                    large_checksum_wrote_anything = True

            # if we are here someone asked for local checksums
            # but is using globus to upload so lets upload now
            if args.destination_dir and large_checksum_wrote_anything:
                large_checksum_taskid = globus_transfer_singleton(
                    args,
                    sha_file,
                    label=f"Oversize files checksum manifest",
                )

                logging.info(
                    f"Globus Transfer of Oversize files checksum manifest: {large_checksum_taskid}"
                )

            if not large_checksum_wrote_anything:
                logging.info("No large files found removing empty checksum file")
                sha_file.unlink()  # delete empty file nothing was written
        elif not args.dryrun and args.checksum:
            logging.info(
                "----> Checksums will be gatherd from Globus at end of packing"
            )

    # Dwalk list parser
    logging.info(
        f"----> [Phase 2] Parse fileted list into sublists of size {args.tar_size}"
    )
    parser = DwalkParser(path=under_t)

    # start parallel pool
    q = mp.Queue()  # input data
    out_q = mp.Queue()  # output return code from pool worker
    iolock = mp.Lock()
    try:
        for index, index_p, tar_list in parser.tarlist(
            prefix=args.prefix,
            minsize=humanfriendly.parse_size(args.tar_size),
            bundle_path=args.bundle_dir,
        ):
            logging.info(f"    Index: {index_p}")
            logging.info(f"    tar: {tar_list}")

            # actauly tar them up
            if not args.dryrun:
                # if compression
                # if remove
                if args.bundle_dir:
                    t_args = {
                        "filename": Path(args.bundle_dir) / f"{args.prefix}-{index}.tar"
                    }
                else:
                    t_args = {"filename": f"{args.prefix}-{index}.tar"}
                if args.remove_files:
                    t_args["purge"] = True
                if args.tar_verbose:
                    t_args["verbose"] = True
                if args.ignore_failed_read:
                    t_args["ignore_failed_read"] = True
                if args.dereference:
                    t_args["dereference"] = True

                # compression options
                if args.gzip:
                    t_args["compress"] = "GZIP"
                if args.zstd:
                    t_args["compress"] = "ZSTD"
                if args.bzip:
                    t_args["compress"] = "BZ2"
                if args.lz4:
                    t_args["compress"] = "LZ4"
                if args.xz:
                    t_args["compress"] = "XZ"
                if args.tar_options:
                    t_args["extra_options"] = args.tar_options.split()

                q.put((t_args, tar_list, index_p))  # put work on the queue

        # bail if --dryrun requested
        if args.dryrun:
            logging.info("--dryrun --dryrun requested exiting")
            sys.exit(0)

        # start parallel pool of workers
        pool = mp.Pool(
            args.tar_processes,
            initializer=process,
            initargs=(q, out_q, iolock, args),
        )

        for _ in range(args.tar_processes):  # tell workers we're done
            q.put(None)

        pool.close()
        pool.join()

        # wait for large_taskid to finish
        # large_taskid only esists if --size given to create a large file option
        # this will break once we have 1EB files
        if (args.wait or args.checksum) and large_taskid:
            if args.force_local_checksum:
                logging.debug("Wait for large_checksum_taskid to finish")
                globus.task_wait(large_checksum_taskid)

            logging.info(
                "Wait for large_taskid to finish for checksums disable with --no-checksum"
            )
            globus.task_wait(large_taskid)

            if args.checksum and not args.force_local_checksum:
                # use globus data to build list of sha1
                logging.info("----> Using Checksums from Globus")
                bundle_dir = Path(args.bundle_dir or Path.cwd())
                sha_file = bundle_dir / f"{args.prefix}-large.DONT_DELETE.sha1"
                logging.debug(f"Large File checksum  manifest is {sha_file}")
                with sha_file.open("w") as f:
                    for entry in globus.task_successful_transfers(large_taskid):
                        stripped = get_relative_path(entry["source_path"])
                        logging.debug(f"{entry['checksum']} {stripped}\n")
                        f.write(f"{entry['checksum']} {stripped}\n")

                # now upload the checksums
                large_checksum_taskid = globus_transfer_singleton(
                    args,
                    sha_file,
                    label=f"Oversize files checksum manifest",
                )

                logging.info(
                    f"Globus Transfer of Oversize files checksum manifest: {large_checksum_taskid}"
                )
                logging.debug("Wait for large_checksum_taskid to finish")
                globus.task_wait(large_checksum_taskid)
                if args.rm_at_files:
                    logging.info("Deleting large_checksum file")
                    sha_file.unlink()

        # cleanup large checksum file
        # It could be empty (no large files)
        # It could be requested deleted --rm-at-files

        # check no pool workers had problems running the tar
        # any task that raised an exception should find a returncode on the out_q
        suspect_tars = list()
        for _ in range(index):
            rc, filename, exception = out_q.get()
            logging.debug(f"Return code from tar {filename} is {rc}")
            if rc != 0:
                # found an issue with one worker log and push onto list
                logging.error(
                    f"An issue was found running the tars for index {filename}"
                )
                suspect_tars.append(filename)

        # raise if we found suspect tars
        if suspect_tars:
            raise TarError(f"An issue was found processing the tars for {suspect_tars}")

    except Exception as e:
        logging.error("Issue during tar process killing")
        raise e
        sys.exit(-1)
