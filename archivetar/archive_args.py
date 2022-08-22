"""archivetar CLI arguments parsing."""
import argparse
import multiprocessing as mp
import pathlib
import re

from environs import Env

# load in defaults from environment
env = Env()


def stat_check(string):
    """Validate input of filter string for values like atime, mtime, ctime.

    These shuld be integer values prefixed by a + or a - only with no spaces

    Eg:
        1
        5
        -10
        +20

    Invalid:
        1.5
        abc
        $#@
        + 5
        a5

    Return:
        String passed in

    Rasies ValueError if check doesn't pass
    """
    matched = re.match(r"^[+,-]?\d+$", string)
    if bool(matched):
        return string
    raise ValueError("Intagers only, optionally prefixed with + or -")


def unix_check(string):
    """Validate input for username and group names which should be alpha numeric only with no spaces or special chars not allowed in user/group names.

    Borrowed from: https://unix.stackexchange.com/questions/157426/what-is-the-regex-to-validate-linux-users

    Should match 31 char unix usernames and groups
    """
    matched = re.match(r"^[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\$)$", string)
    if bool(matched):
        return string
    raise ValueError("Intagers only, optionally prefixed with + or -")


def file_check(string):
    """Check if the user provided list file actaully exists."""
    path = pathlib.Path(string)
    if path.is_file():
        return path
    raise ValueError(f"file {string} not found")


def parse_args(args):
    """CLI options.

    Several options have option groups to logically bundle like commands.
    """
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
    tar_size = env.str("AT_TAR_SIZE", default="100G")
    parser.add_argument(
        "-t",
        "--tar-size",
        help=f"Target tar size before options (eg. 10G 1T) Default: {tar_size}",
        type=str,
        default=tar_size,
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
    parser.add_argument(
        "--bundle-path",
        help="Alternative path to bundle tars and indexes.  Useful if directory being archived is at or over quota and cannot write tars to current location.  Defaults to CWD.",
        default=None,
    )

    build_list_args = parser.add_mutually_exclusive_group()
    build_list_args.add_argument(
        "--save-list",
        help="Save the initial scan of target archive files (including filters)",
        action="store_true",
    )
    build_list_args.add_argument(
        "--list",
        help="Provide a prior scan from --dryrun --save-list",
        type=file_check,
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

    filter_ops = parser.add_argument_group(
        title="Filtering Options",
        description="Options to limit files included in the archive similar to options for unix find.  NOTICE: These should be used with care.  Improper mixing of filter and the --size option could result in unintended behavior if used without globus.",
    )
    filter_ops.add_argument(
        "--atime",
        metavar="N",
        type=stat_check,
        help="File was last accessed exactly N days ago. Use + for more than and - for less than N days ago (not inclusive)",
    )
    filter_ops.add_argument(
        "--mtime",
        metavar="N",
        type=stat_check,
        help="File data was last modified exactly N days ago. Use + for more than and - for less than N days ago (not inclusive)",
    )
    filter_ops.add_argument(
        "--ctime",
        metavar="N",
        type=stat_check,
        help="File status was last modified exactly N days ago. Use + for more than and - for less than N days ago (not inclusive)",
    )
    filter_ops.add_argument(
        "--user",
        metavar="username",
        type=unix_check,
        help="Only include files owned by username.",
    )
    filter_ops.add_argument(
        "--group",
        metavar="group",
        type=unix_check,
        help="Only include files owned by group.",
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
    tar_opts.add_argument(
        "--ignore-failed-read",
        help="Pass --ignore-failed-read to tar, Do not exit with nonzero on unreadable files or directories. .",
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
        "--zstd", help="Compress tar with zstd", action="store_true"
    )
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
    globus.add_argument(
        "--wait",
        help="Wait for  all Globus Transfers to finish before moving to next tar process / existing archivetar",
        action="store_true",
    )
    globus.add_argument(
        "--rm-at-files",
        help="Remove archivetar created files (tar, index, tar-list) after globus transfer of tars",
        action="store_true",
    )

    args = parser.parse_args(args)
    return args
