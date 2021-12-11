"""archivetar CLI arguments parsing."""
import argparse
import multiprocessing as mp

from environs import Env

# load in defaults from environment
env = Env()


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
