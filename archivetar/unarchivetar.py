# Brock Palen
# brockp@umich.edu
# 7/2020
#
# Take a directory that was prepped by archivetar and expand

import argparse
import logging
import multiprocessing as mp
import pathlib

from natsort import natsorted

from SuperTar import SuperTar


def parse_args(args):
    """CLI Optoins takes sys.argv[1:]."""
    parser = argparse.ArgumentParser(
        description="Un-Archive a directory prepped by archivetar",
        epilog="Brock Palen brockp@umich.edu",
    )
    parser.add_argument(
        "--dryrun", help="Print what would do but dont do it", action="store_true"
    )
    parser.add_argument(
        "-p",
        "--prefix",
        help="prefix for tar eg prefix-1.tar prefix-2.tar etc, used to match tarx to expand",
        type=str,
        required=True,
    )
    num_cores = round(mp.cpu_count() / 4)
    parser.add_argument(
        "--tar-processes",
        help=f"Number of parallel tars to invoke a once. Default {num_cores} is dynamic.  Increase for iop bound not using compression",
        type=int,
        default=num_cores,
    )

    parser.add_argument(
        "--folder",
        help="Extract/Search only the given folder similar to tar -xf a.tar folder/sub",
        type=str,
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
        "-k",
        "--keep-old-files",
        help="don't replace existing files when extracting, treat them as errors",
        action="store_true",
    )
    tar_opts.add_argument(
        "--skip-old-files",
        help="don't replace existing files when extracting, silently skip over them",
        action="store_true",
    )
    tar_opts.add_argument(
        "--keep-newer-files",
        help="don't replace existing files that are newer than their archive copies",
        action="store_true",
    )

    args = parser.parse_args(args)
    return args


def find_prefix_files(prefix, path=None, suffix="tar"):
    """
    Find all tar's in current directory matching pattern.

    match <prefix>-####.tar or <prefix>-#####.tar.*

    Return array of pathlibs.Path()
    """
    if path:
        p = pathlib.Path(path)
    else:
        p = pathlib.Path(".")

    tars = natsorted(p.glob(f"{prefix}-[0-9]*.{suffix}*"), key=str)

    logging.debug(f"Found files prefix: {prefix} Suffix: {suffix} : {tars}")

    return tars


def process(q, iolock):
    """process the archives to expand them if they exist on the queue"""
    while True:
        args = q.get()  # tuple (t_args, e_args, archive)
        if args is None:
            break
        with iolock:
            t_args, e_args, archive = args
            tar = SuperTar(
                filename=archive, **t_args
            )  # call inside the lock to keep stdout pretty
        tar.extract(
            **e_args
        )  # this is the long running portion so let run outside the lock it prints nothing anyway
        with iolock:
            logging.info(f"Complete {tar.filename}")


def main(argv):
    """
    Main event loop phases.

    1. Parse arguments and set logging
    2. Find all tars that match <prefix>-####.tar.*
    3. Invoke tar in parallel with multiprocessing
    """
    args = parse_args(argv[1:])
    if args.quiet:
        logging.basicConfig(level=logging.WARNING)
    elif args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # find all archives for prefix
    archives = find_prefix_files(args.prefix)
    logging.info(f"Found {len(archives)} archives with prefix {args.prefix}")

    # start parallel pool
    q = mp.Queue()
    iolock = mp.Lock()
    pool = mp.Pool(args.tar_processes, initializer=process, initargs=(q, iolock))
    for archive in archives:
        logging.info(f"Expanding archive {archive}")

        if args.dryrun:
            logging.info("Dryrun requested will not expand")
        else:
            t_args = {}  # arguments to tar constructor
            if args.tar_verbose:
                t_args["verbose"] = True
            if args.folder:
                t_args["path"] = args.folder

            e_args = {}  # arguments to extract()
            if args.keep_old_files:
                e_args["keep_old_files"] = True
            if args.skip_old_files:
                e_args["skip_old_files"] = True
            if args.keep_newer_files:
                e_args["keep_newer_files"] = True

            q.put((t_args, e_args, archive))  # put work on the queue

    for _ in range(args.tar_processes):  # tell workers we're done
        q.put(None)

    pool.close()
    pool.join()
