"""Simple full/incremental backups to a Globus archive with archivetar (#70).

archivebackup takes care of the timestamp bookkeeping so users do not have to:

* Generation 1 (and any run with --full) archives everything.
* Later generations archive only files whose ctime is newer than the
  previous generation's stamp, using the same dwalk walk archivetar already
  does, filtered with --cnewer.
* Each generation is uploaded to its own folder on the Globus destination,
  with its metadata, a listing of the whole tree at that moment
  ({prefix}-G####.fullindex.txt) and the catalog described below.  The
  destination alone is enough to restore.
* State the next run needs lives in .archivebackup/<prefix>/ inside the tree
  being backed up; --bundle-dir is scratch only.  Generation numbers are
  checked against the destination, so they are never reused.

archiverestore extracts the most recent full and then each incremental after
it, in order, using unarchivetar.

This is not a full backup system.  Incrementals only see files whose ctime
changed, so deleted files come back on restore and directories renamed or
moved in with `mv` on the same filesystem are not archived.  To catch the
latter, a catalog of every path (with its size and mtime) captured since the
last full is kept; if the tree holds anything the catalog does not,
archivebackup lists it and exits with status 3 so the user runs a new full.
"""

import argparse
import datetime
import gzip
import json
import logging
import os
import re
import shutil
import subprocess  # nosec
import sys
import tempfile
import time
from pathlib import Path, PurePosixPath

import archivetar
import archivetar.unarchivetar
from archivetar.archive_args import parse_args as archivetar_parse_args
from archivetar.unarchivetar import find_prefix_files
from mpiFileUtils import DWalk

# seconds to back-date each stamp to cover clock skew between this host and
# the storage servers that set file ctimes.  Overlap only re-archives a few
# files, it never misses any.
STAMP_BACKDATE = archivetar.env.int("AT_BACKUP_STAMP_BACKDATE", 300)

# exit status when files exist that no generation has archived
EXIT_UNCAPTURED = 3

INCREMENTAL_WARNING = """
******************************************************************************
WARNING: Incrementals capture new and changed files, including anything
written, copied, scp'd or rsync'd into this directory.  They do NOT capture:
  * directories renamed, or moved in with `mv` from elsewhere on the same
    filesystem; run a new full backup (archivebackup --full ...) after that
  * deletions; deleted files will reappear in a restore
archivebackup checks for files no backup has captured and exits with an
error (status 3) listing them if it finds any.
******************************************************************************
"""

UNCAPTURED_ERROR = """
******************************************************************************
ERROR: {count} file(s) in this directory are not in ANY backup generation.
This happens when directories are renamed or moved in with `mv`.  They are
listed in {listing}

Run a new full backup now:  archivebackup --full ...
******************************************************************************
"""

RESTORE_WARNING = """
******************************************************************************
WARNING: Restoring a full plus incrementals:
  * Files deleted after the full backup will reappear.
  * Directories renamed after the full appear at their OLD location; see the
    *.fullindex.txt of each generation for how the tree looked at that time.
******************************************************************************
"""


def gen_prefix(prefix, generation):
    """Archivetar prefix for one generation, eg myproj-G0003."""
    return f"{prefix}-G{generation:04d}"


# backup state that must survive between runs lives in the tree being backed
# up, at <tree>/.archivebackup/<prefix>/, and is also uploaded with every
# generation so a restore never needs it.  --bundle-dir is scratch only.
STATE_DIR = ".archivebackup"


def state_dir_for(prefix):
    return Path.cwd() / STATE_DIR / prefix


def meta_path(state_dir, prefix, generation):
    return Path(state_dir) / f"{gen_prefix(prefix, generation)}.backup.json"


def stamp_path(state_dir, prefix, generation):
    return Path(state_dir) / f"{gen_prefix(prefix, generation)}.stamp"


def fullindex_path(scratch, prefix, generation):
    return Path(scratch) / f"{gen_prefix(prefix, generation)}.fullindex.txt"


def pending_path(state_dir, prefix, generation):
    return Path(state_dir) / f"{gen_prefix(prefix, generation)}.pending.json"


def catalog_path(state_dir, prefix, generation):
    return Path(state_dir) / f"{gen_prefix(prefix, generation)}.catalog.txt.gz"


def uncaptured_path(state_dir, prefix, generation):
    return Path(state_dir) / f"{gen_prefix(prefix, generation)}.uncaptured.txt"


def load_generations(state_dir, prefix):
    """Metadata of all completed generations for prefix, oldest first.

    Looks in state_dir itself and in per-generation folders below it (the
    layout archivebackup uses on the Globus destination).
    """
    pattern = f"{prefix}-G[0-9][0-9][0-9][0-9]*.backup.json"
    found = {}
    for p in [*Path(state_dir).glob(pattern), *Path(state_dir).glob(f"*/{pattern}")]:
        with p.open() as f:
            meta = json.load(f)
        if meta.get("backup") == prefix:
            found[meta["generation"]] = meta
    return [found[n] for n in sorted(found)]


def make_stamp(path, epoch):
    """Create the reference file dwalk --cnewer compares against."""
    path.touch()
    os.utime(path, (epoch, epoch))


def is_inside(path, parent):
    """True if path is parent or below it."""
    path, parent = Path(path).resolve(), Path(parent).resolve()
    return path == parent or parent in path.parents


def _dwalk(**kwargs):
    return DWalk(
        inst=archivetar.env.str("AT_MPIFILEUTILS", default=archivetar.fileutils),
        mpirun=archivetar.env.str("AT_MPIRUN", default=archivetar.mpirun),
        umask=0o077,
        **kwargs,
    )


def walk_full(aargs, prefix):
    """Walk the whole tree with archivetar's usual walk; returns the cache."""
    return archivetar.build_list(
        path=".", prefix=prefix, savecache=aargs.save_list, filters=aargs
    )


def write_fullindex(cache, out):
    """Text listing of every entry in the walk: how the tree looked."""
    _dwalk(sort="name").scancache(cachein=cache, textout=out)


def filter_changed(cache, stamp, out):
    """Keep only entries whose ctime is newer than stamp's mtime."""
    _dwalk(filter=["--cnewer", str(stamp)]).scancache(cachein=cache, cacheout=out)


def write_changed_index(cache, out):
    """Text listing of what an incremental is about to archive."""
    _dwalk().scancache(cachein=cache, textout=out)


# perms user group size units <date> /path ; the date never contains " /"
DWALK_LINE = re.compile(
    rb"(\S+)\s+\S+\s+\S+\s+(\d+\.\d+\s+\S+)\s+(.*?)\s(/.*)", re.DOTALL
)


def sort_entries(src, dst):
    """Sort an entries file by path, bytewise, with GNU sort (any size)."""
    subprocess.run(  # nosec
        ["sort", "-t", "\t", "-k1,1", "-T", tempfile.gettempdir(), "-o", dst, src],
        env={**os.environ, "LC_ALL": "C"},
        check=True,
    )


def index_entries(textfile, out):
    """Turn a dwalk text listing into sorted "relpath<TAB>size|mtime" lines.

    Only files and symlinks are kept.  Returns how many were written.
    """
    cwd = os.fsencode(os.getcwd())
    unsorted = Path(f"{out}.unsorted")
    count = 0
    with open(textfile, "rb") as fin, unsorted.open("wb") as fout:
        for line in fin:
            m = DWALK_LINE.match(line.rstrip(b"\n"))
            if not m or m[1][:1] not in (b"-", b"l"):
                continue
            rel = os.path.relpath(m[4], cwd)
            sig = b" ".join(m[2].split()) + b"|" + b" ".join(m[3].split())
            fout.write(rel + b"\t" + sig + b"\n")
            count += 1
    sort_entries(unsorted, out)
    unsorted.unlink()
    return count


def _open(path, mode="rb"):
    """Open plain or gzip-compressed (*.gz) entries files."""
    return gzip.open(path, mode) if str(path).endswith(".gz") else open(path, mode)


def read_entries(path):
    """Yield (relpath, signature) from a sorted entries file."""
    with _open(path) as f:
        for line in f:
            rel, sig = line.rstrip(b"\n").rsplit(b"\t", 1)
            yield rel, sig


def merge_catalog(old, new, out):
    """Catalog = old catalog plus newly archived entries (new wins)."""
    olds, news = read_entries(old), read_entries(new)
    o, n = next(olds, None), next(news, None)
    with _open(out, "wb") as f:
        while o or n:
            if n is None or (o is not None and o[0] < n[0]):
                rec, o = o, next(olds, None)
            else:
                if o is not None and o[0] == n[0]:
                    o = next(olds, None)
                rec, n = n, next(news, None)
            f.write(rec[0] + b"\t" + rec[1] + b"\n")


def find_uncaptured(current, catalog, out):
    """Paths on disk now that no generation archived in this exact form.

    NEW      path is not in the catalog at all (renamed or moved in with mv)
    CHANGED  path is cataloged but size/mtime differ and it was not archived
             (replaced by a different file moved in with mv)
    Returns the number of lines written to out.
    """
    cat = read_entries(catalog)
    c = next(cat, None)
    count = 0
    with open(out, "wb") as f:
        for path, sig in read_entries(current):
            while c is not None and c[0] < path:
                c = next(cat, None)
            if c is not None and c[0] == path:
                if c[1] == sig:
                    continue
                f.write(b"CHANGED\t" + path + b"\n")
            else:
                f.write(b"NEW\t" + path + b"\n")
            count += 1
    return count


def report_uncaptured(listing, count, show=10):
    print(UNCAPTURED_ERROR.format(count=count, listing=listing), file=sys.stderr)
    with open(listing, "rb") as f:
        for i, line in enumerate(f):
            if i == show:
                print(f"  ... and {count - show} more", file=sys.stderr)
                break
            print("  " + os.fsdecode(line.rstrip(b"\n")), file=sys.stderr)


def globus_client(aargs):
    """GlobusTransfer for the backup's source and destination collections."""
    from GlobusTransfer import GlobusTransfer

    return GlobusTransfer(
        aargs.source,
        aargs.destination,
        aargs.destination_dir,
        notify_on_succeeded=aargs.no_notify_on_succeeded,
        notify_on_failed=aargs.no_notify_on_failed,
        notify_on_inactive=aargs.no_notify_on_inactive,
        fail_on_quota_errors=aargs.fail_on_quota_errors,
        skip_source_errors=aargs.skip_source_errors,
        preserve_timestamp=aargs.preserve_timestamp,
    )


def destination_state(aargs, name):
    """Highest <name>-G#### folder on the destination, and if it is complete.

    Returns (generation, complete); (0, True) when there is none yet.  Only
    listings are used; nothing is ever copied back from the destination.
    """
    import globus_sdk

    globus = globus_client(aargs)
    pattern = re.compile(rf"{re.escape(name)}-G(\d{{4,}})")
    try:
        entries = globus.tc.operation_ls(aargs.destination, path=aargs.destination_dir)
    except globus_sdk.TransferAPIError as e:
        if e.http_status == 404:  # destination folder not created yet
            return 0, True
        raise
    numbers = [
        int(m[1])
        for e in entries
        if e["type"] == "dir" and (m := pattern.fullmatch(e["name"]))
    ]
    if not numbers:
        return 0, True
    top = max(numbers)
    folder = PurePosixPath(aargs.destination_dir) / gen_prefix(name, top)
    names = {
        e["name"] for e in globus.tc.operation_ls(aargs.destination, path=str(folder))
    }
    return top, f"{gen_prefix(name, top)}.backup.json" in names


def plan_generation(gens, dest_top, dest_complete):
    """Pick the next generation number from local state and the destination.

    Returns (generation, state_behind).  state_behind means the destination
    has generations the local state does not know about (it was lost or is
    stale), so the next run must be a full under a new number.  A number is
    never reused, except to retry a destination folder that was never
    completed.
    """
    local_last = gens[-1]["generation"] if gens else 0
    if dest_top <= local_last:
        return local_last + 1, False
    if dest_top == local_last + 1 and not dest_complete:
        return dest_top, False  # an interrupted run; retry it
    return dest_top + 1, True


def make_destination_dir(aargs, path):
    """Create this generation's folder on the Globus destination."""
    import globus_sdk

    try:
        globus_client(aargs).tc.operation_mkdir(aargs.destination, str(path))
    except globus_sdk.TransferAPIError as e:
        if "Exists" not in str(e.code):
            raise


def generation_destination(aargs, prefix):
    """Point Globus uploads at a folder of their own for this generation.

    Like rdiff-backup increments: nothing from an earlier generation is ever
    overwritten, including files over --size that Globus sends outside the
    tars.  Updates aargs and returns the options to append for archivetar,
    including --wait so a generation only counts once Globus confirms it.
    """
    aargs.destination_dir = str(PurePosixPath(aargs.destination_dir) / prefix)
    if not aargs.dryrun:
        make_destination_dir(aargs, aargs.destination_dir)
    return ["--destination-dir", aargs.destination_dir, "--wait"]  # last wins


def run_archivetar(argv):
    """Run archivetar in-process; returns its exit code."""
    try:
        archivetar.main(argv)
    except SystemExit as e:
        return e.code or 0
    return 0


def parse_backup_args(argv):
    """Split archivebackup options from the archivetar options passed through."""
    parser = argparse.ArgumentParser(
        prog="archivebackup",
        description="Full and incremental backups using archivetar. "
        "Any other archivetar option (--bundle-dir, --destination-dir, "
        "--tar-size, compression, ...) is passed through to archivetar.",
        epilog=INCREMENTAL_WARNING,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-p",
        "--prefix",
        required=True,
        help="Backup name. Generations are named <prefix>-G0001, <prefix>-G0002, ...",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Start a new full backup instead of an incremental",
    )
    parser.add_argument(
        "--full-every",
        type=int,
        default=0,
        metavar="N",
        help="Automatically run a full after N incrementals (default: never)",
    )
    parser.add_argument(
        "--keep-fulls",
        type=int,
        default=0,
        metavar="N",
        help="After a successful run, list older generations that can be deleted, "
        "keeping the newest N full backups and everything after them. Nothing is "
        "deleted automatically. (default: list nothing)",
    )
    # handled here so they are not forwarded to archivetar
    parser.add_argument("--save-list", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--list", help=argparse.SUPPRESS)
    return parser.parse_known_args(argv)


def check_options(aargs):
    """Refuse archivetar options that would make a backup unsafe or incomplete."""
    if not (aargs.source and aargs.destination and aargs.destination_dir):
        sys.exit(
            "archivebackup requires Globus: --source, --destination and "
            "--destination-dir (the backup lives on the destination)"
        )
    if aargs.remove_files:
        sys.exit("--remove-files deletes your data; it is not allowed for backups")
    if not aargs.bundle_dir:
        sys.exit(
            "--bundle-dir is required: scratch space outside the directory being "
            "backed up for tars and lists (it can be wiped between runs)"
        )
    if is_inside(aargs.bundle_dir, Path.cwd()):
        sys.exit(f"--bundle-dir {aargs.bundle_dir} must be outside {Path.cwd()}")
    for opt in ("atime", "mtime", "ctime"):
        if getattr(aargs, opt):
            sys.exit(
                f"--{opt} is not allowed for backups: files outside the filter would "
                "never be backed up, and archivebackup already selects changed files"
            )


def choose_kind(gens, full=False, full_every=0):
    """Return "full" or "incremental" for the next generation."""
    if not gens or full:
        return "full"
    incrementals_since_full = 0
    for g in reversed(gens):
        if g["type"] == "full":
            break
        incrementals_since_full += 1
    if full_every and incrementals_since_full >= full_every:
        logging.info(f"{incrementals_since_full} incrementals since last full")
        return "full"
    return "incremental"


def recover_interrupted(state_dir, scratch, prefix, generation):
    """Clean up after a run of `generation` that never completed.

    Returns the kind ("full"/"incremental") that run was going to be, or None.
    Its partial tars and lists would otherwise block the retry, which reuses
    the same generation number.  A partial folder on the Globus destination
    is simply overwritten by the retry.
    """
    gp = gen_prefix(prefix, generation)
    pending = pending_path(state_dir, prefix, generation)
    kind = json.loads(pending.read_text())["type"] if pending.exists() else None
    leftovers = [
        p
        for d in (Path(state_dir), Path(scratch))
        for p in [*d.glob(f"{gp}-*"), *d.glob(f"{gp}.*")]
        if p != pending
    ]
    if kind or leftovers:
        logging.warning(
            f"Generation {generation} ({kind or 'unknown'}) did not complete; "
            f"removing its {len(leftovers)} partial local file(s) and retrying it"
        )
        for p in leftovers:
            p.unlink()
    return kind


def upload_and_wait(aargs, paths, label):
    """Upload files with Globus and wait; raises if the transfer fails."""
    globus = globus_client(aargs)
    for path in paths:
        globus.add_item(Path(path).resolve(), label=label, in_root=True)
    taskid = globus.submit_pending_transfer()
    logging.info(f"Globus Transfer of {label}: {taskid}, waiting for it")
    globus.task_wait(taskid)


def prunable(gens, keep_fulls):
    """Generations older than the newest keep_fulls full backups."""
    fulls = [g for g in gens if g["type"] == "full"]
    if not keep_fulls or len(fulls) <= keep_fulls:
        return []
    cutoff = fulls[-keep_fulls]["generation"]
    return [g for g in gens if g["generation"] < cutoff]


def report_prunable(gens, keep_fulls, state_dir):
    old = prunable(gens, keep_fulls)
    if not old:
        return
    lines = [
        "",
        "*" * 78,
        f"Keeping the newest {keep_fulls} full backup(s) and everything after them.",
        "These older generations are no longer needed and can be deleted:",
        "",
    ]
    for g in old:
        if g.get("destination_dir"):
            lines.append(f"  Globus {g['destination_dir']}/")
        lines.append(f"  local  {Path(state_dir).resolve()}/{g['prefix']}[-.]*")
    lines += [
        "",
        "archivebackup does not delete anything itself. They stop being listed",
        "once their local *.backup.json is removed.",
        "*" * 78,
    ]
    print("\n".join(lines), file=sys.stderr)


def check_same_source(name, parent):
    """Incrementals must walk the same directory as the chain they extend."""
    if parent.get("source_dir") not in (None, str(Path.cwd())):
        sys.exit(
            f"archivebackup: {name} backs up {parent['source_dir']}; run it "
            "from there, or use --full to start a new chain from here"
        )


def select_changed(state_dir, name, parent, cache, prefix):
    """Filter the walk to files changed since the parent generation.

    Returns (cache to archive, sorted entries file of it, number of entries).
    """
    parent_stamp = stamp_path(state_dir, name, parent["generation"])
    if not parent_stamp.exists():
        # rebuild a lost stamp from the time recorded in its metadata
        logging.warning(f"Recreating missing {parent_stamp}")
        make_stamp(parent_stamp, parent["stamp_epoch"])
    parent_catalog = catalog_path(state_dir, name, parent["generation"])
    if not parent_catalog.exists():
        sys.exit(
            f"archivebackup: {parent_catalog} is missing so this backup cannot "
            "be checked for files that were missed; run a new full with --full"
        )
    tmp = Path(tempfile.gettempdir())
    to_archive = tmp / f"{prefix}-changed.cache"
    filter_changed(cache, parent_stamp, to_archive)
    changed_txt = tmp / f"{prefix}-changed.txt"
    write_changed_index(to_archive, changed_txt)
    archived = tmp / f"{prefix}.archived.entries"
    n_archived = index_entries(changed_txt, archived)
    changed_txt.unlink()
    return to_archive, archived, n_archived


def start_generation(bargs, aargs, state_dir, scratch, gens):
    """Decide this run's generation number and kind, and mark it pending."""
    dest_top, dest_complete = destination_state(aargs, bargs.prefix)
    generation, state_behind = plan_generation(gens, dest_top, dest_complete)
    interrupted = recover_interrupted(state_dir, scratch, bargs.prefix, generation)
    if state_behind:
        logging.warning(
            f"{bargs.prefix}-G{dest_top:04d} exists on the destination but not in "
            f"{state_dir}; the local backup state is missing or out of date, so "
            f"this run is a new full as generation {generation}"
        )
        gens = []  # nothing local to build an incremental on
    kind = choose_kind(gens, bargs.full or interrupted == "full", bargs.full_every)
    if kind == "incremental":
        check_same_source(bargs.prefix, gens[-1])
    pending = pending_path(state_dir, bargs.prefix, generation)
    pending.write_text(json.dumps({"type": kind, "started": time.time()}))
    return generation, kind, (gens[-1] if gens and kind == "incremental" else None)


def backup_main(argv):
    """archivebackup entry point."""
    bargs, passthrough = parse_backup_args(argv[1:])

    if bargs.list:
        sys.exit("archivebackup builds its own file list; --list is not allowed")

    # validate the pass-through options exactly as archivetar would
    aargs = archivetar_parse_args(["--prefix", bargs.prefix] + passthrough)
    aargs.save_list = bargs.save_list

    logging.basicConfig(
        level=(
            logging.WARNING
            if aargs.quiet
            else logging.DEBUG if aargs.verbose else logging.INFO
        )
    )

    check_options(aargs)

    state_dir = state_dir_for(bargs.prefix)
    state_dir.mkdir(parents=True, exist_ok=True)
    scratch = Path(aargs.bundle_dir)
    scratch.mkdir(parents=True, exist_ok=True)
    gens = load_generations(state_dir, bargs.prefix)
    generation, kind, parent = start_generation(bargs, aargs, state_dir, scratch, gens)
    prefix = gen_prefix(bargs.prefix, generation)
    pending = pending_path(state_dir, bargs.prefix, generation)
    logging.info(f"----> Backup {bargs.prefix} generation {generation} ({kind})")
    if kind == "incremental":
        print(INCREMENTAL_WARNING, file=sys.stderr)

    # lay down this generation's stamp BEFORE walking: anything changed while
    # we run has a newer ctime and is picked up again by the next incremental
    stamp_epoch = time.time() - STAMP_BACKDATE
    stamp = stamp_path(state_dir, bargs.prefix, generation)
    make_stamp(stamp, stamp_epoch)

    cache = walk_full(aargs, prefix)
    fullindex = fullindex_path(scratch, bargs.prefix, generation)
    write_fullindex(cache, fullindex)

    tmp = Path(tempfile.gettempdir())
    current = tmp / f"{prefix}.current.entries"
    n_current = index_entries(fullindex, current)

    if kind == "full":
        to_archive = cache
        archived, n_archived = current, n_current
    else:
        parent_catalog = catalog_path(state_dir, bargs.prefix, parent["generation"])
        to_archive, archived, n_archived = select_changed(
            state_dir, bargs.prefix, parent, cache, prefix
        )

    extra = generation_destination(aargs, prefix)

    empty = n_archived == 0
    if empty:
        logging.info("No new or changed files; recording an empty generation")
    else:
        argv = ["archivetar", "--prefix", prefix, "--list", str(to_archive)]
        rc = run_archivetar(argv + passthrough + extra)
        if rc != 0:
            sys.exit(rc)

    # catalog of everything captured since the last full, and the check for
    # anything on disk that it does not cover
    catalog = catalog_path(state_dir, bargs.prefix, generation)
    uncaptured = uncaptured_path(state_dir, bargs.prefix, generation)
    n_uncaptured = 0
    if kind == "full":
        with open(current, "rb") as fin, gzip.open(catalog, "wb") as fout:
            shutil.copyfileobj(fin, fout)
        current.unlink()
    else:
        merge_catalog(parent_catalog, archived, catalog)
        n_uncaptured = find_uncaptured(current, catalog, uncaptured)
        current.unlink()
        archived.unlink()
    if not n_uncaptured:
        uncaptured.unlink(missing_ok=True)
    logging.info(f"Catalog: {n_current} files on disk, {n_archived} archived now")

    if aargs.dryrun:
        logging.info("--dryrun: not recording this generation")
        if n_uncaptured:
            report_uncaptured(uncaptured, n_uncaptured)
        for path in (stamp, fullindex, catalog, uncaptured, pending):
            path.unlink(missing_ok=True)
        return

    meta = {
        "backup": bargs.prefix,
        "generation": generation,
        "prefix": prefix,
        "type": kind,
        "empty": empty,
        "stamp_epoch": stamp_epoch,
        "stamp_time": datetime.datetime.fromtimestamp(stamp_epoch).isoformat(),
        "parent_generation": parent["generation"] if parent else None,
        "parent_stamp_time": parent["stamp_time"] if parent else None,
        "completed": datetime.datetime.now().isoformat(),
        "source_dir": str(Path.cwd()),
        "fullindex": fullindex.name,
        "catalog": catalog.name,
        "uncaptured": n_uncaptured,
        "destination_dir": aargs.destination_dir,
        "archivetar_options": passthrough,
    }
    mpath = meta_path(state_dir, bargs.prefix, generation)
    with mpath.open("w") as f:
        json.dump(meta, f, indent=2)

    # a generation only counts once its metadata is safely on the destination
    # too; otherwise withdraw it so the next run retries
    try:
        upload_and_wait(aargs, [fullindex, catalog, mpath], label="Backup metadata")
    except Exception:
        mpath.unlink()
        raise
    pending.unlink()
    logging.info(f"Recorded {mpath}")

    # only the newest stamp and catalog are needed; they cover the whole chain
    for g in gens:
        if g["generation"] < generation:
            catalog_path(state_dir, bargs.prefix, g["generation"]).unlink(
                missing_ok=True
            )
            stamp_path(state_dir, bargs.prefix, g["generation"]).unlink(missing_ok=True)

    report_prunable(gens + [meta], bargs.keep_fulls, state_dir)

    if n_uncaptured:
        report_uncaptured(uncaptured, n_uncaptured)
        sys.exit(EXIT_UNCAPTURED)


def restore_chain(gens, generation=None):
    """Generations to extract, in order, to restore up to `generation`."""
    if not gens:
        raise ValueError("no backup generations found")
    numbers = [g["generation"] for g in gens]
    target = generation if generation is not None else numbers[-1]
    if target not in numbers:
        raise ValueError(f"generation {target} not found, have {numbers}")

    upto = [g for g in gens if g["generation"] <= target]
    fulls = [g for g in upto if g["type"] == "full"]
    if not fulls:
        raise ValueError(f"no full backup at or before generation {target}")
    start = fulls[-1]["generation"]

    chain = [g for g in upto if g["generation"] >= start]
    expected = list(range(start, target + 1))
    if [g["generation"] for g in chain] != expected:
        missing = sorted(set(expected) - {g["generation"] for g in chain})
        raise ValueError(f"metadata missing for generation(s) {missing}")
    return [g for g in chain if not g["empty"]]


def copy_large_files(gen_dir, prefix, uargs):
    """Copy files Globus sent outside the tars (over --size) into cwd.

    They sit in the generation's folder under their original relative path;
    archivetar's own files (named <prefix>-... or <prefix>.*) are skipped.
    """
    for src in sorted(gen_dir.rglob("*")):
        rel = src.relative_to(gen_dir)
        if not src.is_file() or (
            len(rel.parts) == 1 and rel.name.startswith((f"{prefix}-", f"{prefix}."))
        ):
            continue
        if uargs.folder and not str(rel).startswith(uargs.folder.rstrip("/") + "/"):
            continue
        dest = Path.cwd() / rel
        logging.info(f"Restoring large file {rel}")
        if not uargs.dryrun:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)


def restore_main(argv):
    """archiverestore entry point."""
    parser = argparse.ArgumentParser(
        prog="archiverestore",
        description="Restore an archivebackup into the current directory: the "
        "most recent full, then each incremental after it in order. Other "
        "unarchivetar options are passed through.",
        epilog=RESTORE_WARNING,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-p", "--prefix", required=True, help="Backup name")
    parser.add_argument(
        "--from",
        dest="source",
        default=".",
        help="Where the backup was copied to: either the per-generation folders "
        "from the Globus destination, or one folder with all tars and "
        "*.backup.json files (default: current directory)",
    )
    parser.add_argument(
        "--generation",
        type=int,
        default=None,
        help="Restore the tree as of this generation (default: latest)",
    )
    parser.add_argument(
        "--list-generations",
        action="store_true",
        help="Show the generations found and exit",
    )
    rargs, passthrough = parser.parse_known_args(argv[1:])
    uargs = archivetar.unarchivetar.parse_args(["--prefix", "x"] + passthrough)
    if uargs.keep_old_files or uargs.skip_old_files or uargs.keep_newer_files:
        # these would stop a later generation replacing a file an earlier one
        # just restored, leaving old versions behind
        sys.exit(
            "archiverestore: --keep-old-files, --skip-old-files and "
            "--keep-newer-files would keep older generations' versions; restore "
            "into an empty directory instead"
        )
    logging.basicConfig(level=logging.INFO)

    source = Path(rargs.source)
    gens = load_generations(source, rargs.prefix)
    if rargs.list_generations:
        for g in gens:
            note = " (empty)" if g["empty"] else ""
            print(f"{g['prefix']}  {g['type']:<11} {g['stamp_time']}{note}")
        return

    try:
        chain = restore_chain(gens, rargs.generation)
    except ValueError as e:
        sys.exit(f"archiverestore: {e}")

    def gen_dir(g):
        sub = source / g["prefix"]
        return sub if sub.is_dir() else source

    missing = [
        g["prefix"] for g in chain if not find_prefix_files(g["prefix"], gen_dir(g))
    ]
    if missing:
        sys.exit(f"archiverestore: no tars found in {source} for {missing}")

    print(RESTORE_WARNING, file=sys.stderr)
    for g in chain:
        logging.info(f"----> Restoring {g['prefix']} ({g['type']}, {g['stamp_time']})")
        argv = ["unarchivetar", "--prefix", g["prefix"]]
        argv += ["--archive-dir", str(gen_dir(g))] + passthrough
        try:
            archivetar.unarchivetar.main(argv)
        except SystemExit as e:
            if e.code:
                sys.exit(e.code)
        # in generation order, so a later version always wins
        if gen_dir(g) != source:
            copy_large_files(gen_dir(g), g["prefix"], uargs)
