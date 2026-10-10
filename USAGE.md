Using Archivetar / Archivepurge / Unarchivetar / Archivebackup
==============================================

Quick Start
-----------

Uses default settings and will create `archivetar-1.tar archivetar-2.tar ...
archivetar-N.tar` This will tar every file as no `--size` cutoff is provided.

```
archivetar --prefix myarchive 
```

### Specify small file cutoff and size before creating a new tar

```
# the tar size is a minimum, so tars may be much larger than listed here. The
# size is also the size before compression
archivetar --prefix myarchive --size 20G --tar-size 10G
```

### Expand archived directory

```
 unarchivetar --prefix project1

 # tars somewhere else, extract into the current directory
 unarchivetar --prefix project1 --archive-dir /scratch/me/tars
```

### Upload via Globus to Archive

```
archivetar --prefix project1 --source <globus UUID> 
 --destination <globus UUID> --destination-path <path on archive>
 ```

Deleting files in Tars
 -------------------

`archivepurge` is a wrapper around `drm` from mpiFileUtils and is much faster
than `rm`. It is intended to be used to remove all the files that were tard as
part of `archivetar` but not any that were not.  This is most commonly used to
prep uploading a directory to an archive when not using Globus.

Run `archivetar` with `--save-purge-list`. This will create an extra file that
is passed to `archivepurge --purge-list <file>.cache`.

Archiving Full Volumes
----------------------

When trying to archive data from a volume without free space requires another
volume with free space and using the `--bundle-path` option to redirect the
creation of tars and indexes to an alternative storage location eg `scratch` or
`tmp`.  This option is safe to use with Globus transfers.

```
archivetar --prefix project1 --bundle-path /tmp/
```

Backups with Archivetar
-----------------------

*NOTE* archivetar is not a backup system. `archivebackup` and `archiverestore`
make simple full + incremental copies to a Globus archive easy, but read the
limitations below before relying on them.

`archivebackup` does the timestamp bookkeeping for you. Every run is a
*generation* named `<prefix>-G0001`, `<prefix>-G0002`, ...

 * The first run (or any run with `--full`) archives everything.
 * Later runs archive only files whose ctime changed since the previous run.
 * Each generation is uploaded to its own folder on the destination, like
   rdiff-backup increments, so nothing from an earlier generation is ever
   overwritten:

```
/archive/project1/project1-G0001/   tars, files over --size, metadata
/archive/project1/project1-G0002/   ...
```

Run it from the top of the directory to back up, like `archivetar`, always with
the same `--prefix`. Globus is required. All other `archivetar` options
(`--tar-size`, `--size`, compression, `--checksum`, `--rm-at-files`, ...) are
passed through.

```
# first run is a full, later runs only archive what changed
archivebackup --prefix project1 --bundle-dir /tmp/project1 \
  --source <UUID> --destination <UUID> --destination-path /archive/project1/ \
  --size 100G --tar-size 100G --zstd

# force a new full, or do one automatically after every 10 incrementals
archivebackup ... --full
archivebackup ... --full-every 10
```

`--size` is safe here: files over `--size` are sent as-is into that
generation's folder under their original path, so every version is kept.

### Where things live

 * **The destination is the backup.** Each generation's folder holds its tars,
   its files over `--size`, `<prefix>-G####.backup.json` (metadata),
   `<prefix>-G####.fullindex.txt` (a listing of every file as the tree looked
   at that moment) and `<prefix>-G####.catalog.txt.gz` (see below). A restore
   needs nothing else, so you can rebuild after losing the source entirely.
 * **`.archivebackup/<prefix>/` in the directory being backed up** holds the
   small state the next run needs (latest metadata, stamp and catalog). It is
   backed up along with everything else. If it is lost, the next run sees the
   generations already on the destination and starts a new full under the next
   number; a generation number is never reused.
 * **`--bundle-dir` is scratch**: tars and lists are built in a private (0700)
   `archivebackup-<prefix>/` directory inside it, so a shared space like `/tmp`
   is fine. It must be outside the directory being backed up and can be wiped
   between runs; `--rm-at-files` removes tars as their uploads finish.

`archivebackup` always adds `--wait`: a generation only counts once Globus
confirms every tar and large file, and then its metadata, fullindex and catalog
are uploaded and confirmed too.

`--prefix` may only contain letters, digits, `.`, `_` and `-`.

Refused, because they would delete data or make the backup silently
incomplete while recording files as backed up: `--list`, `--save-list`,
`--remove-files`, `--save-purge-list`, `--atime` / `--mtime` / `--ctime`,
`--dereference`, `--ignore-failed-read`, `--skip-source-errors`, and
`--tar-options` that exclude files. `--user` and `--group` are fine for choosing
whose data is backed up; filters combine with AND. A file that cannot be read
or vanishes while the backup runs fails that run, and the next run retries it.

Exit status: 0 success; 3 files exist that no generation has captured (run
`--full`); 4 another run of the same backup is still going, so this one did
nothing; anything else means the run failed and the next run retries the
same generation.

### Scheduling

Runs can be as frequent as you like (hourly, daily, ...): incrementals compare
against the exact time of the previous run, not whole days. Only one run of a
backup happens at a time: each run locks `.archivebackup/<prefix>/lock`, and a
run that starts while the previous one is still going exits with status 4
without touching anything, so a scheduled job just skips its turn. The lock is
released automatically when a run ends or dies, so it never goes stale.

Stamps are back-dated 5 minutes to cover clock differences between this host
and the storage servers (`AT_BACKUP_STAMP_BACKDATE` seconds to change). A few
files may be archived twice; none are skipped.

### What incrementals capture

Incrementals archive files whose ctime changed since the last run. That
includes everything written, copied, `scp`'d, `rsync`'d (even `rsync -a` /
`cp -p`, which keep the old mtime: the ctime is always new) or `mv`'d in from
another filesystem. Adding a new genome run or new scans just works.

**What they do not capture:**

 * **Directories renamed, or moved in with `mv` from elsewhere on the same
   filesystem.** A rename does not change the ctime of the files inside it.
 * **Deletions.** Deleted files reappear in a restore.

**The catalog check.** So nobody finds out at restore time, the catalog lists
every file path (with its size and mtime) archived since the last full. After
each incremental, anything on disk that the catalog does not cover is either
*NEW* (renamed / moved in with `mv`) or *CHANGED* (replaced by a different file
moved in with `mv`). If there are any, `archivebackup` still records the
generation, then prints an ERROR, lists them in
`.archivebackup/<prefix>/<prefix>-G####.uncaptured.txt` and **exits with status
3**. It keeps doing so on every run until you run a new full with `--full`.

The catalog is gzip compressed, roughly 10-20MB per million files. The check
compares size and mtime as dwalk prints them (3 significant digits, to the
minute), so a replacement of the same name, nearly the same size and the same
minute moved in with `mv` can still slip past. Paths containing newlines are
not supported.

### New fulls, failures and cleanup

A new full is just the next generation number in its own folder; it never
overwrites or deletes an earlier one, so the previous chain stays complete
while the new full runs, like rotating tape sets.

If a run fails (tar error, Globus failure, killed job), that generation is not
recorded and restores use the previous ones. The next run notices, removes the
partial local files, and retries the same generation number; an interrupted
full is retried as a full. The partial folder on the destination is
overwritten by the retry.

`archivebackup` never deletes backups. With `--keep-fulls N` it lists, after a
successful run, the destination folders of generations older than the newest N
fulls, which can be deleted:

```
archivebackup ... --keep-fulls 2
```

### Restoring

Copy the backup's folder back from the archive (the whole `/archive/project1/`
with its per-generation folders), then run `archiverestore` from an **empty**
directory to restore into, pointing `--from` at the copy. It extracts the most
recent full and then each incremental after it, in order, so the newest version
of each file wins. Files over `--size` are copied in right after their
generation's tars, so they are always the right version too.

```
mkdir /scratch/me/project1-restored && cd /scratch/me/project1-restored
archiverestore --prefix project1 --from /scratch/me/staging --list-generations
archiverestore --prefix project1 --from /scratch/me/staging                 # latest
archiverestore --prefix project1 --from /scratch/me/staging --generation 7  # as of G7
```

`unarchivetar` options such as `--tar-processes`, `--folder` and
`--which-archive` are passed through. `--keep-old-files`, `--skip-old-files` and `--keep-newer-files` are
refused, because they would stop a later generation replacing a file an
earlier one just restored.

Files deleted after the full come back, and renamed directories appear at the
location they had in the full. Use the `*.fullindex.txt` of the generation you
restored to see where everything was at the time.

Archiving Specific Files (Filters)
----------------------------------

`archivetar` wraps
[mpiFileUtils](https://mpifileutils.readthedocs.io/en/latest/).  Thus we are
able to use many of the options in `dfind` to filter the initial list of files
to only archive a subset of data.

*NOTE* If not using Globus to upload using the `--size` option you will not have
an simple way without manually using `dcp` with the `over.cache` created by
archivetar.  So it is not recommended unless using Globus to upload the data to
another location.

Currently `archivetar` understands the following filters:

```
 --atime --mtime --ctime --user --group
```

Multiple filters use logical and, eg `--atime +180 --user brockp`  will archive
only files accessed more than 180days ago AND owned by user `brockp`.

Filters are only applied in the initial scan.  They are ignored if used with the
`--list` option.

It is possible to use filters with `archivepurge` archive all files from a
specific user and delete.  Use `--save-list` rather than `--save-purge-list`
because the first has ALL files to be archived, not just those in tars.

```
# find and archive all files owned by user `brockp` in given group space.
# scan once and get meta-data but do not archive
archivetar --prefix brockp-archive --user brockp --dryrun --save-list

# actually archive using list created above update timestamp
archivetar --prefix brockp-archive --list brockp-archive-<timestamp>.cache
--source <UUID> --destination <UUID> --destination-path
/path/on/dest/brockp-archive/ --size 1G

# once all transfer above finish delete file in the initial list
archivepurge --purge-list brockp-archive-<timestamp>.cache
```

Recovering Specific Folders (partial restores)
----------------------------------------------

Restoring sub folders is a multi-step process.

1. Pull back the `DONT_DELETE.txt` files
1. (optionally) pull back the folder with big files if archived with `--size
   <size>`
1. Find the needed tars with: `unarchivetar --prefix my-prefix --which-archive
   --folder "exactfolder/subfolder"`
1. Recall the required tars returned by the prior command
1. Expand: `unarchivetar --prefix my-prefix --folder "exactfolder/subfolder"`

Folder names must be exact and not have a trailing `/`. You can optionally use
`grep` and look around the `index` and `DONT_DELETE` files yourself if unsure of
the exact name.

Managing Globus Transfers
------------------------

By default `archivetar` will hand off transfers to globus to manage and not wait
for them to finish.  This is ok in most cases but not ones where you want to
know the transfer is complete before modifying / deleting data or scripting
multiple archives. 

The `--wait` option tells archivetar to wait for all Globus transfer to finish.
It will also print print Globus performance information as it runs. 

The option `--rm-at-files`  implies `--wait` for tars _only_ and not transfers
created by the `--size` option.


Environment Variables
---------------------

Several `archivetar` settings are controlled by environment variables (handy
for setting or overriding defaults, or for site-specific customization, e.g.
inside Lmod modules or personal shell starup files).  See the
[configuration section of INSTALL.md](INSTALL.md#configuration) for details.


Checksums
---------

The checksum feature is on by default as most find they want it when it's to
late.  It has 2 primary features `--checksum` or `--no-checksum` and
`--no-force-local-checksum` or `--force-local-checksum` both are on by default.
Disabeling local checksum for files filtered by `--size` when using globus
will use the Globus calculated checksum from the transfer.

To compare checksums grab the `*.sha1` files and run:

```
sha1sum -c *.sha1

# optionally only print if a missmatch
sha1sum --quiet -c *.sha1

# If you have very fast storage or high latency storage
ls *.sha1 | parallel sha1sum --quiet -c {} 
```

Symlinks
--------

Be careful when using symlinks. Archivetar will grab the links but not what they
point to unless you pass `--dereference`  which passes that option to the
underlying `tar`.  Behavior can be odd so be sure you understand what you are
doing. Specificaly if you have multiple links pointing to the same file, will
cause the data to be stored multiple times and the links are replaced with what
they point to.

The `--size` option does not follow symlinks as that's part of mpifileutils and
we don't control it. So you may have `--size 1G` and have symlink pointing to a
100G file will still be added to the tarball as a 'small file' though archivetar
will respect the size of the object the link points to for `--tar-size` to avoid
tar's blowing up in size.
