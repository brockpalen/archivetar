Using Archivetar / Archivepurge / Unarchivetar
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

*NOTE* archivetar is not meant to be a backup tool, but can fake it when used
carefully.  By using the filtering options `--atime` `--mtime` `--ctime`
`archivetar` can select only files matching the filters specifically files that
were changed.  Incorrectly using settings can cause gaps resulting in data loss.
We recommend repeating a full copy periodically to correct for any missing data.

Work-flow, grab all files modified sense last run of `archivetar` and placing
them in their own folder. 

Limitations, `archivetar` cannot track deletion of files. For users not using
Globus (`--destination-path`) setting a size cutoff `--size` you can not tell
what files larger than `--size` need to be copied. If using Globus those large
files and the tars created are uploaded.

```
# initial full backup
archivetar --prefix full-backup --source <UUID> --destination <UUID>
--destination-path /path/on/dest/project/full/  

# 7 day later grab all files changed (ctime) less than 8 days ago (small overlap
# recommended)
archivetar --prefix inc-backup-7day --source <UUID> --destination <UUID>
--destination-path /path/on/dest/project/inc-7day/ --ctime -8
```

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

Managing Globus Transfers
------------------------

By default `archivetar` will hand off transfers to globus to manage and not wait
for them to finish.  This is ok in most cases but not ones where you want to
know the transfer is complete before modifying / deleting data or scripting
multiple archives. 

The `--wait` option tells archivetar to wait for all globus transfer to finish.
It will also print print globus performance information as it runs. 

The option `--rm-at-files`  implies `--wait` for tars _only_ and not transfers
created by the `--size` option.
