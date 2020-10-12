[![Build Status](https://travis-ci.com/brockpalen/archivetar.svg?branch=master)](https://travis-ci.com/brockpalen/archivetar)
[![codecov](https://codecov.io/gh/brockpalen/archivetar/branch/master/graph/badge.svg)](https://codecov.io/gh/brockpalen/archivetar)



Archivetar
==========

archivetar (V2) is a collection of several tools intended to make the archiving and the use big data easier. 
Targeted mostly at the research / HPC use case it is useful in other cases where having fewer files but not one gigantic file is beneficial.

`archivetar` was to make our Spectrum Archive install [Data Den](https://arc-ts.umich.edu/data-den/) more useful. It has no dependencies on SA, and could easily be used with AWS Glacier, HPSS, DMF etc.  Any service where you want to minimize the number of objects keeping the data/object ratio high.

For additional performance `archivetar` will detect many multi-core capable compressors.

#### Example Output

```
# number of files before
$ find . -type f | wc -l
6656

# bundle all files < 1M, into tars 200M in size
# Delete input files
archivetar --prefix boxout --remove-files --size 1M --tar-size 200M

# number of files after
$ find . -type f | wc -l
1831

# expand using tar
for x in $(ls boxout*.tar)
   tar -xf $x
done

# Alternative using provided unarchivetar
unarchivetar --prefix boxout
```

### archivetar vs tar


archivetar doesn't try to replace tar. Actually  it uses it internally rather than Pythons native implementation.  

Usage
-----

### Quick Start

Uses default settings and will create `archivetar-1.tar archivetar-2.tar ... archivetar-N.tar`

```
archivetar --prefix myarchive
```

### Specify small file cutoff and size before creating a new tar

```
# this will work, but the tar size is a minimum, so tars may be much larger than listed here
archivetar --prefix myarchive --size 20G --tar-size 10G
```

### Specify prefix for tar names

This will create `project1-1.tar project1-2.tar` etc.

```
archivetar --prefix project1
```

### Expand archived directory

```
unarchivetar --prefix project1
```

Workflow
========


 * Scan current directory using [mpiFileUtils](https://github.com/hpc/mpifileutils)
 * Optionally filter only files under `--size`
 * Build sub-tar's where each tar aims to be (before compression) to be at least `--tar-size`
 * Optionally delete 
  * Use `--remove-files` to delete files as they are added to tar
 * Re-hydrate an archived directory with `unarchivetar --prefix <prefix>`

Building archivetar
-------------------

### Requirements

 * Patched [mpiFileUtils](https://github.com/brockp/mpifileutils) `build.sh` is a shortcut
 * python3.6+
 * `pip install pipenv`
 * `pipenv install`
 * `pipenv run pyinstaller bin/archivetar --onefile`   # create executable no need for pipenv
 * `pipenv run pyinstaller bin/unarchivetar --onefile`   # create executable no need for pipenv


#### Install using PIP

Archivetar does use setuptools so it can be installed by `pip` to add to your global config. It does require manual setup of the external mpiFileUtils.

 * Need to still build mpiFileUtils and setup environment variables for configuration
 * `pip install git+https://github.com/brockpalen/archivetar.git`

### Configuration

Archivetar uses environment for configuration

```
AT_MPIFILEUTILS=<path to mpifileutils install>
AT_MPIRUN=<path to mpirun used with mpifileutils>
```

#### Dev options

 * pipenv install --dev
 * pipenv shell  ( like venv activate )
 * pytest

### Optional add ons

Most are auto detected in the primary executable is in `$PATH`

 * lbzip2, pbzip (parallel bzip2)
 * pigz  (parallel gzip)
 * pixz  (parallel xz with tar index support)
 * lz4   (fast compressor/decompressor single threaded)

Performance
-----------

### Filter large files with --size

`--size` is is the minimum size a file has to be in to *not* be included in a tar.  Files under this size are grouped in path order into to tar's that aim to be about `--tar-size` before compression.

By skipping larger files that are often binary uncompressible data one can avoid all the IO copying the large files twice and the CPU time on compressing most of the data volume for little benefit for uncompressible data.  For systems like Data Den and HPSS the backend tape systems will compress data at full network speed and thus is much faster than software compression tools.

### Parallel IO Requests

Archivetar makes heavy use of MPI and Python Multiprocess package.  The filesystem walk that finds all files and builds the list of files for each tar is `dwalk` from mpiFileUtils and uses MPI and `libcircle`.  This if often 5-20x faster than normal filesystem walks.  If ran in a batch job if the MPI picks up the environment it will also use multiple nodes.  The rest of archivetar will not use multiple nodes.

The python multiprocess module is used to stuff IO requests pipelines by running multiple `tar` processes at once. By default this is 1/4 the number of threads detected on the system but can also be set with `--tar-processes N`.  This is very useful on network filesystems and flash storage systems where multiple requests can be serviced at once generally to higher iops.  Multiple tar processes will help when the average file size is small, or for compressors like `xz` that struggle to use all cores in modern systems.

Lastly the `SuperTar` package used by archivetar will auto detect if parallel compressors are available. Thus if data are compressible `tar` will be able to use multiple cores to speed compression of larger files from fast storage systems.
