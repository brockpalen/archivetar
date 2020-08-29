[![Build Status](https://travis-ci.com/brockpalen/archivetar.svg?branch=master)](https://travis-ci.com/brockpalen/archivetar)
[![codecov](https://codecov.io/gh/brockpalen/archivetar/branch/master/graph/badge.svg)](https://codecov.io/gh/brockpalen/archivetar)



Archivetar
==========

archivetar (V2) is a of several tools intended to make the archiving and the
use big data easier. Targeted mostly at the research / HPC use case it is useful in other cases where having fewer files but not one gigantic file, or skip time on large files.

`archivetar` was to make our Spectrum Archive install [Data Den](https://arc-ts.umich.edu/data-den/) more useful. It does this taring only small files, it and not waste time/io on large files.  It has no dependencies on SA, and could easily be used with AWS Glacier, HPSS, DMF etc.  where you want to minimize the number of objects keeping the data/object ratio high.

For additional performance `archivetar` will auto detect many multi-core capable compressors.

#### Example Output

```
# number of files before
$ find . -type f | wc -l
6656

# bundle all files < 1M, into tars 200M in size
# Delete input files
archivetar.py --prefix boxout --purge --size 1M --tar-size 200M .

# number of files after
$ find . -type f | wc -l
1831

# expand 
for x in $(ls *.tar)
   tar -xf $x
done
```


### tar-size vs size


`--size` is is the minimum size a file has to be in to *not* be included in a tar.  Files under this size are grouped in path order into to tar's that aim to be about `--tar-size` before compression.

Files are added to the list for each tar one at a time and stops when the estimated size is larger. So tars may be larger (possibly significantly!) depending on the last file added to the tar list.

### archivetar vs tar


archivetar doesn't try to replace tar. Actaully it uses it internally rather than Pythons native implimentation.  

Usage
-----

### Quick Start

Uses default settings and will create `archivetar-1.tar archivetar-2.tar ... archivetar-N.tar`

```
archivetar.py <path>
```

### Specify small file cutoff and size before creating a new tar

```
# this will work, but the tar size is a minimum, so tars may be much larger than listed here
archivetar.py --size 20G --tar-size 10G <path>
```

### Specify prefix for tar names

This will create `project1-1.tar project1-2.tar` etc.

```
archivetar.py --prefix project1 <path>
```

Workflow
========


 * Scan current directory using [mpiFileUtils](https://github.com/hpc/mpifileutils)
 * Filter only files under `--size`
 * Build sub-tar's where each tar aims to be (before compression) to be at least `--min-tar-size`
 * Optionally delete files that were included in tars (delete as they are tar'd)

Building archivetar
-------------------

### Requirements

 * Patched [mpiFileUtils](https://github.com/brockp/mpifileutils) `build.sh` is a shortcut
 * python3.6+
 * `pip install pipenv`
 * `pipenv install`
 * `pipenv run pyinstaller bin/archivetar --onefile`   # create executable no need for pipenv

### Configuration

Archivetar uses envrionment for configuration

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

