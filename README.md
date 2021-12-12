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

See [USAGE.md](USAGE.md)

Building archivetar
-------------------

See [INSTALL.md](INSTALL.md)
