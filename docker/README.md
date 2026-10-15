# Docker Container

Archivetar can be built as a [Docker](https://docker.com) container.  For users without existing MPI and CMake installs the container provides all the tools needed.

## Building Docker Image

### Optional version tag build
```
docker build --build-arg ARCHIVETAR_VERSION=<tag-or-commit> -t archivetar .
```
### Latest build
```
docker build -t archivetar .
```

## Running the container

```
docker run --rm -v "${PWD}:/data" archivetar
docker run --rm -v "${PWD}:/data" archivetar archivetar --help
docker run --rm -v "${PWD}:/data" archivetar archivepurge --help
docker run --rm -v "${PWD}:/data" archivetar archivescan --help
docker run --rm -v "${PWD}:/data" archivetar unarchivetar --help
```

## Aditional Utilities

The image includes several utilities used by `archivetar` but may also be useful on their own:

* mpifileutils eg `mpirun dwalk --help`
* Parallel Compressors `lbzip2` `pigz` `pixz`
* lz4-tools
