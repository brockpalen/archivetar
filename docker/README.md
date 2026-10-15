# Docker Container

Archivetar can be built as a [Docker](https://docker.com) container.  For users without existing MPI and CMake installs the container provides all the tools needed.


## Running Archivetar container

Once singularity is installed you can pull the [offical image](https://cloud.sylabs.io/library/brockp/default/archivetar)

```
singularity pull --arch amd64 library://brockp/archivetar/archivetar:master 
singularity run-help archivetar_master.sif 
singularity exec archivetar_master.sif archivetar --help
```

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
```

## Aditional Utilities

The image includes several utilities used by `archivetar` but may also be useful on their own:

* mpifileutils eg `mpirun dwalk --help`
* Parallel Compressors `lbzip2` `pigz` `pixz`
* lz4-tools
