# Singularity Container

Archivetar can be built as a [Singularity](https://cloud.sylabs.io) container.  For users without existing MPI and CMake installs the container provides all the tools needed.

## Installing Singularity

Refresh to the [Singularity Documentation](https://sylabs.io/guides/3.6/admin-guide/installation.html#distribution-packages-of-singularity) how to install.  For CentOS and RHEL users it can be installed from `epel`.  


## Running Archivetar container

Once singularity is installed you can pull the [offical image](https://cloud.sylabs.io/library/brockp/default/archivetar)

```
singularity pull --arch amd64 library://brockp/default/archivetar:master 
singularity run-help archivetar_master.sif 
singularity exec archivetar_master.sif archivetar --help
```

## Building Singuarlity Image

```
singularity build --remote archivetar.sif archivetar.def
singularity push -U  archivetar.sif library://brockp/default/archivetar:master
```
