# Container (Docker / Apptainer / Singularity)

Archivetar is distributed as a [Docker](https://docker.com) / OCI container image.  For users without existing MPI and CMake installs the container provides all the tools needed.

The same image runs under [Apptainer](https://apptainer.org) and Singularity without root, which is the recommended way to use it on HPC clusters.

> The previous Singularity images on the Sylabs library (`library://brockp/archivetar/archivetar`) are no longer updated.  Pull the Docker image instead as shown below.

## Published Images

Images are built and pushed to [GHCR](https://github.com/brockpalen/archivetar/pkgs/container/archivetar) automatically by GitHub Actions ([container.yml](../.github/workflows/container.yml)) for every published release:

* `ghcr.io/brockpalen/archivetar:v1.1.1` exact release
* `ghcr.io/brockpalen/archivetar:1.1` latest patch of a minor release
* `ghcr.io/brockpalen/archivetar:latest` newest non pre-release

Pin a release tag for reproducible archives; `latest` moves with each release.

## Apptainer / Singularity

Pull the image and convert it to a `.sif` file.  Use a release tag (eg `v1.1.1`) or `latest`.

```
apptainer pull archivetar.sif docker://ghcr.io/brockpalen/archivetar:latest
```

Run the tools with `exec`:

```
apptainer exec archivetar.sif archivetar --help
apptainer exec archivetar.sif unarchivetar --help
apptainer exec archivetar.sif archivepurge --help
apptainer exec archivetar.sif archivescan --help
```

Sites still running Singularity can use the same commands with `singularity` in place of `apptainer`:

```
singularity pull archivetar.sif docker://ghcr.io/brockpalen/archivetar:latest
singularity exec archivetar.sif archivetar --help
```

Your home directory and current directory are normally bound into the container automatically.  Bind other filesystems with `-B`, eg `apptainer exec -B /scratch archivetar.sif archivetar ...`.

The required configuration variables are already set inside the container:

```
apptainer exec archivetar.sif env | grep ^AT_
```

Only the optional variables (see [INSTALL.md](../INSTALL.md#configuration)) need to be set to suit your site.

## Docker

```
docker pull ghcr.io/brockpalen/archivetar:latest
docker run --rm ghcr.io/brockpalen/archivetar:latest
```

Mount the data to archive at `/data` and run as your own user so created tars are not owned by root:

```
docker run --rm --user "$(id -u):$(id -g)" -v "${PWD}:/data" \
    ghcr.io/brockpalen/archivetar:latest archivetar --prefix myarchive --size 1M --tar-size 200M
```

## Building the Image

Build from the root of the repository so the checked-out source is used:

```
docker build -f docker/Dockerfile -t archivetar .
```

Override the patched mpiFileUtils release used by `build.sh`:

```
docker build -f docker/Dockerfile --build-arg MFU_VERSION=<tag> -t archivetar .
```

A locally built image can be converted for Apptainer without pushing it to a registry:

```
apptainer build archivetar.sif docker-daemon://archivetar:latest
```

## Commands Provided

| Command | Purpose |
| --- | --- |
| `archivetar` | Primary tool to prep data for archive |
| `archivepurge` | Purges small files that were included in tars created by archivetar |
| `unarchivetar` | Helper utility to parallel extract tars created by archivetar |
| `archivescan` | Reports how much data is archive (large file) vs cache (small file) candidates, which files are offline, and can trigger recalls |

## Additional Utilities

The image includes several utilities used by `archivetar` that may also be useful on their own:

* Parallel compressors: `pigz` (gzip), `lbzip2` (bzip2), `pixz` (xz / lzma)
* `lz4` and `zstd` fast compressors
* [mpiFileUtils](https://github.com/hpc/mpifileutils), eg `mpirun dwalk --help`
  * `dwalk` parallel filesystem walker
  * `dfind` parallel find
  * `dcp` parallel copy
  * `drm` parallel rm
  * `dchmod` parallel chmod
  * `ddup` parallel find duplicates
