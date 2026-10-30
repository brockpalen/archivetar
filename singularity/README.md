# Singularity Container (moved)

**The Singularity definition file and the Sylabs library images
(`library://brockp/archivetar/archivetar`) are retired and no longer updated.**

Archivetar is now published as a Docker / OCI image, which Singularity and
[Apptainer](https://apptainer.org) can pull and run directly without root.
The commands you already know still work, only the `pull` source changes:

```
# old (no longer updated)
singularity pull --arch amd64 library://brockp/archivetar/archivetar:master

# new
singularity pull archivetar.sif docker://ghcr.io/brockpalen/archivetar:latest
singularity exec archivetar.sif archivetar --help

# or with Apptainer
apptainer pull archivetar.sif docker://ghcr.io/brockpalen/archivetar:latest
apptainer exec archivetar.sif archivetar --help
```

See [docker/README.md](../docker/README.md) for full instructions.
