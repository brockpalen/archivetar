# Archivetar container image
#
# Build from the repository root so the checked-out source is used:
#   docker build -f docker/Dockerfile -t archivetar .
#
# Apptainer / Singularity can run the resulting image without root:
#   apptainer pull archivetar.sif docker://ghcr.io/brockpalen/archivetar:latest

# ----------------------------------------------------------------------
# Builder: compile mpiFileUtils and the standalone PyInstaller programs
# ----------------------------------------------------------------------
FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# universe (pixz, lbzip2) is already enabled in the ubuntu base image
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        cmake \
        git \
        libopenmpi-dev \
        openmpi-bin \
        wget \
        gcc \
        g++ \
        make \
        libarchive-dev \
        pkg-config \
        libssl-dev \
        libcap-dev \
        libattr1-dev \
        libbz2-dev \
        python3 \
        python3-pip \
        python3-dev && \
    rm -rf /var/lib/apt/lists/*

COPY . /archivetar
WORKDIR /archivetar

# Patched mpiFileUtils release to build; unset uses the default pinned in build.sh
ARG MFU_VERSION
RUN bash -x build.sh

RUN pip3 install --no-cache-dir pipenv && \
    pipenv install --dev --python "$(which python3)"

RUN pipenv run pyinstaller --collect-all globus_sdk bin/archivetar  -p . --onefile && \
    pipenv run pyinstaller --collect-all globus_sdk bin/.archivetar -p . --onefile && \
    pipenv run pyinstaller bin/unarchivetar -p . --onefile && \
    pipenv run pyinstaller bin/archivepurge -p . --onefile && \
    pipenv run pyinstaller bin/archivescan  -p . --onefile

# ----------------------------------------------------------------------
# Runtime: only what the built programs need to run
# ----------------------------------------------------------------------
FROM ubuntu:22.04

LABEL maintainer="brockp@umich.edu"
LABEL org.opencontainers.image.source="https://github.com/brockpalen/archivetar"
LABEL org.opencontainers.image.description="Archivetar: tools for archiving big data to tape/HSM and object archives"
LABEL org.opencontainers.image.licenses="MIT"

ENV DEBIAN_FRONTEND=noninteractive

# Compressors are optional at runtime but SuperTar auto-detects and uses them
# universe (pixz, lbzip2) is already enabled in the ubuntu base image
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openmpi-bin \
        libopenmpi3 \
        libarchive13 \
        libexpat1 \
        libssl3 \
        libcap2 \
        libattr1 \
        libbz2-1.0 \
        lbzip2 \
        pigz \
        liblz4-tool \
        pixz \
        bzip2 \
        gzip \
        tar \
        zstd && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /archivetar/dist /archivetar/dist
COPY --from=builder /archivetar/install /archivetar/install

# Docker ENV is imported by Apptainer/Singularity, matching the old .sif %environment
ENV AT_MPIFILEUTILS=/archivetar/install
ENV AT_MPIRUN=mpirun
ENV OMPI_MCA_btl=^openib
# Docker defaults to root and OpenMPI refuses root without these; no effect in Apptainer
ENV OMPI_ALLOW_RUN_AS_ROOT=1
ENV OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1
ENV PATH="/archivetar/dist:/archivetar/install/bin:${PATH}"

WORKDIR /data

CMD ["archivetar", "--help"]
