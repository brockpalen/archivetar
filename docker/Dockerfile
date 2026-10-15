FROM ubuntu:22.04

LABEL maintainer="brockp@umich.edu"
LABEL org.opencontainers.image.source="https://github.com/brockpalen/archivetar"

ENV DEBIAN_FRONTEND=noninteractive

ENV AT_MPIFILEUTILS=/archivetar/install
ENV AT_MPIRUN=mpirun
ENV OMPI_MCA_btl=^openib
ENV PATH="/archivetar/dist:/archivetar/install/bin:/usr/local/bin:${PATH}"

# ----------------------------------------------------------------------
# System packages
# ----------------------------------------------------------------------
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository universe && \
    apt-get update

RUN apt-get install -y \
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
    python3 \
    python3-pip \
    lbzip2 \
    pigz \
    liblz4-tool \
    pixz \
    bzip2 \
    gzip \
    tar \
    libbz2-dev \
    zstd

# Debug versions during build
RUN python3 --version && \
    pip3 --version && \
    gcc --version && \
    cmake --version && \
    mpirun --version

# ----------------------------------------------------------------------
# Clone Archivetar
# ----------------------------------------------------------------------
# Optionally pin a tag or commit: docker build --build-arg ARCHIVETAR_VERSION=<tag> .
ARG ARCHIVETAR_VERSION
RUN git clone https://github.com/brockpalen/archivetar.git /archivetar && \
    if [ -n "$ARCHIVETAR_VERSION" ]; then \
        git -C /archivetar checkout --detach "$ARCHIVETAR_VERSION"; \
    fi

WORKDIR /archivetar

# ----------------------------------------------------------------------
# Build MPI File Utils / native components
# ----------------------------------------------------------------------
RUN bash -x build.sh

# ----------------------------------------------------------------------
# Python environment
# ----------------------------------------------------------------------
RUN pip3 install --no-cache-dir pipenv

RUN which pipenv && pipenv --version

RUN pipenv install --dev --python "$(which python3)"

# ----------------------------------------------------------------------
# Build standalone programs
# ----------------------------------------------------------------------
RUN pipenv run pyinstaller \
    --collect-all globus_sdk \
    bin/archivetar \
    -p . \
    --onefile

RUN pipenv run pyinstaller \
    --collect-all globus_sdk \
    bin/.archivetar \
    -p . \
    --onefile

RUN pipenv run pyinstaller \
    bin/unarchivetar \
    -p . \
    --onefile

RUN pipenv run pyinstaller \
    bin/archivepurge \
    -p . \
    --onefile

RUN pipenv run pyinstaller \
    bin/archivescan \
    -p . \
    --onefile

# ----------------------------------------------------------------------
# Verify output
# ----------------------------------------------------------------------
RUN ls -lah /archivetar/dist && \
    ls -lah /archivetar/install/bin

WORKDIR /data

CMD ["archivetar", "--help"]

