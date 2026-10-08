#!/bin/bash

# Simple script to setup mpiFileUtils 
# brockp@umich.edu Edited from https://github.com/hpc/mpifileutils

# Requires CMake and a working MPI library (Tested with OpenMPI, should work with mpich etc)
# 

mkdir install
installdir=`pwd`/install

mkdir deps
cd deps
  wget https://github.com/hpc/libcircle/releases/download/v0.3/libcircle-0.3.0.tar.gz
  wget https://github.com/llnl/lwgrp/releases/download/v1.0.2/lwgrp-1.0.2.tar.gz
  wget https://github.com/llnl/dtcmp/releases/download/v1.1.0/dtcmp-1.1.0.tar.gz

  tar -zxf libcircle-0.3.0.tar.gz
  cd libcircle-0.3.0
    ./configure --prefix=$installdir
    make install
  cd ..

  tar -zxf lwgrp-1.0.2.tar.gz
  cd lwgrp-1.0.2
    ./configure --prefix=$installdir
    make install
  cd ..

  tar -zxf dtcmp-1.1.0.tar.gz
  cd dtcmp-1.1.0
    ./configure --prefix=$installdir --with-lwgrp=$installdir
    make install
  cd ..
cd ..


# Pinned patched mpiFileUtils release; override with MFU_VERSION=<tag> ./build.sh
# archivetar <= v1.0.x requires v0.10.2-brockp
MFU_VERSION=${MFU_VERSION:-v0.10.2-brockp}
git -c advice.detachedHead=false clone --branch "$MFU_VERSION" --depth 1 https://github.com/brockpalen/mpifileutils.git
mkdir build install
cd build
cmake ../mpifileutils \
  -DWITH_DTCMP_PREFIX=../install \
  -DWITH_LibCircle_PREFIX=../install \
  -DCMAKE_INSTALL_PREFIX=../install
make install
