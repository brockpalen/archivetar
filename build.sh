#!/bin/bash

# Simple script to setup mpiFileUtils 
# brockp@umich.edu Edited from https://github.com/hpc/mpifileutils

# Requires CMake and a working MPI library (Tested with OpenMPI, should work with mpich etc)
# 

mkdir install
installdir=`pwd`/install

mkdir deps
cd deps

  urls=(     https://github.com/hpc/libcircle/releases/download/v0.3/libcircle-0.3.0.tar.gz
             https://github.com/llnl/lwgrp/releases/download/v1.0.6/lwgrp-1.0.6.tar.gz
             https://github.com/llnl/dtcmp/releases/download/v1.1.5/dtcmp-1.1.5.tar.gz
             https://github.com/libarchive/libarchive/releases/download/v3.7.7/libarchive-3.7.7.tar.gz
  )

  rc=0
  for url in ${urls[*]}; do
      if [[ rc -eq 0 ]]; then
          wget $url
          rc=$?
          if [[ $rc -ne 0 ]]; then
              echo
              echo FAILED getting $url
              echo check for releases under $(echo $url | sed 's/releases.*/releases\//')
          fi
      fi
  done

  if [[ rc -eq 0 ]]; then
      tar -zxf libcircle-0.3.0.tar.gz
      cd libcircle-0.3.0
        ./configure --prefix=$installdir
        make install
      cd ..

      tar -zxf lwgrp-1.0.6.tar.gz
      cd lwgrp-1.0.6
        ./configure --prefix=$installdir
        make install
      cd ..

      tar -zxf dtcmp-1.1.5.tar.gz
      cd dtcmp-1.1.5
        ./configure --prefix=$installdir --with-lwgrp=$installdir
        make install
      cd ..

      tar -zxf libarchive-3.7.7.tar.gz
      cd libarchive-3.7.7
        ./configure --prefix=$installdir
        make install
      cd ..
    fi
cd ..

# Pinned patched mpiFileUtils release; override with MFU_VERSION=<tag> ./build.sh
# archivetar <= v1.0.x requires v0.10.2-brockp
MFU_VERSION=${MFU_VERSION:-v0.12.1-brockp}
git -c advice.detachedHead=false clone --branch "$MFU_VERSION" --depth 1 https://github.com/brockpalen/mpifileutils.git
mkdir build install
cd build

cmake ../mpifileutils \
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
  -DWITH_DTCMP_PREFIX=../install \
  -DWITH_LibCircle_PREFIX=../install \
  -DWITH_LibArchive_PREFIX=../install \
  -DCMAKE_INSTALL_PREFIX=../install
make -j install
