#!/usr/bin/env python

# Brock Palen
# brockp@umich.edu
# 7/2020
#
#  prep a directory for placement in dataden
#  process:
#    1. run mpiFileUtils / dwalk  (deafault sort in name / path order) all files < minsize
#    2. Take resulting list build tar lists by summing size until > tarsize (before compression)
#    3. Tar each list:  OR --dryrun create list with est size
#       a. Create Index file of contents
#       b. Optionally compress -z / -j  with gzip/pigz bzip/lbzip2 if installed
#       c. Optionally purge
#    4. (?) Kick out optimized untar script (pigz / lbzip2)

## TODO
# * filter and produce list to feed to scp/globus?
# * allow direct handoff to Globus CLI
# * mpibzip2

import argparse
import os
import pathlib
import re
import shutil
import subprocess
import sys
import logging
import datetime
import tempfile

import humanfriendly

from mpiFileUtils import DWalk


def find_gzip():
    """find pigz if installed in PATH otherwise return gzip"""
    pigz = shutil.which('pigz')
    gzip = shutil.which('gzip')
    if pigz:
        return pigz
    elif gzip:
        return gzip
    else:
        raise Exception("gzip compression but no gzip or pigz found in PATH")


def find_bzip():
    """find pigz if installed in PATH otherwise return gzip"""
    lbzip2 = shutil.which('lbzip2')
    pbzip2 = shutil.which('pbzip2')
    bzip2 = shutil.which('bzip2')
    if lbzip2:
        return lbzip2
    elif pbzip2:
        return pbzip2
    elif bzip2:
        return bzip2
    else:
        raise Exception("gzip compression but no gzip or pigz found in PATH")


def find_xz():
    """find pixz if installed in PATH otherwise return xz"""
    pixz = shutil.which('pixz')
    xz = shutil.which('xz')
    if pixz:
        return pixz
    elif xz:
        return xz
    else:
        raise Exception("lzma/xz compression but no pixz or xz found in PATH")


def find_lzma():
    """alias for find_xz()"""
    return find_xz()


class DwalkLine:
    def __init__(self, line=False, relativeto=False):
        """parse dwalk output line"""
        # -rw-r--r-- bennet support 578.000  B Oct 22 2019 09:35 /scratch/support_root/support/bennet/haoransh/DDA_2D_60x70_kulow_1.batch
        match = re.match(r"\S+\s+\S+\s+\S+\s+(\d+\.\d+)\s+(\S+)\s+.+\s(/.+)", line)
        if relativeto:
            self.relativeto = relativeto
        else:
            self.relativeto = os.getcwd()

        self.size = self._normilizeunits(units=match[2], count=float(match[1]))  # size in bytes
        self.path = self._stripcwd(match[3])

    def _normilizeunits(self, units=False, count=False):
        """convert size by SI units to Bytes"""
        if (units == 'B'):
            return count
        elif (units == 'KB'):
            return 1000 * count
        elif (units == 'MB'):
            return 1000 * 1000 * count
        elif (units == 'GB'):
            return 1000 * 1000 * 1000 * count
        elif (units == 'TB'):
            return 1000 * 1000 * 1000 * 1000 * count
        elif (units == 'PB'):
            return 1000 * 1000 * 1000 * 1000 * count
        else:
            raise Exception(f"{units} is not a known SI unit")

    def _stripcwd(self, path):
        """dwalk print absolute paths, we need relative"""
        return os.path.relpath(path, self.relativeto)


class DwalkParser:
    def __init__(self, path=False):
        # check that path exists
        path = pathlib.Path(path)
        self.indexcount = 1
        if path.is_file():
            self.path = path.open()
        else:
            raise Exception(f"{self.path} doesn't exist")

    def tarlist(self,
                prefix='archivetar',   # prefix for files
                minsize=1e9 * 100):    # min size sum of all files in list
        # OUT tar list suitable for gnutar
        # OUT index list
        """takes dwalk output walks though until sum(size) >= minsize"""

        tartmp_p = pathlib.Path.cwd() / f'{prefix}-{self.indexcount}.tartmp.txt'  # list of files suitable for gnutar
        index_p = pathlib.Path.cwd() / f"{prefix}-{self.indexcount}.index.txt"
        sizesum = 0  # size in bytes thus far
        index = index_p.open('w')
        tartmp = tartmp_p.open('w')
        for line in self.path:
            pl = DwalkLine(line=line)
            sizesum += pl.size
            index.write(line)    # already has newline
            tartmp.write(f"{pl.path}\n")  # write doesn't append newline
            if sizesum >= minsize:
                # max size in tar reached
                self.indexcount += 1
                tartmp.close()
                index.close()
                print(f"Minimum Archive Size {humanfriendly.format_size(minsize)} reached, Expected size: {humanfriendly.format_size(sizesum)}")
                yield index_p, tartmp_p
                # continue after yeilding file paths back to program
                sizesum = 0
                tartmp_p = pathlib.Path.cwd() / f'{prefix}-{self.indexcount}.DONT_DELETE.txt'  # list of files suitable for gnutar
                index_p = pathlib.Path.cwd() / f"{prefix}-{self.indexcount}.index.txt"
                index = index_p.open('w')
                tartmp = tartmp_p.open('w')
        index.close()   # close and return for final round
        tartmp.close()
        yield index_p, tartmp_p


class SuperTar:
    """ tar wrapper class for high speed """
    # requires gnu tar
    def __init__(self,
                 filename=False,  # path to file eg output.tar
                 compress=False,  # compress or not False | GZIP | BZ2 | LZ4
                 verbose=False,  # print extra information when arching
                 purge=False):   # pass --remove-files

        if not filename:   # filename needed  eg tar --file <filename>
            raise Exception("no filename given for tar")

        self._flags = ["tar", "--sparse", '--create', '--file', filename]

        if compress == 'GZIP':
            self._flags.append(f'--use-compress-program={find_gzip()}')
        elif compress == 'BZ2':
            self._flags.append(f'--use-compress-program={find_bzip()}')
        elif compress == 'XZ':
            self._flags.append(f'--use-compress-program={find_xz()}')
        elif compress == 'LZ4':
            self._flags.append('--lz4')
        elif compress:
            raise Exception("Invalid Compressor {compress}")

        if verbose:
            self._flags.append('--verbose')
            self._verbose = True

        if purge:
            self._flags.append('--remove-files')

    def addfromfile(self, path):
        """load list of files from file eg tar -cvf output.tar --files-from=<file>"""
        self._flags.append(f'--files-from={path}')
        rc = subprocess.run(self._flags, check=True)

    def addfrompath(self, path):
        """load from fs path eg tar -cvf output.tar /path/to/tar"""
        pass


#############  MAIN  ################

def parse_args(args):
    """ CLI options"""
    parser = argparse.ArgumentParser(
        description='Prepare a directory for arching',
        epilog="Brock Palen brockp@umich.edu")
    # parser.add_argument('--dryrun', help='Print what would do but dont do it", action="store_true")
    parser.add_argument("path",
                        help="path to walk",
                        type=str)
    parser.add_argument('-p', '--prefix',
                        help="prefix for tar eg prefix-1.tar prefix-2.tar etc",
                        type=str,
                        required=True)

    parser.add_argument('-s', '--size',
                        help="Cutoff size for files include (eg. 10G 100M) Default 20G",
                        type=str,
                        default="20G")
    parser.add_argument('-t', '--tar-size',
                        help="Target tar size before options (eg. 10G 1T) Default 100G",
                        type=str,
                        default="100G")
    parser.add_argument('--remove-files',
                        help="Delete files as/when added to archive (CAREFUL)",
                        action="store_true")

    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument('-v', '--verbose',
                           help="Increase messages, including files as added",
                           action="store_true")
    verbosity.add_argument('-q', '--quiet',
                           help="Decrease messages",
                           action="store_true")

    compression = parser.add_mutually_exclusive_group()
    compression.add_argument('-z', '--gzip',
                             help="Compress tar with GZIP",
                             action="store_true")
    compression.add_argument('-j', '--bzip', '--bzip2',
                             help="Compress tar with BZIP",
                             action="store_true")
    compression.add_argument('--lz4',
                             help="Compress tar with lz4",
                             action="store_true")
    compression.add_argument('--xz', '--lzma',
                             help="Compress tar with xz/lzma",
                             action="store_true")

    args = parser.parse_args(args)
    return args


def build_list(path=False,
               prefix=False,
               savecache=False):
    """
    scan filelist and return path to results

    Parameters:
        path (str/pathlib) Path to scan
        prefix (str) Prefix for scan file eg. prefix-{date}.cache
        savecache (bool) Save cache file in cwd or only in TMPDIR

    Returns:
        cache (pathlib) Path to cache file
    """

    # configure DWalk
    dwalk = DWalk(
        inst='/home/brockp/mpifileutils/install',
        mpirun='/sw/arcts/centos7/stacks/gcc/8.2.0/openmpi/4.0.3/bin/mpirun',
        sort='name',
        progress='10')

    # generate timestamp name
    today = datetime.datetime.today()
    datestr = today.strftime('%Y-%m-%d-%H-%M-%S')

    # put into cwd or TMPDIR ?
    c_path = pathlib.Path.cwd() if savecache else pathlib.Path(tempfile.gettempdir())
    cache = c_path / f'{prefix}-{datestr}.cache'
    logging.debug(f"Scan saved to {cache}")

    # start the actual scan
    dwalk.scanpath(path=path,
                   cacheout=cache)

    return cache


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    if args.quiet:
        logging.basicConfig(level=logging.WARNING)
    elif args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    logging.info("----> [Phase 1] Build List of Files")
    args = {'path': args.path,
            'prefix': args.prefix}
    cache = build_list(**args)

    # list parser
    # for list in tarlist()
    #     SuperTar

    # if globus(dest)
    #     globus put
