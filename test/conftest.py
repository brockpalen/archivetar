import pathlib
from subprocess import check_output

import pytest


def count_files_dir(path):
    """count number of files recursivly in path"""
    # IN pathlib path
    num_f_dest = 0 

    for f in path.glob('**/*'):
       if f.is_file():
          num_f_dest += 1

    return num_f_dest

def count_lines_dir(path):
    """count number of files recursivly in path"""
    # IN pathlib path
    num_f_dest = 0 

    for f in path.glob('**/*'):
       if f.is_file():
          num_f_dest += int(check_output(["wc", "-l", f]).split()[0])

    return num_f_dest
