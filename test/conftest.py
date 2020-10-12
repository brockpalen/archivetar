from subprocess import check_output

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--runglobus", action="store_true", default=False, help="run globus tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "globus: mark test as globus to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runglobus"):
        # --runglobus given in cli: do not skip globus tests
        return
    skip_globus = pytest.mark.skip(reason="need --runglobus option to run")
    for item in items:
        if "globus" in item.keywords:
            item.add_marker(skip_globus)


def count_files_dir(path):
    """count number of files recursivly in path."""
    # IN pathlib path
    num_f_dest = 0

    for f in path.glob("**/*"):
        if f.is_file():
            num_f_dest += 1

    return num_f_dest


def count_lines_dir(path):
    """count number of files recursivly in path"""
    # IN pathlib path
    num_f_dest = 0

    for f in path.glob("**/*"):
        if f.is_file():
            num_f_dest += int(check_output(["wc", "-l", f]).split()[0])

    return num_f_dest
