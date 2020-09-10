from pathlib import Path

from archivetar.purge import purge_empty_folders


def test_purge_empty_folders(tmp_path):
    """test purge empty folders leaves correct number of items"""

    root = tmp_path / "root"
    root.mkdir()

    # root/dir1/dir1_1/file1
    # don't remove anything
    dir1 = root / "dir1"
    dir1_1 = dir1 / "dir1_1"
    file1 = dir1 / "file1"
    dir1.mkdir()
    dir1_1.mkdir()
    file1.touch()

    # root/dir2/dir2_1/dir2_2
    dir2 = root / "dir2"
    dir2_1 = dir2 / "dir2_1"
    dir2_2 = dir2_1 / "dir2_2"
    dir2.mkdir()
    dir2_1.mkdir()
    dir2_2.mkdir()

    # count number entries before
    before = len(list(root.iterdir()))

    # remove emtpy items
    purge_empty_folders(tmp_path)
    print(f"Before: {before} entries")
    assert before == 2

    # count number of entires after
    after = len(list(root.iterdir()))
    print(f"After: {after} entries")
    assert after == 1
