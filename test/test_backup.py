"""archivebackup / archiverestore bookkeeping (issue #70).

mpiFileUtils, archivetar itself and Globus are mocked; these tests cover the
generation, stamp, catalog and restore-chain logic.
"""

import gzip
import json
import os
import stat
import tempfile
import time
from contextlib import ExitStack as does_not_raise

import pytest

import archivetar.backup as backup

GLOBUS = ["--source", "SRC", "--destination", "DST", "--destination-dir", "/bk"]


def dwalk_line(
    src, rel, size="1.000 KiB", date="Mar  4 2020 15:58", perms="-rw-r--r--"
):
    return f"{perms} user group {size} {date} {src}/{rel}\n"


@pytest.fixture
def env(tmp_path, monkeypatch):
    """A source tree, scratch outside it, and mocked walk/archive/Globus steps.

    env["tree"] maps relpath -> (size, date) as dwalk prints them;
    env["changed"] is the set of relpaths dwalk --cnewer would select;
    env["dest"] is (highest generation folder, complete) on the destination.
    """
    src = tmp_path / "src"
    src.mkdir()
    bundle = tmp_path / "bundle"
    monkeypatch.chdir(src)

    env = {
        "src": src,
        "bundle": bundle,
        "state": src / ".archivebackup" / "proj",
        "work": bundle / "archivebackup-proj",
        "archivetar": [],
        "filter": [],
        "mkdir": [],
        "uploads": [],
        "tree": {"a.txt": ("1.000 KiB", "Mar  4 2020 15:58")},
        "changed": {"a.txt"},
        "dest": None,  # None: whatever was uploaded so far
    }

    def write_listing(out, rels):
        with open(out, "w") as f:
            f.write(dwalk_line(src, "dir", perms="drwxr-xr-x"))  # dirs are ignored
            for rel in sorted(rels):
                size, date = env["tree"][rel]
                f.write(dwalk_line(src, rel, size, date))

    def walk_full(aargs, prefix):
        cache = tmp_path / f"{prefix}.cache"
        cache.write_text("full")
        return cache

    def filter_changed(cache, stamp, out):
        env["filter"].append(stamp)
        out.write_text("changed")

    def destination_state(aargs, name):
        if env["dest"] is not None:
            return env["dest"]
        done = [n for n, _ in env["uploads"]]
        return (max(done), True) if done else (0, True)

    def upload_and_wait(aargs, paths, label):
        if env.get("upload_fails"):
            raise RuntimeError("transfer FAILED")
        n = int(aargs.destination_dir.rsplit("-G", 1)[1])
        env["uploads"].append((n, [p.name for p in paths]))

    # backup_main points tempfile at its private work dir; undo after the test
    monkeypatch.setattr(tempfile, "tempdir", tempfile.tempdir)
    monkeypatch.setattr(backup, "walk_full", walk_full)
    monkeypatch.setattr(
        backup, "write_fullindex", lambda c, out: write_listing(out, env["tree"])
    )
    monkeypatch.setattr(backup, "filter_changed", filter_changed)
    monkeypatch.setattr(
        backup,
        "write_changed_index",
        lambda c, out: write_listing(out, env["changed"] & set(env["tree"])),
    )
    monkeypatch.setattr(
        backup, "run_archivetar", lambda argv: env["archivetar"].append(argv) or 0
    )
    monkeypatch.setattr(backup, "destination_state", destination_state)
    monkeypatch.setattr(
        backup, "make_destination_dir", lambda a, p: env["mkdir"].append(p)
    )
    monkeypatch.setattr(backup, "upload_and_wait", upload_and_wait)

    def run(*extra):
        backup.backup_main(
            ["archivebackup", "--prefix", "proj", "--bundle-dir", str(bundle)]
            + GLOBUS
            + list(extra)
        )

    env["run"] = run
    return env


def meta(env, n):
    return json.loads((env["state"] / f"proj-G{n:04d}.backup.json").read_text())


def test_first_run_is_full(env):
    env["run"]()

    m = meta(env, 1)
    assert m["type"] == "full"
    assert m["parent_generation"] is None
    assert env["filter"] == []  # full archives the whole walk
    argv = env["archivetar"][0]
    assert argv[argv.index("--prefix") + 1] == "proj-G0001"
    assert argv[argv.index("--list") + 1].endswith("proj-G0001.cache")
    assert "--bundle-dir" in argv  # pass-through options reach archivetar


def test_state_lives_in_tree_and_scratch_holds_only_scratch(env):
    env["run"]()
    assert sorted(p.name for p in env["state"].iterdir()) == [
        "proj-G0001.backup.json",
        "proj-G0001.catalog.txt.gz",
        "proj-G0001.stamp",
    ]
    assert (env["work"] / "proj-G0001.fullindex.txt").exists()


def test_metadata_catalog_and_fullindex_uploaded_to_generation_folder(env):
    env["run"]()
    assert env["uploads"] == [
        (
            1,
            [
                "proj-G0001.fullindex.txt",
                "proj-G0001.catalog.txt.gz",
                "proj-G0001.backup.json",
            ],
        )
    ]


def test_second_run_is_incremental_against_previous_stamp(env):
    env["run"]()
    env["run"]()

    m = meta(env, 2)
    assert m["type"] == "incremental"
    assert m["parent_generation"] == 1
    assert env["filter"] == [env["state"] / "proj-G0001.stamp"]
    argv = env["archivetar"][1]
    assert argv[argv.index("--prefix") + 1] == "proj-G0002"
    assert argv[argv.index("--list") + 1].endswith("proj-G0002-changed.cache")


def test_scratch_can_be_wiped_between_runs(env):
    import shutil

    env["run"]()
    shutil.rmtree(env["bundle"])
    env["run"]()
    assert meta(env, 2)["type"] == "incremental"


def test_stamp_is_backdated(env):
    before = time.time()
    env["run"]()
    stamp = env["state"] / "proj-G0001.stamp"
    assert os.stat(stamp).st_mtime == pytest.approx(
        before - backup.STAMP_BACKDATE, abs=5
    )
    assert meta(env, 1)["stamp_epoch"] == pytest.approx(os.stat(stamp).st_mtime)


def test_full_flag_forces_full(env):
    env["run"]()
    env["run"]("--full")
    assert meta(env, 2)["type"] == "full"
    assert env["filter"] == []


def test_full_every(env):
    for _ in range(4):
        env["run"]("--full-every", "2")
    types = [meta(env, n)["type"] for n in range(1, 5)]
    assert types == ["full", "incremental", "incremental", "full"]


def test_empty_incremental_recorded_without_archivetar(env):
    env["run"]()
    env["changed"] = set()
    env["run"]()
    assert meta(env, 2)["empty"] is True
    assert len(env["archivetar"]) == 1  # only the full ran archivetar


def test_missing_parent_stamp_is_recreated(env):
    env["run"]()
    stamp = env["state"] / "proj-G0001.stamp"
    stamp.unlink()
    env["run"]()
    assert env["filter"] == [stamp]
    assert meta(env, 2)["type"] == "incremental"


def test_incremental_prints_warning(env, capsys):
    env["run"]()
    assert "WARNING" not in capsys.readouterr().err
    env["run"]()
    assert "run a new full" in capsys.readouterr().err


def test_dryrun_records_nothing(env):
    env["run"]("--dryrun")
    assert list(env["state"].iterdir()) == []
    assert env["mkdir"] == [] and env["uploads"] == []


def test_each_generation_gets_its_own_destination_folder(env):
    env["run"]()
    env["run"]()

    assert env["mkdir"] == ["/bk/proj-G0001", "/bk/proj-G0002"]
    for n, argv in enumerate(env["archivetar"], start=1):
        # appended after the pass-through options so it wins, and --wait so the
        # generation is only recorded once Globus confirms the transfers
        assert argv[-3:] == ["--destination-dir", f"/bk/proj-G{n:04d}", "--wait"]
    assert meta(env, 2)["destination_dir"] == "/bk/proj-G0002"


def test_failed_metadata_upload_does_not_record_generation(env):
    env["upload_fails"] = True
    with pytest.raises(RuntimeError):
        env["run"]()
    assert not (env["state"] / "proj-G0001.backup.json").exists()

    env["upload_fails"] = False
    env["run"]()  # retried as generation 1
    assert meta(env, 1)["type"] == "full"


def test_interrupted_full_is_retried_as_full(env, monkeypatch):
    env["run"]()

    def boom(argv):
        # archivetar dies after writing a partial tar to scratch
        (env["work"] / "proj-G0002-1.tar").write_text("partial")
        raise RuntimeError("tar failed")

    monkeypatch.setattr(backup, "run_archivetar", boom)
    with pytest.raises(RuntimeError):
        env["run"]("--full")
    assert not (env["state"] / "proj-G0002.backup.json").exists()

    # plain run: picks up the interrupted full, cleans its leftovers
    monkeypatch.setattr(backup, "run_archivetar", lambda argv: 0)
    env["run"]()
    assert meta(env, 2)["type"] == "full"
    assert not (env["work"] / "proj-G0002-1.tar").exists()
    assert not (env["state"] / "proj-G0002.pending.json").exists()


def test_incomplete_destination_folder_is_retried(env):
    env["run"]()
    env["dest"] = (2, False)  # G0002 started on the destination, never finished
    env["run"]()
    assert meta(env, 2)["type"] == "incremental"


def test_lost_state_never_reuses_a_generation_number(env, caplog):
    env["run"]()
    env["run"]()
    # the .archivebackup directory is lost, destination has G0001-G0002
    for p in env["state"].iterdir():
        p.unlink()
    env["run"]()
    assert meta(env, 3)["type"] == "full"
    assert env["mkdir"][-1] == "/bk/proj-G0003"
    assert "missing or out of date" in caplog.text


def test_stale_state_starts_new_full_after_destination(env):
    env["run"]()
    env["dest"] = (5, True)  # someone else ran G0002-G0005 from another copy
    env["run"]()
    assert meta(env, 6)["type"] == "full"


@pytest.mark.parametrize(
    "local_last,dest,expected",
    [
        (0, (0, True), (1, False)),
        (3, (3, True), (4, False)),
        (3, (2, True), (4, False)),  # older folders pruned on the destination
        (3, (4, False), (4, False)),  # interrupted G0004, retry it
        (3, (4, True), (5, True)),  # destination ahead: local state is stale
        (0, (7, True), (8, True)),  # local state lost
    ],
)
def test_plan_generation(local_last, dest, expected):
    history = [{"generation": n} for n in range(1, local_last + 1)]
    assert backup.plan_generation(history, *dest) == expected


def test_new_and_copied_files_are_not_flagged(env):
    """New data and edits have a new ctime, so the incremental archives them."""
    env["run"]()
    env["tree"]["new.bam"] = ("2.000 GiB", "Oct  8 2026 01:00")
    env["tree"]["a.txt"] = ("2.000 KiB", "Oct  8 2026 01:00")
    env["changed"] = {"new.bam", "a.txt"}
    env["run"]()  # no SystemExit
    assert meta(env, 2)["uncaptured"] == 0
    assert not (env["state"] / "proj-G0002.uncaptured.txt").exists()


def test_moved_in_directory_fails_loudly(env, capsys):
    """mv keeps old ctimes: not archived, so the run must error out."""
    env["run"]()
    env["tree"]["moved/x.dat"] = ("5.000 MiB", "Jan  1 2020 00:00")
    env["changed"] = set()  # dwalk --cnewer does not see it
    with pytest.raises(SystemExit) as e:
        env["run"]()
    assert e.value.code == backup.EXIT_UNCAPTURED
    listing = (env["state"] / "proj-G0002.uncaptured.txt").read_text()
    assert listing == "NEW\tmoved/x.dat\n"
    assert "not in ANY backup" in capsys.readouterr().err
    # the generation itself was still recorded
    assert meta(env, 2)["uncaptured"] == 1


def test_renamed_directory_fails_loudly(env):
    env["tree"] = {"run1/x.dat": ("5.000 MiB", "Jan  1 2020 00:00")}
    env["run"]()
    env["tree"] = {"run2/x.dat": ("5.000 MiB", "Jan  1 2020 00:00")}  # mv run1 run2
    env["changed"] = set()
    with pytest.raises(SystemExit):
        env["run"]()
    listing = (env["state"] / "proj-G0002.uncaptured.txt").read_text()
    assert listing == "NEW\trun2/x.dat\n"


def test_replaced_file_with_same_name_fails_loudly(env):
    """rm -rf run1; mv elsewhere/run1 . -- same path, different file."""
    env["run"]()
    env["tree"]["a.txt"] = ("3.000 KiB", "Jun  1 2025 09:00")
    env["changed"] = set()
    with pytest.raises(SystemExit):
        env["run"]()
    listing = (env["state"] / "proj-G0002.uncaptured.txt").read_text()
    assert listing == "CHANGED\ta.txt\n"


def test_uncaptured_keeps_failing_until_full(env):
    env["run"]()
    env["tree"]["moved/x.dat"] = ("5.000 MiB", "Jan  1 2020 00:00")
    env["changed"] = set()
    for _ in range(2):
        with pytest.raises(SystemExit):
            env["run"]()
    env["run"]("--full")  # archives everything; check passes again
    env["run"]()
    assert meta(env, 5)["uncaptured"] == 0


def test_only_latest_catalog_and_stamp_kept(env):
    env["run"]()
    env["run"]()
    assert sorted(p.name for p in env["state"].glob("*.catalog.txt.gz")) == [
        "proj-G0002.catalog.txt.gz"
    ]
    assert sorted(p.name for p in env["state"].glob("*.stamp")) == ["proj-G0002.stamp"]
    with gzip.open(env["state"] / "proj-G0002.catalog.txt.gz") as f:
        assert f.read() == b"a.txt\t1.000 KiB|Mar 4 2020 15:58\n"


def test_missing_catalog_requires_full(env):
    env["run"]()
    (env["state"] / "proj-G0001.catalog.txt.gz").unlink()
    with pytest.raises(SystemExit, match="run a new full"):
        env["run"]()
    env["run"]("--full")  # a full re-establishes it


def test_merge_catalog_new_wins(tmp_path):
    old, new, out = tmp_path / "old", tmp_path / "new", tmp_path / "out.gz"
    old.write_bytes(b"a\t1\nb\t1\nd\t1\n")
    new.write_bytes(b"b\t2\nc\t2\n")
    backup.merge_catalog(old, new, out)
    with gzip.open(out) as f:
        assert f.read() == b"a\t1\nb\t2\nc\t2\nd\t1\n"


def test_index_entries_handles_spaces_and_symlinks(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    listing = tmp_path / "listing.txt"
    listing.write_text(
        dwalk_line(tmp_path, "NRM /c.bam")
        + dwalk_line(tmp_path, "link", perms="lrwxrwxrwx")
        + dwalk_line(tmp_path, "d", perms="drwxr-xr-x")
    )
    out = tmp_path / "entries"
    assert backup.index_entries(listing, out) == 2
    paths = [p for p, _ in backup.read_entries(out)]
    assert paths == [b"NRM /c.bam", b"link"]


@pytest.mark.parametrize(
    "argv,message",
    [
        (["--remove-files"], "--remove-files"),
        (["--list", "x.cache"], "--list"),
        (["--atime", "-7"], "--atime is not allowed"),
        (["--mtime", "-7"], "--mtime is not allowed"),
        (["--ctime", "-7"], "--ctime is not allowed"),
        (["--save-list"], "--save-list is not allowed"),
        (["--save-purge-list"], "--save-purge-list is not allowed"),
        (["--dereference"], "--dereference is not allowed"),
        (["--ignore-failed-read"], "--ignore-failed-read is not allowed"),
        (["--skip-source-errors"], "--skip-source-errors is not allowed"),
        (["--tar-options=--exclude=*.bam"], "may not exclude"),
        (["--tar-options", "--sparse -X skip.txt"], "may not exclude"),
    ],
)
def test_rejected_options(env, argv, message):
    with pytest.raises(SystemExit, match=message):
        env["run"](*argv)


def test_globus_required(env):
    with pytest.raises(SystemExit, match="requires Globus"):
        backup.backup_main(["archivebackup", "--prefix", "proj", "--bundle-dir", "/x"])


def test_bundle_dir_required(env):
    with pytest.raises(SystemExit, match="--bundle-dir is required"):
        backup.backup_main(["archivebackup", "--prefix", "proj"] + GLOBUS)


def test_bundle_dir_inside_source_rejected(env):
    with pytest.raises(SystemExit, match="must be outside"):
        backup.backup_main(
            ["archivebackup", "--prefix", "proj", "--bundle-dir", "sub/bundle"] + GLOBUS
        )


def test_must_run_from_same_directory(env, tmp_path, monkeypatch):
    env["run"]()
    # copy the state to another tree and run from there
    other = tmp_path / "other"
    (other / ".archivebackup").mkdir(parents=True)
    os.rename(env["state"], other / ".archivebackup" / "proj")
    monkeypatch.chdir(other)
    with pytest.raises(SystemExit, match="run it from there"):
        env["run"]()
    env["run"]("--full")  # a new chain from here is fine


def test_keep_fulls_lists_old_chain(env, capsys):
    env["run"]()  # G1 full
    env["run"]()  # G2 incr
    env["run"]("--full")  # G3 full
    env["run"]("--keep-fulls", "1")  # G4 incr
    err = capsys.readouterr().err
    assert "/bk/proj-G0001/" in err and "/bk/proj-G0002/" in err
    assert "/bk/proj-G0003/" not in err
    assert (env["state"] / "proj-G0001.backup.json").exists()  # nothing deleted


@pytest.mark.parametrize(
    "kinds,keep,expected",
    [
        (["full", "incremental", "full", "incremental"], 1, [1, 2]),
        (["full", "incremental", "full", "incremental"], 2, []),
        (["full", "full", "full"], 2, [1]),
        (["full", "incremental"], 0, []),
    ],
)
def test_prunable(kinds, keep, expected):
    history = [{"generation": n, "type": k} for n, k in enumerate(kinds, start=1)]
    assert [g["generation"] for g in backup.prunable(history, keep)] == expected


# ---------------------------------------------------------------- restore


def gens(*kinds, empty=()):
    return [
        {"generation": n, "type": k, "empty": n in empty, "prefix": f"p-G{n:04d}"}
        for n, k in enumerate(kinds, start=1)
    ]


@pytest.mark.parametrize(
    "history,target,expected,expex",
    [
        (gens("full", "incremental", "incremental"), None, [1, 2, 3], does_not_raise()),
        (
            gens("full", "incremental", "full", "incremental"),
            None,
            [3, 4],
            does_not_raise(),
        ),
        (
            gens("full", "incremental", "full", "incremental"),
            2,
            [1, 2],
            does_not_raise(),
        ),
        (
            gens("full", "incremental", "incremental", empty=(2,)),
            None,
            [1, 3],
            does_not_raise(),
        ),
        (gens("full"), 5, None, pytest.raises(ValueError, match="not found")),
        ([], None, None, pytest.raises(ValueError, match="no backup")),
    ],
)
def test_restore_chain(history, target, expected, expex):
    with expex:
        chain = backup.restore_chain(history, target)
        assert [g["generation"] for g in chain] == expected


def test_restore_chain_detects_gap():
    history = gens("full", "incremental", "incremental")
    del history[1]
    with pytest.raises(ValueError, match=r"missing for generation\(s\) \[2\]"):
        backup.restore_chain(history)


@pytest.fixture
def staged(tmp_path, monkeypatch):
    """Backup copied back from Globus: one folder per generation."""
    staging = tmp_path / "staging"
    for n, kind, big in [(1, "full", "v1"), (2, "incremental", "v2")]:
        g = staging / f"proj-G{n:04d}"
        (g / "data").mkdir(parents=True)
        (g / f"proj-G{n:04d}-1.tar").touch()
        (g / f"proj-G{n:04d}.fullindex.txt").write_text("listing")
        (g / f"proj-G{n:04d}.catalog.txt.gz").write_text("catalog")
        (g / "data" / "big.dat").write_text(big)  # over --size, sent by Globus
        (g / f"proj-G{n:04d}.backup.json").write_text(
            json.dumps(
                {
                    "backup": "proj",
                    "generation": n,
                    "prefix": f"proj-G{n:04d}",
                    "type": kind,
                    "empty": False,
                    "stamp_time": "t",
                }
            )
        )
    target = tmp_path / "restore"
    target.mkdir()
    monkeypatch.chdir(target)
    calls = []
    monkeypatch.setattr(
        backup.archivetar.unarchivetar, "main", lambda argv: calls.append(argv)
    )
    return staging, target, calls


def test_restore_from_generation_folders(staged, capsys):
    staging, target, calls = staged
    backup.restore_main(
        ["archiverestore", "-p", "proj", "--from", str(staging), "--tar-processes", "2"]
    )

    assert calls == [
        ["unarchivetar", "--prefix", f"proj-G000{n}"]
        + ["--archive-dir", str(staging / f"proj-G000{n}"), "--tar-processes", "2"]
        for n in (1, 2)
    ]
    # large files copied after each generation, newest wins, metadata skipped
    assert (target / "data" / "big.dat").read_text() == "v2"
    assert sorted(p.name for p in target.iterdir()) == ["data"]
    assert "reappear" in capsys.readouterr().err


def test_restore_older_generation_gets_older_large_file(staged):
    staging, target, calls = staged
    backup.restore_main(
        ["archiverestore", "-p", "proj", "--from", str(staging), "--generation", "1"]
    )
    assert (target / "data" / "big.dat").read_text() == "v1"


@pytest.mark.parametrize(
    "opt", ["--keep-old-files", "--skip-old-files", "--keep-newer-files"]
)
def test_restore_refuses_options_that_keep_old_versions(staged, opt):
    staging, target, calls = staged
    with pytest.raises(SystemExit, match="empty directory"):
        backup.restore_main(
            ["archiverestore", "-p", "proj", "--from", str(staging), opt]
        )
    assert calls == []


def test_restore_refuses_when_tars_missing(staged):
    staging, target, calls = staged
    (staging / "proj-G0002" / "proj-G0002-1.tar").unlink()
    with pytest.raises(SystemExit, match="no tars found"):
        backup.restore_main(["archiverestore", "-p", "proj", "--from", str(staging)])


@pytest.mark.parametrize(
    "prefix", ["../evil", "a/b", "_x", ".hidden", "proj*", "", "a b"]
)
def test_bad_prefix_rejected(env, prefix):
    with pytest.raises(SystemExit, match="may only contain"):
        backup.backup_main(
            ["archivebackup", "--prefix", prefix, "--bundle-dir", str(env["bundle"])]
            + GLOBUS
        )


def test_tar_options_without_excludes_allowed(env):
    env["run"]("--tar-options", "--sparse --xattrs")
    assert meta(env, 1)["type"] == "full"


def test_work_dir_is_private_and_used_for_tars(env):
    env["run"]()
    assert stat.S_IMODE(os.stat(env["work"]).st_mode) == 0o700
    argv = env["archivetar"][0]
    # our private dir overrides the --bundle-dir the user gave archivetar
    bundle_args = [argv[i + 1] for i, a in enumerate(argv) if a == "--bundle-dir"]
    assert bundle_args[-1] == str(env["work"])


def test_work_dir_must_be_ours(env, tmp_path):
    env["bundle"].mkdir()
    (env["bundle"] / "archivebackup-proj").symlink_to(tmp_path)
    with pytest.raises(SystemExit, match="not a directory owned by you"):
        env["run"]()


def test_temporary_files_removed(env):
    env["run"]()
    (env["work"] / "proj-G0002-2026-10-08-00-00-00.cache").write_text("walk")
    env["run"]()
    leftovers = sorted(p.name for p in env["work"].iterdir())
    assert leftovers == ["proj-G0001.fullindex.txt", "proj-G0002.fullindex.txt"]


def test_dryrun_keeps_interrupted_run_intact(env, monkeypatch):
    env["run"]()

    def boom(argv):
        (env["work"] / "proj-G0002-1.tar").write_text("partial")
        raise RuntimeError("tar failed")

    monkeypatch.setattr(backup, "run_archivetar", boom)
    with pytest.raises(RuntimeError):
        env["run"]("--full")

    monkeypatch.setattr(backup, "run_archivetar", lambda argv: 0)
    env["run"]("--dryrun")
    # still knows G0002 was meant to be a full
    assert (env["state"] / "proj-G0002.pending.json").exists()
    env["run"]()
    assert meta(env, 2)["type"] == "full"


def test_make_destination_dir_creates_parent_first(monkeypatch):
    made = []

    class TC:
        def operation_mkdir(self, ep, path):
            made.append(path)

    class Client:
        tc = TC()

    monkeypatch.setattr(backup, "globus_client", lambda aargs: Client())
    aargs = type("A", (), {"destination": "DST"})()
    backup.make_destination_dir(aargs, "/bk/proj/proj-G0001")
    assert made == ["/bk/proj", "/bk/proj/proj-G0001"]


def test_restore_which_archive_copies_nothing(staged):
    staging, target, calls = staged
    backup.restore_main(
        ["archiverestore", "-p", "proj", "--from", str(staging)]
        + ["--which-archive", "--folder", "data"]
    )
    assert list(target.iterdir()) == []


def test_restore_replaces_symlink_instead_of_writing_through(staged, tmp_path):
    staging, target, calls = staged
    victim = tmp_path / "victim"
    victim.write_text("keep me")
    (target / "data").mkdir()
    (target / "data" / "big.dat").symlink_to(victim)
    backup.restore_main(["archiverestore", "-p", "proj", "--from", str(staging)])
    assert victim.read_text() == "keep me"
    assert (target / "data" / "big.dat").read_text() == "v2"
    assert not (target / "data" / "big.dat").is_symlink()
