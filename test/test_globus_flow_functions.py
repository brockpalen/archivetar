"""Tests for the archivetar Globus Flow Compute functions.

archivetar and archivepurge are replaced by small stand-in scripts (the
host runtime) that mimic the real tools' outputs: tars, indexes, a purge
list in the work directory, scan lists in $TMPDIR, and refusing a prefix
that is already in use. They also print file names, so the tests can check
that none ever reach a function's result or error.

Set ARCHIVE_FLOW_TEST_SIF to an archivetar image (and optionally
ARCHIVE_FLOW_TEST_APPTAINER to the apptainer or singularity executable) to
also run the end-to-end test against the real container.
"""

import json
import os
import stat
import sys
import textwrap
import uuid

import pytest

from archivetar_flow import functions
from archivetar_flow.functions import FlowError

# archivetar_flow needs Python 3.11+ (tomllib); archivetar itself runs on 3.10
pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 11), reason="archivetar_flow needs Python 3.11+"
)

#: A file name that must never appear in anything returned to the flow.
SECRET_NAME = "PATIENT-Jane-Doe-MRN-12345"

FAKE_ARCHIVETAR = r"""
import argparse, json, os, sys
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--prefix", required=True)
parser.add_argument("--save-purge-list", action="store_true")
parser.add_argument("--tar-size")
parser.add_argument("--gzip", action="store_true")
parser.add_argument("--size")
args = parser.parse_args()

tmp = Path(os.environ["TMPDIR"])
with open(tmp / "archivetar-calls.jsonl", "a") as calls:
    calls.write(json.dumps(sys.argv[1:]) + "\n")

existing = [p.name for p in Path(".").iterdir()
            if p.name.startswith(args.prefix + "-") and ".tar" in p.name]
if existing:
    print(f"Prefix {args.prefix} conflicts with current files {existing}")
    sys.exit(1)

files = sorted(p for p in Path(".").rglob("*") if p.is_file())
print("archiving", [str(p) for p in files])  # names go to the log only
(tmp / f"{args.prefix}.under.txt").write_text("\n".join(map(str, files)))
limit = int(args.size) if args.size else None
small = [p for p in files if limit is None or p.stat().st_size < limit]
suffix = ".tar.gz" if args.gzip else ".tar"
Path(f"{args.prefix}-1{suffix}").write_text("\n".join(map(str, small)))
Path(f"{args.prefix}-1.index.txt").write_text("\n".join(map(str, small)))
Path(f"{args.prefix}-1.DONT_DELETE.txt").write_text("\n".join(map(str, small)))
if args.save_purge_list:
    Path(f"{args.prefix}-2026-01-01-00-00-00.under.cache").write_text(
        "\n".join(map(str, small))
    )
"""

FAKE_ARCHIVEPURGE = r"""
import argparse, os, sys
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--purge-list", required=True)
parser.add_argument("--save-purge-list", action="store_true")
args = parser.parse_args()
if os.environ.get("FAKE_PURGE_FAIL"):
    sys.exit(3)
purge_list = Path(args.purge_list)
for line in purge_list.read_text().splitlines():
    Path(line).unlink(missing_ok=True)  # like drm: already gone is fine
for directory, subdirectories, names in os.walk(".", topdown=False):
    if directory != "." and not os.listdir(directory):
        os.rmdir(directory)
if not args.save_purge_list:
    purge_list.unlink()
"""


def write_script(path, body):
    """Write an executable Python script.

    Args:
        path: Where to write it.
        body: The script's Python source.

    Returns:
        The path.
    """
    path.write_text(f"#!{sys.executable}\n" + textwrap.dedent(body))
    path.chmod(0o755)
    return path


@pytest.fixture
def site(tmp_path, monkeypatch):
    """A staging root, stand-in tools and a site config pointing at them.

    Args:
        tmp_path: pytest temporary directory.
        monkeypatch: pytest monkeypatch fixture.

    Returns:
        Namespace-like dict with ``staging_root``, ``config_path`` and a
        ``write_config`` helper for extra settings.
    """
    staging_root = tmp_path / "staging"
    staging_root.mkdir()
    staging_root.chmod(0o1733)
    tools = tmp_path / "tools"
    tools.mkdir()
    archivetar = write_script(tools / "archivetar", FAKE_ARCHIVETAR)
    archivepurge = write_script(tools / "archivepurge", FAKE_ARCHIVEPURGE)
    config_path = tmp_path / "site.toml"

    def write_config(**extra):
        settings = {
            "staging_root": str(staging_root),
            "staging_collection_base": "/staging",
            "archivetar_runtime": "host",
            "archivetar_command": str(archivetar),
            "archivepurge_command": str(archivepurge),
            **extra,
        }
        lines = [f"{key} = {json.dumps(value)}" for key, value in settings.items()]
        config_path.write_text("\n".join(lines) + "\n")

    write_config()
    monkeypatch.setenv(functions.SITE_CONFIG_ENVIRONMENT_VARIABLE, str(config_path))
    return {
        "staging_root": staging_root,
        "config_path": config_path,
        "write_config": write_config,
    }


@pytest.fixture
def run_id():
    """A fresh flow run ID.

    Returns:
        A canonical UUID string.
    """
    return str(uuid.uuid4())


def stage_files(work_dir, large_bytes=0):
    """Put user files in the work directory, as TransferIn would.

    Args:
        work_dir: The run's work directory.
        large_bytes: If set, also add a file of this size.
    """
    (work_dir / "sub" / "deeper").mkdir(parents=True)
    (work_dir / f"{SECRET_NAME}.txt").write_text("x" * 100)
    (work_dir / "sub" / "notes.txt").write_text("y" * 50)
    (work_dir / "sub" / "deeper" / f"{SECRET_NAME}-scan.dat").write_text("z" * 10)
    if large_bytes:
        (work_dir / "large.bin").write_bytes(b"0" * large_bytes)


def mode_of(path):
    """Permission bits of a path.

    Args:
        path: Path to inspect.

    Returns:
        The permission bits.
    """
    return stat.S_IMODE(os.lstat(path).st_mode)


def assert_no_file_names(value):
    """Fail if a result or message contains a staged file name.

    Args:
        value: A function result (JSON-serializable) or a string.
    """
    text = value if isinstance(value, str) else json.dumps(value)
    assert SECRET_NAME not in text
    assert "notes.txt" not in text


# --------------------------------------------------------------------------
# setup_run
# --------------------------------------------------------------------------
def test_setup_creates_private_run_directory(site, run_id):
    """Setup makes every run directory mode 0700 and returns collection paths."""
    result = functions.setup_run(run_id, "my-run_1.0", "/archive/lab/")
    run_dir = site["staging_root"] / run_id
    for directory in (
        run_dir,
        run_dir / "my-run_1.0",
        run_dir / "tmp",
        run_dir / "logs",
    ):
        assert directory.is_dir()
        assert mode_of(directory) == 0o700
    assert result["run_dir_filesystem"] == str(run_dir)
    assert result["run_dir_collection"] == f"/staging/{run_id}/"
    assert result["work_dir_collection"] == f"/staging/{run_id}/my-run_1.0/"
    assert result["archive_path"] == "/archive/lab/my-run_1.0/"
    assert result["destination_folder"] == "/archive/lab/"
    assert result["destination_ls_filter"] == "name:=my-run_1.0"
    assert result["transfer_deadline"].endswith("+00:00")


def test_setup_is_safe_to_repeat(site, run_id):
    """A second Setup for the same run accepts its own private directories."""
    first = functions.setup_run(run_id, "run", "/archive")
    second = functions.setup_run(run_id, "run", "/archive")
    assert {k: v for k, v in first.items() if k != "transfer_deadline"} == {
        k: v for k, v in second.items() if k != "transfer_deadline"
    }


def test_setup_rejects_run_directory_with_wrong_mode(site, run_id):
    """An existing run directory that isn't 0700 is refused."""
    run_dir = site["staging_root"] / run_id
    run_dir.mkdir(mode=0o755)
    run_dir.chmod(0o755)
    with pytest.raises(FlowError, match="not private"):
        functions.setup_run(run_id, "run", "/archive")


def test_setup_rejects_symlinked_run_directory(site, run_id, tmp_path):
    """A symlink planted where the run directory goes is never followed."""
    target = tmp_path / "elsewhere"
    target.mkdir(mode=0o700)
    (site["staging_root"] / run_id).symlink_to(target)
    with pytest.raises(FlowError, match="not a plain directory"):
        functions.setup_run(run_id, "run", "/archive")
    assert not (target / "run").exists()


def test_setup_rejects_run_directory_owned_by_someone_else(site, run_id, monkeypatch):
    """A run directory owned by another UID is refused."""
    (site["staging_root"] / run_id).mkdir(mode=0o700)
    real_uid = os.getuid()
    monkeypatch.setattr(functions.os, "getuid", lambda: real_uid + 1)
    with pytest.raises(FlowError, match="belongs to another user"):
        functions.setup_run(run_id, "run", "/archive")


def test_setup_failure_removes_directories_it_created(site, run_id, monkeypatch):
    """If Setup fails partway, the directories it made are removed again."""
    real_create = functions.create_private_directory

    def fail_on_logs(path):
        if path.name == "logs":
            raise FlowError("simulated failure")
        return real_create(path)

    monkeypatch.setattr(functions, "create_private_directory", fail_on_logs)
    with pytest.raises(FlowError, match="Setup failed: simulated failure"):
        functions.setup_run(run_id, "run", "/archive")
    assert not (site["staging_root"] / run_id).exists()


def test_setup_requires_staging_root(site, run_id):
    """A missing staging root fails cleanly."""
    site["staging_root"].rmdir()
    with pytest.raises(FlowError, match="staging area is unavailable"):
        functions.setup_run(run_id, "run", "/archive")


@pytest.mark.parametrize(
    "run_name",
    ["a", "Run-2026.11_final", "0abc", "x" * 64],
)
def test_valid_run_names(run_name):
    """Allowed run names pass."""
    assert functions.validate_run_name(run_name) == run_name


@pytest.mark.parametrize(
    "run_name",
    ["", ".hidden", "-dash", "_under", "a/b", "../up", "a b", "x" * 65, "ünï", None, 5],
)
def test_invalid_run_names(run_name):
    """Unsafe or malformed run names are refused."""
    with pytest.raises(FlowError, match="run name"):
        functions.validate_run_name(run_name)


@pytest.mark.parametrize(
    "run_id",
    ["../../etc", "", None, str(uuid.uuid4()).upper(), uuid.uuid4().hex],
)
def test_invalid_run_ids(run_id):
    """Only canonical UUIDs are accepted as run IDs."""
    with pytest.raises(FlowError, match="run ID"):
        functions.validate_run_id(run_id)


@pytest.mark.parametrize(
    "destination,expected",
    [
        ("/archive/lab", "/archive/lab/run/"),
        ("/archive/lab/", "/archive/lab/run/"),
        ("/archive//lab/./x", "/archive/lab/x/run/"),
        ("/", "/run/"),
        ("~", "~/run/"),
        ("~/", "~/run/"),
        ("~/projects/", "~/projects/run/"),
    ],
)
def test_archive_path(site, run_id, destination, expected):
    """The archive always goes in a new <destination>/<run_name>/ folder."""
    result = functions.setup_run(run_id, "run", destination)
    assert result["archive_path"] == expected
    assert result["destination_folder"] == expected[: -len("run/")]


@pytest.mark.parametrize(
    "destination",
    ["relative/path", "/archive/../etc", "/a\nb", "", None, "~user/x"],
)
def test_invalid_destinations(site, run_id, destination):
    """Bad destination paths fail before anything is created."""
    with pytest.raises(FlowError, match="destination path"):
        functions.setup_run(run_id, "run", destination)
    assert not (site["staging_root"] / run_id).exists()


# --------------------------------------------------------------------------
# options
# --------------------------------------------------------------------------
def test_option_defaults():
    """Leaving options out gives gzip, 100G tars and no --size."""
    assert functions.validate_options() == {
        "compression": "gzip",
        "tar_size_bytes": 100 * 1024**3,
        "size_bytes": None,
    }
    assert functions.validate_options("", {"tar_size": "", "size": ""}) == (
        functions.validate_options()
    )


def test_option_values():
    """Sizes use binary units and become exact bytes."""
    assert functions.validate_options(
        "none", {"tar_size": "1.5T", "size": "500MiB"}
    ) == {
        "compression": "none",
        "tar_size_bytes": int(1.5 * 1024**4),
        "size_bytes": 500 * 1024**2,
    }


@pytest.mark.parametrize(
    "compression,advanced,message",
    [
        ("zstd", None, "Compression"),
        (True, None, "Compression"),
        ("gzip", {"tar_options": "--to-command=sh"}, "Unknown advanced"),
        ("gzip", {"tar_size": "100G", "bundle_dir": "/tmp"}, "Unknown advanced"),
        ("gzip", "tar_size=1G", "must be an object"),
        ("gzip", {"tar_size": "lots"}, "tar size"),
        ("gzip", {"tar_size": 100}, "tar size"),
        ("gzip", {"tar_size": True}, "tar size"),
        ("gzip", {"tar_size": "512K"}, "between"),
        ("gzip", {"tar_size": "20T"}, "between"),
        ("gzip", {"size": "0"}, "more than zero"),
        ("gzip", {"size": "-5G"}, "large file size"),
    ],
)
def test_option_allowlist(compression, advanced, message):
    """Only allowlisted options with valid types and ranges pass."""
    with pytest.raises(FlowError, match=message):
        functions.validate_options(compression, advanced)


def test_archivetar_arguments():
    """Options map onto archivetar's flags, with sizes in bytes."""
    options = functions.validate_options("gzip", {"tar_size": "1G", "size": "1M"})
    assert functions.archivetar_arguments("run", options) == [
        "--prefix",
        "run",
        "--save-purge-list",
        "--tar-size",
        str(1024**3),
        "--gzip",
        "--size",
        str(1024**2),
    ]
    plain = functions.validate_options("none")
    assert "--gzip" not in functions.archivetar_arguments("run", plain)
    assert "--size" not in functions.archivetar_arguments("run", plain)


# --------------------------------------------------------------------------
# tar_run and purge_run
# --------------------------------------------------------------------------
def staged_run(site, run_id, run_name="run", large_bytes=0):
    """Set up a run and stage files into it.

    Args:
        site: The site fixture.
        run_id: The run ID.
        run_name: The run name.
        large_bytes: Size of an extra large file, if any.

    Returns:
        The run's :class:`RunPaths`.
    """
    functions.setup_run(run_id, run_name, "/archive")
    config = functions.load_site_config()
    paths = functions.RunPaths.for_run(config, run_id, run_name)
    stage_files(paths.work_dir, large_bytes)
    return paths


def archivetar_calls(paths):
    """Read the arguments of every stand-in archivetar call.

    Args:
        paths: The run's paths.

    Returns:
        List of argument lists.
    """
    calls_file = paths.tmp_dir / "archivetar-calls.jsonl"
    if not calls_file.exists():
        return []
    return [json.loads(line) for line in calls_file.read_text().splitlines()]


def test_tar_run(site, run_id):
    """Tar runs archivetar in the work dir with TMPDIR in the run dir."""
    paths = staged_run(site, run_id)
    result = functions.tar_run(run_id, "run", "gzip", {"tar_size": "1G"})
    assert result == {
        "status": "tarred",
        "tar_count": 1,
        "input_files": 3,
        "input_bytes": 160,
    }
    assert_no_file_names(result)
    assert (paths.work_dir / "run-1.tar.gz").exists()
    # scan lists landed in the run's tmp dir, not the system /tmp
    assert (paths.tmp_dir / "run.under.txt").exists()
    assert archivetar_calls(paths)[0][:5] == [
        "--prefix",
        "run",
        "--save-purge-list",
        "--tar-size",
        str(1024**3),
    ]
    assert json.loads(paths.tar_marker.read_text()) == result
    provenance = json.loads((paths.work_dir / "run-provenance.json").read_text())
    assert provenance["run_id"] == run_id
    assert provenance["archivetar_runtime"] == "host"
    assert provenance["options"]["tar_size_bytes"] == 1024**3
    assert_no_file_names(provenance)
    # the tool's output, which names files, went to the run log
    assert SECRET_NAME in paths.log_file.read_text()


def test_tar_run_is_safe_to_repeat(site, run_id):
    """Once tarred, a repeat returns the stored summary without rerunning."""
    paths = staged_run(site, run_id)
    first = functions.tar_run(run_id, "run")
    second = functions.tar_run(run_id, "run")
    assert first == second
    assert len(archivetar_calls(paths)) == 1


def test_tar_run_retry_after_partial_tar_fails_without_deleting(site, run_id):
    """A rerun after a crash relies on archivetar's prefix check, and deletes nothing."""
    paths = staged_run(site, run_id)
    (paths.work_dir / "run-1.tar").write_text("left over from a crashed attempt")
    with pytest.raises(FlowError) as raised:
        functions.tar_run(run_id, "run")
    assert str(raised.value) == "Tar failed: archivetar exited with code 1"
    assert (paths.work_dir / "run-1.tar").exists()
    assert not paths.tar_marker.exists()


def test_tar_run_with_user_files_matching_prefix(site, run_id):
    """User files that look like this run's tars make archivetar refuse; they survive."""
    paths = staged_run(site, run_id)
    (paths.work_dir / "run-7.tar").write_text("the user's own tar")
    with pytest.raises(FlowError, match="archivetar exited with code 1"):
        functions.tar_run(run_id, "run")
    assert (paths.work_dir / "run-7.tar").read_text() == "the user's own tar"


def test_tar_run_refuses_too_much_input(site, run_id):
    """max_input_bytes caps the staged size."""
    paths = staged_run(site, run_id)
    site["write_config"](max_input_bytes=100)
    with pytest.raises(FlowError, match="larger than this site allows"):
        functions.tar_run(run_id, "run")
    assert archivetar_calls(paths) == []


def test_tar_run_refuses_empty_input(site, run_id):
    """Nothing staged means nothing to archive."""
    functions.setup_run(run_id, "run", "/archive")
    with pytest.raises(FlowError, match="Nothing was staged"):
        functions.tar_run(run_id, "run")


def test_tar_run_requires_setup(site, run_id):
    """Tar refuses a run directory that Setup didn't create."""
    with pytest.raises(FlowError, match="missing from staging"):
        functions.tar_run(run_id, "run")


def test_tar_run_with_lock(site, run_id, tmp_path):
    """A configured lock file is taken around archivetar."""
    lock = tmp_path / "tar.lock"
    lock.touch()
    staged_run(site, run_id)
    site["write_config"](tar_concurrency_lock=str(lock))
    assert functions.tar_run(run_id, "run")["status"] == "tarred"


def test_tar_run_with_missing_lock(site, run_id, tmp_path):
    """A configured but missing lock file is an admin error, not a hang."""
    staged_run(site, run_id)
    site["write_config"](tar_concurrency_lock=str(tmp_path / "missing.lock"))
    with pytest.raises(FlowError, match="lock file is missing"):
        functions.tar_run(run_id, "run")


def test_purge_run(site, run_id):
    """Purge removes tarred files and empty dirs, keeping large files and tars."""
    paths = staged_run(site, run_id, large_bytes=2048)
    functions.tar_run(run_id, "run", "none", {"size": "1K"})
    result = functions.purge_run(run_id, "run")
    remaining = sorted(p.name for p in paths.work_dir.iterdir())
    assert remaining == [
        "large.bin",
        "run-1.DONT_DELETE.txt",
        "run-1.index.txt",
        "run-1.tar",
        "run-provenance.json",
    ]
    assert result["status"] == "purged"
    assert result["remaining_files"] == 5
    assert result["remaining_bytes"] == sum(
        (paths.work_dir / name).stat().st_size for name in remaining
    )
    assert_no_file_names(result)
    assert json.loads(paths.purge_marker.read_text()) == result


def test_purge_run_is_safe_to_repeat(site, run_id):
    """A repeat after success returns the stored summary."""
    staged_run(site, run_id)
    functions.tar_run(run_id, "run")
    assert functions.purge_run(run_id, "run") == functions.purge_run(run_id, "run")


def test_purge_run_retry_after_crash_before_list_removed(site, run_id):
    """If a crash left the purge list behind after the marker, a retry removes it."""
    paths = staged_run(site, run_id)
    functions.tar_run(run_id, "run")
    purge_list = next(paths.work_dir.glob("run-*.under.cache"))
    saved = purge_list.read_text()
    functions.purge_run(run_id, "run")
    purge_list.write_text(saved)
    functions.purge_run(run_id, "run")
    assert not purge_list.exists()


def test_purge_run_tolerates_files_already_gone(site, run_id):
    """Files removed before the purge don't make it fail."""
    paths = staged_run(site, run_id)
    functions.tar_run(run_id, "run")
    (paths.work_dir / "sub" / "notes.txt").unlink()
    assert functions.purge_run(run_id, "run")["status"] == "purged"


def test_purge_run_requires_tar_marker(site, run_id):
    """Purge never runs before tarring has finished."""
    staged_run(site, run_id)
    with pytest.raises(FlowError, match="Tarring has not finished"):
        functions.purge_run(run_id, "run")


def test_purge_run_reports_archivepurge_failure(site, run_id, monkeypatch):
    """A failing archivepurge fails the step and writes no marker."""
    paths = staged_run(site, run_id)
    functions.tar_run(run_id, "run")
    monkeypatch.setenv("FAKE_PURGE_FAIL", "1")
    with pytest.raises(FlowError, match="archivepurge exited with code 3"):
        functions.purge_run(run_id, "run")
    assert not paths.purge_marker.exists()


# --------------------------------------------------------------------------
# errors never name files
# --------------------------------------------------------------------------
def test_unexpected_errors_are_reduced_to_their_type(site, run_id, monkeypatch):
    """A Python error naming a file becomes a generic message with no chain."""
    paths = staged_run(site, run_id)

    def explode(path):
        raise FileNotFoundError(2, "No such file", f"/staging/{SECRET_NAME}.txt")

    monkeypatch.setattr(functions, "summarize_tree", explode)
    with pytest.raises(FlowError) as raised:
        functions.tar_run(run_id, "run")
    message = str(raised.value)
    assert message == (
        "Tar failed with an unexpected FileNotFoundError; contact your administrator"
    )
    assert_no_file_names(message)
    # nothing chained that a serializer could pick up
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    # the detail is in the run log for the admin
    assert SECRET_NAME in paths.log_file.read_text()


def test_tool_failure_output_stays_in_log(site, run_id):
    """archivetar's error text (which names files) never reaches the message."""
    paths = staged_run(site, run_id)
    (paths.work_dir / "run-1.tar").write_text("conflict")
    with pytest.raises(FlowError) as raised:
        functions.tar_run(run_id, "run")
    assert_no_file_names(str(raised.value))
    assert "conflicts with current files" in paths.log_file.read_text()


# --------------------------------------------------------------------------
# site config
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "extra,message",
    [
        ({"staging_dir": "/x"}, "Unknown site config settings: staging_dir"),
        ({"max_input_bytes": "10G"}, "max_input_bytes has the wrong type"),
        ({"max_input_bytes": True}, "max_input_bytes has the wrong type"),
        ({"archivetar_runtime": "docker"}, "container or host"),
        ({"archivetar_runtime": "container"}, "archivetar_image is required"),
        ({"staging_root": "relative/staging"}, "absolute path"),
        ({"staging_collection_base": "staging"}, "must start with / or ~/"),
        ({"transfer_deadline_hours": 0}, "positive"),
    ],
)
def test_site_config_validation(site, extra, message):
    """Typos and bad values in the site config fail loudly."""
    site["write_config"](**extra)
    with pytest.raises(FlowError, match=message):
        functions.load_site_config()


def test_site_config_missing(tmp_path, monkeypatch):
    """A missing config is a clear admin error."""
    monkeypatch.setenv(
        functions.SITE_CONFIG_ENVIRONMENT_VARIABLE, str(tmp_path / "nope.toml")
    )
    with pytest.raises(FlowError, match="site config is missing"):
        functions.load_site_config()


def test_site_config_required_setting(site):
    """Required settings must be present."""
    site["config_path"].write_text('staging_root = "/staging"\n')
    with pytest.raises(FlowError, match="staging_collection_base is required"):
        functions.load_site_config()


def test_container_command(site, run_id):
    """The container binds the run dir at the same path and sets TMPDIR."""
    site["write_config"](
        archivetar_runtime="container",
        archivetar_image="/images/archivetar.sif",
        apptainer_command="apptainer",
    )
    config = functions.load_site_config()
    paths = functions.RunPaths.for_run(config, run_id, "run")
    command, environment = functions.build_tool_command(
        config, paths, "archivetar", ["--prefix", "run"]
    )
    run_dir = str(paths.run_dir)
    assert command == [
        "apptainer",
        "exec",
        "--containall",
        "--bind",
        f"{run_dir}:{run_dir}",
        "--pwd",
        str(paths.work_dir),
        "--env",
        f"TMPDIR={paths.tmp_dir}",
        "/images/archivetar.sif",
        "archivetar",
        "--prefix",
        "run",
    ]
    assert environment["TMPDIR"] == str(paths.tmp_dir)


# --------------------------------------------------------------------------
# optional: the real archivetar container
# --------------------------------------------------------------------------
@pytest.mark.skipif(
    not os.environ.get("ARCHIVE_FLOW_TEST_SIF"),
    reason="set ARCHIVE_FLOW_TEST_SIF to an archivetar image to run",
)
def test_end_to_end_with_container(site, run_id):
    """Setup, Tar and Purge against the real archivetar container."""
    site["write_config"](
        archivetar_runtime="container",
        archivetar_image=os.environ["ARCHIVE_FLOW_TEST_SIF"],
        apptainer_command=os.environ.get("ARCHIVE_FLOW_TEST_APPTAINER", "apptainer"),
    )
    paths = staged_run(site, run_id, large_bytes=3 * 1024**2)
    tarred = functions.tar_run(run_id, "run", "gzip", {"size": "1M"})
    assert tarred["tar_count"] == 1
    purged = functions.purge_run(run_id, "run")
    remaining = sorted(p.name for p in paths.work_dir.iterdir())
    assert "large.bin" in remaining
    assert "run-1.tar.gz" in remaining
    assert "run-provenance.json" in remaining
    assert not (paths.work_dir / "sub").exists()
    assert not list(paths.work_dir.glob("*.under.cache"))
    assert purged["remaining_files"] == len(remaining)
    assert_no_file_names(tarred)
    assert_no_file_names(purged)
