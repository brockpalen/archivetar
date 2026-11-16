"""Globus Compute functions for the archivetar Globus Flow.

These run on the Compute endpoint as the user who started the flow. They use
only the Python standard library (3.11+ for ``tomllib``), so the endpoint
needs nothing beyond a pinned archivetar install. archivetar and
archivepurge themselves run from the published container, or from the host
for development.

The flow calls three functions:

* :func:`setup_run` validates the inputs and creates the private run
  directory in staging.
* :func:`tar_run` runs archivetar on the staged data.
* :func:`purge_run` runs archivepurge to delete what was tarred.

Moving data between collections (stage in, upload, cleanup) is done by
Globus Transfer in the flow, never here: these functions only touch staging.

**Nothing they return or raise names a file.** File names can be sensitive
(for example PHI), and flow run state is stored in Globus's cloud. Results
hold only run paths, counts, sizes and status. Errors carry fixed messages,
and the details go to a log inside the run directory, which cleanup deletes
with the rest of staging.

Staging layout, with ``staging_root`` from the site config::

    <staging_root>/                 root-owned, mode 1733
    <staging_root>/<run_id>/        the run directory, mode 0700
        <run_name>/                 archivetar working directory (user data)
        tmp/                        TMPDIR for archivetar's scan lists
        logs/archive-flow.log       tool output and error details
        .tar_complete               marker with the tar summary
        .purge_complete             marker with the purge summary
"""

import contextlib
import dataclasses
import datetime
import importlib.metadata
import json
import os
import posixpath
import re
import stat

# runs fixed archivetar/archivepurge argument lists, never a shell
import subprocess  # nosec B404
import traceback
import uuid
from pathlib import Path

#: Site config read when ``ARCHIVE_FLOW_SITE_CONFIG`` is not set.
DEFAULT_SITE_CONFIG_PATH = "/etc/archive-flow/site.toml"

#: Environment variable that points at a different site config, for example
#: on a single-user test endpoint.
SITE_CONFIG_ENVIRONMENT_VARIABLE = "ARCHIVE_FLOW_SITE_CONFIG"

#: Allowed run names. The run name becomes a directory name, the tar prefix
#: and the destination folder, so it is kept to safe characters.
RUN_NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$"

#: Sizes in binary units, e.g. ``100G``, ``1.5TiB``, ``500M``.
SIZE_PATTERN = r"^\s*(\d+(?:\.\d+)?)\s*([KMGTP]?)(?:i?B)?\s*$"

#: Compression choices offered on the start form.
COMPRESSION_CHOICES = ("gzip", "none")

#: Defaults for options the user leaves out.
DEFAULT_COMPRESSION = "gzip"
DEFAULT_TAR_SIZE = "100G"

#: Bounds on ``tar_size``: tiny tars defeat the point of archiving small
#: files, and huge ones are slow to recall from tape.
MINIMUM_TAR_SIZE_BYTES = 1024**2
MAXIMUM_TAR_SIZE_BYTES = 10 * 1024**4

TAR_MARKER_NAME = ".tar_complete"
PURGE_MARKER_NAME = ".purge_complete"
LOG_FILE_NAME = "archive-flow.log"

_SIZE_MULTIPLIERS = {
    "": 1,
    "K": 1024,
    "M": 1024**2,
    "G": 1024**3,
    "T": 1024**4,
    "P": 1024**5,
}


class FlowError(Exception):
    """An error whose message is safe to return to the flow.

    Only raise this with fixed text or values already known to be safe (run
    IDs, run names, counts). Never put a file name in the message.
    """


@dataclasses.dataclass(frozen=True)
class SiteConfig:
    """Host-side settings, read from the site config on the endpoint nodes.

    Values like these passed in as flow inputs are ignored, so a user cannot
    point the functions somewhere else.

    Attributes:
        staging_root: Absolute filesystem path of the staging root.
        staging_collection_base: The same directory as a path on the staging
            collection, e.g. ``/`` or ``/staging``.
        archivetar_runtime: ``container`` (production) or ``host``
            (development and testing only).
        archivetar_image: Path to the archivetar Apptainer image; required
            for the container runtime.
        archivetar_image_digest: Image digest recorded in the provenance
            manifest.
        apptainer_command: Apptainer (or Singularity) executable.
        archivetar_command: archivetar executable for the host runtime.
        archivepurge_command: archivepurge executable for the host runtime.
        tar_concurrency_lock: Optional lock file on a shared filesystem; when
            set, only one tar runs at a time across the endpoint's nodes.
        max_input_bytes: Largest staged input to accept; 0 for no limit.
        transfer_deadline_hours: Deadline given to the flow's transfers.
    """

    staging_root: Path
    staging_collection_base: str
    archivetar_runtime: str
    archivetar_image: str = ""
    archivetar_image_digest: str = ""
    apptainer_command: str = "apptainer"
    archivetar_command: str = "archivetar"
    archivepurge_command: str = "archivepurge"
    tar_concurrency_lock: str = ""
    max_input_bytes: int = 0
    transfer_deadline_hours: int = 168


@dataclasses.dataclass(frozen=True)
class RunPaths:
    """Filesystem paths of one run in staging.

    Attributes:
        run_dir: ``<staging_root>/<run_id>``.
        work_dir: ``<run_dir>/<run_name>``, where the user's data lands.
        tmp_dir: ``<run_dir>/tmp``, archivetar's TMPDIR.
        logs_dir: ``<run_dir>/logs``.
        log_file: The run's log file.
        tar_marker: Written when tarring has finished.
        purge_marker: Written when purging has finished.
    """

    run_dir: Path
    work_dir: Path
    tmp_dir: Path
    logs_dir: Path
    log_file: Path
    tar_marker: Path
    purge_marker: Path

    @classmethod
    def for_run(cls, config, run_id, run_name):
        """Build the paths for a run.

        Args:
            config: The site config.
            run_id: Validated flow run ID.
            run_name: Validated run name.

        Returns:
            The run's paths.
        """
        run_dir = config.staging_root / run_id
        logs_dir = run_dir / "logs"
        return cls(
            run_dir=run_dir,
            work_dir=run_dir / run_name,
            tmp_dir=run_dir / "tmp",
            logs_dir=logs_dir,
            log_file=logs_dir / LOG_FILE_NAME,
            tar_marker=run_dir / TAR_MARKER_NAME,
            purge_marker=run_dir / PURGE_MARKER_NAME,
        )

    def private_directories(self):
        """List the run's directories, parents first.

        Returns:
            The directories that must exist and be private to the caller.
        """
        return [self.run_dir, self.work_dir, self.tmp_dir, self.logs_dir]


# --------------------------------------------------------------------------
# site config
# --------------------------------------------------------------------------
def load_site_config(path=None):
    """Read and check the host-side site config.

    Args:
        path: Config file to read. Defaults to ``$ARCHIVE_FLOW_SITE_CONFIG``,
            then :data:`DEFAULT_SITE_CONFIG_PATH`.

    Returns:
        The parsed :class:`SiteConfig`.

    Raises:
        FlowError: If Python is too old, or the file is missing, unreadable
            or invalid.
    """
    try:
        import tomllib
    except ImportError:
        raise FlowError(
            "The archive flow functions need Python 3.11 or newer on the endpoint"
        ) from None

    config_path = Path(
        path
        or os.environ.get(SITE_CONFIG_ENVIRONMENT_VARIABLE)
        or DEFAULT_SITE_CONFIG_PATH
    )
    try:
        with config_path.open("rb") as config_file:
            raw_config = tomllib.load(config_file)
    except (OSError, tomllib.TOMLDecodeError):
        raise FlowError(
            "The archive flow site config is missing or unreadable; "
            "contact your administrator"
        ) from None
    return _site_config_from_dict(raw_config)


def _site_config_from_dict(raw_config):
    """Check types and values in a parsed site config.

    Args:
        raw_config: The parsed TOML document.

    Returns:
        The :class:`SiteConfig`.

    Raises:
        FlowError: If a setting is unknown, missing or has the wrong type.
    """
    fields = {field.name: field for field in dataclasses.fields(SiteConfig)}
    unknown = sorted(set(raw_config) - set(fields))
    if unknown:
        raise FlowError(f"Unknown site config settings: {', '.join(unknown)}")

    values = {}
    for name, field in fields.items():
        if name not in raw_config:
            if field.default is dataclasses.MISSING:
                raise FlowError(f"Site config setting {name} is required")
            continue
        value = raw_config[name]
        expected_type = int if field.type is int else str
        # bool is a subclass of int, but "true" is never a valid size or count
        if not isinstance(value, expected_type) or isinstance(value, bool):
            raise FlowError(f"Site config setting {name} has the wrong type")
        values[name] = value

    values["staging_root"] = Path(values["staging_root"])
    config = SiteConfig(**values)

    if not config.staging_root.is_absolute():
        raise FlowError("Site config staging_root must be an absolute path")
    if config.archivetar_runtime not in ("container", "host"):
        raise FlowError("Site config archivetar_runtime must be container or host")
    if config.archivetar_runtime == "container" and not config.archivetar_image:
        raise FlowError("Site config archivetar_image is required for containers")
    if config.max_input_bytes < 0 or config.transfer_deadline_hours < 1:
        raise FlowError("Site config limits must be positive")
    _normalize_collection_path(config.staging_collection_base, "staging base")
    return config


# --------------------------------------------------------------------------
# input validation
# --------------------------------------------------------------------------
def validate_run_id(run_id):
    """Check that a run ID is a canonical UUID, so it is safe as a path.

    Args:
        run_id: The flow run ID (``$._context.run_id``).

    Returns:
        The run ID.

    Raises:
        FlowError: If it is not a lower-case hyphenated UUID.
    """
    try:
        canonical = str(uuid.UUID(run_id))
    except (TypeError, ValueError, AttributeError):
        canonical = None
    if canonical is None or canonical != run_id:
        raise FlowError("The flow run ID is not a valid UUID")
    return run_id


def validate_run_name(run_name):
    """Check a run name against :data:`RUN_NAME_PATTERN`.

    Args:
        run_name: The run name from the start form.

    Returns:
        The run name.

    Raises:
        FlowError: If the name is not allowed.
    """
    if not isinstance(run_name, str) or not re.fullmatch(RUN_NAME_PATTERN, run_name):
        raise FlowError(
            "The run name must start with a letter or digit and contain only "
            "letters, digits, '.', '_' or '-' (at most 64 characters)"
        )
    return run_name


def _normalize_collection_path(path, description):
    """Normalize an absolute (``/``) or home-relative (``~``) collection path.

    Args:
        path: A path on a Globus collection.
        description: What the path is, for the error message.

    Returns:
        The normalized path, without a trailing slash (except ``/`` and
        ``~``).

    Raises:
        FlowError: If the path is relative, has ``..`` or control characters.
    """
    if not isinstance(path, str) or not path:
        raise FlowError(f"The {description} path is required")
    if not (path.startswith("/") or path == "~" or path.startswith("~/")):
        raise FlowError(f"The {description} path must start with / or ~/")
    if any(ord(character) < 32 or ord(character) == 127 for character in path):
        raise FlowError(f"The {description} path contains control characters")
    if ".." in path.split("/"):
        raise FlowError(f"The {description} path must not contain '..'")
    if path.startswith("~"):
        rest = path[1:].lstrip("/")
        return "~/" + posixpath.normpath(rest) if rest else "~"
    return posixpath.normpath(path)


def _join_collection_path(base, *parts):
    """Join a collection path with more components, ending in ``/``.

    Args:
        base: A normalized collection path.
        *parts: Validated names to append.

    Returns:
        The joined directory path with a trailing slash.
    """
    joined = posixpath.join(base, *parts)
    return joined if joined.endswith("/") else joined + "/"


def parse_size(text, description):
    """Parse a size with optional binary units into bytes.

    Args:
        text: A size such as ``100G``, ``1.5TiB`` or ``104857600``.
        description: What the size is, for the error message.

    Returns:
        The size in bytes.

    Raises:
        FlowError: If the text is not a size.
    """
    match = (
        re.fullmatch(SIZE_PATTERN, text, re.IGNORECASE)
        if isinstance(text, str)
        else None
    )
    if not match:
        raise FlowError(f"The {description} must be a size such as 500M, 100G or 1T")
    number, unit = match.groups()
    return int(float(number) * _SIZE_MULTIPLIERS[unit.upper()])


def validate_options(compression=None, advanced=None):
    """Allowlist and check the archivetar options from the start form.

    Args:
        compression: ``gzip`` (default) or ``none``.
        advanced: Optional dict with ``tar_size`` (default ``100G``) and
            ``size`` (empty for off). Any other key is rejected.

    Returns:
        Dict with ``compression``, ``tar_size_bytes`` and ``size_bytes``
        (``None`` when ``--size`` is off).

    Raises:
        FlowError: If an option is unknown, has the wrong type or is out of
            range.
    """
    compression = DEFAULT_COMPRESSION if compression in (None, "") else compression
    if compression not in COMPRESSION_CHOICES:
        raise FlowError(f"Compression must be one of: {', '.join(COMPRESSION_CHOICES)}")

    advanced = {} if advanced is None else advanced
    if not isinstance(advanced, dict):
        raise FlowError("The advanced options must be an object")
    unknown = sorted(set(advanced) - {"tar_size", "size"})
    if unknown:
        raise FlowError("Unknown advanced options; only tar_size and size are allowed")

    tar_size = advanced.get("tar_size") or DEFAULT_TAR_SIZE
    tar_size_bytes = parse_size(tar_size, "tar size")
    if not MINIMUM_TAR_SIZE_BYTES <= tar_size_bytes <= MAXIMUM_TAR_SIZE_BYTES:
        raise FlowError("The tar size must be between 1M and 10T")

    size = advanced.get("size")
    size_bytes = None
    if size not in (None, ""):
        size_bytes = parse_size(size, "large file size")
        if size_bytes < 1:
            raise FlowError("The large file size must be more than zero")

    return {
        "compression": compression,
        "tar_size_bytes": tar_size_bytes,
        "size_bytes": size_bytes,
    }


# --------------------------------------------------------------------------
# filesystem helpers (staging only)
# --------------------------------------------------------------------------
def check_private_directory(path):
    """Check that a directory is the caller's own, mode 0700, not a symlink.

    Uses ``lstat`` so a symlink planted in place of the directory is never
    followed.

    Args:
        path: Directory to check.

    Raises:
        FlowError: If it is missing, a symlink, not a directory, owned by
            someone else, or not mode 0700.
    """
    try:
        status = os.lstat(path)
    except FileNotFoundError:
        raise FlowError("The run directory is missing from staging") from None
    if stat.S_ISLNK(status.st_mode) or not stat.S_ISDIR(status.st_mode):
        raise FlowError("The run directory in staging is not a plain directory")
    if status.st_uid != os.getuid():
        raise FlowError("The run directory in staging belongs to another user")
    if stat.S_IMODE(status.st_mode) != 0o700:
        raise FlowError("The run directory in staging is not private (mode 0700)")


def create_private_directory(path):
    """Create a mode 0700 directory, or accept it if it is already private.

    Safe to repeat: an existing directory passes only if
    :func:`check_private_directory` accepts it.

    Args:
        path: Directory to create.

    Returns:
        True if it was created now, False if it already existed.

    Raises:
        FlowError: If it exists but is not private to the caller.
    """
    try:
        os.mkdir(path, 0o700)
    except FileExistsError:
        check_private_directory(path)
        return False
    # mkdir's mode is reduced by the umask; make it exactly 0700
    os.chmod(path, 0o700)
    return True


def summarize_tree(path):
    """Count files and bytes under a directory, without following symlinks.

    Args:
        path: Directory to walk.

    Returns:
        Tuple of (file count, total bytes). Symlinks count as files with
        their own size.
    """
    file_count = 0
    total_bytes = 0
    for directory, _subdirectories, file_names in os.walk(path):
        for file_name in file_names:
            file_count += 1
            total_bytes += os.lstat(os.path.join(directory, file_name)).st_size
    return file_count, total_bytes


def _write_json_atomically(path, data):
    """Write JSON to a file through a temporary file and rename.

    Args:
        path: File to write.
        data: JSON-serializable data.
    """
    temporary_path = path.with_name(path.name + ".tmp")
    descriptor = os.open(
        temporary_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(descriptor, "w") as output_file:
        json.dump(data, output_file, indent=2)
        output_file.write("\n")
    os.replace(temporary_path, path)


def _read_marker(path):
    """Read a completion marker if it exists.

    Args:
        path: Marker file.

    Returns:
        The summary stored in the marker, or None if there is no marker.
    """
    try:
        with path.open() as marker_file:
            return json.load(marker_file)
    except FileNotFoundError:
        return None


def _append_log(paths, text):
    """Append text to the run log, if the logs directory exists.

    Args:
        paths: The run's paths, or None before they are known.
        text: Text to append.
    """
    if paths is None or not paths.logs_dir.is_dir():
        return
    descriptor = os.open(
        paths.log_file, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(descriptor, "a") as log_file:
        log_file.write(text if text.endswith("\n") else text + "\n")


@contextlib.contextmanager
def tar_concurrency_lock(config):
    """Hold the site's tar lock, if one is configured.

    The lock file must already exist and be writable by every user; the
    site creates it.

    Args:
        config: The site config.

    Yields:
        None, while holding the lock.

    Raises:
        FlowError: If the lock file is configured but cannot be opened.
    """
    if not config.tar_concurrency_lock:
        yield
        return
    import fcntl  # POSIX only, and only needed when a lock is configured

    try:
        descriptor = os.open(config.tar_concurrency_lock, os.O_RDWR)
    except OSError:
        raise FlowError(
            "The tar concurrency lock file is missing or not writable; "
            "contact your administrator"
        ) from None
    try:
        # blocks until the running tar finishes; the flow's WaitTime bounds it
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


# --------------------------------------------------------------------------
# running archivetar
# --------------------------------------------------------------------------
def build_tool_command(config, paths, tool, arguments):
    """Build the command line and environment for archivetar or archivepurge.

    The container binds the run directory at the same path, so the paths
    archivetar writes into its indexes are real host paths.

    Args:
        config: The site config.
        paths: The run's paths.
        tool: ``archivetar`` or ``archivepurge``.
        arguments: Arguments for the tool.

    Returns:
        Tuple of (command list, environment dict).
    """
    environment = dict(os.environ)
    environment["TMPDIR"] = str(paths.tmp_dir)
    if config.archivetar_runtime == "container":
        command = [
            config.apptainer_command,
            "exec",
            "--containall",
            "--bind",
            f"{paths.run_dir}:{paths.run_dir}",
            "--pwd",
            str(paths.work_dir),
            "--env",
            f"TMPDIR={paths.tmp_dir}",
            config.archivetar_image,
            tool,
        ]
    else:
        executable = {
            "archivetar": config.archivetar_command,
            "archivepurge": config.archivepurge_command,
        }[tool]
        command = [executable]
    return command + list(arguments), environment


def run_tool(config, paths, tool, arguments):
    """Run archivetar or archivepurge in the work directory, logging output.

    Args:
        config: The site config.
        paths: The run's paths.
        tool: ``archivetar`` or ``archivepurge``.
        arguments: Arguments for the tool.

    Returns:
        The tool's exit code.
    """
    command, environment = build_tool_command(config, paths, tool, arguments)
    _append_log(paths, f"--- running: {' '.join(command)}")
    descriptor = os.open(
        paths.log_file, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(descriptor, "a") as log_file:
        try:
            # fixed argument list built above, no shell
            completed = subprocess.run(  # nosec B603
                command,
                cwd=paths.work_dir,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                check=False,
            )
        except OSError:
            raise FlowError(
                f"Could not start {tool}; contact your administrator"
            ) from None
    _append_log(paths, f"--- {tool} exit code {completed.returncode}")
    return completed.returncode


def archivetar_arguments(run_name, options):
    """Build archivetar's arguments from validated options.

    Sizes are passed as exact byte counts, so archivetar's own unit parsing
    never changes their meaning.

    Args:
        run_name: The run name, used as ``--prefix``.
        options: Output of :func:`validate_options`.

    Returns:
        The argument list.
    """
    arguments = [
        "--prefix",
        run_name,
        "--save-purge-list",
        "--tar-size",
        str(options["tar_size_bytes"]),
    ]
    if options["compression"] == "gzip":
        arguments.append("--gzip")
    if options["size_bytes"] is not None:
        arguments += ["--size", str(options["size_bytes"])]
    return arguments


def count_tars(work_dir, run_name):
    """Count the tars archivetar wrote for this run.

    Args:
        work_dir: The work directory.
        run_name: The run name (tar prefix).

    Returns:
        Number of ``<run_name>-<n>.tar[.<ext>]`` files.
    """
    pattern = re.compile(rf"^{re.escape(run_name)}-\d+\.tar(\.[A-Za-z0-9]+)?$")
    return sum(1 for entry in os.scandir(work_dir) if pattern.match(entry.name))


def _flow_version():
    """Look up the installed archivetar package version.

    Returns:
        The version, or ``unknown`` when archivetar is not installed as a
        package.
    """
    try:
        return importlib.metadata.version("archivetar")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


# --------------------------------------------------------------------------
# error handling
# --------------------------------------------------------------------------
def _run_step(step_name, paths_holder, body):
    """Run a step, turning every error into a message safe for the flow.

    Exception messages from Python and from archivetar can name files, so
    only :class:`FlowError` messages are passed on. The full traceback goes
    to the run log. The new exception is raised outside the ``except``
    block, so it carries no chained original exception.

    Args:
        step_name: Step name used in messages.
        paths_holder: A one-item list that the body fills with its
            :class:`RunPaths` once known, so errors can be logged.
        body: Callable that does the work.

    Returns:
        Whatever the body returns.

    Raises:
        FlowError: For any failure, with a message that names no files.
    """
    safe_message = None
    details = None
    try:
        return body()
    except FlowError as error:
        safe_message = f"{step_name} failed: {error}"
        details = traceback.format_exc()
    except Exception as error:  # every other error is reduced to its type
        safe_message = (
            f"{step_name} failed with an unexpected {type(error).__name__}; "
            "contact your administrator"
        )
        details = traceback.format_exc()
    paths = paths_holder[0] if paths_holder else None
    try:
        _append_log(paths, f"--- {safe_message}\n{details}")
    except OSError:
        pass  # logging is best effort; the safe message still goes back
    raise FlowError(safe_message)


def _prepare_existing_run(run_id, run_name, paths_holder):
    """Validate inputs and check an existing run directory.

    Args:
        run_id: The flow run ID.
        run_name: The run name.
        paths_holder: One-item list to receive the run's paths.

    Returns:
        Tuple of (site config, run paths).
    """
    config = load_site_config()
    validate_run_id(run_id)
    validate_run_name(run_name)
    paths = RunPaths.for_run(config, run_id, run_name)
    for directory in paths.private_directories():
        check_private_directory(directory)
    paths_holder.append(paths)
    return config, paths


# --------------------------------------------------------------------------
# the registered functions
# --------------------------------------------------------------------------
def setup_run(run_id, run_name, destination_path):
    """Validate the inputs and create the run directory in staging.

    Safe to repeat for the same run. If it fails after creating
    directories, it removes the empty ones it made, so nothing is left in
    staging.

    Args:
        run_id: The flow run ID (``$._context.run_id``).
        run_name: The run name from the start form.
        destination_path: Existing folder on the destination collection; the
            archive goes in ``<destination_path>/<run_name>/``.

    Returns:
        Dict with ``run_dir_filesystem``, ``run_dir_collection``,
        ``work_dir_collection``, ``destination_folder``, ``archive_path``
        (collection paths of directories end in ``/``),
        ``destination_ls_filter`` (a Transfer ``ls`` filter matching only
        the run name, so checking the destination never copies other names
        into the flow state) and ``transfer_deadline`` (ISO 8601, UTC).

    Raises:
        FlowError: If an input is invalid or staging is unusable.
    """
    paths_holder = []
    created_directories = []

    def body():
        config = load_site_config()
        validate_run_id(run_id)
        validate_run_name(run_name)
        destination = _normalize_collection_path(destination_path, "destination")
        staging_base = _normalize_collection_path(
            config.staging_collection_base, "staging base"
        )
        if not config.staging_root.is_dir():
            raise FlowError(
                "The staging area is unavailable; contact your administrator"
            )

        paths = RunPaths.for_run(config, run_id, run_name)
        for directory in paths.private_directories():
            if create_private_directory(directory):
                created_directories.append(directory)
        paths_holder.append(paths)

        deadline = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            hours=config.transfer_deadline_hours
        )
        _append_log(paths, f"--- setup complete for run {run_id}")
        return {
            "run_dir_filesystem": str(paths.run_dir),
            "run_dir_collection": _join_collection_path(staging_base, run_id),
            "work_dir_collection": _join_collection_path(
                staging_base, run_id, run_name
            ),
            "destination_folder": _join_collection_path(destination),
            "archive_path": _join_collection_path(destination, run_name),
            "destination_ls_filter": f"name:={run_name}",
            "transfer_deadline": deadline.replace(microsecond=0).isoformat(),
        }

    try:
        return _run_step("Setup", paths_holder, body)
    except FlowError:
        # leave staging as we found it: drop our log, then the directories
        # made here (rmdir only removes them if nothing else is inside)
        if paths_holder and paths_holder[0].logs_dir in created_directories:
            with contextlib.suppress(OSError):
                paths_holder[0].log_file.unlink()
        for directory in reversed(created_directories):
            with contextlib.suppress(OSError):
                directory.rmdir()
        raise


def tar_run(run_id, run_name, compression=None, advanced=None):
    """Run archivetar on everything staged for the run.

    Safe to repeat: once ``.tar_complete`` exists it returns the stored
    summary. A rerun after a partial tar fails, because archivetar refuses
    to overwrite files that match the prefix. This function never deletes
    ``<run_name>*`` files itself, since they could be the user's.

    Args:
        run_id: The flow run ID.
        run_name: The run name, used as the tar prefix.
        compression: ``gzip`` (default) or ``none``.
        advanced: Optional dict with ``tar_size`` and ``size``.

    Returns:
        Dict with ``status``, ``tar_count``, ``input_files`` and
        ``input_bytes``.

    Raises:
        FlowError: If an option is invalid, the input is too large, or
            archivetar fails.
    """
    paths_holder = []

    def body():
        config, paths = _prepare_existing_run(run_id, run_name, paths_holder)
        options = validate_options(compression, advanced)
        previous = _read_marker(paths.tar_marker)
        if previous is not None:
            return previous

        input_files, input_bytes = summarize_tree(paths.work_dir)
        if input_files == 0:
            raise FlowError("Nothing was staged to archive")
        if config.max_input_bytes and input_bytes > config.max_input_bytes:
            raise FlowError("The data is larger than this site allows in one archive")

        with tar_concurrency_lock(config):
            exit_code = run_tool(
                config, paths, "archivetar", archivetar_arguments(run_name, options)
            )
        if exit_code != 0:
            raise FlowError(f"archivetar exited with code {exit_code}")

        _write_json_atomically(
            paths.work_dir / f"{run_name}-provenance.json",
            {
                "run_id": run_id,
                "created": datetime.datetime.now(datetime.timezone.utc)
                .replace(microsecond=0)
                .isoformat(),
                "flow_version": _flow_version(),
                "archivetar_runtime": config.archivetar_runtime,
                "archivetar_image": config.archivetar_image,
                "archivetar_image_digest": config.archivetar_image_digest,
                "options": options,
            },
        )
        summary = {
            "status": "tarred",
            "tar_count": count_tars(paths.work_dir, run_name),
            "input_files": input_files,
            "input_bytes": input_bytes,
        }
        _write_json_atomically(paths.tar_marker, summary)
        return summary

    return _run_step("Tar", paths_holder, body)


def purge_run(run_id, run_name):
    """Delete the staged files that were tarred, using archivepurge.

    archivepurge also removes the empty directories left behind. Safe to
    repeat: archivepurge tolerates files that are already gone, and once
    ``.purge_complete`` exists this returns the stored summary.

    Args:
        run_id: The flow run ID.
        run_name: The run name.

    Returns:
        Dict with ``status``, ``remaining_files`` and ``remaining_bytes``:
        what will be uploaded (tars, indexes and any files not tarred).

    Raises:
        FlowError: If tarring has not finished or archivepurge fails.
    """
    paths_holder = []

    def body():
        config, paths = _prepare_existing_run(run_id, run_name, paths_holder)
        if _read_marker(paths.tar_marker) is None:
            raise FlowError("Tarring has not finished, so nothing can be purged")

        purge_lists = sorted(paths.work_dir.glob(f"{run_name}-*.under.cache"))
        previous = _read_marker(paths.purge_marker)
        if previous is None:
            if len(purge_lists) != 1:
                raise FlowError(
                    f"Expected one archivetar purge list, found {len(purge_lists)}"
                )
            # --save-purge-list: archivepurge would delete the list itself,
            # and a retry would then fail; we remove it after the marker
            exit_code = run_tool(
                config,
                paths,
                "archivepurge",
                ["--purge-list", purge_lists[0].name, "--save-purge-list"],
            )
            if exit_code != 0:
                raise FlowError(f"archivepurge exited with code {exit_code}")
            remaining_files, remaining_bytes = summarize_tree(paths.work_dir)
            previous = {
                "status": "purged",
                # the purge list is about to be removed; don't upload or count it
                "remaining_files": remaining_files - 1,
                "remaining_bytes": remaining_bytes - purge_lists[0].stat().st_size,
            }
            _write_json_atomically(paths.purge_marker, previous)
        for purge_list in purge_lists:
            purge_list.unlink(missing_ok=True)
        return previous

    return _run_step("Purge", paths_holder, body)
