"""Tests for the archivetar flow templates, values and deploy helper.

The rendered flow is checked as a state machine: every target exists,
every state is reachable, every working step has a Catch, and every
failure after Setup passes through staging cleanup (and an upload failure
through destination rollback) before the run ends as failed.
"""

import json
import re
from importlib import resources

import pytest

# archivetar_flow needs Python 3.11+ (tomllib); archivetar itself runs on 3.10
tomllib = pytest.importorskip("tomllib")
jinja2 = pytest.importorskip("jinja2")

from archivetar_flow import deploy, functions, registered  # noqa: E402

CATCH_ERRORS = {"ActionUnableToRun", "ActionFailedException", "ActionTimeout"}
FAILURE_ENDS = {"RunFailed"}


@pytest.fixture
def values():
    """The example values file, parsed and checked.

    Returns:
        The values dict.
    """
    text = (
        resources.files("archivetar_flow").joinpath("values.example.toml").read_text()
    )
    return deploy.check_values(tomllib.loads(text))


@pytest.fixture
def rendered(values):
    """The rendered definition and input schema.

    Args:
        values: The values fixture.

    Returns:
        Tuple of (definition, input schema).
    """
    return deploy.render(values)


def targets(state):
    """List every state a state can move to.

    Args:
        state: A state from the definition.

    Returns:
        Set of state names.
    """
    found = set()
    if "Next" in state:
        found.add(state["Next"])
    if "Default" in state:
        found.add(state["Default"])
    for choice in state.get("Choices", []):
        found.add(choice["Next"])
    for catch in state.get("Catch", []):
        found.add(catch["Next"])
    return found


def failure_paths(states, start):
    """Follow a failure handler to the end of the run.

    Args:
        states: The definition's states.
        start: The first state of the failure handler.

    Returns:
        List of state names in order, following ``Next`` (not Catch).
    """
    path = [start]
    while "Next" in states[path[-1]]:
        path.append(states[path[-1]]["Next"])
    return path


def test_every_target_exists_and_every_state_is_reachable(rendered):
    """No dangling Next/Catch/Default, and no unreachable states."""
    definition, _ = rendered
    states = definition["States"]
    for name, state in states.items():
        assert targets(state) <= set(states), name
    reachable, pending = set(), [definition["StartAt"]]
    while pending:
        name = pending.pop()
        if name not in reachable:
            reachable.add(name)
            pending.extend(targets(states[name]))
    assert reachable == set(states)


def test_only_expected_terminal_states(rendered):
    """The run ends only by success, a failure, or a post-success cleanup failure."""
    states = rendered[0]["States"]
    terminal = {
        name
        for name, state in states.items()
        if state.get("End") or state["Type"] in ("Fail", "Succeed")
    }
    assert terminal == {"Cleanup", "RunFailed", "SetupFailed", "StagingCleanupFailed"}


def test_every_action_catches_every_failure(rendered):
    """Each Action raises on failure and catches all three exception types."""
    for name, state in rendered[0]["States"].items():
        if state["Type"] != "Action":
            continue
        assert state["ExceptionOnActionFailure"] is True, name
        caught = {error for catch in state["Catch"] for error in catch["ErrorEquals"]}
        assert caught == CATCH_ERRORS, name
        for catch in state["Catch"]:
            # error details never overwrite the run's state
            assert catch["ResultPath"].startswith("$."), name


@pytest.mark.parametrize(
    "step", ["CheckDestination", "TransferIn", "Tar", "Purge", "TransferOut"]
)
def test_failures_after_setup_clean_up_staging(rendered, step):
    """Every failure after Setup removes staging before ending as failed."""
    states = rendered[0]["States"]
    handler = states[step]["Catch"][0]["Next"]
    path = failure_paths(states, handler)
    assert "CleanupAfterFailure" in path
    assert path[-1] in FAILURE_ENDS
    # a user-facing message is set before cleanup
    assert states[path[0]]["ResultPath"] == "$.failure"


def test_upload_failure_rolls_back_destination(rendered):
    """A failed upload removes <destination>/<run_name>/ and then staging."""
    states = rendered[0]["States"]
    path = failure_paths(states, states["TransferOut"]["Catch"][0]["Next"])
    assert path.index("RollbackDestination") < path.index("CleanupAfterFailure")
    rollback = states["RollbackDestination"]["Parameters"]
    assert rollback["DATA"] == [{"path.$": "$.run.archive_path"}]
    assert rollback["endpoint.$"] == "$.site.destination_collection_id"
    # even a failed rollback still cleans staging and fails the run
    failed_rollback = failure_paths(
        states, states["RollbackDestination"]["Catch"][0]["Next"]
    )
    assert failed_rollback[-2:] == ["CleanupAfterFailure", "RunFailed"]


def test_existing_archive_stops_before_any_transfer(rendered):
    """An existing <destination>/<run_name> is caught before staging data."""
    states = rendered[0]["States"]
    check = states["CheckDestination"]["Parameters"]
    # filtered to the run name, so other names never enter the run state
    assert check["filter.$"] == "$.run.destination_ls_filter"
    assert check["path.$"] == "$.run.destination_folder"
    choice = states["ArchiveFolderFree"]
    assert choice["Choices"][0]["IsPresent"] is True
    assert choice["Choices"][0]["Next"] == "ArchiveAlreadyExists"
    assert choice["Default"] == "TransferIn"
    path = failure_paths(states, "ArchiveAlreadyExists")
    assert "TransferIn" not in path and "RollbackDestination" not in path


def test_cleanup_deletes_only_the_run_directory(rendered):
    """Both staging cleanups delete the run directory Setup returned."""
    states = rendered[0]["States"]
    for name in ("Cleanup", "CleanupAfterFailure"):
        parameters = states[name]["Parameters"]
        assert parameters["endpoint.$"] == "$.site.staging_collection_id"
        assert parameters["DATA"] == [{"path.$": "$.run.run_dir_collection"}]


def test_transfer_options(rendered):
    """Transfers verify checksums, keep timestamps, fail on quota and have a deadline."""
    states = rendered[0]["States"]
    for name in ("TransferIn", "TransferOut"):
        parameters = states[name]["Parameters"]
        assert parameters["verify_checksum"] is True
        assert parameters["preserve_timestamp"] is True
        assert parameters["fail_on_quota_errors"] is True
        assert parameters["skip_source_errors"] is False
        assert parameters["deadline.$"] == "$.run.transfer_deadline"
    assert states["TransferOut"]["Parameters"]["DATA"][0]["destination_path.$"] == (
        "$.run.archive_path"
    )


def test_site_values_live_in_one_pass_state(rendered, values):
    """IDs appear only in LoadSiteConfig; no staging path is hard-coded."""
    definition = rendered[0]
    site = definition["States"]["LoadSiteConfig"]
    assert site["ResultPath"] == "$.site"
    assert site["Result"]["function_ids"] == values["function_ids"]
    others = {k: v for k, v in definition["States"].items() if k != "LoadSiteConfig"}
    text = json.dumps(others)
    for name in deploy.REQUIRED_IDS:
        assert values[name] not in text
    for function_id in values["function_ids"].values():
        assert function_id not in text
    assert "/staging" not in json.dumps(definition)


def test_wait_times_come_from_values(rendered, values):
    """Each step's WaitTime is the value file's setting."""
    states = rendered[0]["States"]
    assert states["Tar"]["WaitTime"] == values["wait_seconds"]["tar"]
    assert states["TransferIn"]["WaitTime"] == values["wait_seconds"]["transfer"]
    assert states["Cleanup"]["WaitTime"] == values["wait_seconds"]["delete"]


def test_compute_steps_pass_only_expected_arguments(rendered):
    """The flow passes run identity and allowlisted options, nothing else."""
    states = rendered[0]["States"]
    kwargs = {
        name: states[name]["Parameters"]["tasks"][0]["kwargs"]
        for name in ("Setup", "Tar", "Purge")
    }
    assert set(kwargs["Setup"]) == {"run_id.$", "run_name.$", "destination_path.$"}
    assert set(kwargs["Tar"]) == {
        "run_id.$",
        "run_name.$",
        "compression.$",
        "advanced.$",
    }
    assert set(kwargs["Purge"]) == {"run_id.$", "run_name.$"}
    for arguments in kwargs.values():
        assert arguments["run_id.$"] == "$._context.run_id"


# --------------------------------------------------------------------------
# input schema
# --------------------------------------------------------------------------
def test_input_schema_matches_functions(rendered):
    """The start form uses the same run name rule and defaults as the endpoint."""
    schema = rendered[1]
    properties = schema["properties"]
    assert properties["run_name"]["pattern"] == functions.RUN_NAME_PATTERN
    assert properties["compression"]["enum"] == list(functions.COMPRESSION_CHOICES)
    assert properties["compression"]["default"] == functions.DEFAULT_COMPRESSION
    advanced = properties["advanced"]
    assert advanced["title"] == (
        "Advanced: leave as default unless you use archivetar on the command line"
    )
    assert (
        functions.validate_options(
            properties["compression"]["default"], advanced["default"]
        )
        == functions.validate_options()
    )
    assert properties["source"]["format"] == "globus-collection"


def test_every_field_has_a_description(rendered):
    """Each start-form field explains itself in one line."""

    def walk(properties):
        for name, field in properties.items():
            assert field.get("description"), name
            assert "\n" not in field["description"], name
            if field.get("type") == "object" and name != "source":
                walk(field["properties"])

    walk(rendered[1]["properties"])


@pytest.mark.parametrize(
    "size",
    ["100G", "1.5T", "500MiB", "10gb", " 2 T ", "104857600", "lots", "5X", "-1G", ""],
)
def test_form_size_pattern_agrees_with_endpoint(size):
    """A size the form accepts is one the endpoint can parse, and vice versa."""
    form_accepts = re.fullmatch(deploy.JSON_SCHEMA_SIZE_PATTERN, size) is not None
    try:
        functions.parse_size(size, "size")
        endpoint_accepts = True
    except functions.FlowError:
        endpoint_accepts = False
    assert form_accepts == endpoint_accepts


def test_example_input_validates(rendered):
    """A typical start-form submission passes the schema."""
    jsonschema = pytest.importorskip("jsonschema")
    example = {
        "source": {"id": "00000000-0000-0000-0000-0000000000aa", "path": "/~/data/"},
        "destination_path": "/archive/lab/",
        "run_name": "project-2026-11",
        "compression": "gzip",
        "advanced": {"tar_size": "100G", "size": ""},
    }
    jsonschema.validate(example, rendered[1])
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({**example, "run_name": "../escape"}, rendered[1])
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            {**example, "advanced": {"tar_size": "1G", "size": "", "tar_options": "x"}},
            rendered[1],
        )


# --------------------------------------------------------------------------
# values and the command line
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "change,message",
    [
        ({"compute_endpoint_id": "not-a-uuid"}, "compute_endpoint_id must be a UUID"),
        ({"title": ""}, "title is required"),
        ({"function_ids": {"setup": "x"}}, "function_ids.setup must be a UUID"),
        ({"wait_seconds": {"setup": 0}}, "wait_seconds.setup must be a positive"),
    ],
)
def test_values_validation(values, change, message):
    """Bad values fail before rendering."""
    with pytest.raises(deploy.ValuesError, match=message):
        deploy.check_values({**values, **change})


def test_render_command(tmp_path):
    """archivetar-flow render writes both JSON files."""
    example = resources.files("archivetar_flow").joinpath("values.example.toml")
    output_dir = tmp_path / "out"
    assert (
        deploy.main(
            [
                "archivetar-flow",
                "render",
                "--values",
                str(example),
                "--output-dir",
                str(output_dir),
            ]
        )
        == 0
    )
    assert json.loads((output_dir / "definition.json").read_text())["StartAt"] == (
        "LoadSiteConfig"
    )
    assert "properties" in json.loads((output_dir / "input_schema.json").read_text())


def test_render_command_bad_values(tmp_path, capsys):
    """A bad values file exits 2 with a clear message."""
    bad = tmp_path / "values.toml"
    bad.write_text('title = "x"\n')
    assert (
        deploy.main(
            [
                "archivetar-flow",
                "render",
                "--values",
                str(bad),
                "--output-dir",
                str(tmp_path),
            ]
        )
        == 2
    )
    assert "compute_endpoint_id must be a UUID" in capsys.readouterr().err


def test_registered_stubs_call_the_real_functions(monkeypatch):
    """Each registered stub forwards to the matching function."""
    calls = []
    for name in ("setup_run", "tar_run", "purge_run"):
        monkeypatch.setattr(
            functions, name, lambda _name=name, **kwargs: calls.append((_name, kwargs))
        )
    registered.archive_flow_setup(run_id="r", run_name="n", destination_path="/d")
    registered.archive_flow_tar(run_id="r", run_name="n")
    registered.archive_flow_purge(run_id="r", run_name="n")
    assert [name for name, _ in calls] == ["setup_run", "tar_run", "purge_run"]
    assert set(registered.REGISTERED_FUNCTIONS) == set(deploy.FUNCTION_NAMES)
