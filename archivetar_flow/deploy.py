"""Register the functions, render the templates and deploy the flow.

Needs the ``archivetar[globus-flow]`` extra (globus-sdk,
globus-compute-sdk, jinja2). Run it from an admin machine, not the
endpoint::

    archivetar-flow register-functions
    archivetar-flow render --values values.toml --output-dir rendered/
    archivetar-flow deploy --values values.toml [--flow-id FLOW_ID]

``render`` writes the flow definition and input schema for review, or for
deploying with other tools. ``deploy`` creates the flow, or updates it when
``--flow-id`` is given.
"""

import argparse
import json
import sys
import uuid
from importlib import resources
from pathlib import Path

from archivetar_flow import functions

#: archivetar's native Globus app, shared with archivetar's own transfers.
GLOBUS_CLIENT_ID = "8359fb34-39cf-410d-bd93-e8502aa68c46"

#: The start form's size pattern. JSON Schema uses ECMAScript regexes,
#: which have no case-insensitive flag, so the cases are spelled out. It
#: accepts the same sizes as :data:`functions.SIZE_PATTERN`.
JSON_SCHEMA_SIZE_PATTERN = r"^\s*\d+(\.\d+)?\s*[KMGTPkmgtp]?([iI]?[bB])?\s*$"

#: Settings the values file must have, by table.
REQUIRED_IDS = (
    "compute_endpoint_id",
    "staging_collection_id",
    "destination_collection_id",
)
REQUIRED_STRINGS = (
    "title",
    "subtitle",
    "destination_collection_name",
    "default_destination_path",
)
FUNCTION_NAMES = ("setup", "tar", "purge")
WAIT_NAMES = ("setup", "ls", "transfer", "tar", "purge", "delete")


class ValuesError(Exception):
    """The values file is missing a setting or has a bad one."""


def _check_uuid(value, name):
    """Check that a setting is a UUID string.

    Args:
        value: The setting's value.
        name: Its name, for the error message.

    Raises:
        ValuesError: If it is not a UUID.
    """
    try:
        uuid.UUID(value)
    except (TypeError, ValueError, AttributeError):
        raise ValuesError(f"{name} must be a UUID") from None


def load_values(path):
    """Read and check a values file.

    Args:
        path: The values TOML file.

    Returns:
        The values as a dict.

    Raises:
        ValuesError: If a setting is missing or invalid.
    """
    import tomllib  # Python 3.11+, like the rest of archivetar_flow

    with Path(path).open("rb") as values_file:
        values = tomllib.load(values_file)
    return check_values(values)


def check_values(values):
    """Check the settings in a parsed values file.

    Args:
        values: The parsed values.

    Returns:
        The same values.

    Raises:
        ValuesError: If a setting is missing or invalid.
    """
    for name in REQUIRED_IDS:
        _check_uuid(values.get(name), name)
    for name in REQUIRED_STRINGS:
        if not isinstance(values.get(name), str) or not values[name]:
            raise ValuesError(f"{name} is required")
    function_ids = values.get("function_ids", {})
    for name in FUNCTION_NAMES:
        _check_uuid(function_ids.get(name), f"function_ids.{name}")
    wait_seconds = values.get("wait_seconds", {})
    for name in WAIT_NAMES:
        seconds = wait_seconds.get(name)
        if not isinstance(seconds, int) or isinstance(seconds, bool) or seconds < 1:
            raise ValuesError(f"wait_seconds.{name} must be a positive integer")
    return values


def template_context(values):
    """Combine the values with the constants the templates need.

    The run name pattern and option defaults come from
    :mod:`archivetar_flow.functions`, so the start form and the endpoint
    checks can't drift apart.

    Args:
        values: Checked values.

    Returns:
        The Jinja context.
    """
    from importlib.metadata import PackageNotFoundError, version

    try:
        flow_version = version("archivetar")
    except PackageNotFoundError:
        flow_version = "unknown"
    return {
        **values,
        "flow_version": flow_version,
        "run_name_pattern": functions.RUN_NAME_PATTERN,
        "size_pattern": JSON_SCHEMA_SIZE_PATTERN,
        "compression_choices": list(functions.COMPRESSION_CHOICES),
        "default_compression": functions.DEFAULT_COMPRESSION,
        "default_tar_size": functions.DEFAULT_TAR_SIZE,
    }


def render(values):
    """Render the flow definition and input schema.

    Args:
        values: Checked values.

    Returns:
        Tuple of (definition dict, input schema dict).
    """
    import jinja2

    environment = jinja2.Environment(
        loader=jinja2.FunctionLoader(_load_template),
        undefined=jinja2.StrictUndefined,
        # values are inserted with |tojson; HTML escaping would corrupt JSON
        autoescape=jinja2.select_autoescape(
            enabled_extensions=("html", "xml"), default_for_string=False
        ),
        keep_trailing_newline=True,
    )
    context = template_context(values)
    definition = json.loads(
        environment.get_template("definition.json.j2").render(context)
    )
    input_schema = json.loads(
        environment.get_template("input_schema.json.j2").render(context)
    )
    return definition, input_schema


def _load_template(name):
    """Load a template packaged with archivetar_flow.

    Args:
        name: Template file name.

    Returns:
        The template source.
    """
    return resources.files("archivetar_flow").joinpath("templates", name).read_text()


def register_functions():
    """Register the stub functions with Globus Compute.

    Returns:
        Dict of values-file name (``setup``, ``tar``, ``purge``) to function
        ID.
    """
    from globus_compute_sdk import Client

    from archivetar_flow.registered import REGISTERED_FUNCTIONS

    client = Client()
    return {
        name: client.register_function(function, description=f"archivetar flow: {name}")
        for name, function in REGISTERED_FUNCTIONS.items()
    }


def deploy(values, flow_id=None):
    """Create the flow, or update an existing one.

    Args:
        values: Checked values.
        flow_id: Existing flow to update; None to create a new flow.

    Returns:
        The flow ID.
    """
    import globus_sdk

    definition, input_schema = render(values)
    app = globus_sdk.UserApp("archivetar-flow", client_id=GLOBUS_CLIENT_ID)
    flows_client = globus_sdk.FlowsClient(app=app)
    fields = {
        "title": values["title"],
        "subtitle": values["subtitle"],
        "definition": definition,
        "input_schema": input_schema,
    }
    if flow_id:
        flows_client.update_flow(flow_id, **fields)
        return flow_id
    return flows_client.create_flow(**fields)["id"]


def parse_args(argv):
    """Parse the command line.

    Args:
        argv: Arguments, without the program name.

    Returns:
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        prog="archivetar-flow",
        description="Register, render and deploy the archivetar Globus Flow.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "register-functions",
        help="register the Compute functions and print their IDs as TOML",
    )
    render_parser = commands.add_parser(
        "render", help="write definition.json and input_schema.json"
    )
    render_parser.add_argument("--values", required=True, help="values TOML file")
    render_parser.add_argument(
        "--output-dir", required=True, help="directory to write the JSON files to"
    )
    deploy_parser = commands.add_parser("deploy", help="create or update the flow")
    deploy_parser.add_argument("--values", required=True, help="values TOML file")
    deploy_parser.add_argument(
        "--flow-id", help="update this flow instead of creating one"
    )
    return parser.parse_args(argv)


def main(argv):
    """Run the archivetar-flow command.

    Args:
        argv: ``sys.argv``.

    Returns:
        Exit status.
    """
    args = parse_args(argv[1:])
    try:
        if args.command == "register-functions":
            function_ids = register_functions()
            print("[function_ids]")
            for name, function_id in function_ids.items():
                print(f'{name} = "{function_id}"')
        elif args.command == "render":
            definition, input_schema = render(load_values(args.values))
            output_dir = Path(args.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            for name, document in (
                ("definition.json", definition),
                ("input_schema.json", input_schema),
            ):
                (output_dir / name).write_text(json.dumps(document, indent=2) + "\n")
                print(f"wrote {output_dir / name}")
        else:
            flow_id = deploy(load_values(args.values), args.flow_id)
            print(f"flow id: {flow_id}")
    except ValuesError as error:
        print(f"archivetar-flow: {error}", file=sys.stderr)
        return 2
    except ModuleNotFoundError as error:
        print(
            f"archivetar-flow: {error.name} is missing; "
            "install with: pip install 'archivetar[globus-flow]'",
            file=sys.stderr,
        )
        return 2
    return 0
