"""The functions registered with Globus Compute for the archivetar flow.

Each is a thin stub that imports the real function on the endpoint. That
way the code that runs is always the archivetar_flow release installed on
the endpoint, whichever serialization strategy Globus Compute uses: by
reference (the default, ``DillCode``) or by source. Nothing is imported at
module level, so registering these needs nothing from the endpoint.
"""


def archive_flow_setup(**kwargs):
    """Run :func:`archivetar_flow.functions.setup_run` on the endpoint.

    Args:
        **kwargs: Passed to ``setup_run``.

    Returns:
        The result of ``setup_run``.
    """
    from archivetar_flow import functions

    return functions.setup_run(**kwargs)


def archive_flow_tar(**kwargs):
    """Run :func:`archivetar_flow.functions.tar_run` on the endpoint.

    Args:
        **kwargs: Passed to ``tar_run``.

    Returns:
        The result of ``tar_run``.
    """
    from archivetar_flow import functions

    return functions.tar_run(**kwargs)


def archive_flow_purge(**kwargs):
    """Run :func:`archivetar_flow.functions.purge_run` on the endpoint.

    Args:
        **kwargs: Passed to ``purge_run``.

    Returns:
        The result of ``purge_run``.
    """
    from archivetar_flow import functions

    return functions.purge_run(**kwargs)


#: Name used in the values file's ``[function_ids]`` table, mapped to the
#: function to register.
REGISTERED_FUNCTIONS = {
    "setup": archive_flow_setup,
    "tar": archive_flow_tar,
    "purge": archive_flow_purge,
}
