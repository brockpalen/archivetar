"""Globus Flow wrapper for archiving with archivetar, without a shell.

An optional add-on: archivetar works on its own and never imports this
package. It holds the pieces a site deploys so users can archive from the
Globus web app:

* :mod:`archivetar_flow.functions`: the Globus Compute functions that
  run on the endpoint (standard library only, Python 3.11+).
* ``templates/``: the Jinja templates for the flow definition and the start
  form's input schema.
* :mod:`archivetar_flow.deploy`: registers the functions, renders the
  templates and creates or updates the flow. It needs the
  ``archivetar[globus-flow]`` extra.

See ``docs/globus-flow.md`` for what a site must provide.
"""
