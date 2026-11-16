# archivetar Globus Flow (optional)

`archivetar_flow` lets people without a shell (for example Windows users
on Globus Connect Personal) archive a folder with archivetar from the Globus
web app. It is an optional wrapper. archivetar works without it and never
imports it.

The user picks a folder, a destination folder, a run name and a few
options. The flow then:

1. checks that `<destination folder>/<run_name>/` does not exist yet
2. copies the folder into a private staging area
3. runs standard archivetar on it
4. deletes what was tarred, using archivepurge
5. uploads the tars, indexes and anything left over to
   `<destination folder>/<run_name>/`
6. deletes the staging copy

**Any failure rolls the run back:** staging is removed, and so is a partial
upload. The run ends as Failed with a plain-language reason. The user's
original folder is never changed.

## How it works

The design is deliberately opinionated, so users who aren't archivetar
experts can't lose data or be misled:

- **Every archive gets a new folder.** It is named after the run name and
  goes inside an existing destination folder. If that folder already
  exists, the run stops before copying anything. To archive again, use a
  new run name.
- **The run name is used everywhere:** the destination folder, the tar
  prefix (`<run_name>-1.tar`, …) and the staging work folder.
- **Every step runs as the user**, through identity mapping. No service
  account touches user data.
- **Compute functions only touch staging.** Anything that crosses
  collections is a Globus operation: transfer in, transfer out, `ls` to
  check the destination, and delete to clean up. So the destination can be
  any collection, on any machine.
- **No file names leave the site.** Function results and errors hold only
  run paths, counts, sizes and status. Flow run state is stored in Globus's
  cloud, and file names can be sensitive. The destination check lists only
  the run name. archivetar's output goes to a log inside the run directory,
  which is deleted with staging.

```
LoadSiteConfig -> Setup -> CheckDestination -> TransferIn -> Tar -> Purge -> TransferOut -> Cleanup
                     \__________ any failure: (rollback upload) -> clean up staging -> Failed __________/
```

Staging layout:

```
<staging_root>/                    root-owned, mode 1733
<staging_root>/<run_id>/           created by the user, mode 0700
    <run_name>/                    archivetar working directory
    tmp/                           TMPDIR for archivetar's scan lists
    logs/archive-flow.log          tool output and error details
    .tar_complete, .purge_complete markers that make steps safe to repeat
```

## What a site must provide

| Piece | Requirements |
| --- | --- |
| Staging collection | A mapped Globus Connect Server collection whose path includes `staging_root`. The staging root is root-owned and mode `1733`: users can create run directories but can't list or remove other people's. Staging needs about twice the input size per run until Purge. Per-user quotas are recommended. |
| Destination collection | Any collection the users can write to, for example the tape-backed archive. |
| Compute endpoint | A **multi-user** Globus Compute endpoint using the same identity mapping as the collections, so each function runs as the user who started the flow. It needs Python 3.11 or newer, and its function allowlist should list only this flow's three functions. |
| archivetar_flow | Installed into the Python environment the endpoint's workers use, from a pinned archivetar release: `pip install --no-deps archivetar==<version>`. The endpoint functions need nothing beyond the standard library. |
| Apptainer + image | Apptainer (or Singularity) on the endpoint nodes, and the archivetar image pulled to a fixed path. Pin it by digest: `apptainer pull archivetar.sif docker://ghcr.io/brockpalen/archivetar@sha256:<digest>`. |
| Site config | `/etc/archive-flow/site.toml` on every endpoint node. Start from [`site.example.toml`](../archivetar_flow/site.example.toml). |

The functions read their settings only from the site config. Values passed
as flow inputs are ignored, so a user can't point the functions at another
staging root or image.

## Deploying

On an admin machine:

```sh
pip install 'archivetar[globus-flow]'

# 1. Register the three Compute functions. Paste the printed IDs into
#    your values file, and add them to the endpoint's function allowlist.
archivetar-flow register-functions

# 2. Fill in your values file, starting from values.example.toml.
cp "$(python -c 'import archivetar_flow, os; print(os.path.dirname(archivetar_flow.__file__))')/values.example.toml" values.toml

# 3. Optional: render the flow and review it.
archivetar-flow render --values values.toml --output-dir rendered/

# 4. Create the flow. To update it later, add --flow-id <id>.
archivetar-flow deploy --values values.toml
```

Then share the flow with its users (set the flow's starters in the Globus
web app).

The values file holds the Compute endpoint ID, the staging and destination
collection IDs, the function IDs and each step's `WaitTime`. They are
rendered into one `LoadSiteConfig` Pass state at the start of the flow, and
every other state reads them from `$.site`. Size the `WaitTime` values for
your largest archives: when one runs out, the step fails and the run rolls
back.

The registered functions are thin stubs that import `archivetar_flow` on
the endpoint. Upgrading the code on the endpoint therefore needs no
re-registration, as long as the function names stay the same.

## Testing with a single-user endpoint

A single-user Compute endpoint runs every task as its owner. That's fine
for trying the flow with your own identity, but don't let other people run
the flow against it. Their data would be handled as you.

1. Install archivetar into the endpoint's virtualenv:
   `pip install --no-deps archivetar==<version>`.
2. Write a site config and point the endpoint at it by adding
   `ARCHIVE_FLOW_SITE_CONFIG=/path/to/site.toml` to the endpoint's
   environment. `archivetar_runtime = "host"` runs `archivetar` and
   `archivepurge` from `PATH`. `container` uses the image.
3. Register the functions, deploy the flow and run it as above.

## Things to confirm on a test deployment

The Globus documentation doesn't cover these, so check them before
production:

- **Missing destination folder:** the `ls` action fails when the
  destination folder is missing (CheckDestination relies on this), and the
  `name:=` filter returns only the run name.
- **`ignore_missing`:** the delete action accepts it (Cleanup and Rollback
  use it).
- **Error text:** what a failed Compute task puts in the flow's event log.
  The functions raise only sanitized messages either way.
- **User cancel:** what a cancel does (see below).

## Known limitations

- **One source folder per run.** The folder's contents are archived. Pick
  a folder, not a single file.
- **Empty directories aren't archived.** archivetar tars files, and
  archivepurge removes the empty directories left behind. This is
  archivetar's normal behaviour.
- **A user cancel probably skips the rollback.** Nothing documents a Catch
  running on cancel, so a cancelled run may leave its staging directory
  and, if cancelled mid-upload, a partial `<destination>/<run_name>/`.
- **No size check before copying.** The flow can't learn a remote folder's
  size up front. `max_input_bytes` is checked after staging, and the run is
  rolled back if the input is too big.
- **Logs go with staging.** On failure, the event log shows which step
  failed and a sanitized reason. archivetar's full output is deleted along
  with staging.

## Tests

```sh
pipenv run pytest test/test_globus_flow_functions.py test/test_globus_flow_templates.py
```

The function tests use stand-ins for archivetar and archivepurge. To also
run setup, tar and purge against the real container:

```sh
ARCHIVE_FLOW_TEST_SIF=/path/to/archivetar.sif \
ARCHIVE_FLOW_TEST_APPTAINER=apptainer \
pipenv run pytest test/test_globus_flow_functions.py -k end_to_end
```
