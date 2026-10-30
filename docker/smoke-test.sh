#!/bin/bash
# Smoke test an archivetar container image; used locally and by CI.
#
#   docker/smoke-test.sh [image]     (default archivetar:test)
#
# Set DOCKER="sudo docker" if your user is not in the docker group.

set -euo pipefail

image=${1:-archivetar:test}
docker=${DOCKER:-docker}

echo "== tools start"
for tool in archivetar unarchivetar archivepurge archivescan; do
    $docker run --rm "$image" "$tool" --help > /dev/null
    echo "ok $tool"
done

echo "== mpiFileUtils shared libraries resolve in the runtime stage"
$docker run --rm "$image" bash -c '! ldd /archivetar/install/bin/* | grep "not found"'

echo "== mpirun works as the default (root) docker user"
$docker run --rm "$image" mpirun -np 2 dwalk /usr/share/doc | tail -3

echo "== archive / unarchive round trip as a non-root user, like Apptainer"
workdir=$(mktemp -d)
trap 'rm -rf "$workdir"' EXIT
mkdir -p "$workdir/src"
cp -r "$(dirname "$0")" "$workdir/src/sample"
chmod -R a+rwX "$workdir"
$docker run --rm --user "$(id -u):$(id -g)" -v "$workdir:/data" -w /data/src \
    "$image" archivetar --prefix smoke --size 1M --tar-size 1M --remove-files
ls "$workdir"/src/smoke-*.tar
test ! -e "$workdir/src/sample/Dockerfile"
$docker run --rm --user "$(id -u):$(id -g)" -v "$workdir:/data" -w /data/src \
    "$image" unarchivetar --prefix smoke
test -f "$workdir/src/sample/Dockerfile"

echo "== all smoke tests passed"
