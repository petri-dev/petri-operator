#!/usr/bin/env bash
set -euo pipefail

helm=${HELM:-helm}
tag=${RELEASE_VERSION:?Set RELEASE_VERSION to vX.Y.Z (or vX.Y.Z-prerelease)}
if [[ ! "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$ ]]; then
  printf 'Invalid release version: %s\n' "$tag" >&2
  exit 1
fi
destination=${1:-dist}
mkdir -p "$destination"
chart="$destination/petri-${tag#v}.tgz"
"$helm" package charts/petri --version "${tag#v}" --app-version "$tag" --destination "$destination"
"$helm" lint "$chart"
"$helm" template petri "$chart" --namespace petri-system \
  --set namespace.create=true > "$destination/install.yaml"
"$helm" template petri "$chart" --namespace petri-system \
  --show-only templates/crds.yaml > "$destination/crds.yaml"
