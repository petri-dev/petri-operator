#!/usr/bin/env bash
set -euo pipefail

helm=${HELM:-helm}
tag=${RELEASE_VERSION:?Set RELEASE_VERSION to the published version tag}
artifacts=${1:-dist}
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT

export DOCKER_CONFIG="$temporary"
export HELM_REGISTRY_CONFIG="$temporary/helm-registry.json"
"$helm" pull oci://ghcr.io/petri-dev/charts/petri --version "${tag#v}" --destination "$temporary"
cmp "$artifacts/petri-${tag#v}.tgz" "$temporary/petri-${tag#v}.tgz"
"$helm" template petri "$temporary/petri-${tag#v}.tgz" --namespace petri-system \
  --set namespace.create=true > "$temporary/install.yaml"
cmp "$artifacts/install.yaml" "$temporary/install.yaml"
for image in petri-operator petri-deployer; do
  for arch in amd64 arm64; do
    docker pull --platform "linux/$arch" "ghcr.io/petri-dev/$image:$tag"
  done
done
