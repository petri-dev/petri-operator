#!/usr/bin/env bash
set -euo pipefail

helm=${HELM:-helm}
for namespace in petri-system custom; do
  rendered=$("$helm" template petri charts/petri --namespace "$namespace" \
    --set namespace.create=true --show-only templates/namespace.yaml)
  test "$(grep -c "^  name: ${namespace}$" <<< "$rendered")" -eq 1
done

temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT
HELM="$helm" RELEASE_VERSION=v0.0.0-test bash hack/release-artifacts.sh "$temporary"
metadata=$("$helm" show chart "$temporary/petri-0.0.0-test.tgz")
grep -qx 'version: 0.0.0-test' <<< "$metadata"
grep -qx 'appVersion: v0.0.0-test' <<< "$metadata"
grep -qx '          image: ghcr.io/petri-dev/petri-operator:v0.0.0-test' "$temporary/install.yaml"
grep -qx '              value: ghcr.io/petri-dev/petri-deployer:v0.0.0-test' "$temporary/install.yaml"
test "$(grep -c '^kind: CustomResourceDefinition$' "$temporary/install.yaml")" -eq 4
test "$(grep -c '^kind: CustomResourceDefinition$' "$temporary/crds.yaml")" -eq 4
grep -qx 'kind: Namespace' "$temporary/install.yaml"
if grep -Eq ' (image|value): .*:latest$' "$temporary/install.yaml"; then
  exit 1
fi
if RELEASE_VERSION=latest bash hack/release-artifacts.sh "$temporary"; then
  exit 1
fi

# Exercise the shared Makefile image path without regenerating source files.
for image in localhost:5001/petri:test localhost:5001/petri "localhost:5001/petri@sha256:$(printf '%064d' 0)"; do
  make -s build-installer -o manifests -o generate INSTALLER_DIR="$temporary" HELM="$helm" IMG="$image" DEPLOYER_IMG="$image"
  grep -Fxq "          image: $image" "$temporary/install.yaml"
  grep -Fxq "              value: $image" "$temporary/install.yaml"
done
grep -q ':v0.0.0-dev' <<< "$("$helm" template petri charts/petri)"
without_namespace=$("$helm" template petri charts/petri)
if grep -q '^kind: Namespace$' <<< "$without_namespace"; then
  exit 1
fi

crds=$("$helm" template petri charts/petri --show-only templates/crds.yaml)
test "$(grep -c '^kind: CustomResourceDefinition$' <<< "$crds")" -eq 4
test "$(grep -c 'helm.sh/resource-policy: keep' <<< "$crds")" -eq 4
for resource in environmenttemplates ephemeralenvironments sharedcomponentproviders sharedcomponents; do
  test "$(grep -c "^  name: ${resource}.core.petri.run$" <<< "$crds")" -eq 1
done

without_crds=$("$helm" template petri charts/petri --set crds.enabled=false)
if grep -q '^kind: CustomResourceDefinition$' <<< "$without_crds"; then
  exit 1
fi

for release in petri other; do
  rbac=$("$helm" template "$release" charts/petri \
    --show-only templates/rbac/role.yaml \
    --show-only templates/rbac/role_binding.yaml)
  test "$(grep -c "^  name: ${release}-manager-role$" <<< "$rbac")" -eq 2
  test "$(grep -c '^kind: ClusterRole$' <<< "$rbac")" -eq 1
done
