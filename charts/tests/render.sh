#!/usr/bin/env bash
set -euo pipefail

helm=${HELM:-helm}
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
