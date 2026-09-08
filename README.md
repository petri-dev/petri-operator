# Petri

> [!WARNING]
> Petri is under active development. APIs and behavior may change between releases.

Petri is a Kubernetes operator for creating isolated, short-lived environments. An `EnvironmentTemplate` defines Helm components and their dependencies. An `EphemeralEnvironment` creates those components in a dedicated namespace and removes them on manual deletion or when its TTL expires.

Petri also supports shared components backed by pluggable providers, allowing environments to reuse infrastructure such as databases and caches.

## Install

Petri is tested on Kubernetes 1.36. **The published `v0.1.7` release predates
this chart's configuration contract. Do not pair it (or `latest` images) with
the chart from this checkout.** A new matched release must be published and
its GHCR packages made anonymously readable before the commands below work.
The source chart uses the deliberately unpublished `v0.0.0-dev` image tag;
local development must supply images built from the same checkout.

### Fresh Installation

Choose a verified release newer than `v0.1.7` from the
[release page](https://github.com/petri-dev/petri-operator/releases). Use the
same version for the chart, operator, deployer, and manifests. The placeholder
below is intentional, not an already available release:

```sh
export PETRI_VERSION=vX.Y.Z
helm install petri oci://ghcr.io/petri-dev/charts/petri \
  --version "${PETRI_VERSION#v}" --namespace petri-system --create-namespace
```

Alternatively, for a fresh installation without Helm:

```sh
kubectl apply -f "https://github.com/petri-dev/petri-operator/releases/download/${PETRI_VERSION}/install.yaml"
```

Each release also attaches `petri-X.Y.Z.tgz` as an alternative to OCI chart
delivery. This does not bypass image permissions: both GHCR images must still
be pullable by the cluster. Do not override the packaged image versions with
older images. For digest pinning, `operator.image.reference` and
`deployer.image.reference` accept full `repository@sha256:...` references from
that same release (including registries with ports).

### Upgrades

For an existing Helm installation already compatible with this chart, select
the new `PETRI_VERSION` and run:

```sh
kubectl apply -f "https://github.com/petri-dev/petri-operator/releases/download/${PETRI_VERSION}/crds.yaml"
helm upgrade petri oci://ghcr.io/petri-dev/charts/petri \
  --version "${PETRI_VERSION#v}" --namespace petri-system -f your-values.yaml
```

Review `your-values.yaml` against the new chart; remove stale image overrides
and do not use `--reuse-values`. Keep `crds.enabled=false` if CRDs are managed
outside Helm. For an existing standalone installation, apply the pinned new
`install.yaml` without switching to Helm.

The CRDs carry the `helm.sh/resource-policy: keep` annotation, so `helm uninstall` leaves them (and therefore your `EphemeralEnvironment`s) in place. That guarantee is Helm-only: `kubectl delete -f install.yaml` will delete the CRDs and cascade-delete every `EphemeralEnvironment` with them, so to remove the operator installed from the standalone manifest, delete the individual resources rather than the whole file, or keep the CRDs out of the delete.

Apply the runnable sample:

```sh
kubectl apply -k config/samples
kubectl get ephemeralenvironments -n default
```

Delete the sample before uninstalling Petri:

```sh
kubectl delete -k config/samples
helm uninstall petri --namespace petri-system
```

## Workload Namespaces

The operator persists `status.targetNamespace` before creating it. Names use
`petri-env-<first 8 hex characters of SHA256(metadata.uid)>`; collisions extend the
prefix to 12, 16, and so on through 52 hex characters (62 characters total).
Namespaces require both `petri.run/managed=true` and
`petri.run/environment-uid=<full environment UID>`. Managed-only or foreign namespaces
are never adopted or deleted. The `NamespaceBound=True` status condition is persisted
before workload deployment and locks the assignment across restarts and spec changes.

Read the actual name rather than constructing it from the environment name:

```sh
kubectl get ephemeralenvironment <name> -n <management-namespace> -o jsonpath='{.status.targetNamespace}'
```

Deletion skips all cleanup for an absent or foreign persisted candidate; shared
allocations may then need manual cleanup. UID-based workload placement does not add
multi-tenancy: provider identities and the `petri-shared` runtime remain unchanged.

## Development

Development requires Go 1.26, Docker and Kind:

```sh
make test
make lint
make test-e2e
```

Run `make help` for all development targets.

### Release Delivery

Local checks do not publish anything or launch a cluster:

```sh
make helm-lint
make release-artifacts RELEASE_VERSION=v0.0.0-test
goreleaser check
```

A new `vX.Y.Z` tag runs tests and packages one chart with `version: X.Y.Z` and
`appVersion: vX.Y.Z`. Both images use that tag, and `install.yaml` and `crds.yaml`
are rendered from that exact archive. GoReleaser attaches all three artifacts
to a draft release. The workflow explicitly logs Helm into GHCR, pushes the
same chart archive, then uses an empty credential store to pull and compare
the chart and pull both images for Linux amd64 and arm64. Only a successful
anonymous smoke check publishes the draft. It never updates `latest` images.

Maintainers must grant this repository Actions write access to all three
GHCR packages (`petri-operator`, `petri-deployer`, and `charts/petri`) and make
them public. A public GitHub repository does not imply public packages;
`packages: write` authorizes publishing, not anonymous reading. For a chart
`403` or image `401`, verify that the package/version was published and have a
package administrator check visibility and access grants. The public release
workflow deliberately does not accept private
pulls as a passing smoke check.

Never reuse a release version, including a failed draft's version. Protect
release tags against updates/deletion and enable GitHub immutable releases
in repository settings; GHCR tags themselves are mutable, so enforce no
overwrite administratively or pin the image digests for stronger guarantees.
The workflow refuses existing releases. After fixing permissions on a failed
draft, maintainers can run `RELEASE_VERSION=vX.Y.Z bash hack/release-smoke.sh dist`
against that draft's downloaded artifacts, then explicitly approve publishing
it, or issue a new version. No automation here changes package visibility.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Report vulnerabilities according to [SECURITY.md](SECURITY.md).

## License

Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
