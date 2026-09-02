# Petri

> [!WARNING]
> Petri is under active development. APIs and behavior may change between releases.

Petri is a Kubernetes operator for creating isolated, short-lived environments. An `EnvironmentTemplate` defines Helm components and their dependencies. An `EphemeralEnvironment` creates those components in a dedicated namespace and removes them on manual deletion or when its TTL expires.

Petri also supports shared components backed by pluggable providers, allowing environments to reuse infrastructure such as databases and caches.

## Install

Petri is tested on Kubernetes 1.36. Try out the latest release:

```sh
kubectl apply -f https://github.com/petri-dev/petri-operator/releases/latest/download/install.yaml
```

The installer uses matching versioned images from `ghcr.io/petri-dev/petri-operator` and `ghcr.io/petri-dev/petri-deployer`. Select a specific release URL instead of `latest` to pin installation manifests.

Apply the runnable sample:

```sh
kubectl apply -k config/samples
kubectl get ephemeralenvironments -n default
```

Delete the sample before uninstalling Petri:

```sh
kubectl delete -k config/samples
kubectl delete -f https://github.com/petri-dev/petri-operator/releases/latest/download/install.yaml
```

## Development

Development requires Go 1.26, Docker and Kind:

```sh
make test
make lint
make test-e2e
```

Run `make help` for all development targets.

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
