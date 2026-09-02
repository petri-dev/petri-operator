# Contributing

Open an issue before starting a large behavioral or API change. Small fixes can go directly to a pull request.

Before submitting a pull request, run:

```sh
go test ./...
go vet ./...
make lint
```

Changes to generated APIs or RBAC must also run `make manifests generate` and include the generated output. E2E changes should run `make test-e2e` in an isolated Kind cluster.
