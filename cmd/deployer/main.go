package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/nuromirg/petri/internal/deployer"
)

func main() {
	operation := os.Getenv(deployer.EnvOp)
	specJSON := os.Getenv(deployer.EnvSpec)

	opts, err := parseSpec(specJSON)
	if err != nil {
		fail(err)
	}

	switch operation {
	case deployer.OpDeploy:
		err = deployer.Install(context.Background(), opts)
	case deployer.OpUndeploy:
		err = deployer.Uninstall(context.Background(), opts)
	default:
		err = fmt.Errorf("unknown operation: %q", operation)
	}

	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	_ = os.WriteFile("/dev/termination-log", []byte(err.Error()), 0644)
	os.Exit(1)
}

func parseSpec(specJSON string) (deployer.DeployOptions, error) {
	dec := json.NewDecoder(strings.NewReader(specJSON))
	dec.DisallowUnknownFields()

	var opts deployer.DeployOptions
	if err := dec.Decode(&opts); err != nil {
		return deployer.DeployOptions{}, err
	}

	return opts, nil
}
