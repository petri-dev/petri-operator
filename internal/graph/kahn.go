package graph

import (
	"errors"

	"github.com/nuromirg/petri/api/v1alpha1"
)

var ErrCycleDependencies = errors.New("cycle dependencies found")

// BuildLevels splits components into deployment levels using Kahn's algorithm.
// Components in the same level have no dependencies on each other and can be deployed in parallel.
// Returns an error if any dependsOn name is unknown, any component name is duplicated, or a cycle exists.
func BuildLevels(components []v1alpha1.ComponentSpec) ([][]v1alpha1.ComponentSpec, error) {
	inDegree := make(map[string]int)
	var errs error

	for _, c := range components {
		if _, found := inDegree[c.Name]; found {
			errs = errors.Join(errs, errors.New("duplicated component: "+c.Name))
			continue
		}
		inDegree[c.Name] = len(c.DependsOn)
	}
	if errs != nil {
		return nil, errs
	}

	adjacency := make(map[string][]string)
	queue := make([]string, 0)

	for _, c := range components {
		for _, dep := range c.DependsOn {
			if _, found := inDegree[dep]; !found {
				errs = errors.Join(errs, errors.New("unknown dependency "+dep+" in component "+c.Name))
			} else {
				adjacency[dep] = append(adjacency[dep], c.Name)
			}
		}
		if len(c.DependsOn) == 0 {
			queue = append(queue, c.Name)
		}
	}
	if errs != nil {
		return nil, errs
	}

	byName := make(map[string]v1alpha1.ComponentSpec, len(components))
	for _, c := range components {
		byName[c.Name] = c
	}

	levels := make([][]v1alpha1.ComponentSpec, 0)

	for len(queue) > 0 {
		levelSize := len(queue)
		level := make([]v1alpha1.ComponentSpec, 0)

		for _, name := range queue[:levelSize] {
			level = append(level, byName[name])
			for _, dep := range adjacency[name] {
				inDegree[dep]--
				if inDegree[dep] == 0 {
					queue = append(queue, dep)
				}
			}
		}

		queue = queue[levelSize:]
		levels = append(levels, level)
	}

	visited := 0
	for _, level := range levels {
		visited += len(level)
	}

	if visited < len(components) {
		return nil, ErrCycleDependencies
	}

	return levels, nil
}
