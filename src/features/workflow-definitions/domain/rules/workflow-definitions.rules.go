package rules

import (
	"fmt"
	"strings"

	contracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	"distributed-transaction-engine/src/shared/errors"
)

type ValidationResult struct {
	IsValid          bool
	TopologicalOrder []string
	Error            error
}

func ValidateDAGTopology(steps []contracts.StepDefinition) ValidationResult {
	stepMap := make(map[string]contracts.StepDefinition, len(steps))
	inDegree := make(map[string]int, len(steps))
	adjList := make(map[string][]string, len(steps))
	undirectedAdj := make(map[string][]string, len(steps))

	for _, step := range steps {
		stepMap[step.StepID] = step
		inDegree[step.StepID] = 0
		adjList[step.StepID] = make([]string, 0)
		undirectedAdj[step.StepID] = make([]string, 0)
	}

	for _, step := range steps {
		for _, depID := range step.Dependencies {
			if _, exists := stepMap[depID]; !exists {
				return ValidationResult{
					IsValid: false,
					Error:   errors.NewValidationError(fmt.Sprintf("step %s depends on non-existent step %s", step.StepID, depID)),
				}
			}
			adjList[depID] = append(adjList[depID], step.StepID)
			undirectedAdj[depID] = append(undirectedAdj[depID], step.StepID)
			undirectedAdj[step.StepID] = append(undirectedAdj[step.StepID], depID)
			inDegree[step.StepID]++
		}
	}

	if len(steps) > 1 {
		componentCount := countWeaklyConnectedComponents(steps, undirectedAdj)
		if componentCount > 1 {
			return ValidationResult{
				IsValid: false,
				Error:   errors.NewConflictError("workflow graph contains disconnected components (must be a single weakly connected DAG)"),
			}
		}
	}

	queue := make([]string, 0)
	for stepID, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, stepID)
		}
	}

	var topologicalOrder []string
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		topologicalOrder = append(topologicalOrder, curr)

		for _, neighbor := range adjList[curr] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if len(topologicalOrder) != len(steps) {
		cyclePath := extractCyclePath(steps, adjList, inDegree)
		return ValidationResult{
			IsValid: false,
			Error:   errors.NewConflictError(fmt.Sprintf("cyclic dependency detected in workflow DAG definition: %s", cyclePath)),
		}
	}

	if err := validatePivotPlacement(stepMap, topologicalOrder); err != nil {
		return ValidationResult{
			IsValid: false,
			Error:   err,
		}
	}

	return ValidationResult{
		IsValid:          true,
		TopologicalOrder: topologicalOrder,
		Error:            nil,
	}
}

func extractCyclePath(steps []contracts.StepDefinition, adjList map[string][]string, inDegree map[string]int) string {
	cycleCandidates := make(map[string]bool)
	for stepID, deg := range inDegree {
		if deg > 0 {
			cycleCandidates[stepID] = true
		}
	}

	visited := make(map[string]int)
	parent := make(map[string]string)
	var cycleStart, cycleEnd string

	var dfs func(u string) bool
	dfs = func(u string) bool {
		visited[u] = 1
		for _, v := range adjList[u] {
			if !cycleCandidates[v] {
				continue
			}
			if visited[v] == 1 {
				cycleStart = v
				cycleEnd = u
				return true
			}
			if visited[v] == 0 {
				parent[v] = u
				if dfs(v) {
					return true
				}
			}
		}
		visited[u] = 2
		return false
	}

	for stepID := range cycleCandidates {
		if visited[stepID] == 0 {
			if dfs(stepID) {
				break
			}
		}
	}

	if cycleStart == "" {
		return "cycle detected"
	}

	var path []string
	curr := cycleEnd
	path = append(path, curr)
	for curr != cycleStart && curr != "" {
		curr = parent[curr]
		if curr != "" {
			path = append([]string{curr}, path...)
		}
	}
	path = append(path, cycleStart)

	return fmt.Sprintf("[%s]", strings.Join(path, " -> "))
}

func countWeaklyConnectedComponents(steps []contracts.StepDefinition, undirectedAdj map[string][]string) int {
	visited := make(map[string]bool, len(steps))
	components := 0

	for _, step := range steps {
		if !visited[step.StepID] {
			components++
			bfsQueue := []string{step.StepID}
			visited[step.StepID] = true

			for len(bfsQueue) > 0 {
				curr := bfsQueue[0]
				bfsQueue = bfsQueue[1:]

				for _, neighbor := range undirectedAdj[curr] {
					if !visited[neighbor] {
						visited[neighbor] = true
						bfsQueue = append(bfsQueue, neighbor)
					}
				}
			}
		}
	}
	return components
}

func validatePivotPlacement(stepMap map[string]contracts.StepDefinition, topoOrder []string) error {
	pivotIdx := -1
	for idx, stepID := range topoOrder {
		if stepMap[stepID].IsPivot {
			pivotIdx = idx
			break
		}
	}

	if pivotIdx == -1 {
		return nil
	}

	for i := 0; i < pivotIdx; i++ {
		step := stepMap[topoOrder[i]]
		if step.IsPivot {
			return errors.NewValidationError(fmt.Sprintf("multiple pivot steps disallowed: %s", step.StepID))
		}
	}

	return nil
}
