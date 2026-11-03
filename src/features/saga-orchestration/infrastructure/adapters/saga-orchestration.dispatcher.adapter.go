package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	ports "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/ports"
	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	"distributed-transaction-engine/src/shared/errors"
)

type HttpStepDispatcherAdapter struct {
	client *http.Client
}

var _ ports.StepDispatcherPort = (*HttpStepDispatcherAdapter)(nil)

func NewHttpStepDispatcherAdapter() *HttpStepDispatcherAdapter {
	return &HttpStepDispatcherAdapter{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (d *HttpStepDispatcherAdapter) DispatchStep(ctx context.Context, stepDef workflowContracts.StepDefinition, input map[string]interface{}) (map[string]interface{}, error) {
	if !strings.HasPrefix(stepDef.ActionEndpoint, "http://") && !strings.HasPrefix(stepDef.ActionEndpoint, "https://") {
		return map[string]interface{}{"status": "simulated_success", "step_id": stepDef.StepID}, nil
	}

	bodyBytes, err := json.Marshal(input)
	if err != nil {
		return nil, errors.NewValidationError("failed to encode step input")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, stepDef.ActionEndpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, errors.NewInternalError(err.Error())
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, errors.NewInternalError("step execution network failure: " + err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, errors.NewInternalError("step downstream service failed with status: " + resp.Status)
	}

	var output map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&output)
	if output == nil {
		output = map[string]interface{}{"status": "success"}
	}
	return output, nil
}

func (d *HttpStepDispatcherAdapter) CompensateStep(ctx context.Context, stepDef workflowContracts.StepDefinition, input map[string]interface{}) error {
	if stepDef.CompensationEndpoint == "" {
		return nil
	}

	if !strings.HasPrefix(stepDef.CompensationEndpoint, "http://") && !strings.HasPrefix(stepDef.CompensationEndpoint, "https://") {
		return nil
	}

	bodyBytes, err := json.Marshal(input)
	if err != nil {
		return errors.NewValidationError("failed to encode compensation input")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, stepDef.CompensationEndpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return errors.NewInternalError(err.Error())
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return errors.NewInternalError("compensation network failure: " + err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return errors.NewInternalError("compensation downstream service failed with status: " + resp.Status)
	}
	return nil
}

