package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	ports "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/ports"
	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	"distributed-transaction-engine/src/shared/errors"
)

type AmbiguousStepError struct {
	StatusCode int
	Message    string
}

func (e *AmbiguousStepError) Error() string {
	return e.Message
}

func IsAmbiguousError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*AmbiguousStepError)
	return ok
}

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

func (d *HttpStepDispatcherAdapter) DispatchStep(ctx context.Context, stepDef workflowContracts.StepDefinition, input map[string]interface{}, idempotencyKey string) (map[string]interface{}, error) {
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
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, &AmbiguousStepError{
			StatusCode: 504,
			Message:    "ambiguous network dispatch failure: " + err.Error(),
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode == 500 || resp.StatusCode == 502 || resp.StatusCode == 503 || resp.StatusCode == 504 {
		return nil, &AmbiguousStepError{
			StatusCode: resp.StatusCode,
			Message:    fmt.Sprintf("ambiguous downstream status code: %d", resp.StatusCode),
		}
	}

	if resp.StatusCode >= 400 {
		return nil, errors.NewInternalError(fmt.Sprintf("permanent downstream rejection: %d", resp.StatusCode))
	}

	var output map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&output)
	if output == nil {
		output = map[string]interface{}{"status": "success"}
	}
	return output, nil
}

func (d *HttpStepDispatcherAdapter) CompensateStep(ctx context.Context, stepDef workflowContracts.StepDefinition, input map[string]interface{}, output map[string]interface{}, idempotencyKey string) error {
	if stepDef.CompensationEndpoint == "" {
		return nil
	}

	if !strings.HasPrefix(stepDef.CompensationEndpoint, "http://") && !strings.HasPrefix(stepDef.CompensationEndpoint, "https://") {
		return nil
	}

	compPayload := make(map[string]interface{}, len(input)+len(output)+1)
	for k, v := range input {
		compPayload["input_"+k] = v
	}
	for k, v := range output {
		compPayload[k] = v
	}

	bodyBytes, err := json.Marshal(compPayload)
	if err != nil {
		return errors.NewValidationError("failed to encode compensation input")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, stepDef.CompensationEndpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return errors.NewInternalError(err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return errors.NewInternalError("compensation network failure: " + err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return errors.NewInternalError(fmt.Sprintf("compensation downstream service rejected: %d", resp.StatusCode))
	}
	return nil
}
