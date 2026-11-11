package tests

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"distributed-transaction-engine/src/features/dead-letter-queue/contracts"
	"distributed-transaction-engine/src/features/dead-letter-queue/domain/machines"
	"distributed-transaction-engine/src/features/dead-letter-queue/domain/rules"
	"distributed-transaction-engine/src/features/dead-letter-queue/infrastructure/adapters"
	"distributed-transaction-engine/src/features/dead-letter-queue/service"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"
)

const schemaDir = "../../../../messaging/schemas/"

func compileSchema(t *testing.T, file string) *jsonschema.Schema {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	schema, err := compiler.Compile(schemaDir + file)
	if err != nil {
		t.Fatalf("schema %s does not compile: %v", file, err)
	}
	return schema
}

func decode(t *testing.T, raw string) any {
	t.Helper()
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func admitAndCapturePayload(t *testing.T) string {
	t.Helper()
	repo := adapters.NewMemoryDeadLetterRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{Repo: repo, Rules: rules.NewDeadLetterRules(), Machine: machines.NewDeadLetterMachine(), Tracer: observability.NewTracer("dlq-schema-test")})
	_, err := svc.AdmitRecord(context.Background(), contracts.AdmitDLQInput{
		TenantID: "tenant-schema", CorrelationID: uuid.New(), SagaID: uuid.New(), StepIndex: 2, StepType: "STEP_AUTH", AttemptID: uuid.New(),
		IdempotencyKey: "schema-idem", OutcomeClass: contracts.OutcomeNotExecuted, FailureClass: contracts.FailurePoison,
		ErrorFingerprint: "ERR_POISON_SCHEMA", RawMessage: "poison", PayloadRef: "s3://p/1.json", SchemaVersion: 1, Currency: "USD",
	})
	if err != nil || len(repo.OutboxEvents()) != 1 {
		t.Fatalf("admission must emit exactly one outbox event: %v", err)
	}
	return repo.OutboxEvents()[0]
}

func TestSchema_RealDeadLetterEventMatchesItsPublishedSchema(t *testing.T) {
	schema := compileSchema(t, "saga.dead-letter-quarantined.v1.schema.json")
	if err := schema.Validate(decode(t, admitAndCapturePayload(t))); err != nil {
		t.Fatalf("the emitted event violates its own schema: %v", err)
	}
}

func TestSchema_DeadLetterSchemaRejectsMalformedEvents(t *testing.T) {
	schema := compileSchema(t, "saga.dead-letter-quarantined.v1.schema.json")
	valid := decode(t, admitAndCapturePayload(t)).(map[string]any)
	mutations := map[string]func(map[string]any){
		"missing_saga":      func(m map[string]any) { delete(m, "sagaId") },
		"saga_not_uuid":     func(m map[string]any) { m["sagaId"] = "not-a-uuid" },
		"negative_step":     func(m map[string]any) { m["stepIndex"] = -1 },
		"step_is_string":    func(m map[string]any) { m["stepIndex"] = "2" },
		"bad_public_id":     func(m map[string]any) { m["publicId"] = "dlq_short" },
		"empty_step_type":   func(m map[string]any) { m["stepType"] = "" },
		"unknown_field":     func(m map[string]any) { m["extra"] = true },
		"fingerprint_empty": func(m map[string]any) { m["fingerprint"] = "" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := map[string]any{}
			for key, value := range valid {
				candidate[key] = value
			}
			mutate(candidate)
			if schema.Validate(candidate) == nil {
				t.Fatalf("%s must be rejected by the schema", name)
			}
		})
	}
}

func TestSchema_StateTransitionedSchemaAcceptsObjectsOnly(t *testing.T) {
	schema := compileSchema(t, "saga.state-transitioned.v1.schema.json")
	for raw, valid := range map[string]bool{`{"from":"RUNNING","to":"COMPLETED"}`: true, `{}`: true, `[]`: false, `"text"`: false, `42`: false, `null`: false} {
		if (schema.Validate(decode(t, raw)) == nil) != valid {
			t.Fatalf("payload %s: expected valid=%v", raw, valid)
		}
	}
}

func TestSchema_RegistryReferencesExistingSchemasAndCoversEmittedEvents(t *testing.T) {
	raw, err := os.ReadFile(schemaDir + "registry.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var registry struct {
		Events []struct {
			EventType string `yaml:"event_type"`
			Schema    string `yaml:"schema"`
			Key       string `yaml:"key"`
		} `yaml:"events"`
	}
	if err = yaml.Unmarshal(raw, &registry); err != nil {
		t.Fatal(err)
	}
	registered := map[string]bool{}
	for _, entry := range registry.Events {
		registered[entry.EventType] = true
		compileSchema(t, entry.Schema)
		if entry.Key != "sagaId" {
			t.Fatalf("%s must be keyed by sagaId", entry.EventType)
		}
	}
	for _, emitted := range []string{"DEAD_LETTER_QUARANTINED", "SAGA_STATE_TRANSITIONED"} {
		if !registered[emitted] {
			t.Fatalf("event type %s is emitted by the engine but missing from the registry", emitted)
		}
	}
	files, _ := filepath.Glob(schemaDir + "*.schema.json")
	if len(files) != len(registry.Events) {
		t.Fatalf("schema files (%d) and registry entries (%d) must match", len(files), len(registry.Events))
	}
	for _, file := range files {
		if !strings.HasSuffix(file, ".v1.schema.json") {
			t.Fatalf("schema %s must carry a version suffix", file)
		}
	}
}
