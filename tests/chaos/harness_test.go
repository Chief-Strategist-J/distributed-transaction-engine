package chaos

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

var engineBinary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "chaos-engine-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	engineBinary = filepath.Join(dir, "engine")
	build := exec.Command("go", "build", "-o", engineBinary, "../../src/main.go")
	build.Stderr = os.Stderr
	if err = build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build engine:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type hit struct {
	Path string
	Key  string
}

type downstream struct {
	mu        sync.Mutex
	hits      []hit
	blockPath string
	failPath  string
	arrived   chan struct{}
	once      sync.Once
	url       string
}

func nonLoopbackIPv4(t *testing.T) string {
	t.Helper()
	addresses, _ := net.InterfaceAddrs()
	for _, address := range addresses {
		if network, ok := address.(*net.IPNet); ok && network.IP.To4() != nil && !network.IP.IsLoopback() && !network.IP.IsLinkLocalUnicast() {
			return network.IP.String()
		}
	}
	t.Skip("no non-loopback IPv4 address: workflow validation forbids loopback step endpoints")
	return ""
}

func newDownstream(t *testing.T, blockPath string, failPath string) *downstream {
	t.Helper()
	ip := nonLoopbackIPv4(t)
	listener, err := net.Listen("tcp", ip+":0")
	if err != nil {
		t.Fatal(err)
	}
	d := &downstream{blockPath: blockPath, failPath: failPath, arrived: make(chan struct{}), url: "http://" + listener.Addr().String()}
	server := &http.Server{Handler: http.HandlerFunc(d.serve)}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return d
}

func (d *downstream) serve(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	d.hits = append(d.hits, hit{Path: r.URL.Path, Key: r.Header.Get("Idempotency-Key")})
	armed := r.URL.Path == d.blockPath
	d.mu.Unlock()
	if armed {
		d.once.Do(func() { close(d.arrived) })
		<-r.Context().Done()
		return
	}
	if r.URL.Path == d.failPath {
		http.Error(w, "rejected", http.StatusUnprocessableEntity)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (d *downstream) release() {
	d.mu.Lock()
	d.blockPath = ""
	d.mu.Unlock()
}

func (d *downstream) hitsFor(path string) []hit {
	d.mu.Lock()
	defer d.mu.Unlock()
	var matched []hit
	for _, h := range d.hits {
		if h.Path == path {
			matched = append(matched, h)
		}
	}
	return matched
}

type engine struct {
	cmd       *exec.Cmd
	adminPort int
	apiPort   int
	log       *bytes.Buffer
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func scratchDatabase(t *testing.T) (*sql.DB, *url.URL) {
	t.Helper()
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL is not set")
	}
	admin, err := sql.Open("postgres", base)
	if err != nil || admin.Ping() != nil {
		t.Skip("PostgreSQL is offline")
	}
	name := "chaos_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	if _, err = admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)")
		_ = admin.Close()
	})
	parsed, _ := url.Parse(base)
	scratch := *parsed
	scratch.Path = "/" + name
	db, err := sql.Open("postgres", scratch.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, &scratch
}

func startEngine(t *testing.T, database *url.URL, workerID string) *engine {
	t.Helper()
	password, _ := database.User.Password()
	e := &engine{adminPort: freePort(t), apiPort: freePort(t), log: &bytes.Buffer{}}
	e.cmd = exec.Command(engineBinary)
	e.cmd.Dir = "../.."
	e.cmd.Env = append(os.Environ(),
		fmt.Sprintf("PORT=%d", e.apiPort), fmt.Sprintf("ADMIN_PORT=%d", e.adminPort),
		"DB_HOST="+database.Hostname(), "DB_PORT="+database.Port(), "DB_USER="+database.User.Username(), "DB_PASSWORD="+password,
		"DB_NAME="+strings.TrimPrefix(database.Path, "/"), "DB_SSLMODE=disable", "WORKER_ID="+workerID,
		"POLLER_LEASE_DURATION_SECONDS=3", "POLLER_INTERVAL_MILLIS=200", "OUTBOX_RELAY_ENABLED=false", "KAFKA_BROKERS=",
		"ENV=", "AUTH_MODE=disabled", "DB_RLS_ENFORCED=false",
	)
	e.cmd.Stdout, e.cmd.Stderr = e.log, e.log
	if err := e.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.stop() })
	e.waitReady(t)
	return e
}

func (e *engine) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health/ready", e.adminPort))
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("engine did not become ready:\n%s", e.log.String())
}

func (e *engine) kill9() {
	if e.cmd.Process != nil {
		_ = e.cmd.Process.Signal(syscall.SIGKILL)
		_, _ = e.cmd.Process.Wait()
	}
}

func (e *engine) stop() {
	if e.cmd.ProcessState == nil && e.cmd.Process != nil {
		_ = e.cmd.Process.Signal(syscall.SIGKILL)
		_, _ = e.cmd.Process.Wait()
	}
}

func (e *engine) post(t *testing.T, path string, tenant string, body any) *http.Response {
	t.Helper()
	payload, _ := json.Marshal(body)
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d%s", e.apiPort, path), bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("x-tenant-id", tenant)
	request.Header.Set("x-request-id", "chaos-"+uuid.NewString())
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return response
}

func stepDefinitions(base string, ids ...string) []workflowContracts.StepDefinition {
	steps := make([]workflowContracts.StepDefinition, 0, len(ids))
	for index, id := range ids {
		dependencies := []string{}
		if index > 0 {
			dependencies = []string{ids[index-1]}
		}
		steps = append(steps, workflowContracts.StepDefinition{
			StepID: id, StepName: id, ActionType: "REST", ActionEndpoint: base + "/" + id,
			CompensationAction: "UNDO_" + id, CompensationEndpoint: base + "/undo-" + id, TimeoutSeconds: 30, Dependencies: dependencies,
		})
	}
	return steps
}

func submitSaga(t *testing.T, e *engine, tenant string, steps []workflowContracts.StepDefinition) {
	t.Helper()
	register := e.post(t, "/api/v1/workflow-definitions", tenant, workflowContracts.CreateWorkflowDefinitionInput{
		TenantID: tenant, Name: "chaos-flow", Version: 1, Source: "CHAOS", ExternalID: "ext_wf_" + tenant,
		CorrelationID: uuid.NewString(), Steps: steps,
	})
	if register.StatusCode != http.StatusCreated {
		t.Fatalf("workflow registration failed: %d\n%s", register.StatusCode, e.log.String())
	}
	execute := e.post(t, "/api/v1/sagas/execute", tenant, map[string]any{
		"tenantId": tenant, "workflowName": "chaos-flow", "workflowVersion": 1, "source": "CHAOS",
		"externalId": "ext_saga_" + tenant, "correlationId": uuid.NewString(), "initialPayload": map[string]any{},
	})
	if execute.StatusCode != http.StatusAccepted {
		t.Fatalf("saga submission failed: %d\n%s", execute.StatusCode, e.log.String())
	}
}

func awaitState(t *testing.T, db *sql.DB, tenant string, wanted string, within time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(within)
	state := ""
	for time.Now().Before(deadline) {
		_ = db.QueryRow(`SELECT current_state FROM saga_instances WHERE tenant_id = $1`, tenant).Scan(&state)
		if state == wanted {
			return state
		}
		time.Sleep(250 * time.Millisecond)
	}
	return state
}
