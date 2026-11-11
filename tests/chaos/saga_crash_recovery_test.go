package chaos

import (
	"testing"
	"time"
)

const recoveryWindow = 60 * time.Second

func awaitArrival(t *testing.T, d *downstream) {
	t.Helper()
	select {
	case <-d.arrived:
	case <-time.After(30 * time.Second):
		t.Fatal("the engine never reached the targeted step")
	}
}

func requireDistinctKeysPerStep(t *testing.T, d *downstream, paths ...string) {
	t.Helper()
	for _, path := range paths {
		seen := map[string]bool{}
		for _, h := range d.hitsFor(path) {
			seen[h.Key] = true
		}
		if len(seen) > 1 {
			t.Fatalf("%s was dispatched with %d different idempotency keys: downstream cannot deduplicate", path, len(seen))
		}
	}
}

func TestChaos_KillNineDuringAnyForwardStepStillCompletesEachStepOnce(t *testing.T) {
	for _, killed := range []string{"s1", "s2", "s3"} {
		t.Run("kill_during_"+killed, func(t *testing.T) {
			db, database := scratchDatabase(t)
			stub := newDownstream(t, "/"+killed, "")
			first := startEngine(t, database, "chaos-a")
			tenant := "tenant_chaos_fwd_" + killed
			submitSaga(t, first, tenant, stepDefinitions(stub.url, "s1", "s2", "s3"))
			awaitArrival(t, stub)
			first.kill9()
			stub.release()
			startEngine(t, database, "chaos-b")
			if state := awaitState(t, db, tenant, "COMPLETED", recoveryWindow); state != "COMPLETED" {
				t.Fatalf("saga ended in %q after recovery", state)
			}
			assertForwardExecution(t, stub, killed)
		})
	}
}

func assertForwardExecution(t *testing.T, d *downstream, killed string) {
	t.Helper()
	for _, step := range []string{"s1", "s2", "s3"} {
		count := len(d.hitsFor("/" + step))
		switch {
		case step == killed && (count < 1 || count > 2):
			t.Fatalf("interrupted step %s must be retried at most once, saw %d dispatches", step, count)
		case step != killed && count != 1:
			t.Fatalf("step %s must run exactly once across the crash, saw %d dispatches", step, count)
		}
	}
	requireDistinctKeysPerStep(t, d, "/s1", "/s2", "/s3")
	for _, step := range []string{"s1", "s2", "s3"} {
		if len(d.hitsFor("/undo-"+step)) != 0 {
			t.Fatalf("a completed saga must never compensate %s", step)
		}
	}
}

func TestChaos_KillNineDuringCompensationResumesWithoutRerunningForwardSteps(t *testing.T) {
	db, database := scratchDatabase(t)
	stub := newDownstream(t, "/undo-s2", "/s3")
	first := startEngine(t, database, "chaos-a")
	tenant := "tenant_chaos_comp"
	submitSaga(t, first, tenant, stepDefinitions(stub.url, "s1", "s2", "s3"))
	awaitArrival(t, stub)
	first.kill9()
	stub.release()
	startEngine(t, database, "chaos-b")
	if state := awaitState(t, db, tenant, "FAILED_COMPENSATED", recoveryWindow); state != "FAILED_COMPENSATED" {
		t.Fatalf("saga ended in %q instead of finishing compensation", state)
	}
	for _, step := range []string{"s1", "s2", "s3"} {
		if count := len(stub.hitsFor("/" + step)); count != 1 {
			t.Fatalf("forward step %s must not be re-run during recovery, saw %d dispatches", step, count)
		}
	}
	if count := len(stub.hitsFor("/undo-s1")); count != 1 {
		t.Fatalf("undo-s1 must run exactly once, saw %d", count)
	}
	if count := len(stub.hitsFor("/undo-s2")); count < 1 || count > 2 {
		t.Fatalf("the interrupted compensation may repeat once, saw %d", count)
	}
	requireDistinctKeysPerStep(t, stub, "/undo-s1", "/undo-s2")
}

func TestChaos_RepeatedCrashesNeverDuplicateACompletedStep(t *testing.T) {
	db, database := scratchDatabase(t)
	stub := newDownstream(t, "/s2", "")
	engineA := startEngine(t, database, "chaos-a")
	tenant := "tenant_chaos_repeat"
	submitSaga(t, engineA, tenant, stepDefinitions(stub.url, "s1", "s2", "s3"))
	awaitArrival(t, stub)
	engineA.kill9()
	engineB := startEngine(t, database, "chaos-b")
	awaitDispatches(t, stub, "/s2", 2)
	engineB.kill9()
	stub.release()
	startEngine(t, database, "chaos-c")
	if state := awaitState(t, db, tenant, "COMPLETED", recoveryWindow); state != "COMPLETED" {
		t.Fatalf("saga ended in %q after two crashes", state)
	}
	if count := len(stub.hitsFor("/s1")); count != 1 {
		t.Fatalf("s1 finished before the first crash and must never repeat, saw %d dispatches", count)
	}
	requireDistinctKeysPerStep(t, stub, "/s1", "/s2", "/s3")
}

func awaitDispatches(t *testing.T, d *downstream, path string, count int) {
	t.Helper()
	deadline := time.Now().Add(recoveryWindow)
	for time.Now().Before(deadline) {
		if len(d.hitsFor(path)) >= count {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s was not dispatched %d times within %s", path, count, recoveryWindow)
}
