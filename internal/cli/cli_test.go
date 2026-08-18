package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/version"
)

func TestVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := Run([]string{"version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != version.Current {
		t.Fatalf("stdout=%q", stdout.String())
	}
}

func TestUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := Run([]string{"unknown"}, &stdout, &stderr); code != 2 {
		t.Fatalf("code=%d", code)
	}
	if !strings.Contains(stderr.String(), "unknown command") {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestFileWorkflowFromCorpusToPromotedEval(t *testing.T) {
	dir := t.TempDir()
	corpusPath := filepath.Join(dir, "corpus.json")
	storePath := filepath.Join(dir, "traces.jsonl")
	issuePath := filepath.Join(dir, "issue.json")
	candidatePath := filepath.Join(dir, "candidate.json")
	testPath := filepath.Join(dir, "failure_test.go")
	var stdout, stderr bytes.Buffer
	commands := [][]string{
		{"corpus", "generate", "--output", corpusPath},
		{"ingest", "--input", corpusPath, "--store", storePath},
		{"detect", "--store", storePath, "--output", filepath.Join(dir, "signals.json")},
		{"issue", "review", "--store", storePath, "--reason", "retry_limit_exceeded", "--actor", "issue-reviewer", "--output", issuePath},
		{"eval", "export", "--store", storePath, "--issue", issuePath, "--actor", "eval-reviewer", "--output", candidatePath, "--go-test", testPath},
	}
	for _, command := range commands {
		stdout.Reset()
		stderr.Reset()
		if code := Run(command, &stdout, &stderr); code != 0 {
			t.Fatalf("command=%v code=%d stdout=%s stderr=%s", command, code, stdout.String(), stderr.String())
		}
	}
	for _, path := range []string{corpusPath, storePath, issuePath, candidatePath, testPath} {
		if info, err := os.Stat(path); err != nil || info.Size() == 0 {
			t.Fatalf("artifact=%s info=%v err=%v", path, info, err)
		}
	}
	candidate, _ := os.ReadFile(candidatePath)
	if !strings.Contains(string(candidate), `"promotion_state": "promoted"`) || strings.Contains(string(candidate), "CANARY") {
		t.Fatalf("candidate=%s", candidate)
	}
}

func TestDemoAndSignedStatus(t *testing.T) {
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	if code := Run([]string{"demo", "--output-dir", dir}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "100000") {
		t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	reportPath := filepath.Join(dir, "replay.json")
	signaturePath := filepath.Join(dir, "signature.json")
	if code := Run([]string{"status", "--report", reportPath, "--signature", signaturePath}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "**PASSED**") {
		t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.Replace(data, []byte(`"canary_leaks": 0`), []byte(`"canary_leaks": 1`), 1)
	if err := os.WriteFile(reportPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"status", "--report", reportPath, "--signature", signaturePath}, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "signature") {
		t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
}
