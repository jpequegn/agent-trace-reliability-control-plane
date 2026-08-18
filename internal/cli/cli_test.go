package cli

import (
	"bytes"
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
