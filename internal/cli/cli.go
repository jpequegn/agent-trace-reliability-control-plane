package cli

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/corpus"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/detect"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/domain"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/evalexport"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/evaluation"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/governance"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/ingest"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/investigate"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/reliability"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/version"
)

func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printHelp(stdout)
		return 0
	}
	var err error
	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, version.Current)
		return 0
	case "help", "-h", "--help":
		printHelp(stdout)
		return 0
	case "corpus":
		err = runCorpus(args[1:], stdout, stderr)
	case "ingest":
		err = runIngest(args[1:], stdout, stderr)
	case "detect":
		err = runDetect(args[1:], stdout, stderr)
	case "issue":
		err = runIssue(args[1:], stdout, stderr)
	case "eval":
		err = runEval(args[1:], stdout, stderr)
	case "replay":
		err = runReplay(args[1:], stdout, stderr)
	case "status":
		return runStatus(args[1:], stdout, stderr)
	case "demo":
		err = runDemo(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command: %s\n", args[0])
		printHelp(stderr)
		return 2
	}
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 2
	}
	return 0
}

func runCorpus(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "generate" {
		return errors.New("usage: tracecontrol corpus generate --output <path>")
	}
	flags := flag.NewFlagSet("corpus generate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	output := flags.String("output", "", "corpus JSON path")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	data := corpus.Generate()
	if err := data.Validate(); err != nil {
		return err
	}
	if *output != "" {
		if err := writeJSONFile(*output, data); err != nil {
			return err
		}
	}
	fmt.Fprintf(stdout, "generated %d runs and %d fictional spans\n", len(data.Truth), len(data.Spans))
	return nil
}

func runIngest(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("ingest", flag.ContinueOnError)
	flags.SetOutput(stderr)
	input := flags.String("input", "", "corpus JSON path")
	storePath := flags.String("store", "", "sanitized JSONL store")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *input == "" || *storePath == "" {
		return errors.New("ingest requires --input and --store")
	}
	var data corpus.Corpus
	if err := readJSON(*input, &data); err != nil {
		return err
	}
	store, err := ingest.NewStore(*storePath)
	if err != nil {
		return err
	}
	ingestor := ingest.New(store, ingest.DefaultMaxBatch, 4)
	result, err := ingestor.Ingest(context.Background(), data.Spans)
	if err != nil {
		return err
	}
	return writeJSON(stdout, result)
}

func runDetect(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("detect", flag.ContinueOnError)
	flags.SetOutput(stderr)
	storePath := flags.String("store", "", "sanitized JSONL store")
	output := flags.String("output", "", "signal JSON path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	spans, err := readStore(*storePath)
	if err != nil {
		return err
	}
	signals := detect.New(detect.DefaultConfig()).Detect(spans)
	if *output != "" {
		if err := writeJSONFile(*output, signals); err != nil {
			return err
		}
	}
	fmt.Fprintf(stdout, "detected %d deterministic signals\n", len(signals))
	return nil
}

func runIssue(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "review" {
		return errors.New("usage: tracecontrol issue review --store <path> --reason <code> --actor <name> --output <path>")
	}
	flags := flag.NewFlagSet("issue review", flag.ContinueOnError)
	flags.SetOutput(stderr)
	storePath := flags.String("store", "", "sanitized JSONL store")
	reason := flags.String("reason", detect.ReasonRetry, "classifier reason")
	actor := flags.String("actor", "", "review actor")
	output := flags.String("output", "", "reviewed issue JSON")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	spans, err := readStore(*storePath)
	if err != nil {
		return err
	}
	issue, _, err := reviewIssue(spans, *reason, *actor)
	if err != nil {
		return err
	}
	if *output != "" {
		return writeJSONFile(*output, issue)
	}
	return writeJSON(stdout, issue)
}

func runEval(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "export" {
		return errors.New("usage: tracecontrol eval export --store <path> --issue <path> --actor <name> --output <path>")
	}
	flags := flag.NewFlagSet("eval export", flag.ContinueOnError)
	flags.SetOutput(stderr)
	storePath := flags.String("store", "", "sanitized JSONL store")
	issuePath := flags.String("issue", "", "confirmed issue JSON")
	actor := flags.String("actor", "", "promotion reviewer")
	output := flags.String("output", "", "candidate JSON")
	goTest := flags.String("go-test", "", "generated Go test path")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	spans, err := readStore(*storePath)
	if err != nil {
		return err
	}
	var issue domain.ReliabilityIssue
	if err := readJSON(*issuePath, &issue); err != nil {
		return err
	}
	signals := filterSignals(detect.New(detect.DefaultConfig()).Detect(spans), classifierReason(issue.ClassifierID))
	_, handoff, err := (investigate.Service{Router: investigate.DefaultRouter()}).Execute(context.Background(), investigate.RouteRequest{Issue: issue, Signals: signals, At: issue.UpdatedAt})
	if err != nil {
		return err
	}
	candidate, err := evalexport.Build(issue, signals, spans, handoff, issue.UpdatedAt.Add(time.Minute))
	if err != nil {
		return err
	}
	candidate, err = evalexport.Promote(candidate, issue, *actor, "CLI-reviewed regression fixture")
	if err != nil {
		return err
	}
	if *output != "" {
		if err := writeJSONFile(*output, candidate); err != nil {
			return err
		}
	}
	if *goTest != "" {
		source, err := evalexport.GenerateGoTest(candidate, "generatedeval")
		if err != nil {
			return err
		}
		if err := writeFile(*goTest, source); err != nil {
			return err
		}
	}
	if *output == "" {
		return writeJSON(stdout, candidate)
	}
	fmt.Fprintln(stdout, candidate.ID)
	return nil
}

func runReplay(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("replay", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonPath := flags.String("json-report", "", "JSON report path")
	markdownPath := flags.String("markdown-report", "", "Markdown report path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	artifacts, err := evaluation.Run(context.Background())
	if err != nil {
		return err
	}
	if *jsonPath != "" {
		if err := writeJSONFile(*jsonPath, artifacts.Report); err != nil {
			return err
		}
	}
	markdown := evaluation.Markdown(artifacts.Report)
	if *markdownPath != "" {
		if err := writeFile(*markdownPath, []byte(markdown)); err != nil {
			return err
		}
	}
	fmt.Fprint(stdout, markdown)
	if !artifacts.Report.QualityGatePassed {
		return errors.New("replay quality gate failed")
	}
	return nil
}

func runStatus(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	reportPath := flags.String("report", "", "replay report JSON")
	signaturePath := flags.String("signature", "", "optional signed digest JSON")
	if err := flags.Parse(args); err != nil || *reportPath == "" {
		fmt.Fprintln(stderr, "error: status requires --report")
		return 2
	}
	var report evaluation.Report
	if err := readJSON(*reportPath, &report); err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 2
	}
	if *signaturePath != "" {
		var signed governance.SignedDigest
		if err := readJSON(*signaturePath, &signed); err != nil || governance.Verify(report, signed) != nil {
			fmt.Fprintln(stderr, "error: report signature verification failed")
			return 2
		}
	}
	fmt.Fprint(stdout, evaluation.Markdown(report))
	if !report.QualityGatePassed {
		return 1
	}
	return 0
}

func runDemo(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("demo", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dir := flags.String("output-dir", "demo-output", "artifact directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	artifacts, err := evaluation.Run(context.Background())
	if err != nil {
		return err
	}
	seed := sha256.Sum256([]byte("fixture-only-report-signing-key"))
	signed, err := governance.Sign(artifacts.Report, ed25519.NewKeyFromSeed(seed[:]))
	if err != nil {
		return err
	}
	files := []struct {
		name  string
		value any
	}{{"replay.json", artifacts.Report}, {"signature.json", signed}, {"issue.json", artifacts.Issue}, {"handoff.json", artifacts.Handoff}, {"candidate.json", artifacts.Candidate}, {"lineage-bundle.json", artifacts.Bundle}}
	for _, file := range files {
		if err := writeJSONFile(filepath.Join(*dir, file.name), file.value); err != nil {
			return err
		}
	}
	if err := writeFile(filepath.Join(*dir, "replay.md"), []byte(evaluation.Markdown(artifacts.Report))); err != nil {
		return err
	}
	source, err := evalexport.GenerateGoTest(artifacts.Candidate, "generatedeval")
	if err != nil {
		return err
	}
	if err := writeFile(filepath.Join(*dir, "promoted_failure_test.go"), source); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "demo complete: %s\nquality gate: passed\nprocessed load spans: %d\n", *dir, artifacts.Report.Load.Processed)
	return nil
}

func reviewIssue(spans []domain.Span, reason, actor string) (domain.ReliabilityIssue, []domain.Signal, error) {
	if actor == "" {
		return domain.ReliabilityIssue{}, nil, errors.New("review actor is required")
	}
	classifier, ok := detect.Registry()[reason]
	if !ok {
		return domain.ReliabilityIssue{}, nil, fmt.Errorf("unknown classifier reason %s", reason)
	}
	signals := filterSignals(detect.New(detect.DefaultConfig()).Detect(spans), reason)
	if len(signals) == 0 {
		return domain.ReliabilityIssue{}, nil, errors.New("no matching deterministic signals")
	}
	asOf := spans[len(spans)-1].EndedAt
	impact, err := reliability.CalculateImpact(spans, signals, asOf)
	if err != nil {
		return domain.ReliabilityIssue{}, nil, err
	}
	issue, err := reliability.ProposeIssue(classifier, signals, impact, "team-signal", asOf)
	if err != nil {
		return domain.ReliabilityIssue{}, nil, err
	}
	issue, err = reliability.Transition(issue, domain.IssueConfirmed, actor, "CLI reviewer confirmed fixture reproduction", asOf.Add(time.Minute))
	return issue, signals, err
}

func readStore(path string) ([]domain.Span, error) {
	if path == "" {
		return nil, errors.New("store path is required")
	}
	store, err := ingest.NewStore(path)
	if err != nil {
		return nil, err
	}
	return store.ReadAll()
}

func filterSignals(signals []domain.Signal, reason string) []domain.Signal {
	result := []domain.Signal{}
	for _, signal := range signals {
		if signal.ReasonCode == reason {
			result = append(result, signal)
		}
	}
	return result
}

func classifierReason(id string) string {
	for reason, classifier := range detect.Registry() {
		if classifier.ID == id {
			return reason
		}
	}
	return ""
}

func readJSON(path string, target any) error {
	if path == "" {
		return errors.New("input path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func writeJSON(output io.Writer, value any) error {
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func writeJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(path, append(data, '\n'))
}

func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func printHelp(output io.Writer) {
	fmt.Fprintln(output, "agent-trace-reliability-control-plane")
	fmt.Fprintln(output, "usage: tracecontrol <command>")
	fmt.Fprintln(output, "commands:")
	fmt.Fprintln(output, "  corpus generate --output <path>")
	fmt.Fprintln(output, "  ingest --input <corpus.json> --store <traces.jsonl>")
	fmt.Fprintln(output, "  detect --store <traces.jsonl> [--output <signals.json>]")
	fmt.Fprintln(output, "  issue review --store <path> --reason <code> --actor <name> --output <issue.json>")
	fmt.Fprintln(output, "  eval export --store <path> --issue <issue.json> --actor <name> --output <candidate.json>")
	fmt.Fprintln(output, "  replay [--json-report <path>] [--markdown-report <path>]")
	fmt.Fprintln(output, "  status --report <path> [--signature <path>]")
	fmt.Fprintln(output, "  demo [--output-dir <path>]")
	fmt.Fprintln(output, "  version")
}
