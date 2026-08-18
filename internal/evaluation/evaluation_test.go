package evaluation

import (
	"context"
	"strings"
	"testing"
)

func TestFullReplayPassesQualityGateAndLineage(t *testing.T) {
	artifacts, err := Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	report := artifacts.Report
	if !report.QualityGatePassed || report.Classifier.Precision != 1 || report.Classifier.Recall != 1 || report.CanaryLeaks != 0 {
		t.Fatalf("report=%#v", report)
	}
	if artifacts.Candidate.PromotionState != "promoted" || artifacts.Bundle.Digest == "" || artifacts.Bundle.Candidate.IssueID != artifacts.Issue.ID {
		t.Fatalf("artifacts=%#v", artifacts)
	}
	if !strings.Contains(Markdown(report), "**PASSED**") {
		t.Fatal("markdown report did not pass")
	}
}

func TestStreamingLoadProcessesOneHundredThousandSpans(t *testing.T) {
	report, err := RunLoad(context.Background(), 100_000)
	if err != nil {
		t.Fatal(err)
	}
	if report.Processed != 100_000 || report.Dropped != 0 || report.CanaryLeaks != 0 || report.PeakBatch > 5000 || report.SpansPerSecond <= 0 {
		t.Fatalf("report=%#v", report)
	}
}
