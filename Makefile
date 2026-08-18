.PHONY: fmt vet test race fuzz check

fmt:
	test -z "$$(gofmt -l .)"

vet:
	go vet ./...

test:
	go test ./...

race:
	go test -race ./...

fuzz:
	go test -run=^$$ -fuzz=FuzzSanitizeNeverReturnsSecretCanary -fuzztime=2s ./internal/ingest

check: fmt vet test
