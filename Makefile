.PHONY: fmt vet test race check

fmt:
	test -z "$$(gofmt -l .)"

vet:
	go vet ./...

test:
	go test ./...

race:
	go test -race ./...

check: fmt vet test
