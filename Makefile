NO_COLOR=\033[0m
OK_COLOR=\033[32;01m
ERROR_COLOR=\033[31;01m
WARN_COLOR=\033[33;01m

.PHONY: all test build

all: test build

# Builds the project
build:
	@echo "$(OK_COLOR)==> Building... $(NO_COLOR)"
	@goreleaser --snapshot --rm-dist --skip-validate

test:
	@echo "$(OK_COLOR)==> Running tests$(NO_COLOR)"
	@CGO_ENABLED=0 go test -cover ./... -coverprofile=coverage.txt -covermode=atomic
