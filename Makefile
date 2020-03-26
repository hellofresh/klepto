NO_COLOR=\033[0m
OK_COLOR=\033[32;01m
ERROR_COLOR=\033[31;01m
WARN_COLOR=\033[33;01m

# Space separated patterns of packages to skip in list, test, format.
IGNORED_PACKAGES := /vendor/

.PHONY: all clean deps build

all: clean deps build

deps:
	@echo "$(OK_COLOR)==> Installing dependencies$(NO_COLOR)"
	@go get -u golang.org/x/lint/golint
	@go mod vendor

# Builds the project
build:
	@echo "$(OK_COLOR)==> Building... $(NO_COLOR)"
	@goreleaser --snapshot --rm-dist --skip-validate

test: lint format vet
	@echo "$(OK_COLOR)==> Running tests$(NO_COLOR)"
	@CGO_ENABLED=0 go test -cover ./... -coverprofile=coverage.txt -covermode=atomic

lint:
	@echo "$(OK_COLOR)==> Checking code style with 'golint' tool$(NO_COLOR)"
	@go list ./... | xargs -n 1 golint -set_exit_status

format:
	@echo "$(OK_COLOR)==> Checking code formating with 'gofmt' tool$(NO_COLOR)"
	@gofmt -l -s cmd pkg | grep ".*\.go"; if [ "$$?" = "0" ]; then exit 1; fi

vet:
	@echo "$(OK_COLOR)==> Checking code correctness with 'go vet' tool$(NO_COLOR)"
	@go vet ./...

test-docker:
	docker-compose up -d
	@TEST_POSTGRES="postgres://hello:fresh@localhost:8050/klepto?sslmode=disable" \
	TEST_MYSQL="root:hellofresh@tcp(localhost:8052)/" \
	/bin/sh -c "./build/test.sh $(allpackages)"

# Cleans our project: deletes binaries
clean:
	@echo "$(OK_COLOR)==> Cleaning project$(NO_COLOR)"
	@go clean
	@rm -rf dist

# cd into the GOPATH to workaround ./... not following symlinks
_allpackages = $(shell ( go list ./... 2>&1 1>&3 | \
    grep -v -e "^$$" $(addprefix -e ,$(IGNORED_PACKAGES)) 1>&2 ) 3>&1 | \
    grep -v -e "^$$" $(addprefix -e ,$(IGNORED_PACKAGES)))

# memoize allpackages, so that it's executed only once and only if used
allpackages = $(if $(__allpackages),,$(eval __allpackages := $$(_allpackages)))$(__allpackages)
