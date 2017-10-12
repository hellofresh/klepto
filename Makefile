# Config for the binaries you want to build
NAME=klepto
REPO=github.com/hellofresh/${NAME}
VERSION ?= "dev"

BINARY=${NAME}
BINARY_SRC=$(REPO)

# Build configuration
BUILD_DIR ?= $(CURDIR)/out
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
GO_LINKER_FLAGS=-ldflags="-s -w"

# Other config
NO_COLOR=\033[0m
OK_COLOR=\033[32;01m
ERROR_COLOR=\033[31;01m
WARN_COLOR=\033[33;01m

.PHONY: all clean deps install

all: clean deps test-unit install

test: test-unit

# Install dependencies
deps:
	git config --global http.https://gopkg.in.followRedirects true
	@go get -u github.com/golang/dep/cmd/dep
	@go get -u github.com/golang/lint/golint
	@echo "$(OK_COLOR)==> Installing dependencies$(NO_COLOR)"
	@dep ensure

# Builds the project
build:
	@mkdir -p ${BUILD_DIR}

	@printf "$(OK_COLOR)==> Building for ${GOOS}/${GOARCH} $(NO_COLOR)\n"
	@GOARCH=${GOARCH} GOOS=${GOOS} go build -o ${BUILD_DIR}/${BINARY} ${GO_LINKER_FLAGS} ${BINARY_SRC}

# Installs our project: copies binaries
install:
	@echo "$(OK_COLOR)==> Installing project$(NO_COLOR)"
	go install -v

# Test our project
test-unit:
	@printf "$(OK_COLOR)==> Running tests$(NO_COLOR)\n"
	@go test -v

# Cleans our project: deletes binaries
clean:
	@printf "$(OK_COLOR)==> Cleaning project$(NO_COLOR)\n"
	if [ -d ${BUILD_DIR} ] ; then rm -rf ${BUILD_DIR}/* ; fi
