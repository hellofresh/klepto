#!/usr/bin/env sh
set -e

# Configuration
PROJECT_SRC=${GOPATH}/src/github.com/hellofresh/klepto

# Move go code to the source directory
mkdir -p ${PROJECT_SRC}
cp -r . ${PROJECT_SRC}
cd ${PROJECT_SRC}

# Run the unit tests
make test-unit
