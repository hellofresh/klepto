#!/usr/bin/env sh
set -e

# Configuration
ROOT_DIR="$(dirname $(pwd))"
PROJECT_SRC=${GOPATH}/src/github.com/hellofresh/klepto

# Load environment variables
. "${ROOT_DIR}/docker/docker_ports"
if [ ! -z "${PROJECT_VARS}" ]; then
    eval ${PROJECT_VARS}
    unset PROJECT_VARS
fi

# Move go code to the source directory
mkdir -p ${PROJECT_SRC}
cp -r . ${PROJECT_SRC}
cd ${PROJECT_SRC}

# Run the integration tests
make test-integration

