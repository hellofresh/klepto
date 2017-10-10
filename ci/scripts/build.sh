#!/usr/bin/env sh
set -e

# Configuration
ROOT_DIR="$(dirname $(pwd))"
OUTPUT_DIR="${ROOT_DIR}/build"

VERSION_FILE="${ROOT_DIR}/version/version"
VERSION=$(cat ${VERSION_FILE} | tr -d '\n')

PROJECT_SRC=${GOPATH}/src/github.com/hellofresh/klepto

# Move go code to the source directory
mkdir -p ${PROJECT_SRC}
cp -r . ${PROJECT_SRC}
cd ${PROJECT_SRC}

# Build binaries
for OS in linux darwin windows freebsd openbsd; do
  for ARCH in 386 amd64; do
    echo "Building binary for $OS/$ARCH..."
    BUILD_DIR="${OUTPUT_DIR}/klepto-${ARCH}-${OS}-${VERSION}"

    # Build go binary
    GOARCH=${ARCH} GOOS=${OS} CGO_ENABLED=0 BUILD_DIR="${BUILD_DIR}" make build
    done
done
