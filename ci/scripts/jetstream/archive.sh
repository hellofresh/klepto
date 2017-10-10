#!/usr/bin/env sh

set -e

ARCHIVE_DIR="$(dirname $(pwd))/archive"

# Package outputs
for i in ./*; do
    RELEASE=$(basename "${i}")

    echo "Packing binary for ${RELEASE}..."
    tar -czf "${ARCHIVE_DIR}/${RELEASE}.tar.gz" "${RELEASE}"
done
