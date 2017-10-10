#!/usr/bin/env sh
set -e

# Configuration
PROJECT_SRC=${GOPATH}/src/github.com/hellofresh/klepto
FILE_EXTENSIONS='\.go$'

# Detect the changed files
BASE_BRANCH=$(git config --get pullrequest.basebranch)
git diff --name-only --diff-filter=ACMR "${BASE_BRANCH}" | (grep -i -E "${FILE_EXTENSIONS}" || true) > changed_files.txt

CHANGE_COUNT=$(cat changed_files.txt | wc -l)
if [ "${CHANGE_COUNT}" = "0" ]; then
echo "No files affected. Skipping"
exit 0
fi
echo "Affected files: ${CHANGE_COUNT}"

# Move go code to the source directory
mkdir -p ${PROJECT_SRC}
cp -r . ${PROJECT_SRC}
cd ${PROJECT_SRC}

# Get the go targets of the project
TARGETS=$(go list ./... | grep -v /vendor/)

echo -n "Checking gofmt: "
ERRS=$(cat changed_files.txt | xargs gofmt -l 2>&1 || true)
if [ -n "${ERRS}" ]; then
    echo "FAIL - the following files need to be gofmt'ed:"
    for e in ${ERRS}; do
        echo "    $e"
    done
    echo
    exit 1
fi
echo "PASS"
echo

echo -n "Checking go vet: "
ERRS=$(go vet ${TARGETS} 2>&1 || true)
if [ -n "${ERRS}" ]; then
    echo "FAIL"
    echo "${ERRS}"
    echo
    exit 1
fi
echo "PASS"
echo

echo -n "Checking goimports: "
ERRS=$(goimports -l $(cat changed_files.txt) 2>&1 || true)
if [ -n "${ERRS}" ]; then
    echo "FAIL"
    echo "${ERRS}"
    echo
    exit 1
fi
echo "PASS"
echo
