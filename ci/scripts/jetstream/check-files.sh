#!/usr/bin/env sh
set -e

# Fetch the branch name
BASE_BRANCH=$(git config --get pullrequest.basebranch)

# Detect the changed files
git diff --name-only --diff-filter=ACMR "${BASE_BRANCH}" > changed_files.txt
echo "Affected files: $(cat changed_files.txt | wc -l)"

# Check for ignored files
if [ -s ci/assets/file-checker-ignore ]; then
    echo "Ignored files: $(cat ci/assets/file-checker-ignore | wc -l)"
    grep -v -F -x -f ci/assets/file-checker-ignore changed_files.txt > changed_files_without_ignored.txt
    mv -f changed_files_without_ignored.txt changed_files.txt
fi

set +e
INVALID=""

# Checking Json files
for FILE in $(grep -i -E '\.json$' changed_files.txt); do
    jsonlint -q "${FILE}"
    if [ $? -ne 0 ]; then
        INVALID="${INVALID}${FILE}\n"
    fi
done

# Checking XML files
for FILE in $(grep -i -E '\.(xml|xsd)' changed_files.txt); do
    xmllint "${FILE}"
    if [ $? -ne 0 ]; then
        INVALID="${INVALID}${FILE}\n"
    fi
done

# Checking YAML files
for FILE in $(grep -i -E '\.ya?ml$' changed_files.txt); do
    yaml-lint -q "${FILE}"
    if [ $? -ne 0 ]; then
        INVALID="${INVALID}${FILE}\n"
    fi
done

# Checking PHP files
for FILE in $(grep -i -E '\.ph(h|tml)$' changed_files.txt); do
    php -l "${FILE}"
    if [ $? -ne 0 ]; then
        INVALID="${INVALID}${FILE}\n"
    fi
done

if [ ! -z "${INVALID}" ]; then
    echo "The following files contain invalid syntax:"
    echo -e ${INVALID}
    exit 1
fi

echo "No files with invalid syntax detected"
