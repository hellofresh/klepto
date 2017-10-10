#!/usr/bin/env bash
set -e

DIR=$( cd "$( dirname "$0" )" && pwd )

# Update following line with correct pipeline
PIPELINE=klepto

#############################################################

# Ensure we have a fly target
# The target is depending on how you named the fly target during 'fly --login ...'.
if [ -z "${FLY_TARGET}" ]; then
    echo "Missing FLY_TARGET environment variable"
    exit 1
fi

# Load pipeline vars
VARS_FILE="${DIR}/assets/vars.yml"
VARS_FILE_ENCRYPTED="${DIR}/assets/vars.yml.enc"
if [ -f "${VARS_FILE_ENCRYPTED}" ]; then
    if [ -z "${ANSIBLE_VAULT_PASSWORD_FILE}" ]; then
        if [ ! -z "${VPASS}" ]; then
            ANSIBLE_VAULT_PASSWORD_FILE="${VPASS}"
        else
            ANSIBLE_VAULT_PASSWORD_FILE="$(dirname ${DIR})/automation/.vpass"
        fi
    fi

    if [ ! -f "${ANSIBLE_VAULT_PASSWORD_FILE}" ]; then
        echo "Missing .vpass file. Do you have \${ANSIBLE_VAULT_PASSWORD_FILE} set?\n"
        exit 1
    fi

    LOAD_VARS=$(ansible-vault --vault-password-file="${ANSIBLE_VAULT_PASSWORD_FILE}" decrypt "${VARS_FILE_ENCRYPTED}" --output=-)
elif [ -f "${VARS_FILE}" ]; then
    LOAD_VARS=$(cat "${VARS_FILE}")
else
    echo "Missing vars.yml.enc or vars.yml file\n"
    exit 1
fi

# Set the pipeline
fly -t "${FLY_TARGET}" set-pipeline -p "${PIPELINE}" -c "${DIR}/pipeline.yml" --load-vars-from=<(echo "${LOAD_VARS}")
