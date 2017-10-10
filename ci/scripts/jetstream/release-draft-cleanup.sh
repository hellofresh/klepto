#!/usr/bin/env sh
set -e

# Get all open PR's
OPEN_PRS=$(curl --silent --get "https://${ACCESS_TOKEN}@api.github.com/repos/${REPOSITORY}/pulls?state=open"  | jq -r '.[] | .number')
OPEN_PR_COUNT=$(echo ${OPEN_PRS} | wc -l)

echo "Found ${OPEN_PR_COUNT} open PR's"

# Get draft releases
DRAFTS=$(curl --silent --get "https://${ACCESS_TOKEN}@api.github.com/repos/${REPOSITORY}/releases?per_page=100" | jq -r '.[] | select(.draft==true) | [ .tag_name, .id|tostring ] | join(";")')
DRAFT_COUNT=$(echo ${DRAFTS} | wc -l)

echo "Found ${DRAFT_COUNT} draft releases"

# Loop over all drafts and remove PR's releases that aren't open
printf "\nStarting cleanup:\n"
for DRAFT in ${DRAFTS}; do
    PR_ID=$(echo ${DRAFT} | cut -d\; -f1)
    printf " - ${PR_ID}: "

    for OPEN_PR in ${OPEN_PRS}; do
        if [ "${PR_ID}" = "PR-${OPEN_PR}" ]; then
            echo "Skipping"
            continue 2
        fi
    done

    RELEASE_ID=$( echo ${DRAFT} | cut -d\; -f2 )
    printf "Delete release ${RELEASE_ID}: "
    curl --silent --request DELETE "https://${ACCESS_TOKEN}@api.github.com/repos/${REPOSITORY}/releases/${RELEASE_ID}"

    printf 'done\n'
done


printf '\nCleanup completed\n'
