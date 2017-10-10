#!/usr/bin/env sh
set -e

PR_ID=$(git config --get pullrequest.id)
PR_AUTHOR=$(git log -n 1 --pretty=format:'%an')
VERSION="PR-${PR_ID}"

echo "${VERSION}" > ../draft-version/version
echo -e "**NOT FOR PRODUCTION**\nPR: #${PR_ID}, by ${PR_AUTHOR}\nDeploy version: \`${VERSION}\`\nDeploy using: \`ansible-playbook -i ../staging.ini -t deployment -e deployment_force=true -e deployment_github_release_type=draft -e deployment_version=${VERSION}\`" > ../draft-version/message
