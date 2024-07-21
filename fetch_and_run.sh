#!/bin/bash

# This script can help you pull github repository and execute files from the repository.
# See below for usage instructions.

PATH="$PATH:/bin:/usr/bin:/sbin:/usr/sbin:/usr/local/bin:/usr/local/sbin"
BASENAME="${0##*/}"

usage() {
  if [ "${#@}" -ne 0 ]; then
    echo "* ${*}"
    echo
  fi
  cat <<ENDUSAGE
Usage:
export GITHUB_USERNAME="USERNAME"
export GITHUB_AUTH_TOKEN="TOKEN"
export GITHUB_TAG="RELEASE_TAG"
export GITHUB_REPOSITORY_URL="REPOSITORY_URL"
${BASENAME} executable-from-github [ <script arguments> ]
ENDUSAGE

  exit 2
}

# Standard function to print an error and exit with a failing return code
error_exit() {
  echo "${BASENAME} - ${1}" >&2
  exit 1
}

# Check what environment variables are set
if [ -z "${GITHUB_USERNAME}" ]; then
  usage "GITHUB_USERNAME not set. Need USERNAME to get the repository from gitub."
fi

if [ -z "${GITHUB_AUTH_TOKEN}" ]; then
  usage "GITHUB_AUTH_TOKEN not set. Need AUTH-TOKEN to get the repository from gitub."
fi

if [ -z "${GITHUB_TAG}" ]; then
  usage "GITHUB_TAG not set. Need GITHUB_TAG to get script from gitub."
fi

if [ -z "${GITHUB_REPOSITORY_URL}" ]; then
  usage "GITHUB_REPOSITORY_URL not set. Need GITHUB_REPOSITORY_URL to get the script from gitub."
fi

# Check that necessary programs are available
#which aws >/dev/null 2>&1 || error_exit "Unable to find AWS CLI executable."
#which unzip >/dev/null 2>&1 || error_exit "Unable to find unzip executable."

# Create a temporary directory to hold the downloaded contents, and make sure
# it's removed later, unless the user set KEEP_BATCH_FILE_CONTENTS.
cleanup() {
  if [ -z "${KEEP_BATCH_FILE_CONTENTS}" ] &&
    [ -n "${TMPDIR}" ] &&
    [ "${TMPDIR}" != "/" ]; then
    rm -rf "${TMPDIR}"
  fi
}
trap 'cleanup' EXIT HUP INT QUIT TERM

# mktemp arguments are not very portable.  We make a temporary directory with
# portable arguments, then use a consistent filename within.
TMPDIR="$(mktemp -d -t tmp.XXXXXXXXX)" || error_exit "Failed to create temp directory."
TMPFILE="${TMPDIR}/batch-file-temp"
install -m 0600 /dev/null "${TMPFILE}" || error_exit "Failed to create temp file."

# Fetch and run a script
fetch_repository() {
  # Create a temporary directory and clone the repo
  cd "${TMPDIR}" || error_exit "Unable to cd to temporary directory."

  # Configure git to use /tmp/.git-credentials
  git config --global credential.helper 'store --file /tmp/.git-credentials' || error_exit "Failed to configure git to use /tmp/.git-credentials"
  # Insert credentials in /tmp/.git-credentials
  echo "https://${GITHUB_USERNAME}:${GITHUB_AUTH_TOKEN}@github.com" >/tmp/.git-credentials || error_exit "Failed to save data to /tmp/.git-credentials"
  # Clone the repo with submodule
  git clone -j8 --recurse-submodule --branch ${GITHUB_TAG} https://${GITHUB_REPOSITORY_URL} || error_exit "Failed to get scripts from Github."
}

# Executing commands passed as arguments
execute_command() {

  BASEDIR=$(echo ${GITHUB_REPOSITORY_URL} | rev | cut -d/ -f1 | rev | cut -d. -f1)
  cd "${TMPDIR}/${BASEDIR}" || exit
  echo "Executing command: ${@}"
  "${@}" || error_exit " Failed to execute script."
}

# Main - dispatch user request to appropriate function
if [ ${#@} -eq 0 ]; then
  usage "Script requires at least one argument to run from inside"
fi
fetch_repository
execute_command "${@}"
cleanup
