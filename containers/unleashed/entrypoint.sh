#!/bin/bash
set -eo pipefail

echo "Starting OpenHands..."
if [[ $NO_SETUP == "true" ]]; then
  echo "Skipping setup, running as $(whoami)"
  "$@"
  exit 0
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "The OpenHands entrypoint.sh must run as root"
  exit 1
fi

if [ -z "$WORKSPACE_MOUNT_PATH" ]; then
  # This is set to /opt/workspace in the Dockerfile. But if the user isn't mounting, we want to unset it so that OpenHands doesn't mount at all
  unset WORKSPACE_BASE
fi

if [[ "$SANDBOX_USER_ID" -eq 0 ]]; then
  echo "Running OpenHands as root"
  export RUN_AS_OPENHANDS=false
  mkdir -p /root/.cache/ms-playwright/
  if [ -d "/home/openhands/.cache/ms-playwright/" ]; then
    mv /home/openhands/.cache/ms-playwright/ /root/.cache/
  fi
fi

/bin/bash -c "${*@Q}" # This magically runs any arguments passed to the script as a command
