#!/usr/bin/env bash
source ./scripts/include.sh

if ! need_rondb; then
  echo "Tarball not needed"
  exit
fi

cd /tmp
completed="${TARBALL_NAME}.tarball-is-complete"
if [ -e "$TARBALL_NAME" ] && [ -e "$completed" ]; then
  echo "RonDB tarball already exists"
  exit
fi
TARBALL_SOURCE=https://repo.hops.works/master/${TARBALL_NAME}
echo "Downloading RonDB tarball source: ${TARBALL_SOURCE}"
rm -f "$completed"
curl --retry 10 --retry-delay 10 --retry-connrefused --no-progress-meter -O "$TARBALL_SOURCE"
touch "$completed"
