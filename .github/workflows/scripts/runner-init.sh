#!/bin/bash

set -e

echo "----- Freeing disk space -----"
# Workaround to provide additional free space for testing.
#   https://github.com/actions/virtual-environments/issues/2840
df -h
sudo rm -rf /usr/share/dotnet
sudo rm -rf /usr/local/lib/android
sudo rm -rf /opt/ghc
sudo rm -rf "/usr/local/share/boost"
sudo rm -rf "$AGENT_TOOLSDIRECTORY"
df -h
echo ""