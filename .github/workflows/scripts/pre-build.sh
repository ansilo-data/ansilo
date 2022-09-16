#!/bin/bash

set -e

echo "----- Print env -----"
env
echo ""

echo "----- Configuring ld paths -----"
echo "$JAVA_HOME/lib/server" | sudo tee /etc/ld.so.conf.d/jdk.conf 
sudo ldconfig
echo ""

echo "----- Printing ldconfig -----"
sudo ldconfig -v
echo ""
