#!/bin/bash

CDIR="$(pwd)"
echo "Current Directory "
echo $CDIR

echo "Dependency Packages"
case "$OSTYPE" in
  darwin*)
    brew install folly
    ;;
  linux*)
    apt-get install -y g++ curl git cmake build-essential autoconf libtool
    ;;
esac

# Download third party header libraries.
echo "Updating Submodules"
cd $CDIR
git submodule update --init --recursive