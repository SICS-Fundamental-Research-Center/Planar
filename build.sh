#!/bin/bash

set -e

cd `dirname "$BASH_SOURCE"`
SCRIPT_DIR=`pwd`

MY_CMAKE_OPTIONS=""
runtests=false
for option in "${@:1}"
do
  case "$option" in
    "test")
      echo "*** Run tests after compilation ***"
      MY_CMAKE_OPTIONS="$MY_CMAKE_OPTIONS -DTEST=ON"
      runtests=true
      ;;

    "doc")
      echo "*** Generate documents after compilation ***"
      MY_CMAKE_OPTIONS="$MY_CMAKE_OPTIONS -Ddoc=ON"
      ;;

    "clean")
      echo "*** Cleaning ***"
      $SCRIPT_DIR/clean.sh
      ;;
  esac
done

mkdir -p build
cd build
cmake $MY_CMAKE_OPTIONS ..
make -j4
if [ "$runtests" = true ] ; then
  echo "Runnning all unit tests..."
  make test
fi

cd $SCRIPT_DIR