#!/bin/bash
set -e
set -x

G_BASE_PATH=$(cd `dirname $0`; pwd)
cd ${G_BASE_PATH}

\rm -rf ~/.gradle
unzip -u tools/gradle-*-bin.zip
./gradle-*/bin/gradle clean bootJar --no-daemon
\cp -rf ./build/libs/resume-search-*.jar ./release
\rm -rf ~/.gradle
