#!/bin/bash
set -e
set -x

G_BASE_PATH=$(cd `dirname $0`; pwd)
cd ${G_BASE_PATH}

if [ -f release/FilterKnn.tar.gz ]; then
    \rm -rf release/FilterKnn.tar.gz
fi
aws s3 cp s3://htm-test/BaiXuefeng/FilterKnn/2021-12-23/FilterKnn.tar.gz release/

\rm -rf ~/.gradle

if [ -f gradle-*-bin.zip ]; then
    \rm -rf gradle-*-bin.zip
fi
aws s3 cp s3://htm-test/BaiXuefeng/FilterKnn/gradle-7.3.3-bin.zip .
unzip -u gradle-*-bin.zip

if [ -f release/resume-search-*.jar ]; then
    \rm -rf release/resume-search-*.jar
fi
./gradle-*/bin/gradle clean bootJar --no-daemon
\cp -rf ./build/libs/resume-search-*.jar ./release

\rm -rf ~/.gradle
