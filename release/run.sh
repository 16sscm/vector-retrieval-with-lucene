#!/bin/bash
set -e
set -x

G_BASE_PATH=$(cd `dirname $0`; pwd)
cd ${G_BASE_PATH}

java  -jar -server -Xmx8g -Xms8g -Xmn2g -Xss16m  /root/resume-search-*.jar
