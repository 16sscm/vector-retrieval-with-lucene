#!/bin/bash
set -e
set -x

G_BASE_PATH=$(cd `dirname $0`; pwd)
cd ${G_BASE_PATH}


#---- 参数配置 ---------------------------------------------------------

# docker label
DOCKER_LABEL=

#----------------------------------------------------------------------

LOWER_LABEL=${DOCKER_LABEL,,}
LABEL_NAME=${LOWER_LABEL:-$(date +%Y_%m_%d)}
IMAGE_NAME=vector-retrieval-base:${LABEL_NAME}
TAR_NAME=vector-retrieval-base_${LABEL_NAME}.tar
docker build . -t ${IMAGE_NAME}
docker save ${IMAGE_NAME} -o ${TAR_NAME}
