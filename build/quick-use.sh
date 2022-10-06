#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
sourcepath=$(cd `dirname $0`/../; pwd)
. $basepath/env.sh
. $basepath/log.sh
cd $basepath

tag=`cat image/tag`
x=`docker images $tag|wc -l`
if [[ $x -eq 1 ]]; then
    docker pull $tag
fi

docker ps|grep $use_container_name &> /dev/null
if [[ $? -ne 0 ]]; then
    docker run -p 3000:3000 -p 27017:27017 -v `pwd`/data:/data/db/ \
    -v /Users/du/work/sdvdxl/tapdata/manager/tm/src/main/resources/dev/logback.xml:/tapdata/apps/manager/conf/logback.xml \
    -v /Users/du/work/sdvdxl/tapdata/manager/build/start.sh:/tapdata/apps/manager/bin/start.sh \
    -v /Users/du/work/sdvdxl/tapdata/iengine/build/start.sh:/tapdata/apps/iengine/bin/start.sh \
    -e mode=use -itd --name=$use_container_name `cat image/tag` bash
fi

info "tapdata all in one env started, you can use 'bash $sourcepath/bin/tapshell.sh' go into terminal env now..."
bash $sourcepath/bin/tapshell.sh
