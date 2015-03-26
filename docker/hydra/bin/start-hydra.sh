#!/bin/bash

cd $HYDRA_ROOT
exec exec-mesh.sh &
exec exec-spawn.sh &
exec exec-mqmaster.sh &
exec exec-mqworker.sh &
exec exec-minion.sh &

while :
do
  sleep 10
done
