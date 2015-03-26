#!/bin/bash

source /opt/hydra/bin/hydra.sh
name="mesh"
arguments="com.addthis.hydra.Main mss"

cd /opt/hydra

startLoggedJar $name $arguments