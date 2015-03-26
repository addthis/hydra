#!/bin/bash

source /opt/hydra/bin/hydra.sh
name="spawn"
arguments="com.addthis.hydra.Main spawn"

cd /opt/hydra

startLoggedJar $name $arguments