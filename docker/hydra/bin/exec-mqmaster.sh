#!/bin/bash

source /opt/hydra/bin/hydra.sh
name="mqmaster"
arguments="com.addthis.hydra.Main mqmaster"

cd /opt/hydra

startLoggedJar $name $arguments