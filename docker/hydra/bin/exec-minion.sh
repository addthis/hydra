#!/bin/bash

source /opt/hydra/bin/hydra.sh
name="minion"
arguments="com.addthis.hydra.Main minion"

cd /opt/hydra

startLoggedJar $name $arguments
