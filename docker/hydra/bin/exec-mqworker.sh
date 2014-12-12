#!/bin/bash

source /opt/hydra/bin/hydra.sh

name="mqworker"
serverPort=`getParameter mqworker qmaster.mesh.port`
peerPort=`getParameter mqworker qmaster.mesh.peer.port`
rootDir=`getParameter mqworker qmaster.mesh.root`
hardPeers="localhost:$serverPort"

arguments="com.addthis.hydra.Main mqworker server $peerPort $rootDir $hardPeers"

repeatedlyStartJar $name $arguments
