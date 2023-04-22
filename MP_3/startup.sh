#!/bin/bash
if [ ! -d tmp ]; then
    mkdir tmp;
fi;

./coord -p 9090 & sleep 0.5

./tsd -i localhost -c 9090 -p 3030 -d 1 -t master &
./tsd -i localhost -c 9090 -p 3031 -d 1 -t slave 

./tsd -i localhost -c 9090 -p 3032 -d 2 -t master &
./tsd -i localhost -c 9090 -p 3033 -d 2 -t slave 

./tsd -i localhost -c 9090 -p 3034 -d 3 -t master &
./tsd -i localhost -c 9090 -p 3035 -d 3 -t slave 