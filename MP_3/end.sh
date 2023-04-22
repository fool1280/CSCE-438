#!/bin/bash

kill -9 $(pgrep -f './coord -p 9090' -n)

kill -9 $(pgrep -f './tsd -i 0.0.0.0 -c 9090 -p 3030 -d 1 -t master' -n)
kill -9 $(pgrep -f './tsd -i 0.0.0.0 -c 9090 -p 3031 -d 1 -t slave' -n)

kill -9 $(pgrep -f './tsd -i 0.0.0.0 -c 9090 -p 3032 -d 2 -t master' -n)
kill -9 $(pgrep -f './tsd -i 0.0.0.0 -c 9090 -p 3033 -d 2 -t slave' -n) 

kill -9 $(pgrep -f './tsd -i 0.0.0.0 -c 9090 -p 3034 -d 3 -t master' -n)
kill -9 $(pgrep -f './tsd -i 0.0.0.0 -c 9090 -p 3035 -d 3 -t slave' -n) 

rm -rf /home/csce438/CSCE-438/MP_3/tmp/ /home/csce438/CSCE-438/MP_3/master_*/ /home/csce438/CSCE-438/MP_3/slave_*/
rm -f -r *.txt