#!/bin/bash
# terminate current subscription (if it exists)
kill $(cat sub.pid)
# start new subscriber
nohup python3 wis2-subscribe.py >& subscribe-`date --iso-8601=minutes`.log &
# get pid so we can kill if we need to restart
echo $! > sub.pid