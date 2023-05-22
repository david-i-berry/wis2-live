#!/bin/bash
# terminate current subscription (if it exists)
kill $(cat sub.pid)
kill $(cat p0.pid)
kill $(cat p1.pid)
kill $(cat p2.pid)
kill $(cat p3.pid)
# start new subscriber
nohup python3 wis2-subscribe.py >& subscribe-`date --iso-8601=minutes`.log &
# get pid so we can kill if we need to restart
echo $! > sub.pid
nohup python3 processor.py 0 >& p0.log &
echo $! > p0.pid
nohup python3 processor.py 1 >& p1.log &
echo $! > p1.pid
nohup python3 processor.py 2 >& p2.log &
echo $! > p2.pid
nohup python3 processor.py 3 >& p3.log &
echo $! > p3.pid