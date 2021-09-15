#!/bin/sh
maxSize=1000
activeHome=/home/apache-activemq-5.16.3
monitorSize=$($activeHome/bin/activemq query -QQueue=log.service | grep QueueSize | awk '{print $3}')
if [[ -n $monitorSize ]] && [[ $monitorSize -gt $maxSize ]]; then
 pid=$(ps -ef | grep activemq | grep 'java -Xms' | awk '{print $2}')
 kill -9 $pid
 rm -rf $activeHome/data/*
 $activeHome/bin/activemq start
 exit 1
fi
