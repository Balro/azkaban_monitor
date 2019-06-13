#!/bin/bash
. /etc/profile
. ~/.bash_profile
BIN=`dirname $0`
BIN=`cd -P $BIN/../;pwd`

MONITOR_OPTS="-Xmx256m"

zkcli="/opt/cloudera/parcels/CDH/lib/zookeeper/bin/zkCli.sh -server bigdata01:2181"

for f in $(find $BIN/lib -type f); do
  if [ "$CLASSPATH" ]; then
    CLASSPATH=$CLASSPATH:$f
  else
    CLASSPATH=$f
  fi
done

CLASSPATH=$CLASSPATH:$BIN/conf

log=$BIN/logs/monitor.out

start() {
  is_run=`$zkcli get /baluo/monitor/azkb/heartbeat 2>&1 | grep "Node does not exist" | wc -l`
  if [ $is_run -eq 0 ]; then
    echo "AzkbMonitor is running!"
    status
    exit 1
  fi
  nohup java $MONITOR_OPTS -cp $CLASSPATH demo.baluo.monitor.app.AZKBMonitor >> $log 2>&1 &
}

stop() {
  ps -ef | grep demo.baluo.monitor.app.AZKBMonitor | grep -v grep | awk '{print $2}' | xargs kill -15
}

status() {
 $zkcli get /baluo/monitor/azkb/heartbeat 2>&1 | grep -A 5 "WatchedEvent state"
}

USAGE="$0 <start|stop|status>"

case $1 in
  start)
    start
  ;;
  stop)
    stop
  ;;
  status)
    status
  ;;
  *)
    echo "$USAGE"
  ;;
esac