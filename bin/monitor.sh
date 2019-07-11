#!/bin/bash
. /etc/profile
. ~/.bash_profile

HOME=`dirname $0`
HOME=`cd -P $HOME/../;pwd`

# log dir
export LOG_DIR=$HOME/logs

# jvm opts
MONITOR_OPTS="-Xmx256m"

zkcli="/opt/cloudera/parcels/CDH/lib/zookeeper/bin/zkCli.sh -server bigdata01:2181"

# classpath
CLASSPATH=
for f in $(find $HOME/lib -type f); do
  if [ "$CLASSPATH" ]; then
    CLASSPATH=$CLASSPATH:$f
  else
    CLASSPATH=$f
  fi
done
CLASSPATH=$CLASSPATH:$HOME/lib
CLASSPATH=$CLASSPATH:$HOME/conf

start() {
  is_run=`$zkcli get /baluo/monitor/azkb/heartbeat 2>&1 | grep "Node does not exist" | wc -l`
  if [ $is_run -eq 0 ]; then
    echo "AzkbMonitor is running!"
    status
    exit 1
  fi
  nohup java $MONITOR_OPTS -cp $CLASSPATH baluo.monitor.azkaban.AzkabanBMonitor >> $LOG_DIR/start.out 2>&1 &
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
  restart)
    stop
    start
   ;;
  *)
    echo "$USAGE"
  ;;
esac
