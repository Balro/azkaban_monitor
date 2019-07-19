#!/bin/bash
. /etc/profile
. ~/.bash_profile

MONITOR_HOME=`dirname $0`
MONITOR_HOME=`cd -P $MONITOR_HOME/../;pwd`

# log dir
export LOG_DIR=$MONITOR_HOME/logs
mkdir -p $LOG_DIR

# jvm opts
MONITOR_OPTS="-Xmx256m"

# classpath
CLASSPATH=
for f in $(find $MONITOR_HOME/lib -type f); do
  if [ "$CLASSPATH" ]; then
    CLASSPATH=$CLASSPATH:$f
  else
    CLASSPATH=$f
  fi
done
CLASSPATH=$CLASSPATH:$MONITOR_HOME/lib
CLASSPATH=$CLASSPATH:$MONITOR_HOME/conf

MAIN_CLASS=balro.monitor.azkaban.AzkabanMonitor

start() {
  status_res=`status 2>&1`
  if [ "$status_res" == "Azkaban-monitor is down." ]; then
    nohup java $MONITOR_OPTS -cp $CLASSPATH $MAIN_CLASS start >> $LOG_DIR/start.out 2>&1 &
    sleep 3s
  else
    echo "Azkaban-monitor is running."
  fi
  status
}

stop() {
  ps -ef | grep $MAIN_CLASS | grep -v grep | awk '{print $2}' | xargs kill -15
  sleep 3s
  status
}

status() {
  java -cp $CLASSPATH $MAIN_CLASS status
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
