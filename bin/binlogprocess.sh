.jar:/opt/pc/BinLogProcess/lib/httpcore-4.1.2.jar:/opt/pc/BinLogProcess/lib/jackson-core-asl-1.9.13.jar:/opt/pc/BinLogProcess/lib/jackson-jaxrs-1.8.3.jar:/opt/pc/#!/bin/bash
##BinLogProcess

#set -x
cd `dirname $0`
BIN_DIR=`pwd`
cd ..
DEPLOY_DIR=`pwd`
CONF_DIR=$DEPLOY_DIR/conf
LOG_DIR=$DEPLOY_DIR/logs
LIB_DIR=$DEPLOY_DIR/lib
LIB_JARS=`ls $LIB_DIR|grep .jar|awk '{print "'$LIB_DIR'/"$0}'|tr "\n" ":"`
LIB_JARS=${LIB_JARS}
LOG_FILE=$LOG_DIR/binlogprocess.log
APP_MAIN_CLASS=com.treefinance.binlog.process.BinLogTransfer

start()
{

        if [ ! -d $LOG_DIR]; then
                mkdir $LOG_DIR
        fi

        if [ ! -e ${CONF_DIR}/instance.properties ]; then
                echo "${CONF_DIR}/instance.properties does not exist "
                exit 0
        fi

        checkpid

        if [ $psid -ne 0 ]; then
                echo "==========================="
                echo "warn: $APP_MAIN_CLASS already started! (pid=$psid) "
                echo "==========================="
        else
                echo "binlog process is starting ..."
                nohup java -server -Xms2g -Xmx4g -classpath $CONF_DIR:$LIB_JARS ${APP_MAIN_CLASS} > ${LOG_FILE} 2>&1 &

                checkpid

                if [ $psid -ne 0 ]; then
                        echo "(pid=$psid) [OK]"
                else
                        echo "[Failed]"
                fi
        fi

}

checkpid()
{
        javaps=`ps -ef | grep ${APP_MAIN_CLASS} | grep -v "grep"`
        if [ -n "$javaps" ]; then
                psid=`echo $javaps | awk '{print $2}'`
        else
                psid=0
        fi
}

stop()
{

        checkpid

        if [ $psid -ne 0 ]; then
                echo -n "Stopping $APP_MAINCLASS ...(pid=$psid) "
                kill -9 $psid
                if [ $? -eq 0 ]; then
                        echo "[OK]"
                else
                        echo "[Failed]"
                fi

                checkpid
                if [ $psid -ne 0 ]; then
                        stop
                fi
        else
                echo "================================"
                echo "warn: $APP_MAINCLASS is not running"
                echo "================================"
        fi

}

status()
{
        checkpid

        if [ $psid -ne 0 ];  then
                echo "$APP_MAINCLASS is running! (pid=$psid)"
        else
                echo "$APP_MAINCLASS is not running"
        fi
}

case "$1" in
   'start')
      start
     ;;
   'stop')
     stop
     ;;
   'restart')
     stop
     start
     ;;
   'status')
     status
     ;;
  *)
     echo "Usage: $0 {start|stop|restart|status|info}"
     exit 1
esac