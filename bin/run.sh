bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`
RUN_HOME=`cd "$bin/..">/dev/null; pwd`

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx1000m 

if [ $# = 0 ]
then
  echo "Usage: runCase <command>"
  echo "where <command> an option from one of these categories:"
  echo "  sql_run                   sql : run sql test"
  echo ""
  echo "  sql_example               sql : run sql example"
  echo ""
  echo " or"
  echo "  CLASSNAME        run the class named CLASSNAME"
  exit -1
fi
# get arguments
COMMAND=$1
shift

CONF_DIR=${RUN_HOME}/conf/
# CLASSPATH initially contains $CONF_DIR
CLASSPATH="${CONF_DIR}"
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar

for f in $RUN_HOME/*.jar; do
  if [ -f $f ]
  then
    CLASSPATH=${CLASSPATH}:$f;
  fi
done

# Add libs to CLASSPATH
for f in $RUN_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

for f in `ls /usr/lib/hive/lib/*.jar|grep -v "hive-exec"`; do
  CLASSPATH=${CLASSPATH}:$f;
done

for f in /usr/lib/hadoop/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

for f in /usr/lib/hbase/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# Run

if [ "$COMMAND" = "sql_run" ]
then
  CLASS=io.transwarp.sql.TestSQL
elif [ "$COMMAND" = "sql_example" ]
then
  CLASS=io.transwarp.sql.TestExample
else
  CLASS=$COMMAND
fi

#java -XX:OnOutOfMemoryError="kill -9 %p" $JAVA_HEAP_MAX -ea -classpath "$CLASSPATH" $CLASS "$@"
#java -XX:OnOutOfMemoryError="kill -9 %p" $JAVA_HEAP_MAX -ea -classpath "$CLASSPATH" -Djava.library.path=/usr/local/lib $CLASS "$@"
java -XX:OnOutOfMemoryError="kill -9 %p" $JAVA_HEAP_MAX -classpath "$CLASSPATH" -Djava.library.path=/usr/local/lib $CLASS "$@"
