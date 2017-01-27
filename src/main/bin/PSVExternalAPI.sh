#!/bin/sh

JMX_SETTINGS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=19050 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
GC_SETTINGS="-XX:+UseParallelGC -XX:+AggressiveOpts -XX:+UseFastAccessorMethods -Xms5G -Xmx10G"
HIBERNATE_SETTINGS="-Dhibernate.bytecode.provider=cglib"
JVM_ARGS="$GC_SETTINGS $JMX_SETTINGS  $HIBERNATE_SETTINGS -Dlog4j.configuration=file:context/log4j.xml -Duser.timezone=Europe/Moscow"

#java -server $JVM_ARGS -jar PSVExternalAPI.jar
java -server $JVM_ARGS -jar PSVExternalAPI.jar 1>/dev/null 2>&1 &
