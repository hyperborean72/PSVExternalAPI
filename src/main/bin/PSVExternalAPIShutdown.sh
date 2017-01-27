#!/bin/sh

java -Dlog4j.configuration=file:context/log4j.xml -Duser.timezone=Europe/Moscow -cp PSVExternalAPI.jar ru.csbi.transport.psv.externalapi.RemoteControlImpl localhost:19050 SHUTDOWN