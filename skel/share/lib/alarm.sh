##
#   Prints a list of all internally predefined alarms
#
list_alarms()
{
    CLASSPATH="$(getProperty dcache.paths.classpath)" quickJava org.dcache.alarms.shell.ListPredefinedTypes
}

##
#   Invokes the main method of SendAlarm with the given arguments
#
send_alarm() # $@ = [-d=DOMAIN] [-s=SERVICE] [-t=TYPE] message
{
    local host
    local port

    host=$(getProperty dcache.log.server.host)
    port=$(getProperty dcache.log.server.port)

    CLASSPATH="$(getProperty dcache.paths.classpath)" quickJava org.dcache.alarms.shell.SendAlarmCLI -r=${host} -p=${port} "$@"
}
