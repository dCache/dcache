#!/bin/sh
#
# $Id: deploy_srmv2.sh,v 1.27 2006-11-21 21:34:02 timur Exp $
#


if [ $# -gt 1 ]; then
	echo "Usage: $0 [setup file]";
	exit 1;
fi

SETUP_FILE=$1
if [ "x${SETUP_FILE}" = "x" ]; then
	SETUP_FILE="./srm_setup.env"
fi


if [ ! -r ${SETUP_FILE} ] ; then
    echo "can't find ${SETUP_FILE}" 2>&1
    exit 1
fi

. ${SETUP_FILE}

#
# tomcat started in other process, so export JAVA_HOME
#
export JAVA_HOME


#
# add hostname into srmDomain
#
shortHostname=`hostname | awk -F. '{print $1}'`


DEBUG=false
#
# check that everything is set and built
#
if [ "${JAVA_HOME}" = "" ] ; then
  echo "JAVA_HOME enviroment variable is not defined " 1>&2
  exit 1
fi
JAVA=${JAVA_HOME}/bin/java

if [ ! -f ${SRM_JARS_DIR}/srm.jar ] ; then
  echo SRM_JARS_DIR=${SRM_JARS_DIR} not found or srm is not built 1>&2
  exit 1
fi
if [ ! -f ${CELLS_JARS_DIR}/cells-protocols.jar ] ; then
  echo "CELLS_JARS_DIR=${CELLS_JARS_DIR} not found or cells' cells-protocols.jar is not built" 1>&2
  exit 1
fi
if [ ! -f ${DCACHE_JARS_DIR}/dcache.jar ] ; then
  echo DCACHE_JARS_DIR=${DCACHE_JARS_DIR} not found or dcache is not built 1>&2
  exit 1
fi
if [ ! -d ${AXIS_BASE_PATH} ] ; then
  echo AXIS_BASE_PATH=${AXIS_BASE_PATH} not found 1>&2
  exit 1
fi
if [ ! -d ${TOMCAT_BASE_PATH} ] ; then
  echo TOMCAT_BASE_PATH=${TOMCAT_BASE_PATH} not found 1>&2
  exit 1
fi
if [ ! -f ${AXIS_ARCHIVE} ] ; then
  echo AXIS_ARCHIVE=${AXIS_ARCHIVE} not found 1>&2
  exit 1
fi
if [ ! -f ${TOMCAT_ARCHIVE} ] ; then
  echo TOMCAT_ARCHIVE=${TOMCAT_ARCHIVE} not found 1>&2
  exit 1
fi
if [ ! -d ${GLOBUS_JARS_DIR} ] ; then
  echo GLOBUS_JARS_DIR=${GLOBUS_JARS_DIR} not found 1>&2
  exit 1
fi
if [ ! -f ${SRM_BATCH} ] ; then
  echo SRM_BATCH=${SRM_BATCH} not found 1>&2
  exit 1
fi
if [ ! -f ${SRM_SETUP} ] ; then
  echo SRM_SETUP=${SRM_SETUP} not found 1>&2
  exit 1
fi
if [ ! -d ${DCACHE_HOME} ] ; then
  echo DCACHE_HOME=${DCACHE_HOME} not found 1>&2
  exit 1
fi
if [ ! -f ${SRMV1_WSDD} ] ; then
  echo SRMV1_WSDD=${SRMV1_WSDD} not found 1>&2
  exit 1
fi
if [ ! -f ${SRMV2_WSDD} ] ; then
  echo SRMV2_WSDD=${SRMV2_WSDD} not found 1>&2
  exit 1
fi
if [ ! -f ${SRM_LOG_CONFIG} ] ; then
  echo SRM_LOG_CONFIG=${SRM_LOG_CONFIG} not found 1>&2
  exit 1
fi
# compute name of axis dir
AXIS_PATH=${AXIS_BASE_PATH}/`basename ${AXIS_ARCHIVE} | sed "s/\.tar\.gz$//"`
# compute name of tomcat dir
TOMCAT_PATH=${TOMCAT_BASE_PATH}/`basename ${TOMCAT_ARCHIVE} | sed "s/\.tar\.gz$//"`
# if old axis exists, move it
if [ -e ${AXIS_PATH} ] ; then
  if [ "$DELETE_OLD_AXIS_TOMCAT" = "true" ] ; then
    echo deleting previous version of Axis at ${AXIS_PATH}
    rm -rf ${AXIS_PATH}
  else
    OLD_AXIS=${AXIS_PATH}.${RANDOM}
    echo moving previous version at ${AXIS_PATH} to ${OLD_AXIS}
    mv ${AXIS_PATH} ${OLD_AXIS}
  fi
fi
# if old tomcat exists, stop it and move it
if [ -e ${TOMCAT_PATH} ] ; then
  OLD_TOMCAT=${TOMCAT_PATH}.${RANDOM}
  ${TOMCAT_PATH}/bin/shutdown.sh
  if [ "$DELETE_OLD_AXIS_TOMCAT" = "true" ] ; then
    echo deleting previous version of Tomcat at ${TOMCAT_PATH}
    rm -rf ${TOMCAT_PATH}
  else
    echo moving previous version of tomcat at {TOMCAT_PATH} to ${OLD_TOMCAT}
    mv ${TOMCAT_PATH} ${OLD_TOMCAT}
  fi
fi
echo AXIS_PATH=$AXIS_PATH
echo TOMCAT_PATH=$TOMCAT_PATH
export CATALINA_HOME=$TOMCAT_PATH

# some temp file
tmp=/tmp/substitute`date  +%h%d%H%M%s%m`
tmp1=/tmp/tmp1`date  +%h%d%H%M%s%m`
tmp2=/tmp/tmp2`date  +%h%d%H%M%s%m`
tmp3=/tmp/tmp3`date  +%h%d%H%M%s%m`

#execute will execute command and exit if its command fails
execute() {
  command="$*"
  if [ "${DEBUG}" = "true" ]
  then 
      echo "executing [ ${command} ]"
  fi
  ${command}
  rc=$?
  if [ $rc != 0 ]
  then
    echo "execution of command [ ${command} ] failed with return code $rc" 1>&1
    exit $rc 
  fi
}
# no matter what the reason for exiting, delete the temp files
trap 'rm -f $tmp $tmp1 $tmp2 $tmp3 >/dev/null 2>&1' 0
trap "exit 1" 1 2 3 15

#comment_multiline will comment multiline elements in xml documents
comment_multiline ()
{
    pattern="s/(";
    first=true
    for i in $1;
    do
        #first remove elements that perl might not be happy with
        element=`echo $i | sed -e 's/"/./g' -e 's#/#\\\/#g'`
        if [ "$first" = "true" ] ; then
           pattern=${pattern}${element}
        else
           pattern=${pattern}'\s*.\s*'${element}
        fi
        first=false
    done;
    pattern=${pattern}')/<!-- $1 -->/s';
    perl -00pe "${pattern}" $2 >${tmp}
    cat ${tmp} >$2
}


printf "installing tomcat and axis ..."
cmd="tar --directory=${AXIS_BASE_PATH} -xzf ${AXIS_ARCHIVE}"
execute "$cmd"
cmd="tar --directory=${TOMCAT_BASE_PATH} -xzf ${TOMCAT_ARCHIVE}"
execute "$cmd"

if [ ! -d ${AXIS_PATH} ] ; then
  echo AXIS_PATH=${AXIS_PATH} not found 1>&2
  exit 1
fi
if [ ! -d ${TOMCAT_PATH} ] ; then
  echo TOMCAT_PATH=${TOMCAT_PATH} not found 1>&2
  exit 1
fi
AXIS_WEBAPP_DIR="${AXIS_PATH}/webapps/axis"
echo Done

#
# globus jars go to common directory so that the credentials are created by the top level tomcat class loader
#
for i in ${GLOBUS_JARS_DIR}/*jar ${DCACHE_JARS_DIR}/security/*.jar
do
   cmd="cp ${i} ${TOMCAT_PATH}/common/lib"
#   echo "$cmd"
   execute "$cmd"
done
#cmd="mv ${TOMCAT_PATH}/common/lib/cog-axis.jar ${AXIS_WEBAPP_DIR}/WEB-INF/lib"
#execute "$cmd"
#cmd="mv ${TOMCAT_PATH}/common/lib/cog-url.jar ${AXIS_WEBAPP_DIR}/WEB-INF/lib"
#execute "$cmd"


mv ${TOMCAT_PATH}/common/lib/cog-tomcat.jar ${TOMCAT_PATH}/server/lib/
mv ${TOMCAT_PATH}/common/lib/cog-url.jar ${TOMCAT_PATH}/server/lib/
cmd="cp ${CELLS_JARS_DIR}/cells-protocols.jar ${TOMCAT_PATH}/server/lib/"
execute "$cmd"
#cmd="cp ${SRM_JARS_DIR}/srm-tomcat.jar ${TOMCAT_PATH}/server/lib/"
#execute "$cmd"

#
# To get the correct parsers for opensaml-1.0.1, dom3 jars must go to common/endorsed directory in place of those there
#
#mv ${AXIS_WEBAPP_DIR}/WEB-INF/lib/dom3*.jar ${TOMCAT_PATH}/common/endorsed/.
#mkdir ${TOMCAT_PATH}/common/endorsed/notused
#mv ${TOMCAT_PATH}/common/endorsed/xercesImpl.jar ${TOMCAT_PATH}/common/endorsed/notused/.
#mv ${TOMCAT_PATH}/common/endorsed/xmlParserAPIs.jar ${TOMCAT_PATH}/common/endorsed/notused/. 

echo "Done"

CATALINA_SH=${TOMCAT_PATH}/bin/catalina.sh
SETCLASS_SH=${TOMCAT_PATH}/bin/setclasspath.sh

add_java_opt()
{
  java_opt=$1
  java_opt_line="JAVA_OPTS=\"\${JAVA_OPTS} $java_opt\""
  grep -m 1 -e ".*$java_opt.*" ${CATALINA_SH} > /dev/null
  if [ $? == 1 ] ; then
    sed -e  "s/\(.*bootstrap.jar.*\)/\1\n\n$java_opt_line/"  ${CATALINA_SH} > $tmp
    mv $tmp ${CATALINA_SH}
    chmod 755 ${CATALINA_SH}
  fi
}

printf "modifying java options in ${CATALINA_SH} ...\n"
add_java_opt "-Djava.protocol.handler.pkgs=org.globus.net.protocol"

add_classpath()
{
  ADDED_PATH=$1
  CATALINA_CP_ADD1=CLASSPATH=\\\"\\\$CLASSPATH\\\":\\\"\\\$CATALINA_HOME\\\"/$ADDED_PATH
  echo $CATALINA_CP_ADD1 | sed "s/\//\\\\\//g" > $tmp
  CATALINA_CP_ADD=`cat $tmp`
  grep -m 1 -e $ADDED_PATH ${SETCLASS_SH} > /dev/null
  if [ $? == 1 ] ; then
    cp -p ${SETCLASS_SH} $tmp
    echo >> $tmp
    echo ${CATALINA_CP_ADD} | sed "s/\\\//g" >> $tmp
    mv $tmp ${SETCLASS_SH} 
  fi
}

printf "modifying system CLASSPATH in ${SETCLASS_SH} ...\n"
add_classpath "server/lib/cells-protocols.jar"
add_classpath "server/lib/cog-url.jar"


if [ -d ${SRM_WEBAPP_DIR} ] ; then
printf "Removing previous srm webapp directory"
rm -rf ${SRM_WEBAPP_DIR}  
fi

printf "Creating srm webapp directory"
mkdir ${SRM_WEBAPP_DIR}
mkdir ${SRM_WEBAPP_DIR}/WEB-INF
mkdir ${SRM_WEBAPP_DIR}/WEB-INF/lib

#
# copy jar files to srm webapp dir
#
for i in ${AXIS_WEBAPP_DIR}/WEB-INF/lib/*jar ${SRM_JARS_DIR}/glue/*jar ${SRM_JARS_DIR}/srm.jar ${SRM_JARS_DIR}/jdbc-drivers/*.jar ${SRM_JARS_DIR}/jdom/jdom.jar ${SRM_JARS_DIR}/concurrent/concurrent.jar ${DCACHE_JARS_DIR}/dcache.jar ${DCACHE_JARS_DIR}/dcache-srm.jar ${DCACHE_JARS_DIR}/gplazma.jar ${SRM_JARS_DIR}/gplazma-libs/*.jar ${DCACHE_JARS_DIR}/cells.jar ${DCACHE_JARS_DIR}/jpox/*.jar ${DCACHE_JARS_DIR}/smc/*.jar
do
   cmd="cp ${i} ${SRM_WEBAPP_DIR}/WEB-INF/lib"
#   echo $cmd
   execute "$cmd"
done

cmd="mv ${TOMCAT_PATH}/common/lib/cog-axis.jar ${SRM_WEBAPP_DIR}/WEB-INF/lib"
#   echo $cmd
execute "$cmd"

#
# copy web.xml to srm webapp dir
#
cmd="cp ${AXIS_WEBAPP_DIR}/WEB-INF/web.xml ${SRM_WEBAPP_DIR}/WEB-INF/."
execute "$cmd"

# Deploy SRM web service
printf "Creating srm webapp deployment file"
cat >>${TOMCAT_PATH}/conf/Catalina/localhost/srm.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<Context path="/srm" docBase="${SRM_WEBAPP_DIR}" />
EOF

echo "Done"
TOMCAT_CONFIG="${TOMCAT_PATH}/conf/server.xml"

if [ -z "${TOMCAT_PORT}" ] ; then 
  TOMCAT_PORT=8080
fi

if [ -n "${TOMCAT_PORT}" -a ${TOMCAT_PORT} -gt 0 -a ${TOMCAT_PORT} -ne 8080 ] ; then
  printf "modifying default tomcat port to ${TOMCAT_PORT} ..."
  cp -p ${TOMCAT_CONFIG} $tmp
  sed -e "s/8080/${TOMCAT_PORT}/g" ${TOMCAT_CONFIG} >$tmp
  mv $tmp ${TOMCAT_CONFIG}
  echo "Done"
fi


printf "Starting up tomcat ..."
#
# startup tomcat
#
cmd="${TOMCAT_PATH}/bin/startup.sh"
execute ${cmd}
if [ "${DEBUG}" = "true" ] 
then
	echo sleeping for 5 seconds to let tomcat complete its strartup
fi
sleep ${TOMCAT_STARTUP_DELAY}
echo "Done"
#
# setup axis classpath for runnning the axis deploy tool
#
echo "deploying srm v2 application using axis AdminClient ..."
unset AXIS_CLASSPATH
export AXIS_CLASSPATH
for i in ${AXIS_PATH}/lib/*jar ; do  AXIS_CLASSPATH=${AXIS_CLASSPATH}${AXIS_CLASSPATH+:}${i}; done
cmd="${JAVA} -cp ${AXIS_CLASSPATH}:${SRM_JARS_DIR}/srm.jar org.apache.axis.client.AdminClient -lhttp://localhost:${TOMCAT_PORT}/srm/servlet/AxisServlet ${SRMV1_WSDD}"
execute ${cmd}
cmd="${JAVA} -cp ${AXIS_CLASSPATH}:${SRM_JARS_DIR}/srm.jar org.apache.axis.client.AdminClient -lhttp://localhost:${TOMCAT_PORT}/srm/servlet/AxisServlet ${SRMV2_WSDD}"
execute ${cmd}
echo "Done"

AXIS_WEB_XML="${AXIS_WEBAPP_DIR}/WEB-INF/web.xml"
SRM_WEB_XML="${SRM_WEBAPP_DIR}/WEB-INF/web.xml"
printf "creating config files and adding configuration info into ${SRM_WEB_XML} ..."
DCACHE_PARAMS_CONFIG="${SRM_WEBAPP_DIR}/WEB-INF/dCacheParams.xml"
echo "<parameters>">${DCACHE_PARAMS_CONFIG}
echo "  <parameter>srm-${shortHostname}Domain</parameter>">>${DCACHE_PARAMS_CONFIG}
echo "  <parameter>-param</parameter>">>${DCACHE_PARAMS_CONFIG}
echo "  <parameter>setupFile=${SRM_SETUP}</parameter>">>${DCACHE_PARAMS_CONFIG}
echo "  <parameter>ourHomeDir=${DCACHE_HOME}</parameter>">>${DCACHE_PARAMS_CONFIG}
echo "  <parameter>ourName=srm</parameter>">>${DCACHE_PARAMS_CONFIG}
echo "  <parameter>-batch</parameter>">>${DCACHE_PARAMS_CONFIG}
echo "  <parameter>${SRM_BATCH}</parameter>">>${DCACHE_PARAMS_CONFIG}
echo "</parameters>">>${DCACHE_PARAMS_CONFIG}

DEST_SRM_LOG_CONFIG="${SRM_WEBAPP_DIR}/WEB-INF/logConfig.xml"
cmd="cp ${SRM_LOG_CONFIG} ${DEST_SRM_LOG_CONFIG}"
execute ${cmd}


SRM_CONFIG="${SRM_WEBAPP_DIR}/WEB-INF/srmConfigFile.xml"
echo "<SRMConfigInfo>">${SRM_CONFIG}
echo "  <dCacheParametersFileName> ${DCACHE_PARAMS_CONFIG} </dCacheParametersFileName>">>${SRM_CONFIG}
echo "  <logFile> ${DEST_SRM_LOG_CONFIG} </logFile>">>${SRM_CONFIG}
echo "  <storageClassName> ${SRM_STORAGE_CLASS} </storageClassName>">>${SRM_CONFIG}
echo "</SRMConfigInfo>">>${SRM_CONFIG}

#find the line number containing "<servlet-mapping> - this is the beginning of the service section
n=`grep -n -m 1 "^\W*<servlet-mapping>" ${AXIS_WEB_XML} | sed "s/:.*$//"`
((n--))
#break file in two parts
sed -n "1,${n}p" ${AXIS_WEB_XML} >${tmp1}
((n++))
sed -n "${n},\$p" ${AXIS_WEB_XML} >${tmp3}
cat >${tmp2} <<EOF
  <servlet-mapping>
    <servlet-name>AxisServlet</servlet-name>
    <url-pattern>/*</url-pattern>
  </servlet-mapping>

EOF

#now put it all together
cat ${tmp1} ${tmp2} ${tmp3} > ${SRM_WEB_XML}

WEB_XML_TMP="${SRM_WEBAPP_DIR}/WEB-INF/web.xml.tmp"
grep -v "</web-app>" ${SRM_WEB_XML} > ${WEB_XML_TMP}

echo " <env-entry>" >> ${WEB_XML_TMP}
echo " <env-entry-name>srmConfigFile</env-entry-name>" >> ${WEB_XML_TMP}
echo "  <env-entry-value>${SRM_WEBAPP_DIR}/WEB-INF/srmConfigFile.xml</env-entry-value>" >> ${WEB_XML_TMP}
echo "  <env-entry-type>java.lang.String</env-entry-type>" >> ${WEB_XML_TMP}
echo " </env-entry>" >> ${WEB_XML_TMP}
echo " </web-app>" >> ${WEB_XML_TMP}
mv ${WEB_XML_TMP} ${SRM_WEB_XML}


# comment out web.xml parts related to SOAPMonitorService
comment_multiline " <servlet-mapping>
    <servlet-name>SOAPMonitorService</servlet-name>
    <url-pattern>/SOAPMonitor</url-pattern>
  </servlet-mapping>" ${SRM_WEB_XML}

comment_multiline "<servlet>
    <servlet-name>SOAPMonitorService</servlet-name>
    <display-name>SOAPMonitorService</display-name>
    <servlet-class>
        org.apache.axis.monitor.SOAPMonitorService
    </servlet-class>
    <init-param>
      <param-name>SOAPMonitorPort</param-name>
      <param-value>5001</param-value>
    </init-param>
    <load-on-startup>100</load-on-startup>
  </servlet>" ${SRM_WEB_XML}

rm -rf ${AXIS_PATH}
echo "Done"

printf "enabling GSI HTTP in tomcat by modifying ${TOMCAT_CONFIG} ..."
#find the line number containing "<Service" - this is the beginning of the service section
n=`grep -n -m 1 "^\W*<Service" ${TOMCAT_CONFIG} | sed "s/:.*$//"`
#break file in two parts
sed -n "1,${n}p" ${TOMCAT_CONFIG} >${tmp1}
((n++))
sed -n "${n},\$p" ${TOMCAT_CONFIG} >${tmp3}
#now fill the middle
cat >${tmp2} <<EOF
    <!-- Define a GSI HTTPS/1.1 Connector on port 8443 
         Supported parameters include:
         proxy="/path/to/file" // proxy file for server to use
            - or -
         cert="/path/to/file"  // server certificate file in PEM format 
         key="/path/to/file"   // server key file in PEM format

         cacertdir="/path/to/dir" // directory containing trusted CA certs

         mode="ssl"            // use 'standard' SSL via "https://" - default
            - or -     
         mode="gsi"            // use 'GSI' SSL via "httpg://" to delegate 
                               // a (proxy) credential over TLS
              
    -->
    <Connector
               className="org.globus.tomcat.coyote.net.HTTPSConnector"
EOF

echo "               port=\"${SRM_V2_PORT}\""  >> ${tmp2}

if [ "${TOMCAT_MAX_THREADS}" = "" ] ; then
maxThreads=500
else 
maxThreads=${TOMCAT_MAX_THREADS}
fi
 
if [ "${TOMCAT_MIN_SPARE_THREADS}" = "" ] ; then
minSpareThreads=25 
else
minSpareThreads=${TOMCAT_MIN_SPARE_THREADS}
fi
 
if [ "${TOMCAT_MAX_SPARE_THREADS}" = "" ] ; then
maxSpareThreads=200
else
maxSpareThreads=${TOMCAT_MAX_SPARE_THREADS}
fi
 
echo  "               maxThreads=\"${maxThreads}\" minSpareThreads=\"${minSpareThreads}\" maxSpareThreads=\"${maxSpareThreads}\" ">> ${tmp2}
echo  "               maxProcessors=\"${maxThreads}\" minProcessors=\"${minSpareThreads}\" maxSpareProcessors=\"${maxSpareThreads}\" ">> ${tmp2}

cat >>${tmp2} <<EOF
               enableLookups="true" disableUploadTimeout="true"
               acceptCount="10" debug="1" scheme="https" autoFlush="true"
               protocolHandlerClassName="org.apache.coyote.http11.Http11Protocol"
               socketFactory="org.globus.tomcat.catalina.net.BaseHTTPSServerSocketFactory"
EOF

echo "               cert=\"${X509_CERT}\"" >> ${tmp2}
echo "               key=\"${X509_KEY}\"" >> ${tmp2}
echo "               cacertdir=\"${X509_CA_CERT_DIR}\"" >> ${tmp2}
echo "               mode=\"gsi\"/>" >> ${tmp2}
echo "tmp2:${tmp2}"

#now put it all together
cat ${tmp1} ${tmp2} ${tmp3} > ${TOMCAT_CONFIG}

#now insert Certificate Valve

#find the line number containing "<Engine" - this is the beginning of the engine section
sed "s/^\(\W*<Engine.*\)/\1 \n    <Valve className=\"org.globus.tomcat.coyote.valves.HTTPSValve55\"\/> /"  ${TOMCAT_CONFIG} >${tmp1}
#now put it all together again
cat ${tmp1} > ${TOMCAT_CONFIG}

if [ "$TOMCAT_HTTP_ENABLED" != "true" ] ; then
  printf "commenting out HTTP Connector on port ${TOMCAT_PORT} in ${TOMCAT_CONFIG} ..."
  text_to_comment0='<Connector port="8080" maxHttpHeaderSize="8192"
               maxThreads="150" minSpareThreads="25" maxSpareThreads="75"
               enableLookups="false" redirectPort="8443" acceptCount="100"
               connectionTimeout="20000" disableUploadTimeout="true" />'
  text_to_comment=`echo ${text_to_comment0} | sed -e "s/8080/${TOMCAT_PORT}/"`
#  echo  "text_to_comment=${text_to_comment}"
  comment_multiline "${text_to_comment}" ${TOMCAT_CONFIG}
  echo "Done"
fi

printf "commenting out AJP CoyoteConnector on port 8009..."
comment_multiline '<Connector port="8009"
               enableLookups="false" redirectPort="8443" protocol="AJP/1.3" />' ${TOMCAT_CONFIG}
echo "Done"


#modify server-config.wsdd so that the sendMultiRefs option is turned off for compatibility with gSoap
SRM_WS_CONFIG=${SRM_WEBAPP_DIR}/WEB-INF/server-config.wsdd
echo "turning off sending of Multi Refs in ${SRM_WS_CONFIG}"
sed "s/\(.*sendMultiRefs.*value=\"\)true\(.*\)/\1false\2/" ${SRM_WS_CONFIG} >${tmp1}
mv ${SRM_WS_CONFIG} ${SRM_WS_CONFIG}.original
cat ${tmp1} > ${SRM_WS_CONFIG}

echo "Done"

echo "shutdown Tomcat"
cmd="${TOMCAT_PATH}/bin/shutdown.sh"
execute ${cmd}

echo "installing config for startup/shutdown script"
cmd="cp ${SETUP_FILE} /usr/etc/setupSrmTomcat"
execute ${cmd}

echo "Installation complete" 
echo "please use ${DCACHE_HOME}/bin/dcache-srm start|stop|restart to startup, shutdown or  restart srm server"
