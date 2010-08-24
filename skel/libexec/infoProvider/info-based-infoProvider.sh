#!/bin/bash
#
#  Script to invoke XSLT program that transforms the output from dCache's
#  info service into LDIF.  This conversion process is achieved using
#  Xylophone and is controlled by a configuration file.  This file
#  is, by default, located at:
#
#      /opt/d-cache/etc/glue-1.3.xml
#
#  The general documentation for the format of this is in
#  /opt/d-cache/share/doc/xylophone/Guide.txt


#  Apply any sanity checks before launching the XSLT processor
sanityCheck()
{
    if [ ! -r "$xylophoneXMLFile" ]; then
        printp "[ERROR] Unable to read $xylophoneXMLFile.  Try creating this file or editing the variable 'xylophoneConfigurationDir' (currently \"$xylophoneConfigurationDir\") or 'xylophoneConfigurationFile' (currently \"$xylophoneConfigurationFile\") in $dCacheSetupFile"
        exit 1
    fi

    if [ ! -r "$xylophoneXSLTFile" ]; then
        printp "[ERROR] Unable to read ${xylophoneXSLTFile}.  If the file exists, try editing the variable 'xylophoneXSLTDir' (currently \"$xylophoneXSLTDir\") in $dCacheSetupFile"
        exit 1
    fi
}


# Initialize environment. /etc/default/ is the normal place for this
# on several Linux variants. For other systems we provide
# /etc/dcache.env. Those files will typically declare JAVA_HOME and
# DCACHE_HOME and nothing else.
[ -f /etc/default/dcache ] && . /etc/default/dcache
[ -f /etc/dcache.env ] && . /etc/dcache.env

# Set home path
if [ -z "$DCACHE_HOME" ]; then
    DCACHE_HOME="/opt/d-cache"
fi
if [ ! -d "$DCACHE_HOME" ]; then
    echo "$DCACHE_HOME is not a directory"
    exit 2
fi

# Load libraries
. ${DCACHE_HOME}/share/lib/loadConfig.sh -q
. ${DCACHE_LIB}/utils.sh

xsltProcessor="$(getProperty xsltProcessor)"
xylophoneConfigurationFile="$(getProperty xylophoneConfigurationFile)"
xylophoneConfigurationDir="$(getProperty xylophoneConfigurationDir)"
httpHost="$(getPropety httpHost)"
httpPort="$(getProperty httpPort)"
xylophoneXSLTDir="$(getProperty xylophoneXSLTDir)"
saxonDir="$(getProperty saxonDir)"
dCacheSetupFile="$DCACHE_HOME/etc/dcache.conf"

#  Apply any environment overrides
if [ -n "$XSLT_PROCESSOR" ]; then
    xsltProcessor=$XSLT_PROCESSOR
fi

if [ -n "$XYLOPHONE_CONFIG_DIR" ]; then
    xylophoneConfigurationDir=$XYLOPHONE_CONFIG_DIR
fi

if [ -n "$HTTP_HOST" ]; then
    httpHost=$HTTP_HOST
fi

if [ -n "$HTTP_PORT" ]; then
    httpPort=$HTTP_PORT
fi


#  Build derived variables after allowing changes from default values
xylophoneXSLTFile="$xylophoneXSLTDir/xsl/xylophone.xsl"
xylophoneXMLFile="$xylophoneConfigurationDir/$xylophoneConfigurationFile"
dCacheInfoUri="http://${httpHost}:${httpPort}/info"


sanityCheck


#  Generate LDIF
case $xsltProcessor in
  xsltproc)
	xsltproc -stringparam xml-src-uri "$dCacheInfoUri" "$xylophoneXSLTFile" "$xylophoneXMLFile"
	;;

  saxon)
	"${JAVA}" -classpath "${saxonDir}/saxon.jar" com.icl.saxon.StyleSheet "$xylophoneXMLFile" "$xylophoneXSLTFile" xml-src-uri="$dCacheInfoUri"
	;;
    
  *)
	printp "[ERROR] Unknown type of XSLT processor (\"$xsltProcessor\")"
	printp "Please use either \"xsltproc\" or \"saxon\"" >&2
	exit 1
	;;
esac