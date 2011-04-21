#!/bin/bash
#
#  Script to invoke XSLT program that transforms the output from dCache's
#  info service into LDIF.  This conversion process is achieved using
#  Xylophone and is controlled by a configuration file.  The xylophone
#  config file is built from files in
#
#      /opt/d-cache/share/info-provider
#
#  and the site-specific configuration in
#
#      /opt/d-cache/etc/info-provider.xml


#  Apply sanity checks before launching the XSLT processor
sanityCheck()
{
    local dcacheConfFile

    dcacheConfFile="$DCACHE_ETC/dcache.conf"

    case $(getProperty info-provider.publish) in
	1.3 | 2.0 | both)
	    ;;
	*)
            printp "[ERROR] Value of info-provider.publish in wrong.  Allowed
                    values are '1.3', '2.0' or 'both'.  The current value is
                    \"$publish\""
            exit 1
	;;
    esac

    if [ ! -r "$xylophoneXMLFile" ]; then
        printp "[ERROR] Unable to read $xylophoneXMLFile. Try creating this
                file or adjusting the info-provider.configuration.dir property
                (currently \"$xylophoneConfigurationDir\") or
                info-provider.configuration.file (currently
                \"$xylophoneConfigurationFile\") in $dcacheConfFile"
        exit 1
    fi

    if [ ! -r "$xylophoneXSLTFile" ]; then
        printp "[ERROR] Unable to read $xylophoneXSLTFile.  If the file exists
                try editing the property info-provider.xylophone.dir
                (currently \"$xylophoneXSLTDir\") in $dcacheConfFile"
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
    DCACHE_HOME="@dcache.home@"
fi
if [ ! -d "$DCACHE_HOME" ]; then
    echo "$DCACHE_HOME is not a directory"
    exit 2
fi

# Load libraries
. @dcache.paths.bootloader@/loadConfig.sh
. ${DCACHE_LIB}/utils.sh


xsltProcessor="$(getProperty info-provider.processor)"
xylophoneXMLFile="$(getProperty info-provider.configuration.location)"
host="$(getProperty info-provider.http.host)"
port="$(getProperty httpdPort)"
xylophoneXSLTDir="$(getProperty info-provider.xylophone.dir)"
saxonDir="$(getProperty info-provider.saxon.dir)"

#  Apply any environment overrides
if [ -n "$XSLT_PROCESSOR" ]; then
    xsltProcessor=$XSLT_PROCESSOR
fi

if [ -n "$HTTP_HOST" ]; then
    httpHost=$HTTP_HOST
fi

if [ -n "$HTTP_PORT" ]; then
    httpPort=$HTTP_PORT
fi


#  Build derived variables after allowing changes from default values
xylophoneXSLTFile="$xylophoneXSLTDir/xsl/xylophone.xsl"
uri="http://${host}:${port}/info"

sanityCheck

#  Generate LDIF
case $xsltProcessor in
  xsltproc)
	xsltproc --xinclude --stringparam xml-src-uri "$uri" \
	    "$xylophoneXSLTFile" "$xylophoneXMLFile"
	;;

  saxon)
        #  Unfortunately, xerces (the XML parser in Java) doesn't support xpointer() scheme
        #  for the xpointer attribute in an xinclude statement.  The xpointer() scheme is
        #  needed to include subtrees.  This scheme is supported by xmllint and xsltproc
        #  but xerces project has no plans to implement support.  So we must preprocess
        #  the XML file using xmllint to process the xinclude statements and store the
        #  results in a temporary file.
        #
        t=$(mktemp)
        xmllint --xinclude $xylophoneXMLFile > $t
	"${JAVA}" -classpath "${saxonDir}/saxon.jar" \
            com.icl.saxon.StyleSheet $t  \
	    "$xylophoneXSLTFile" xml-src-uri="$uri"
        rm $t
	;;

  *)
	printp "[ERROR] Unknown value of info-provider.processor (\"$xsltProcessor\")"
	printp "info-provider.processor must be either 'xsltproc' or 'saxon'" >&2
	exit 1
	;;
esac
