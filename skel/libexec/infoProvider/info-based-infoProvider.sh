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


# Utility function for printing to stderr with a line width
# maximum of 75 characters. Longer lines are broken into several
# lines. Each argument is interpreted as a separate paragraph.
printp() # $* = list of paragraphs
{
    local line
    local line2

    while [ $# -gt 0 ]; do
        # If line is non empty, then we need to print a
        # paragraph separator.
        if [ -n "$line" ]; then
            echo
        fi
        line=
        for word in $1; do
            line2="$line $word"
            if [ ${#line2} -gt 75 ]; then
                echo $line >&2
                line=$word
            else
                line=$line2
            fi
        done
        echo $line
        shift
    done
}


#  Read in the dCacheSetup file
readSetup()
{
    if [ -r $dCacheSetupFile ]; then
        . $dCacheSetupFile
    else
        printp "[WARNING] The dCacheSetup file (expected in $dCacheSetupDir) could not be read."
    fi
}

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


#  Default value for ourHomeDir
if [ -z "$ourHomeDir" ]; then
    ourHomeDir=/opt/d-cache
fi

#  Default values: don't edit these values; instead, change them in dCacheSetup
dCacheSetupDir=$ourHomeDir/config
dCacheSetupFile=$dCacheSetupDir/dCacheSetup
xsltProcessor=saxon
xylophoneConfigurationFile=glue-1.3.xml
xylophoneConfigurationDir=$ourHomeDir/etc
httpHost=localhost
httpPort=2288
xylophoneXSLTDir=$ourHomeDir/share/xml/xylophone
saxonDir=$ourHomeDir/classes/saxon


#  Import the dCacheSetup configuration.
readSetup


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
xylophoneXSLTFile=$xylophoneXSLTDir/xsl/xylophone.xsl
xylophoneXMLFile=$xylophoneConfigurationDir/$xylophoneConfigurationFile
dCacheInfoUri=http://${httpHost}:${httpPort}/info


sanityCheck


#  Generate LDIF
case $xsltProcessor in
  xsltproc)
	xsltproc -stringparam xml-src-uri "$dCacheInfoUri" "$xylophoneXSLTFile" "$xylophoneXMLFile"
	;;

  saxon)
	${java} -classpath ${saxonDir}/saxon.jar com.icl.saxon.StyleSheet "$xylophoneXMLFile" "$xylophoneXSLTFile" xml-src-uri="$dCacheInfoUri"
	;;
    
  *)
	printp "[ERROR] Unknown type of XSLT processor (\"$xsltProcessor\")"
	printp "Please use either \"xsltproc\" or \"saxon\"" >&2
	exit 1
	;;
esac