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

ourHomeDir=${ourHomeDir:-/opt/d-cache}

# Utility function for printing to stdout with a line width
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
                echo $line
                line=$word
            else
                line=$line2
            fi
        done
        echo $line
        shift
    done
}


readSetup()
{
    if [ -r ${ourHomeDir}/config/dCacheSetup ]; then
        . ${ourHomeDir}/config/dCacheSetup
    else
        printp "${ourHomeDir}/config/dCacheSetup does not exist. You have
            to install and setup dCache before you can use this
            script." 1>&2
        exit 1
    fi

    # Sanity check for xylophoneConfigurationFile variable.
    if [ -z "$xylophoneConfigurationFile" ]; then
        printp "[ERROR] The variable 'xylophoneConfigurationFile' in
            ${ourHomeDir}/config/dCacheSetup has to be set properly. Exiting."
        exit 1
    fi
}


#  Import the dCacheSetup configuration.
readSetup

#  Include our default values.
httpHost=${httpHost:-localhost}
xylophoneConfigurationDir=${xylophoneConfigurationDir:-${ourHomeDir}/etc}

#  Build derived variables.
xylophoneDir=${ourHomeDir}/share/xml/xylophone
xylophoneXMLFile=$xylophoneConfigurationDir/$xylophoneConfigurationFile
dCacheInfoUri=http://${httpHost}:2288/info

#  Generate LDIF
xsltproc -stringparam xml-src-uri $dCacheInfoUri $xylophoneDir/xsl/xylophone.xsl $xylophoneXMLFile
