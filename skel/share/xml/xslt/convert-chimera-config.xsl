<?xml version="1.0" encoding="utf-8"?>

<!--+
    |  This XSLT converts the pre-1.9.11 Chimera XML configuration file
    |  (typically '/opt/d-cache/config/chimera-config.xml') into a series of
    |  property assignments suitable for including either within the
    |  dcache.conf file or a layout file.
    |
    |  If a configuration option has the same value as the dCache default value
    |  then no option is emitted.  To achieve this, the defaults must be
    |  available as an XML file.
    |
    |  The defaults XML file may be generated from the supplied
    |  chimera.properties file with the following script (sed-script.sh):
    |
    |    #!/bin/sh
    |    sed -n '1 i <defaults>
    |    s%.*\(url\|user\|password\|dialect\|driver\) *= *\([^ ]*\) *$%  <\1>\2</\1>%p
    |    $ a </defaults>'
    |
    |  The output from this script must be saved and the location of this
    |  generated XML file be provided to the stylesheet as the 'defaults-uri'
    |  parameter.
    |
    |  A example invocation is:
    |
    |    ./sed-script.sh </opt/d-cache/config/chimera.properties \
    |            >/tmp/defaults.xml
    |    xsltproc DASH-DASH-stringparam defaults-uri /tmp/defaults.xml \
    |            convert-chimera-config.xsl chimera-config.xml
    |    rm /tmp/defaults.xml
    |
    |  where 'DASH-DASH-' represents two consecutive dash symbols.
    +-->

<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
		xmlns:dcache="http://www.dcache.org/2008/01/Info">

<xsl:param name="defaults-uri"/>

<xsl:output method="text" media-type="text/plain"/>

<xsl:template match="/">
  <xsl:call-template name="emit-property">
    <xsl:with-param name="name" select="'user'"/>
  </xsl:call-template>

  <xsl:call-template name="emit-property">
    <xsl:with-param name="name" select="'password'"/>
    <xsl:with-param name="config-attribute-name" select="'pass'"/>
  </xsl:call-template>

  <xsl:call-template name="emit-property">
    <xsl:with-param name="name" select="'dialect'"/>
  </xsl:call-template>

  <xsl:call-template name="emit-property">
    <xsl:with-param name="name" select="'url'"/>
  </xsl:call-template>

  <xsl:call-template name="emit-property">
    <xsl:with-param name="name" select="'driver'"/>
    <xsl:with-param name="config-attribute-name" select="'drv'"/>
  </xsl:call-template>
</xsl:template>


<xsl:template name="emit-property">
  <xsl:param name="name"/>
  <xsl:param name="defaults-element-name" select="$name"/>
  <xsl:param name="config-attribute-name" select="$name"/>

  <xsl:variable name="config-value" select="/config/db[1]/attribute::*[name()=$config-attribute-name]"/>

  <xsl:variable name="default-value" select="document($defaults-uri)/defaults/*[name()=$defaults-element-name]"/>

  <xsl:if test="normalize-space($config-value) != $default-value">
    <xsl:value-of select="concat('chimera.db.', $name,' = ',$config-value,'&#x0a;')"/>
  </xsl:if>
</xsl:template>

</xsl:stylesheet>