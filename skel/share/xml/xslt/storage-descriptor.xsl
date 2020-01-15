<?xml version="1.0" encoding="utf-8"?>

<!DOCTYPE stylesheet [
<!ENTITY % dCache-config PUBLIC "-//dCache//ENTITIES dCache Properties//EN" "/unused/path" >
%dCache-config;
]>


<!--+
    | Copyright (c) 2018, Deutsches Elektronen-Synchrotron (DESY)
    | All rights reserved.
    |
    | Redistribution and use in source and binary forms, with
    | or without modification, are permitted provided that the
    | following conditions are met:
    |
    |   o  Redistributions of source code must retain the above
    |      copyright notice, this list of conditions and the
    |      following disclaimer.
    |
    |   o  Redistributions in binary form must reproduce the
    |      above copyright notice, this list of conditions and
    |      the following disclaimer in the documentation and/or
    |      other materials provided with the distribution.
    |
    |   o  Neither the name of Deutsches Elektronen-Synchrotron
    |      (DESY) nor the names of its contributors may be used
    |      to endorse or promote products derived from this
    |      software without specific prior written permission.
    |
    | THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
    | CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
    | INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
    | MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    | DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
    | CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    | SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
    | NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    | LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
    | HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
    | CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
    | OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
    | SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
    +-->


<!--+
    | This XSLT template converts dCache info format into WLCG
    | Storage Descriptor format.
    +-->

<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:date="http://exslt.org/dates-and-times"
                xmlns:d="http://www.dcache.org/2008/01/Info"
                extension-element-prefixes="date"
                >

<xsl:output method="text" media-type="application/json"/>

<xsl:strip-space elements="*"/>

<xsl:template match="/">
  <xsl:text>{"storageservice": {&#x0a;</xsl:text>
  <xsl:if test="normalize-space('&storage-descriptor.name;')">
    <xsl:call-template name="object-key-value">
      <xsl:with-param name="indentation" select="'    '"/>
      <xsl:with-param name="key" select="'name'"/>
      <xsl:with-param name="value" select="'&storage-descriptor.name;'"/>
    </xsl:call-template>
  </xsl:if>

  <xsl:call-template name="object-key-value">
    <xsl:with-param name="indentation" select="'    '"/>
    <xsl:with-param name="key" select="'id'"/>
    <xsl:with-param name="value" select="'&storage-descriptor.unique-id;'"/>
  </xsl:call-template>

  <xsl:text>    "servicetype": "org.wlcg.se",&#x0a;</xsl:text>
  <xsl:text>    "implementation": "dcache",&#x0a;</xsl:text>
  <xsl:call-template name="object-key-value">
    <xsl:with-param name="indentation" select="'    '"/>
    <xsl:with-param name="key" select="'implementationversion'"/>
    <xsl:with-param name="value" select="'&dcache.version;'"/>
  </xsl:call-template>
  <xsl:text>    "capabilities": [&#x0a;</xsl:text>
  <xsl:text>        "data.transfer",&#x0a;</xsl:text>
  <xsl:text>        "data.access.flatfiles",&#x0a;</xsl:text>
  <xsl:text>        "data.access.streaming",&#x0a;</xsl:text>
  <xsl:text>        "data.management.storage",&#x0a;</xsl:text>
  <xsl:text>        "data.management.transfer"&#x0a;</xsl:text>
  <xsl:text>    ],&#x0a;</xsl:text>

  <xsl:call-template name="object-key-value">
    <xsl:with-param name="indentation" select="'    '"/>
    <xsl:with-param name="key" select="'qualitylevel'"/>
    <xsl:with-param name="value" select="'&storage-descriptor.quality-level;'"/>
  </xsl:call-template>

  <xsl:text>    "storagecapacity": {&#x0a;</xsl:text>
  <xsl:apply-templates select="*" mode="online-capacity"/>
  <xsl:apply-templates select="document('&storage-descriptor.paths.tape-info;')" mode="nearline-capacity"/>
  <xsl:text>    },&#x0a;</xsl:text>
  <xsl:value-of select="concat('    &quot;lastupdated&quot;: ',date:seconds(),',&#x0a;')"/>
  <xsl:text>    "storageendpoints": [&#x0a;</xsl:text>
  <xsl:apply-templates select="/d:dCache/d:doors/d:door[d:tags/d:tag[@id='&storage-descriptor.door.tag;']][d:interfaces/d:interface/d:metric[@name='scope'] = 'global']" mode="publish-door"/>
  <xsl:text>    ],&#x0a;</xsl:text>
  <xsl:text>    "storageshares": [&#x0a;</xsl:text>
  <xsl:apply-templates select="*" mode="reservations"/>
  <xsl:text>    ]&#x0a;</xsl:text>
  <xsl:text>}}&#x0a;</xsl:text>
</xsl:template>


<xsl:template match="d:door" mode="publish-door">
  <xsl:variable name="hostname" select="d:interfaces/d:interface[d:metric[@name='scope']='global'][1]/d:metric[@name='url-name']"/>
  <xsl:variable name="id" select="concat($hostname,'#',d:metric[@name='cell'],'@',d:metric[@name='domain'])"/>
  <xsl:variable name="type" select="d:protocol/d:metric[@name='family']"/>
  <xsl:variable name="version">
    <xsl:choose>
      <xsl:when test="$type = 'srm'">
        <xsl:text>2.2</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="d:protocol/d:metric[@name='version']"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:variable name="port" select="d:metric[@name='port']"/>
  <xsl:variable name="endpoint">
    <xsl:choose>
      <xsl:when test="$type = 'srm'">
        <xsl:value-of select="concat('httpg://',$hostname,':',$port,'/srm/managerv2')"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="concat($type,'://',$hostname,':',$port,'/')"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <!--+
      |  We assume capabilities have the following meaning:
      |
      |      data.management.transfer - supports 3rd-party transfers
      |      data.management.storage  - ability to manage namespace
      |      data.access.flatfiles    - ability to upload/download data
      |                                 into some namespace
      |      data.transfer            - ability to upload/download data.
      +-->
  <xsl:variable name="capabilities">
    <xsl:choose>
      <xsl:when test="$type = 'srm'">
        <xsl:text>                "data.management.transfer",&#x0a;</xsl:text>
        <xsl:text>                "data.management.storage"&#x0a;</xsl:text>
      </xsl:when>
      <xsl:when test="$type = 'http'">
        <xsl:text>                "data.management.transfer",&#x0a;</xsl:text>
        <xsl:text>                "data.management.storage",&#x0a;</xsl:text>
        <xsl:text>                "data.access.flatfiles",&#x0a;</xsl:text>
        <xsl:text>                "data.transfer"&#x0a;</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>                "data.management.storage",&#x0a;</xsl:text>
        <xsl:text>                "data.access.flatfiles",&#x0a;</xsl:text>
        <xsl:text>                "data.transfer"&#x0a;</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:variable name="have-subsequent" select="position() != last()"/>
  <xsl:variable name="name">
    <xsl:choose>
      <xsl:when test="$type = 'root'">
        <xsl:value-of select="concat('xrootd endpoint on ',$hostname)"/>
      </xsl:when>
      <xsl:when test="$type = 'http'">
        <xsl:value-of select="concat('HTTP and WebDAV endpoint on ',$hostname)"/>
      </xsl:when>
      <xsl:when test="$type = 'dcap'">
        <xsl:value-of select="concat('Plain dcap endpoint on ',$hostname)"/>
      </xsl:when>
      <xsl:when test="$type = 'gsidcap'">
        <xsl:value-of select="concat('GSI dcap endpoint on ',$hostname)"/>
      </xsl:when>
      <xsl:when test="$type = 'ftp'">
        <xsl:value-of select="concat('Plain FTP endpoint on ',$hostname)"/>
      </xsl:when>
      <xsl:when test="$type = 'gsiftp'">
        <xsl:value-of select="concat('GridFTP (gsiftp) endpoint on ',$hostname)"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="concat($type,' endpoint on ',$hostname)"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:text>        {&#x0a;</xsl:text>
  <xsl:value-of select="concat('            &quot;name&quot;: &quot;',$name,'&quot;,&#x0a;')"/>
  <xsl:value-of select="concat('            &quot;id&quot;: &quot;',$id,'&quot;,&#x0a;')"/>
  <xsl:value-of select="concat('            &quot;endpointurl&quot;: &quot;',$endpoint,'&quot;,&#x0a;')"/>
  <xsl:value-of select="concat('            &quot;interfacetype&quot;: &quot;',$type,'&quot;,&#x0a;')"/>
  <xsl:value-of select="concat('            &quot;interfaceversion&quot;: &quot;',$version,'&quot;,&#x0a;')"/>
  <xsl:text>            "capabilities": [&#x0a;</xsl:text>
  <xsl:value-of select="$capabilities"/>
  <xsl:text>            ],&#x0a;</xsl:text>

  <xsl:call-template name="object-key-value">
    <xsl:with-param name="indentation" select="'            '"/>
    <xsl:with-param name="key" select="'qualitylevel'"/>
    <xsl:with-param name="value" select="'&storage-descriptor.quality-level;'"/>
  </xsl:call-template>

  <xsl:text>            "assignedshares": [&#x0a;</xsl:text>
  <xsl:text>                "all"&#x0a;</xsl:text>
  <xsl:text>            ]&#x0a;</xsl:text>
  <xsl:text>        }</xsl:text>
  <xsl:if test="$have-subsequent">
      <xsl:text>,</xsl:text>
  </xsl:if>
  <xsl:text>&#x0a;</xsl:text>
</xsl:template>


<xsl:template match="d:dCache/d:summary/d:pools/d:space" mode="online-capacity">
  <xsl:variable name="total" select="number(d:metric[@name='total'])"/>
  <xsl:variable name="free" select="number(d:metric[@name='free'])"/>
  <xsl:variable name="removable" select="number(d:metric[@name='removable'])"/>
  <xsl:variable name="used" select="$total - $free - $removable"/>
  <xsl:variable name="publish-nearline" select="boolean(document('&storage-descriptor.paths.tape-info;')/tape-info/collection)"/>

  <xsl:text>        "online": {&#x0a;</xsl:text>
  <xsl:value-of select="concat('            &quot;totalsize&quot;: ',$total,',&#x0a;')"/>
  <xsl:value-of select="concat('            &quot;usedsize&quot;: ',$used,',&#x0a;')"/>
  <xsl:value-of select="concat('            &quot;reservedsize&quot;: ',0,'&#x0a;')"/>
  <xsl:text>        }</xsl:text>
  <xsl:if test="$publish-nearline">
      <xsl:text>,</xsl:text>
  </xsl:if>
  <xsl:text>&#x0a;</xsl:text>
</xsl:template>


<xsl:template match="tape-info" mode="nearline-capacity">
  <xsl:if test="collection">
    <xsl:variable name="total" select="sum(collection/space/total)"/>
    <xsl:variable name="used" select="sum(collection/space/used)"/>
    <xsl:variable name="reserved" select="sum(collection/space/reserved)"/>

    <xsl:text>        "nearline": {&#x0a;</xsl:text>
    <xsl:value-of select="concat('            &quot;totalsize&quot;: ',$total,',&#x0a;')"/>
    <xsl:value-of select="concat('            &quot;usedsize&quot;: ',$used,',&#x0a;')"/>
    <xsl:value-of select="concat('            &quot;reservedsize&quot;: ',$reserved,'&#x0a;')"/>
    <xsl:text>        }&#x0a;</xsl:text>
  </xsl:if>
</xsl:template>


<xsl:template match="d:dCache/d:reservations/d:reservation[d:metric[@name='description']/text()]" mode="reservations">
  <xsl:variable name="description" select="d:metric[@name='description']"/>
  <xsl:variable name="total" select="d:space/d:metric[@name='total']"/>
  <xsl:variable name="used" select="d:space/d:metric[@name='used']"/>
  <xsl:variable name="filecount" select="d:metric[@name='file-count']"/>
  <xsl:variable name="timestamp">
    <xsl:choose>
      <xsl:when test="d:space/d:metric[@name='total']/@last-updated">
	<xsl:value-of select="d:space/d:metric[@name='total']/@last-updated"/>
      </xsl:when>
      <xsl:otherwise>
	<xsl:value-of select="date:seconds()"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:variable name="vo-no-start-slash">
    <xsl:choose>
      <xsl:when test="starts-with(d:authorisation/d:metric[@name='FQAN'],'/')">
	<xsl:value-of select="substring(d:authorisation/d:metric[@name='FQAN'],2)"/>
      </xsl:when>
      <xsl:otherwise>
	<xsl:value-of select="d:authorisation/d:metric[@name='FQAN']"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:variable name="vo">
    <xsl:choose>
      <xsl:when test="contains($vo-no-start-slash,'/')">
	<xsl:value-of select="substring-before($vo-no-start-slash,'/')"/>
      </xsl:when>
      <xsl:otherwise>
	<xsl:value-of select="$vo-no-start-slash"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:variable name="have-subsequent" select="position() != last()"/>

  <xsl:text>        {&#xa;</xsl:text>
  <xsl:value-of select="concat('            &quot;name&quot;: &quot;',$description, '&quot;,&#xa;')"/>
  <xsl:value-of select="concat('            &quot;totalsize&quot;: ',$total, ',&#xa;')"/>
  <xsl:value-of select="concat('            &quot;usedsize&quot;: ',$used, ',&#xa;')"/>
  <xsl:if test="$filecount">
    <xsl:value-of select="concat('            &quot;numberoffiles&quot;: ',$filecount,',&#x0a;')"/>
  </xsl:if>
  <xsl:value-of select="concat('            &quot;timestamp&quot;: ',$timestamp,',&#x0a;')"/>
  <xsl:value-of select="concat('            &quot;vos&quot;: [&quot;',$vo,'&quot;],&#x0a;')"/>
  <xsl:text>            "assignedendpoints": ["all"]&#x0a;</xsl:text>
  <xsl:text>        }</xsl:text>
  <xsl:if test="$have-subsequent">
    <xsl:text>,</xsl:text>
  </xsl:if>
  <xsl:text>&#xa;</xsl:text>
</xsl:template>


<xsl:template match="text()"/>
<xsl:template match="text()" mode="online-capacity"/>
<xsl:template match="text()" mode="nearline-capacity"/>
<xsl:template match="text()" mode="reservations"/>


<xsl:template name="object-key-value">
  <xsl:param name="key"/>
  <xsl:param name="value"/>
  <xsl:param name="indentation"/>
  <xsl:value-of select="concat($indentation,'&quot;')"/>
  <xsl:call-template name="string-markup">
    <xsl:with-param name="value" select="$key"/>
  </xsl:call-template>
  <xsl:text>&quot;: &quot;</xsl:text>
  <xsl:call-template name="string-markup">
    <xsl:with-param name="value" select="$value"/>
  </xsl:call-template>
  <xsl:text>&quot;,&#xa;</xsl:text>
</xsl:template>


<xsl:template name="string-markup">
  <xsl:param name="value"/>
  <xsl:call-template name="string-markup-quote">
    <xsl:with-param name="value">
      <xsl:call-template name="string-markup-backslash">
        <xsl:with-param name="value" select="$value"/>
      </xsl:call-template>
    </xsl:with-param>
  </xsl:call-template>
</xsl:template>


<xsl:template name="string-markup-backslash">
  <xsl:param name="value"/>
  <xsl:choose>
    <xsl:when test="contains($value,'\')">
      <xsl:value-of select="concat(substring-before($value,'\'),'\\')"/>
      <xsl:call-template name="string-markup-backslash">
        <xsl:with-param name="value" select="substring-after($value,'\')"/>
      </xsl:call-template>
    </xsl:when>
    <xsl:otherwise>
      <xsl:value-of select="$value"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>


<xsl:template name="string-markup-quote">
  <xsl:param name="value"/>
  <xsl:choose>
    <xsl:when test="contains($value,'&quot;')">
      <xsl:value-of select="concat(substring-before($value,'&quot;'),'\&quot;')"/>
      <xsl:call-template name="string-markup-quote">
        <xsl:with-param name="value" select="substring-after($value,'&quot;')"/>
      </xsl:call-template>
    </xsl:when>
    <xsl:otherwise>
      <xsl:value-of select="$value"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

</xsl:stylesheet>
