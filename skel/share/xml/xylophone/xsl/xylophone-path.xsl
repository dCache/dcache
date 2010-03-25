<?xml version='1.0'?>

<!--+
    | Copyright (c) 2008, Deutsches Elektronen-Synchrotron (DESY)
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
    |  This file contains a number of useful utility templates for dealing with
    |  path-stack
    +-->


<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
		xmlns:math="http://exslt.org/math"
		xmlns:exsl="http://exslt.org/common"
		extension-element-prefixes="math exsl"
                version='1.0'>


<!--+
    |  Add a new element to the path stack.  This template will expand
    |  to a new RTF with an item element for each path; for example
    |
    |    <item path="/path">5</item>
    |    <item path="/path/to/">6</item>
    |    <item path="/path">8</item>
    +-->
<xsl:template name="path-stack-add">
  <xsl:param name="current-path-stack"/>
  <xsl:param name="path"/>
  <xsl:param name="depth" select="count(ancestor-or-self::object)"/>

  <xsl:if test="boolean($current-path-stack)">
    <xsl:apply-templates select="exsl:node-set($current-path-stack)/item"
			 mode="path-stack-copy">
      <xsl:with-param name="exclude-depth" select="$depth"/>
    </xsl:apply-templates>
  </xsl:if>

  <item path="{$path}"><xsl:value-of select="$depth"/></item>
</xsl:template>


<xsl:template match="item" mode="path-stack-copy">
  <xsl:param name="exclude-depth"/>

  <xsl:if test="number(.) != $exclude-depth">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()" mode="path-stack-copy"/>
    </xsl:copy>
  </xsl:if>
</xsl:template>


<xsl:template match="@*|node()" mode="path-stack-copy">
  <xsl:copy>
    <xsl:apply-templates select="@*|node()" mode="path-stack-copy"/>
  </xsl:copy>
</xsl:template>



<!--+
    |  Find the path at the highest depth that is lower than the
    |  depth passed in as a parameter, or '/' otherwise.
    +-->
<xsl:template name="path-stack-find-path">
  <xsl:param name="depth" select="count(ancestor-or-self::object)"/>
  <xsl:param name="path-stack"/>

  <xsl:variable name="result">
    <xsl:if test="boolean($path-stack)">
      <xsl:value-of select="math:highest(exsl:node-set($path-stack)/item[. &lt;= $depth])/@path"/>
    </xsl:if>
  </xsl:variable>

  <xsl:choose>
    <xsl:when test="normalize-space( $result)">
      <xsl:value-of select="$result"/>
    </xsl:when>
    <xsl:otherwise>/</xsl:otherwise>
  </xsl:choose>
</xsl:template>



<!--+
    |  Exapands to a human-friendly view of the stack.  This is for
    |  debugging purposes.
    +-->
<xsl:template name="path-stack-show">
  <xsl:param name="path-stack"/>
  <xsl:param name="label"/>

  <xsl:if test="$label">
    <xsl:value-of select="concat( '>>> ', $label, '&#x0a;')"/>
  </xsl:if>

  <xsl:choose>
    <xsl:when test="not($path-stack)">
      <xsl:text>    (empty) &#x0a;</xsl:text>
    </xsl:when>

    <xsl:otherwise>
      <xsl:apply-templates select="exsl:node-set($path-stack)" mode="path-stack-show"/>
    </xsl:otherwise>
  </xsl:choose>

  <xsl:if test="$label">
    <xsl:value-of select="concat( '>>> ', $label, ' DONE&#x0a;&#x0a;')"/>
  </xsl:if>
</xsl:template>


<xsl:template match="/" mode="path-stack-show">
  <xsl:text>Path stack dump:&#x0a;</xsl:text>

  <xsl:apply-templates mode="path-stack-show"/>
</xsl:template>

<xsl:template match="item" mode="path-stack-show">
  <xsl:value-of select="concat( '  [Depth: ',., '] ', @path,'&#x0a;')"/>
</xsl:template>

<xsl:template match="text()" mode="path-stack-show">
  <xsl:value-of select="concat( '  text[',.,']&#xa;')"/>
</xsl:template>

</xsl:stylesheet>
