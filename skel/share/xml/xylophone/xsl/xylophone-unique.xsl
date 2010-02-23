<?xml version="1.0" encoding="utf-8"?>

<!--+
    | Copyright (c) 2009, Deutsches Elektronen-Synchrotron (DESY)
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
    |
    |  This file contains support for implementing the unique elements:
    |  as a child of object or of attr.
    |
    +-->

<xsl:stylesheet version="1.0"
		xmlns:exsl="http://exslt.org/common"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
		extension-element-prefixes="exsl">

  <!--+
      |  Check the unique elements within this object or attr
      |  definition to see if publishing the index-th XPath (from
      |  the xpath param) would be a duplicate of an already
      |  published (lower index value) XPath.
      |
      |  Expands to a non-empty string if duplicate, to an empty string otherwise.
      +-->
  <xsl:template name="is-duplicate">
    <xsl:param name="list-item"/>
    <xsl:param name="path-stack"/>
    <xsl:param name="xpaths"/>
    <xsl:param name="index"/>

    <!-- variable contains "DUPLICATE" for each unique condition that is violated -->
    <xsl:variable name="duplicates">
      <xsl:apply-templates select="unique" mode="eval-unique">
	<xsl:with-param name="list-item" select="$list-item"/>
	<xsl:with-param name="path-stack" select="$path-stack"/>
	<xsl:with-param name="xpaths" select="$xpaths"/>
	<xsl:with-param name="index" select="$index"/>
      </xsl:apply-templates>
    </xsl:variable>

    <xsl:value-of select="boolean(normalize-space($duplicates))"/>
  </xsl:template>



  <!--+
      |  In eval-unique mode we check whether any unique elements
      |  are violated.
      +-->


  <!-- Ignore any text within the unique elements -->
  <xsl:template match="text()" mode="eval-unique"/>



  <!--+
      |  Check a uniqueness constraint.
      |
      |  Expands to an empty string if this uniqueness
      |  constraint is satisfied.
      +-->
  <xsl:template match="unique" mode="eval-unique">
    <xsl:param name="list-item"/>
    <xsl:param name="path-stack"/>
    <xsl:param name="xpaths"/>
    <xsl:param name="index"/>

    <xsl:variable name="reference-unique-value">
      <xsl:apply-templates select="*" mode="eval-attr">
	<xsl:with-param name="depth" select="count(ancestor-or-self::object)"/>
	<xsl:with-param name="list-item" select="$list-item"/>
	<xsl:with-param name="path-stack">
	  <xsl:call-template name="path-stack-add">
	    <xsl:with-param name="current-path-stack" select="$path-stack"/>
	    <xsl:with-param name="path" select="exsl:node-set($xpaths)/results/match[$index]"/>
	  </xsl:call-template>
	</xsl:with-param>
      </xsl:apply-templates>
    </xsl:variable>

    <!-- Expand the results for searching for uniqueness constraint violations -->
    <xsl:call-template name="sufficiently-unique-itr">
      <xsl:with-param name="list-item" select="$list-item"/>
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="xpaths" select="$xpaths"/>
      <xsl:with-param name="index" select="$index - 1"/>
      <xsl:with-param name="reference-unique-value" select="$reference-unique-value"/>
    </xsl:call-template>
  </xsl:template>


  <!-- Check uniqueness ref. value against evaluation for this index and tail-recurse -->
  <xsl:template name="sufficiently-unique-itr">
    <xsl:param name="list-item"/>
    <xsl:param name="path-stack"/>
    <xsl:param name="xpaths"/>
    <xsl:param name="index"/>
    <xsl:param name="reference-unique-value"/>

    <xsl:if test="$index > 0">
      <xsl:variable name="this-unique-value">
	<xsl:apply-templates select="*" mode="eval-attr">
	  <xsl:with-param name="depth" select="count(ancestor-or-self::object)"/>
	  <xsl:with-param name="list-item" select="$list-item"/>
	  <xsl:with-param name="path-stack">
	    <xsl:call-template name="path-stack-add">
	      <xsl:with-param name="current-path-stack" select="$path-stack"/>
	      <xsl:with-param name="path" select="exsl:node-set($xpaths)/results/match[$index]"/>
	    </xsl:call-template>
	  </xsl:with-param>
	</xsl:apply-templates>
      </xsl:variable>

      <xsl:choose>
	<xsl:when test="$this-unique-value = $reference-unique-value">
	  <xsl:text>DUPLICATE</xsl:text>
	</xsl:when>

	<!-- NB as an optimisation, we only tail-recurse if no violation has been detected -->
	<xsl:otherwise>
	  <!-- Check the next lower count for violation -->
	  <xsl:call-template name="sufficiently-unique-itr">
	    <xsl:with-param name="list-item" select="$list-item"/>
	    <xsl:with-param name="path-stack" select="$path-stack"/>
	    <xsl:with-param name="xpaths" select="$xpaths"/>
	    <xsl:with-param name="index" select="$index - 1"/>
	    <xsl:with-param name="reference-unique-value"
			    select="$reference-unique-value"/>
	  </xsl:call-template>
	</xsl:otherwise>
      </xsl:choose>
    </xsl:if>
  </xsl:template>

</xsl:stylesheet>
