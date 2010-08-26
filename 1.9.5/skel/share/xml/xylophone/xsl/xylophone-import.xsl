<?xml version="1.0" encoding="utf-8"?>

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
    |
    |  This file contains support for importing data from external source
    |
    +-->

<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:dyn="http://exslt.org/dynamic"
                xmlns:saxon655="http://icl.com/saxon"
                xmlns:saxon9="http://saxon.sf.net/"
		xmlns:d="http://www.dcache.org/2008/01/Info"
                extension-element-prefixes="dyn saxon9 saxon655">


<!--+
    |  Evaluate a lookup element.  The path to the current path is used to set the context
    |  for the lookup.  This allows relative XPaths to be processed correctly.
    +-->
<xsl:template name="eval-path">
  <xsl:param name="path-stack"/>
  <xsl:param name="depth" select="count(ancestor-or-self::object)"/>
  <xsl:param name="lookup-path" select="'.'"/>
  <xsl:param name="default-value"/>

  <xsl:variable name="context-node-xpath">
    <xsl:call-template name="context-node-xpath-with-document">
      <xsl:with-param name="context-node-xpath">
	<xsl:call-template name="path-stack-find-path">
	  <xsl:with-param name="path-stack" select="$path-stack"/>
	  <xsl:with-param name="depth" select="$depth"/>
	</xsl:call-template>
      </xsl:with-param>
    </xsl:call-template>
  </xsl:variable>

  <!-- Emit the expansion from the given XPath -->
  <xsl:choose>
    <!-- Try with EXSLT (xsltproc, xalan) -->
    <xsl:when test="function-available('dyn:evaluate')">
      <xsl:apply-templates select="dyn:evaluate($context-node-xpath)" mode="eval-path">
        <xsl:with-param name="lookup-path" select="$lookup-path"/>
        <xsl:with-param name="default-value" select="$default-value"/>
      </xsl:apply-templates>
    </xsl:when>

    <!-- Try with Saxon 6.5.5 -->
    <xsl:when test="function-available('saxon655:evaluate')">
      <xsl:apply-templates select="saxon655:evaluate($context-node-xpath)" mode="eval-path">
        <xsl:with-param name="lookup-path" select="$lookup-path"/>
        <xsl:with-param name="default-value" select="$default-value"/>
      </xsl:apply-templates>
    </xsl:when>

    <!-- Try with Saxon v9 -->
    <xsl:when test="function-available('saxon9:evaluate')">
      <xsl:apply-templates select="saxon9:evaluate($context-node-xpath)" mode="eval-path">
        <xsl:with-param name="lookup-path" select="$lookup-path"/>
        <xsl:with-param name="default-value" select="$default-value"/>
      </xsl:apply-templates>
    </xsl:when>

    <xsl:otherwise>
      <xsl:message>You are using an XSLT processor without evaluate() support.</xsl:message>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>



<!-- This template emits an XPath expression that references the
     context node.  The XPath starts with the appropriate "document()"
     function, with a single string parameter that is the XML src
     document's URI, taking into account any user override using the
     xml-src attribute.  The supplied absolute XPath Expression
     (referencing nodes within the src document) is also included. -->
<xsl:template name="context-node-xpath-with-document">
  <xsl:param name="context-node-xpath" select="'/'"/>

  <xsl:variable name="this-xml-src-uri">
    <xsl:choose>
      <xsl:when test="@xml-src">
	<xsl:choose>
	  <xsl:when test="/xylophone/locations/location[@name=current()/@xml-src]">
	    <xsl:value-of select="/xylophone/locations/location[@name=current()/@xml-src]"/>
	  </xsl:when>

	  <xsl:otherwise>
	    <xsl:message>External XML reference <xsl:value-of select="@xml-src"/> is not defined</xsl:message>
	  </xsl:otherwise>
	</xsl:choose>
      </xsl:when>

      <xsl:otherwise>
	<!-- pick up (global) stylesheet parameter -->
	<xsl:value-of select="$xml-src-uri"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:value-of select="concat(&quot;document('&quot;,$this-xml-src-uri,&quot;')&quot;)"/>
  <xsl:if test="not($context-node-xpath = '/')">
    <xsl:value-of select="$context-node-xpath"/>
  </xsl:if>
</xsl:template>



<!-- This template matches our initial bounce into the XML source
     document.  Now that we have the right context, we can evaluate
     the lookup-path -->
<xsl:template match="/|node()|@*" mode="eval-path">
  <xsl:param name="lookup-path"/>
  <xsl:param name="default-value"/>

  <xsl:variable name="does-lookup-path-expand-to-non-empty-result">
    <xsl:choose>
      <!-- Try with EXSLT (xsltproc, xalan) -->
      <xsl:when test="function-available('dyn:evaluate')">
        <xsl:value-of select="boolean(dyn:evaluate($lookup-path))"/>
      </xsl:when>

      <!-- Try with Saxon 6.5.5 -->
      <xsl:when test="function-available('saxon655:evaluate')">
        <xsl:value-of select="boolean(saxon655:evaluate($lookup-path))"/>
      </xsl:when>

      <!-- Try with Saxon v9 -->
      <xsl:when test="function-available('saxon9:evaluate')">
        <xsl:value-of select="boolean(saxon9:evaluate($lookup-path))"/>
      </xsl:when>
    </xsl:choose>
  </xsl:variable>

  <!-- Emit the output -->
  <xsl:choose>
    <xsl:when test="$does-lookup-path-expand-to-non-empty-result = 'true'">
      <xsl:choose>
        <!-- Try with EXSLT (xsltproc, xalan) -->
        <xsl:when test="function-available('dyn:evaluate')">
          <xsl:value-of select="dyn:evaluate($lookup-path)"/>
        </xsl:when>

        <!-- Try with Saxon 6.5.5 -->
        <xsl:when test="function-available('saxon655:evaluate')">
          <xsl:value-of select="saxon655:evaluate($lookup-path)"/>
        </xsl:when>

        <!-- Try with Saxon v9 -->
        <xsl:when test="function-available('saxon9:evaluate')">
          <xsl:value-of select="saxon9:evaluate($lookup-path)"/>
        </xsl:when>
      </xsl:choose>
    </xsl:when>

    <xsl:otherwise>
      <xsl:value-of select="$default-value"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>





<!--+
    |
    |  Path-based sum
    |
    |  Support for calculating the sum of elements selecting by specifying an XPath.
    +-->
<xsl:template name="sum-path">
  <xsl:param name="path-stack"/>
  <xsl:param name="depth" select="count(ancestor-or-self::object)"/>
  <xsl:param name="sum-path" select="@path"/>

  <xsl:variable name="context-node-xpath">
    <xsl:call-template name="context-node-xpath-with-document">
      <xsl:with-param name="context-node-xpath">
	<xsl:call-template name="path-stack-find-path">
	  <xsl:with-param name="path-stack" select="$path-stack"/>
	  <xsl:with-param name="depth" select="$depth"/>
	</xsl:call-template>
      </xsl:with-param>
    </xsl:call-template>
  </xsl:variable>

  <!-- Emit the sum: first bounce into the document at the context node -->
  <xsl:choose>
    <!-- Try with EXSLT (xsltproc, xalan) -->
    <xsl:when test="function-available('dyn:evaluate')">
      <xsl:apply-templates select="dyn:evaluate($context-node-xpath)" mode="emit-sum-path">
	<xsl:with-param name="sum-path" select="$sum-path"/>
      </xsl:apply-templates>
    </xsl:when>

    <!-- Try with Saxon 6.5.5 -->
    <xsl:when test="function-available('saxon655:evaluate')">
      <xsl:apply-templates select="saxon655:evaluate($context-node-xpath)" mode="emit-sum-path">
	<xsl:with-param name="sum-path" select="$sum-path"/>
      </xsl:apply-templates>
    </xsl:when>

    <!-- Try with Saxon v9 -->
    <xsl:when test="function-available('saxon9:evaluate')">
      <xsl:apply-templates select="saxon9:evaluate($context-node-xpath)" mode="emit-sum-path">
	<xsl:with-param name="sum-path" select="$sum-path"/>
      </xsl:apply-templates>
    </xsl:when>

    <xsl:otherwise>
      <xsl:message>You are using an XSLT processor without evaluate() support.</xsl:message>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>



<!-- This template matches our initial bounce into the document.
     Doing this allows us to calculate the sum for nodes that match
     the user-supplied XPath from this context, allowing the user to
     supply relative XPaths. -->
<xsl:template match="/|node()|@*" mode="emit-sum-path">
  <xsl:param name="sum-path"/>

  <xsl:choose>
    <!-- Support for Saxon 6.5.5 -->
    <xsl:when test="function-available('saxon655:evaluate')">
      <xsl:value-of select="sum(saxon655:evaluate($sum-path))"/>
    </xsl:when>

    <!-- Support for Saxon v9 -->
    <xsl:when test="function-available('saxon9:evaluate')">
      <xsl:value-of select="sum(saxon9:evaluate($sum-path))"/>
    </xsl:when>

    <!-- Support for EXSLT-supporting processors (e.g., xsltproc, xalan) -->
    <xsl:when test="function-available('dyn:evaluate')">
      <xsl:value-of select="sum(dyn:evaluate($sum-path))"/>
    </xsl:when>
  </xsl:choose>
</xsl:template>



<!--+
    |  Build a list of XPaths for items matching given path.  The result will look
    |  like:
    |
    |    <results>
    |      <match>/foo/bar[1]</match>
    |      <match>/foo/bar[2]</match>
    |    </result>
    +-->
<xsl:template name="build-item-list">
  <xsl:param name="path-stack"/>
  <xsl:param name="depth" select="count(ancestor-or-self::object)"/>
  <xsl:param name="lookup-path" select="@select"/>

  <xsl:variable name="context-node-xpath">
    <xsl:call-template name="context-node-xpath-with-document">
      <xsl:with-param name="context-node-xpath">
	<xsl:call-template name="path-stack-find-path">
	  <xsl:with-param name="path-stack" select="$path-stack"/>
	  <xsl:with-param name="depth" select="$depth"/>
	</xsl:call-template>
      </xsl:with-param>
    </xsl:call-template>
  </xsl:variable>

  <!-- Emit the expansion from the given XPath: first we bounce into the document at the context node
       and conduct the search from there. -->
  <results>
    <xsl:choose>
      <!-- Try with EXSLT (xsltproc, xalan) -->
      <xsl:when test="function-available('dyn:evaluate')">
	<xsl:apply-templates select="dyn:evaluate($context-node-xpath)" mode="emit-matching-items">
	  <xsl:with-param name="lookup-path" select="$lookup-path"/>
	</xsl:apply-templates>
      </xsl:when>

      <!-- Try with Saxon 6.5.5 -->
      <xsl:when test="function-available('saxon655:evaluate')">
	<xsl:apply-templates select="saxon655:evaluate($context-node-xpath)" mode="emit-matching-items">
	  <xsl:with-param name="lookup-path" select="$lookup-path"/>
	</xsl:apply-templates>
      </xsl:when>

      <!-- Try with Saxon v9 -->
      <xsl:when test="function-available('saxon9:evaluate')">
	<xsl:apply-templates select="saxon9:evaluate($context-node-xpath)" mode="emit-matching-items">
	  <xsl:with-param name="lookup-path" select="$lookup-path"/>
	</xsl:apply-templates>
      </xsl:when>

      <xsl:otherwise>
	<xsl:message>You are using an XSLT processor without evaluate() support.</xsl:message>
      </xsl:otherwise>
    </xsl:choose>
  </results>
</xsl:template>


<!-- This is our initial bounce into the document.  This allows us to search for nodes that
     match the user-supplied XPath from this context, allowing the user to supply relative
     XPaths. -->
<xsl:template match="/|node()|@*" mode="emit-matching-items">
  <xsl:param name="lookup-path"/>

  <!-- Emit the expansion from the given XPath -->
  <xsl:choose>
    <!-- Try with EXSLT (xsltproc, xalan) -->
    <xsl:when test="function-available('dyn:evaluate')">
      <xsl:apply-templates select="dyn:evaluate($lookup-path)" mode="emit-matching-item-result"/>
    </xsl:when>

    <!-- Try with Saxon 6.5.5 -->
    <xsl:when test="function-available('saxon655:evaluate')">
      <xsl:apply-templates select="saxon655:evaluate($lookup-path)" mode="emit-matching-item-result"/>
    </xsl:when>

    <!-- Try with Saxon v9 -->
    <xsl:when test="function-available('saxon9:evaluate')">
      <xsl:apply-templates select="saxon9:evaluate($lookup-path)" mode="emit-matching-item-result"/>
    </xsl:when>

    <xsl:otherwise>
      <!-- We've already flagged this problem, just emit nothing -->
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>


<!-- Emit <match>XPath-of-node</match>, where XPath-of-node is the absolute XPath reference
     for this node -->
<xsl:template match="/|node()|@*" mode="emit-matching-item-result">
  <match><xsl:call-template name="build-XPath"/></match>
</xsl:template>


<!--+
    |  This template expands to the absolute XPath expression for the current node.  This is
    |  achieved by walking the element hierarchy, from the top-most element to the current
    |  element; for each element, emit '/' followed by an XPath Expression that uniquely selects
    |  that element amongst its siblings.
    +-->
<xsl:template name="build-XPath">
  <xsl:apply-templates select="ancestor-or-self::*" mode="build-XPath"/>
</xsl:template>


<!-- Emit a '/' followed by the XPath Expression to uniquely select
     this element amongst its siblings.  This may be used to build an
     absolute XPath Expression.  The process is slightly involved as
     we must map the element's source document qname to a qname that
     will work within the stylesheet's namespace declarations -->
<xsl:template match="*" mode="build-XPath">
  <xsl:variable name="current-node-document-qname" select="name()"/>

  <xsl:variable name="current-node-stylesheet-namespace-prefix">
    <xsl:if test="namespace-uri() != ''">
      <xsl:call-template name="stylesheet-namespace-prefix-for-current-node"/>
    </xsl:if>
  </xsl:variable>

  <xsl:variable name="current-node-stylesheet-qname">
    <xsl:choose>
      <xsl:when test="normalize-space($current-node-stylesheet-namespace-prefix)">
	<xsl:value-of select="concat($current-node-stylesheet-namespace-prefix, ':', local-name())"/>
      </xsl:when>
      <xsl:otherwise>
	<xsl:value-of select="local-name()"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="count-preceding-siblings-of-current-node"
		select="count(preceding-sibling::*[name() = $current-node-document-qname])"/>
  <xsl:variable name="count-following-siblings-of-current-node"
		select="count(following-sibling::*[name() = $current-node-document-qname])"/>
  <xsl:variable name="count-siblings-of-current-node"
		select="$count-preceding-siblings-of-current-node + $count-following-siblings-of-current-node"/>

  <xsl:value-of select="concat('/',$current-node-stylesheet-qname)"/>
  <xsl:if test="$count-siblings-of-current-node > 0">
    <xsl:value-of select="concat('[', $count-preceding-siblings-of-current-node + 1, ']')"/>
  </xsl:if>
</xsl:template>


<!-- This template emits the namespace prefix (defined by some "xmlns" attribute within the
     stylesheet element of this document) for the current node. -->
<xsl:template name="stylesheet-namespace-prefix-for-current-node">
  <xsl:variable name="current-node-namespace-uri" select="namespace-uri()"/>

  <xsl:variable name="found-namespace-declaration">
    <xsl:for-each select="document('')/xsl:stylesheet/namespace::*">
      <xsl:if test=". = $current-node-namespace-uri">FOUND</xsl:if>
    </xsl:for-each>
  </xsl:variable>

  <xsl:variable name="stylesheet-namespace-prefix">
    <xsl:for-each select="document('')/xsl:stylesheet/namespace::*">
      <xsl:if test=". = $current-node-namespace-uri">
	<xsl:value-of select="name()"/>
      </xsl:if>
    </xsl:for-each>
  </xsl:variable>

  <xsl:if test="not(normalize-space($found-namespace-declaration))">
    <xsl:message>Error: cannot find namespace prefix for URI <xsl:value-of select="$current-node-namespace-uri"/></xsl:message>
  </xsl:if>

  <xsl:value-of select="$stylesheet-namespace-prefix"/>
</xsl:template>

</xsl:stylesheet>
