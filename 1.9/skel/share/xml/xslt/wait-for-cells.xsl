<?xml version="1.0" encoding="utf-8"?>

<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
		xmlns:dcache="http://www.dcache.org/2008/01/Info">

<xsl:output method="text" media-type="text/plain"/>

<xsl:param name="dCache-web-host" select="'localhost'"/>
<xsl:param name="dCache-web-port" select="'2288'"/>
<xsl:param name="cells"/>

<xsl:strip-space elements="*"/>


<!--+
    |  Main entry point.
    +-->
<xsl:template match="/">
  <xsl:variable name="missing-cells">
    <xsl:call-template name="missing-cells">
      <xsl:with-param name="cells-this">
	<xsl:call-template name="first-list-item">
	  <xsl:with-param name="list" select="normalize-space($cells)"/>
	</xsl:call-template>
      </xsl:with-param>
      
      <xsl:with-param name="cells-todo"
		      select="substring-after(normalize-space($cells),' ')"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:if test="normalize-space($missing-cells)">
    <xsl:text>Missing: </xsl:text>
    <xsl:value-of select="$missing-cells"/>
    <xsl:text>&#xA;</xsl:text>
  </xsl:if>
</xsl:template>


<!--+
    |  Iterate over white-space list of cell references.
    +-->
<xsl:template name="missing-cells">
  <xsl:param name="cells-todo"/>
  <xsl:param name="cells-this"/>

  <xsl:if test="normalize-space($cells-this)">
    
    <xsl:call-template name="emit-if-missing">
      <xsl:with-param name="complete-name" select="$cells-this"/>
    </xsl:call-template>

    <!-- Iterate onto next cell item -->
    <xsl:call-template name="missing-cells">
      <xsl:with-param name="cells-this">
	<xsl:call-template name="first-list-item">
	  <xsl:with-param name="list" select="$cells-todo"/>
	</xsl:call-template>
      </xsl:with-param>

      <xsl:with-param name="cells-todo"
		      select="normalize-space(substring-after($cells-todo,' '))"/>
    </xsl:call-template>
  </xsl:if>
</xsl:template>



<!--+
    |  Emit the first item in white-space limited list.
    +-->
<xsl:template name="first-list-item">
  <xsl:param name="list"/>

  <xsl:choose>
    <xsl:when test="contains( $list, ' ')">
      <xsl:value-of select="substring-before($list,' ')"/>
    </xsl:when>
    <xsl:otherwise>
      <xsl:value-of select="$list"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>


<!--+
    |  Emit a cell reference if it's currently not running.  If a supposed well-known
    |  cell is unknown, it is emitted as cell@<unknown> to indicate this.
    +-->
<xsl:template name="emit-if-missing">
  <xsl:param name="complete-name"/>

  <xsl:variable name="cell">
    <xsl:call-template name="extract-cell">
      <xsl:with-param name="complete-name" select="$complete-name"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:variable name="domain">
    <xsl:call-template name="extract-domain">
      <xsl:with-param name="complete-name" select="$complete-name"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:if test="not(/dcache:dCache/dcache:domains/dcache:domain[@name=$domain]/dcache:cells/dcache:cell[@name=$cell])">
    <xsl:value-of select="concat( $cell,'@',$domain, ' ')"/>
  </xsl:if>
</xsl:template>


<!--+
    |  Lookup a cell reference's cell, either the complete name or the bit before the
    |  '@'.
    +-->
<xsl:template name="extract-cell">
  <xsl:param name="complete-name"/>

  <xsl:choose>
    <xsl:when test="contains($complete-name,'@')">
      <xsl:value-of select="substring-before($complete-name,'@')"/>
    </xsl:when>
    
    <xsl:otherwise>
      <xsl:value-of select="$complete-name"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>



<!--+
    |  Lookup a cell reference's domain, either extracting explicitly, taking it from
    |  the dCacheDomain's routing table or emitting a hardcoded "<unknown>".
    +-->
<xsl:template name="extract-domain">
  <xsl:param name="complete-name"/>

  <xsl:choose>
    <!-- Explicitly specified -->
    <xsl:when test="contains($complete-name,'@')">
      <xsl:value-of select="substring-after($complete-name,'@')"/>
    </xsl:when>
    
    <!-- or the domain of a "Well Known" cell -->
    <xsl:when test="/dcache:dCache/dcache:domains/dcache:domain[@name='dCacheDomain']/dcache:routing/dcache:named-cells/dcache:cell[@name=$complete-name]">
      <xsl:value-of select="/dcache:dCache/dcache:domains/dcache:domain[@name='dCacheDomain']/dcache:routing/dcache:named-cells/dcache:cell[@name=$complete-name]/dcache:domainref/@name"/>
    </xsl:when>

    <!-- or just say <unknown> -->
    <xsl:otherwise>
      <xsl:text>&lt;unknown&gt;</xsl:text>
    </xsl:otherwise>
  </xsl:choose>  
</xsl:template>

</xsl:stylesheet>
