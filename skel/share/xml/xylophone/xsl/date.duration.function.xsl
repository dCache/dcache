<?xml version="1.0"?>
<!--+
    |  date:duration implementation by Jeni Tennison, from
    |      http://exslt.org/date/functions/duration/index.html
    +-->
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:date="http://exslt.org/dates-and-times"
                xmlns:func="http://exslt.org/functions"
                extension-element-prefixes="date func">

<func:function name="date:duration">
	<xsl:param name="seconds">
      <xsl:choose>
         <xsl:when test="function-available('date:seconds')">
            <xsl:value-of select="date:seconds()" />
         </xsl:when>
         <xsl:otherwise>0</xsl:otherwise>
      </xsl:choose>
   </xsl:param>
   <xsl:variable name="duration">
      <xsl:variable name="day-s" select="60 * 60 * 24" />
      <xsl:variable name="hour-s" select="60 * 60" />
      <xsl:variable name="min-s" select="60" />
      <xsl:if test="$seconds &lt; 0">-</xsl:if>
      <xsl:text>P</xsl:text>
      <xsl:variable name="s" select="$seconds * ((($seconds >= 0) * 2) - 1)" />
      <xsl:variable name="days" select="floor($s div $day-s)" />
      <xsl:variable name="hours" select="floor(($s - ($days * $day-s)) div $hour-s)" />
      <xsl:variable name="mins" select="floor(($s - ($days * $day-s) - ($hours * $hour-s)) div $min-s)" />
      <xsl:variable name="secs" select="$s - ($days * $day-s) - ($hours * $hour-s) - ($mins * $min-s)" />
      <xsl:if test="$days">
         <xsl:value-of select="$days" />
         <xsl:text>D</xsl:text>
      </xsl:if>
      <xsl:if test="$hours or $mins or $secs">T</xsl:if>
      <xsl:if test="$hours">
         <xsl:value-of select="$hours" />
         <xsl:text>H</xsl:text>
      </xsl:if>
      <xsl:if test="$mins">
         <xsl:value-of select="$mins" />
         <xsl:text>M</xsl:text>
      </xsl:if>
      <xsl:if test="$secs">
         <xsl:value-of select="$secs" />
         <xsl:text>S</xsl:text>
      </xsl:if>
   </xsl:variable>
   <func:result select="string($duration)" />
</func:function>

</xsl:stylesheet>
