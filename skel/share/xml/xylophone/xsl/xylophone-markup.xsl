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
    |  This file contains utility templates for marking up values
    |  according to RFC 2253.
    +-->


<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version='1.0'>


<!--+
    |  Markup an attribute value.  This is distinct from marking up the value
    |  when it is part of an RDN.
    +-->
<xsl:template name="markup-attribute-value">
  <xsl:param name="value"/>

  <!-- Potentially escape first character -->
  <xsl:variable name="value-w-init-markup">
    <xsl:choose>
      <xsl:when test="starts-with($value,' ')">
	<xsl:value-of select="concat('\',$value)"/>
      </xsl:when>

      <xsl:otherwise>
	<xsl:value-of select="$value"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <!-- Emit either string with final space marked up or the string so far -->
  <xsl:choose>
    <xsl:when test="contains(substring($value-w-init-markup, string-length($value-w-init-markup)),' ')">
      <xsl:value-of select="concat(substring($value-w-init-markup, 1, string-length($value-w-init-markup)-1), '\ ')"/>
    </xsl:when>

    <xsl:otherwise>
      <xsl:value-of select="$value-w-init-markup"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>



<!--+
    |  Convert attribute values so reserved characters may be included.  RFC 2253
    |  states that the following characters must be escaped:
    |
    |     o a space or "#" character occuring at the beginning of a string,
    |     o a space character occuring at the end of the string,
    |     o any instaces of ",", "+", """, "\", "<", ">", "=" or ";" characters.
    |
    |  Characters are escaped by prefixing them with a "\", so "<" is escaped as
    |  "\<".  Other characters may be escaped by replacing them with "\nn" where
    |  "nn" represents a two-digit hexadecimal number, which forms a single byte
    |  in the code of the character.
    +-->
<xsl:template name="markup-rdn-value">
  <xsl:param name="value"/>

  <!-- Mark up the bulk of the text -->
  <xsl:variable name="value-w-markup"><xsl:call-template name="markup-rdn-value-main"><xsl:with-param name="value" select="$value"/></xsl:call-template></xsl:variable>

  <!-- Potentially escape first character -->
  <xsl:variable name="value-w-init-markup">
    <xsl:choose>
      <xsl:when test="starts-with($value-w-markup,' ') or starts-with($value-w-markup,'#')">
	<xsl:value-of select="concat('\',$value-w-markup)"/>
      </xsl:when>

      <xsl:otherwise>
	<xsl:value-of select="$value-w-markup"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <!-- Emit either string with final space marked up or the string so far -->
  <xsl:choose>
    <xsl:when test="contains(substring($value-w-init-markup, string-length($value-w-init-markup)),' ')">
      <xsl:value-of select="concat(substring($value-w-init-markup, 1, string-length($value-w-init-markup)-1), '\ ')"/>
    </xsl:when>

    <xsl:otherwise>
      <xsl:value-of select="$value-w-init-markup"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>


<!--+
    |  Emit a string where all characters ",", "+", """, "\", "<", ">" and ";" are
    |  replaced by their escaped equivalent "\,", "\+", "\"", "\\", "\<", "\>" and
    |  "\;" respectively. 
    |
    |  NB.  RFC 2253 has a contradiction about whether "=" is escaped.  The BNF has
    |  "=" as a possible value for "special", which is then required to be
    |  marked-up; however, the body-text for section 2.4 has the following:
    |
    |     If the UTF-8 string does not have any of the following
    |     characters which need escaping, then that string can be
    |     used as the string representation of the value.
    |
    |      o   a space or "#" character occurring at the beginning
    |          of the string
    |
    |      o   a space character occurring at the end of the string
    |
    |      o   one of the characters ",", "+", """, "\", "<", ">" or
    |          ";"
    |
    |     Implementations MAY escape other characters.
    |
    |  Since we MAY escape other characters, we do so for "=" to be safe.
    +-->
<xsl:template name="markup-rdn-value-main">
  <xsl:param name="value"/>

  <xsl:call-template name="markup-value-char">
    <xsl:with-param name="markup-char" select="','"/>
    <xsl:with-param name="value">
      
      <xsl:call-template name="markup-value-char">
	<xsl:with-param name="markup-char" select="'&quot;'"/>
	<xsl:with-param name="value">

	  <xsl:call-template name="markup-value-char">
	    <xsl:with-param name="markup-char" select="'+'"/>
	    <xsl:with-param name="value">
	  
	      <xsl:call-template name="markup-value-char">
		<xsl:with-param name="markup-char" select="'='"/>
		<xsl:with-param name="value">
		  
		  <xsl:call-template name="markup-value-char">
		    <xsl:with-param name="markup-char" select="'&lt;'"/>
		    <xsl:with-param name="value">
		      
		      <xsl:call-template name="markup-value-char">
			<xsl:with-param name="markup-char" select="'>'"/>
			<xsl:with-param name="value">
			  
			  <xsl:call-template name="markup-value-char">
			    <xsl:with-param name="markup-char" select="';'"/>
			    <xsl:with-param name="value">
			      
			      <!-- We must process back-slash characters first to avoid double-markup -->
			      <xsl:call-template name="markup-value-char">
				<xsl:with-param name="markup-char" select="'\'"/>
				<xsl:with-param name="value" select="$value"/>
			      </xsl:call-template>
			    </xsl:with-param>
			  </xsl:call-template>

			</xsl:with-param>
		      </xsl:call-template>
		      
		    </xsl:with-param>
		  </xsl:call-template>
		  
		</xsl:with-param>
	      </xsl:call-template>

	    </xsl:with-param>
	  </xsl:call-template>

	</xsl:with-param>
      </xsl:call-template>

    </xsl:with-param>
  </xsl:call-template>
</xsl:template>



<!--+
    |  Mark up all instances of $markup-char with the prefix '\', so
    |  any instances of the single character string "$markup-char"
    |  is replaced with the two-character string "\$markup-char".
    |  For example, if $markup-char is "," and $value is "foo, bar"
    |  then the this template will emit "foo\, bar"
    +-->
<xsl:template name="markup-value-char">
  <xsl:param name="value"/>
  <xsl:param name="markup-char"/>

  <xsl:choose>
    <xsl:when test="contains($value, $markup-char)">
      <xsl:value-of select="concat(substring-before($value,$markup-char),'\',$markup-char)"/>
      <xsl:call-template name="markup-value-char">
	<xsl:with-param name="value" select="substring-after($value,$markup-char)"/>
	<xsl:with-param name="markup-char" select="$markup-char"/>
      </xsl:call-template>
    </xsl:when>

    <xsl:otherwise>
      <xsl:value-of select="$value"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>


</xsl:stylesheet>
