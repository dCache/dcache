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
    |  This file contains support for mapping one substring
    |  into another.
    +-->


<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version='1.0'>


<!--+
    |   <sub/> element
    |
    |   Replace a substring with another.
    +-->
<xsl:template match="sub" mode="eval-mapping">
  <xsl:param name="before"/>
  <xsl:param name="successful-sub"/>

  <!-- Update our atleast-one-sub-been-successful variable. -->
  <xsl:variable name="new-successful-sub">
    <xsl:value-of select="$successful-sub"/>
    <xsl:if test="contains( $before, @match)">
      <xsl:text>MATCHES</xsl:text>
    </xsl:if>
  </xsl:variable>

  <!-- Calculate the value after the substitution -->
  <xsl:variable name="after">
    <xsl:call-template name="eval-mapping-eval-sub">
      <xsl:with-param name="before" select="$before"/>
    </xsl:call-template>
  </xsl:variable>

  <!-- Process next one, if there is a next -->
  <xsl:call-template name="eval-mapping-next">
    <xsl:with-param name="before" select="$after"/>
    <xsl:with-param name="successful-sub" select="$new-successful-sub"/>
  </xsl:call-template>
</xsl:template>





<!--+
    |   <default/> element
    |
    |   This template expands to the default value.  It only has effect
    |   with the special mode eval-mapping-eval-default.
    +-->
<xsl:template match="default" mode="eval-mapping-eval-default">
  <xsl:param name="item"/>

  <xsl:choose>
    <xsl:when test="@value">
      <xsl:value-of select="@value"/>
    </xsl:when>

    <xsl:otherwise>
      <xsl:apply-templates mode="eval-mapping-eval-default">
	<xsl:with-param name="item" select="$item"/>
      </xsl:apply-templates>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>


<!--+
    |   The <item/> element
    |
    |  This is expanded to (more or less) the original text within
    |  the default.
    +-->
<xsl:template match="item" mode="eval-mapping-eval-default">
  <xsl:param name="item"/>

  <xsl:value-of select="$item"/>
</xsl:template>


<!--+
    |  <remove/> element
    |
    |  Remove any occurances of certain characters.
    +-->
<xsl:template match="remove" mode="eval-mapping">
  <xsl:param name="before"/>
  <xsl:param name="successful-sub"/>

  <!-- Calculate the new text -->
  <xsl:variable name="after">
    <xsl:choose>
      <xsl:when test="not(@chars)">
	<xsl:message>Missing "chars" attribute in remove element</xsl:message>
	<xsl:value-of select="$before"/>
      </xsl:when>

      <xsl:otherwise>
	<xsl:value-of select="translate($before,@chars,'')"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <!-- Process the next element -->
  <xsl:call-template name="eval-mapping-next">
    <xsl:with-param name="before" select="$after"/>
    <xsl:with-param name="successful-sub" select="$successful-sub"/>
  </xsl:call-template>
</xsl:template>



<!--+
    |  <translate/>
    |
    |  Replace all occurances of certain characters with
    |  other characters.  Different modes are available:
    |      convert:  @from @to (of equal length).
    |      to-lower:  change upper-case to lower-case
    |      to-upper:  change lower-case to upper-case
    +-->
<xsl:template match="translate" mode="eval-mapping">
  <xsl:param name="before"/>
  <xsl:param name="successful-sub"/>

  <xsl:variable name="after">
    <xsl:choose>    

      <!-- Convert certain characters -->
      <xsl:when test="not(@mode) or @mode = 'convert'">
	<xsl:choose>
	  <xsl:when test="not(@from)">
	    <xsl:message>Missing "from" attribute in translate</xsl:message>
	    <xsl:value-of select="$before"/>
	  </xsl:when>

	  <xsl:when test="not(@to)">
	    <xsl:message>Missing "to" attribute in translate</xsl:message>
	    <xsl:value-of select="$before"/>
	  </xsl:when>

	  <xsl:otherwise>
	    <xsl:value-of select="translate($before,@from,@to)"/>
	  </xsl:otherwise>
	</xsl:choose>
      </xsl:when>

      <xsl:when test="@mode = 'to-lower'">
	<xsl:value-of select="translate($before,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')"/>
      </xsl:when>

      <xsl:when test="@mode = 'to-upper'">
	<xsl:value-of select="translate($before,'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ')"/>
      </xsl:when>

      <xsl:otherwise>
	<xsl:message>Unknown "mode" attribute value <xsl:value-of select="@mode"/> in translate element</xsl:message>
	<xsl:value-of select="$before"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:call-template name="eval-mapping-next">
    <xsl:with-param name="before" select="$after"/>
    <xsl:with-param name="successful-sub" select="$successful-sub"/>
  </xsl:call-template>
</xsl:template>


<!--+
    |  Apply a sub-like (sub or default-sub) operation.  This is
    |  useful for both the <sub/> and <default-sub/> elements
    +-->
<xsl:template name="eval-mapping-eval-sub">
  <xsl:param name="before"/>

  <xsl:choose>
    <xsl:when test="contains( $before, @match)">
      <xsl:value-of select="concat( substring-before( $before, @match), @replace-with, substring-after( $before, @match))"/>
    </xsl:when>
    
    <xsl:otherwise>
      <xsl:value-of select="$before"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>


<!-- Unknown elements are ignored, but we must chain onto the next one -->
<xsl:template match="*" mode="eval-mapping">
  <xsl:param name="before"/>
  <xsl:param name="successful-sub"/>

  <xsl:call-template name="eval-mapping-next">
    <xsl:with-param name="before" select="$before"/>
	<xsl:with-param name="successful-sub" select="$successful-sub"/>
  </xsl:call-template>
</xsl:template>



<!--+
    |  Process the next element, or emit the mapping result.
    +-->
<xsl:template name="eval-mapping-next">
  <xsl:param name="before"/>
  <xsl:param name="successful-sub"/>

  <xsl:choose>

    <!-- If there's more elements to process, do the next one -->
    <xsl:when test="following-sibling::*">
      <xsl:apply-templates select="following-sibling::*[1]" mode="eval-mapping">
	<xsl:with-param name="before" select="$before"/>
	<xsl:with-param name="successful-sub" select="$successful-sub"/>
      </xsl:apply-templates>
    </xsl:when>


    <!-- If there's no more to do... -->
    <xsl:otherwise>
      <xsl:call-template name="eval-mapping-publish-result">
	<xsl:with-param name="result" select="$before"/>
	<xsl:with-param name="successful-sub" select="$successful-sub"/>
      </xsl:call-template>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>



<!--+
    |  Emit the data as it has been mangled.  There may be further altered
    |  if a <default/> element is present and no <sub/> has been
    |  triggered.
    +-->
<xsl:template name="eval-mapping-publish-result">
  <xsl:param name="result"/>
  <xsl:param name="successful-sub"/>



  <!-- Emit something -->
  <xsl:choose>

    <!-- if no <sub/> was triggered and we have a <default/> element... -->
    <xsl:when test="not(normalize-space($successful-sub)) and (preceding-sibling::default | self::default)">
      <xsl:choose>

	<!-- Are we currently processing the <default/> ? -->
	<xsl:when test="self::default">
	  <xsl:apply-templates select="self::default" mode="eval-mapping-eval-default">
	    <xsl:with-param name="item" select="$result"/>
	  </xsl:apply-templates>
	</xsl:when>

	<!-- Otherwise, choose the last one ? -->
	<xsl:otherwise>
	  <xsl:apply-templates select="preceding-sibling::default[1]" mode="eval-mapping-eval-default">
	    <xsl:with-param name="item" select="$result"/>
	  </xsl:apply-templates>
	</xsl:otherwise>
      </xsl:choose>
    </xsl:when>

    <!-- Otherwise, we simply publish what we have -->
    <xsl:otherwise>
      <xsl:value-of select="$result"/>
    </xsl:otherwise>
  </xsl:choose>

</xsl:template>


</xsl:stylesheet>