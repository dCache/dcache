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
    |  This file contains support for evaluating simple predicates.
    |
    |  NB these named templates do not know whether they are in an
    |     object tree or class tree, so the depth must be supplied
    |     as an explicit parameter.
    +-->

<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform">


<!--+
    |  Check a single predicate element (e.g., <suppress/> or <allow/> elements).
    |
    |  If an element test matches, then this template will expand to a
    |  non-zero-length string.  If it does not match, the expansion is
    |  to a zero-length string.  This allows a simple logical OR by expanding all
    |  instances of the predicates and testing for non-zero-length expansion.
    +-->
<xsl:template match="*" mode="eval-predicate">
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>
  <xsl:param name="depth" select="count(ancestor-or-self::object)"/>

  <xsl:variable name="data">
    <xsl:apply-templates select="*|text()" mode="eval-attr">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="depth" select="$depth"/>
      <xsl:with-param name="list-item" select="$list-item"/>
    </xsl:apply-templates>
  </xsl:variable>

  <xsl:variable name="this-should-expand">
    <xsl:choose>

      <!-- The 'is' check (the default) -->
      <xsl:when test="@check = 'is' or count(@check) = 0">
	<xsl:if test="count(@test) = 0">
	  <xsl:message>Missing test attribute in <xsl:value-of select="name()"/> element.</xsl:message>
	</xsl:if>

	<xsl:value-of select="$data = @test"/>
      </xsl:when>


      <!-- The 'starts-with' check -->
      <xsl:when test="@check = 'starts-with'">
	<xsl:if test="count(@test) = 0">
	  <xsl:message>Missing test attribute in <xsl:value-of select="name()"/> element.</xsl:message>
	</xsl:if>

	<xsl:value-of select="starts-with($data,@test)"/>	
      </xsl:when>


      <!-- The 'ends-with' check -->
      <xsl:when test="@check = 'ends-with'">
	<xsl:if test="count(@test) = 0">
	  <xsl:message>Missing test attribute in <xsl:value-of select="name()"/> element.</xsl:message>
	</xsl:if>

	<xsl:choose>
	  <xsl:when test="string-length($data) &lt; string-length(@test)">
	    <xsl:value-of select="false()"/>
	  </xsl:when>

	  <xsl:otherwise>
	    <xsl:value-of select="substring($data,1+string-length($data)-string-length(@test)) = @test"/>
	  </xsl:otherwise>
	</xsl:choose>
      </xsl:when>


      <!-- The 'contains' check -->
      <xsl:when test="@check = 'contains'">
	<xsl:if test="count(@test) = 0">
	  <xsl:message>Missing test attribute in <xsl:value-of select="name()"/> element.</xsl:message>
	</xsl:if>

	<xsl:value-of select="contains($data,@test)"/>	
      </xsl:when>


      <!--+
          |  Numerical tests
          +-->

      <!-- The 'less-than' check -->
      <xsl:when test="@check = 'less-than'">
	<xsl:if test="count(@test) = 0">
	  <xsl:message>Missing test attribute in <xsl:value-of select="name()"/> element.</xsl:message>
	</xsl:if>

	<xsl:value-of select="number($data) &lt; number(@test)"/>
      </xsl:when>

      <!-- The 'less-than-or-equal' check -->
      <xsl:when test="@check = 'less-than-or-equal'">
	<xsl:if test="count(@test) = 0">
	  <xsl:message>Missing test attribute in <xsl:value-of select="name()"/> element.</xsl:message>
	</xsl:if>

	<xsl:value-of select="number($data) &lt;= number(@test)"/>
      </xsl:when>

      <!-- The 'equal' check -->
      <xsl:when test="@check = 'equal'">
	<xsl:if test="count(@test) = 0">
	  <xsl:message>Missing test attribute in <xsl:value-of select="name()"/> element.</xsl:message>
	</xsl:if>

	<xsl:value-of select="number($data) = number(@test)"/>
      </xsl:when>

      <!-- The 'not-equal' check -->
      <xsl:when test="@check = 'not-equal'">
	<xsl:if test="count(@test) = 0">
	  <xsl:message>Missing test attribute in <xsl:value-of select="name()"/> element.</xsl:message>
	</xsl:if>

	<xsl:value-of select="not(number($data) = number(@test))"/>
      </xsl:when>

      <!-- The 'greater-than-or-equal' check -->
      <xsl:when test="@check = 'greater-than-or-equal'">
	<xsl:if test="count(@test) = 0">
	  <xsl:message>Missing test attribute in <xsl:value-of select="name()"/> element.</xsl:message>
	</xsl:if>

	<xsl:value-of select="number($data) >= number(@test)"/>
      </xsl:when>


      <!-- The 'greater-than' check -->
      <xsl:when test="@check = 'greater-than'">
	<xsl:if test="count(@test) = 0">
	  <xsl:message>Missing test attribute in <xsl:value-of select="name()"/> element.</xsl:message>
	</xsl:if>

	<xsl:value-of select="number($data) > number(@test)"/>
      </xsl:when>



      <xsl:otherwise>
	<xsl:message>Unknown mode attribute: <xsl:value-of select="@mode"/> in <xsl:value-of select="name()"/> element.</xsl:message>
	<xsl:value-of select="false()"/>
      </xsl:otherwise>

    </xsl:choose>
  </xsl:variable>

  <xsl:if test="$this-should-expand = 'true'">
    <xsl:text>MATCHES</xsl:text>
  </xsl:if>
</xsl:template>




</xsl:stylesheet>
