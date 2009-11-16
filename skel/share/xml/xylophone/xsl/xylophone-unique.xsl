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
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

  <!--+
      |  This template expands to a string contains DUPLICATE
      |  for each unique element that would be violated if
      |  the object with this count is published.
      +-->
  <xsl:template name="check-duplication-object">
    <xsl:param name="list-item"/>
    <xsl:param name="path-stack"/>
    <xsl:param name="count"/>

    <!-- Scan unique objects, expanding any that are violated -->
    <xsl:apply-templates select="unique" mode="eval-unique">
      <xsl:with-param name="list-item" select="$list-item"/>
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="count" select="$count"/>

      <xsl:with-param name="abs-path">
	<xsl:call-template name="combine-paths">
	  <xsl:with-param name="path" select="@select"/>

	  <xsl:with-param name="rel-path">
	    <xsl:call-template name="path-stack-find-path">
	      <xsl:with-param name="path-stack" select="$path-stack"/>
	    </xsl:call-template>
	  </xsl:with-param>
	</xsl:call-template>
      </xsl:with-param>
    </xsl:apply-templates>
  </xsl:template>

  <!--+
      |  This template expands to a string contains DUPLICATE
      |  for each unique element that would be violated if
      |  the attribute with this count is published.
      +-->
  <xsl:template name="check-duplication-attr">
    <xsl:param name="list-item"/>
    <xsl:param name="path-stack"/>
    <xsl:param name="count"/>

    <!-- Scan unique objects, expanding any that are violated -->
    <xsl:apply-templates select="unique" mode="eval-unique">
      <xsl:with-param name="list-item" select="$list-item"/>
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="count" select="$count"/>
      <xsl:with-param name="abs-path">
	<xsl:call-template name="combine-paths">
	  <xsl:with-param name="path" select="@select"/>

	  <xsl:with-param name="rel-path">
	    <xsl:call-template name="path-stack-find-path">
	      <xsl:with-param name="path-stack" select="$path-stack"/>
	      <xsl:with-param name="depth" select="count(ancestor-or-self::object)"/>
	    </xsl:call-template>
	  </xsl:with-param>
	</xsl:call-template>
      </xsl:with-param>
    </xsl:apply-templates>
  </xsl:template>


  <!--+
      |  In eval-unique mode we check whether any unique elements
      |  are violated.
      +-->

  <!-- Ignore any text within the unique elements -->
  <xsl:template match="text()" mode="eval-unique"/>

  <!--+
      |  Check a uniqueness constraint.  The count parameter
      |  contains the current iterator numbers (either for
      |  objects or attributes).  We count down to see if
      |  any earlier object violates  this uniqueness
      |  constraint.
      +-->
  <xsl:template match="unique" mode="eval-unique">
    <xsl:param name="list-item"/>
    <xsl:param name="path-stack"/>
    <xsl:param name="count"/>
    <xsl:param name="abs-path"/>

    <!-- evaluate unique's value with current count -->
    <xsl:variable name="this-unique-value">
      <xsl:apply-templates select="*" mode="eval-attr">
	<xsl:with-param name="depth" select="count(ancestor-or-self::object)"/>
	<xsl:with-param name="list-item" select="$list-item"/>
	<xsl:with-param name="path-stack">
	  <xsl:call-template name="path-stack-add">
	    <xsl:with-param name="current-path-stack" select="$path-stack"/>
	    <xsl:with-param name="path" select="concat($abs-path,'[',$count,']')"/>
	  </xsl:call-template>
	</xsl:with-param>
      </xsl:apply-templates>
    </xsl:variable>

    <!-- Iterate over earlier count values, checking for violations -->
    <xsl:call-template name="sufficiently-unique-itr">
      <xsl:with-param name="count" select="$count - 1"/>
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="abs-path" select="$abs-path"/>
      <xsl:with-param name="list-item" select="$list-item"/>
      <xsl:with-param name="reference-unique-value" select="$this-unique-value"/>
    </xsl:call-template>
  </xsl:template>


  <!-- Check an earlier count value -->
  <xsl:template name="sufficiently-unique-itr">
    <xsl:param name="count"/>
    <xsl:param name="path-stack"/>
    <xsl:param name="abs-path"/>
    <xsl:param name="list-item"/>
    <xsl:param name="reference-unique-value"/>

    <xsl:if test="$count > 0">
      <xsl:variable name="this-unique-val">
	<xsl:apply-templates select="*" mode="eval-attr">
	  <xsl:with-param name="depth" select="count(ancestor-or-self::object)"/>
	  <xsl:with-param name="list-item" select="$list-item"/>
	  <xsl:with-param name="path-stack">
	    <xsl:call-template name="path-stack-add">
	      <xsl:with-param name="current-path-stack" select="$path-stack"/>
	      <xsl:with-param name="path" select="concat($abs-path,'[',$count,']')"/>
	    </xsl:call-template>
	  </xsl:with-param>
	</xsl:apply-templates>
      </xsl:variable>

      <xsl:choose>
	<!-- If we find a violation, don't bother to check further -->
	<xsl:when test="$this-unique-val = $reference-unique-value">
	  <xsl:text>DUPLICATE</xsl:text>
	</xsl:when>

	<xsl:otherwise>
	  <!-- Check the next lower count for violation -->
	  <xsl:call-template name="sufficiently-unique-itr">
	    <xsl:with-param name="count" select="$count - 1"/>
	    <xsl:with-param name="path-stack" select="$path-stack"/>
	    <xsl:with-param name="abs-path" select="$abs-path"/>
	    <xsl:with-param name="list-item" select="$list-item"/>
	    <xsl:with-param name="reference-unique-value"
			    select="$reference-unique-value"/>
	  </xsl:call-template>
	</xsl:otherwise>
      </xsl:choose>
    </xsl:if>
  </xsl:template>

</xsl:stylesheet>
