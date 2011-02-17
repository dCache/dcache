<?xml version="1.0" encoding="utf-8"?>

<!--+
    | Copyright (c) 2011, Deutsches Elektronen-Synchrotron (DESY)
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
    |  This file contains support for processing the conditional elements
    |  These are elements that follow the if-then pattern:
    |
    |     <optionally>
    |        <when test="foo">foo</when>
    |        - - publish activity - -
    |     </optionally>
    |
    |  and the case-test-test-...-otherwise pattern:
    |
    |     <choose>
    |       <optionally>
    |         <when test="foo">foo</when>
    |         - - publish activity - -
    |       </optionally>
    |
    |       <optionally>
    |         <when test="bar">foo</when>
    |         - - publish activity - -
    |       </optionally>
    |
    |       <otherwise>
    |         - - publish activity - -
    |       </otherwise>
    |     </choose>
    +-->

<xsl:stylesheet version="1.0"
		xmlns:exsl="http://exslt.org/common"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
		extension-element-prefixes="exsl">


<!-- Skip any text nodes when deciding if attr should be published -->
<xsl:template match="text()" mode="publish-conditional-attr"/>
<xsl:template match="text()" mode="publish-conditional-object"/>

<!-- Skip unknown elements -->
<xsl:template match="*" mode="publish-conditional-attr"/>
<xsl:template match="*" mode="publish-conditional-object"/>



<!-- Publish attributes inside an <optionally/>, if appropriate -->
<xsl:template match="optionally" mode="publish-conditional-attr">
  <xsl:param name="list-item"/>
  <xsl:param name="path-stack"/>

  <xsl:variable name="have-when">
    <xsl:apply-templates select="when" mode="eval-predicate">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="list-item" select="$list-item"/>
    </xsl:apply-templates>
  </xsl:variable>

  <xsl:if test="normalize-space($have-when)">
    <xsl:apply-templates select="attr" mode="publish">
      <xsl:with-param name="list-item" select="$list-item"/>
      <xsl:with-param name="path-stack" select="$path-stack"/>
    </xsl:apply-templates>
  </xsl:if>
</xsl:template>



<!-- Publish objects inside an <optionally/>, if appropriate -->
<xsl:template match="optionally" mode="publish-conditional-object">
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>
  <xsl:param name="parent-dn"/>

  <xsl:variable name="have-when">
    <xsl:apply-templates select="when" mode="eval-predicate">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="list-item" select="$list-item"/>
    </xsl:apply-templates>
  </xsl:variable>

  <xsl:if test="normalize-space($have-when)">
    <xsl:apply-templates select="object" mode="publish">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="list-item" select="$list-item"/>
      <xsl:with-param name="parent-dn" select="$parent-dn"/>
    </xsl:apply-templates>
  </xsl:if>
</xsl:template>


<!--+
    |  SUPPORT FOR <choose/> AND <attr/> ELEMENTS
    +-->

<!-- Publish attributes inside an <choose/>, if appropriate -->
<xsl:template match="choose" mode="publish-conditional-attr">
  <xsl:param name="list-item"/>
  <xsl:param name="path-stack"/>

  <!-- Try each <optionally/> in turn -->
  <xsl:apply-templates select="optionally[1]" mode="publish-conditional-attr-itr">
    <xsl:with-param name="path-stack" select="$path-stack"/>
    <xsl:with-param name="list-item" select="$list-item"/>
  </xsl:apply-templates>
</xsl:template>


<xsl:template match="optionally" mode="publish-conditional-attr-itr">
  <xsl:param name="list-item"/>
  <xsl:param name="path-stack"/>

  <xsl:variable name="have-when">
    <xsl:apply-templates select="when" mode="eval-predicate">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="list-item" select="$list-item"/>
    </xsl:apply-templates>
  </xsl:variable>

  <xsl:choose>
    <xsl:when test="normalize-space($have-when)">
      <xsl:apply-templates select="attr" mode="publish">
	<xsl:with-param name="list-item" select="$list-item"/>
	<xsl:with-param name="path-stack" select="$path-stack"/>
      </xsl:apply-templates>
    </xsl:when>

    <xsl:when test="following-sibling::optionally">
      <xsl:apply-templates select="following-sibling::optionally[1]"
	                   mode="publish-conditional-attr-itr">
	<xsl:with-param name="path-stack" select="$path-stack"/>
	<xsl:with-param name="list-item" select="$list-item"/>
      </xsl:apply-templates>
    </xsl:when>

    <xsl:when test="../otherwise">
      <xsl:apply-templates select="../otherwise[1]/attr"
			   mode="publish">
	<xsl:with-param name="path-stack" select="$path-stack"/>
	<xsl:with-param name="list-item" select="$list-item"/>
      </xsl:apply-templates>
    </xsl:when>
  </xsl:choose>
</xsl:template>



<!--+
    |  SUPPORT FOR <choose/> AND <object/> ELEMENTS
    +-->

<!-- Publish objects inside an <choose/>, if appropriate -->
<xsl:template match="choose" mode="publish-conditional-object">
  <xsl:param name="list-item"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="parent-dn"/>

  <!-- Try each <optionally/> in turn -->
  <xsl:apply-templates select="optionally[1]" mode="publish-conditional-object-itr">
    <xsl:with-param name="path-stack" select="$path-stack"/>
    <xsl:with-param name="list-item" select="$list-item"/>
    <xsl:with-param name="parent-dn" select="$parent-dn"/>
  </xsl:apply-templates>
</xsl:template>


<xsl:template match="optionally" mode="publish-conditional-object-itr">
  <xsl:param name="list-item"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="parent-dn"/>

  <xsl:variable name="have-when">
    <xsl:apply-templates select="when" mode="eval-predicate">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="list-item" select="$list-item"/>
    </xsl:apply-templates>
  </xsl:variable>

  <xsl:choose>
    <xsl:when test="normalize-space($have-when)">
      <xsl:apply-templates select="object" mode="publish">
	<xsl:with-param name="path-stack" select="$path-stack"/>
	<xsl:with-param name="list-item" select="$list-item"/>
	<xsl:with-param name="parent-dn" select="$parent-dn"/>
      </xsl:apply-templates>
    </xsl:when>

    <xsl:when test="following-sibling::optionally">
      <xsl:apply-templates select="following-sibling::optionally[1]"
	                   mode="publish-conditional-object-itr">
	<xsl:with-param name="path-stack" select="$path-stack"/>
	<xsl:with-param name="list-item" select="$list-item"/>
	<xsl:with-param name="parent-dn" select="$parent-dn"/>
      </xsl:apply-templates>
    </xsl:when>

    <xsl:when test="../otherwise">
      <xsl:apply-templates select="../otherwise[1]/object"
			   mode="publish">
	<xsl:with-param name="path-stack" select="$path-stack"/>
	<xsl:with-param name="list-item" select="$list-item"/>
	<xsl:with-param name="parent-dn" select="$parent-dn"/>
      </xsl:apply-templates>
    </xsl:when>
  </xsl:choose>
</xsl:template>


</xsl:stylesheet>
