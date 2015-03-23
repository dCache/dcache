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
    |  This file contains definitions for how to evaluate elements found
    |  within the attr element.
    |
    |  These elements may appear within an object element or a class
    |  element.  If it's within a class, then the corresponding
    |  object element isn't known, so the depth within the object
    |  tree cannot be calculated automatically.  This is important
    |  for the path-stack, so an explicit depth must be
    |  passed.
    +-->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version='1.0'
                xmlns:exsl="http://exslt.org/common"
                xmlns:date="http://exslt.org/dates-and-times"
                extension-element-prefixes="exsl date">

<xsl:include href="date.format-date.function.xsl"/>
<xsl:include href="date.add.function.xsl"/>
<xsl:include href="date.duration.function.xsl"/>

<!--+
    |
    |  The <attr/> element
    |
    |  Evaluate an LDIF attribute value.
    |
    +-->
<xsl:template match="attr" mode="eval-attr">
  <xsl:param name="depth" select="count(ancestor-or-self::object)"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <xsl:apply-templates mode="eval-attr">
    <xsl:with-param name="path-stack" select="$path-stack"/>
    <xsl:with-param name="depth" select="$depth"/>
    <xsl:with-param name="list-item" select="$list-item"/>
  </xsl:apply-templates>
</xsl:template>



<!--+
    |
    |  The <lookup/> element
    |
    |  Lookup some dynamic dynamic information from the src XML.  If
    |  the content is missing, the (child) contents of the lookup
    |  element are used as a default value.
    |
    |  One can specify a default value should the lookup fail.  This
    |  default value can be specified in two ways:
    |
    |  The first method is using the "default" attribute.  This always
    |  available and takes precedence over other methods and declarations.
    |  It allows only for a static default value.
    |
    |  The second method is to specify the "child" attribute with the
    |  value of "default".  This allows a computed default value.
    |
    |  The child attribute may also given the value "path".  If so and
    |  the "path" attribute is not specified, then the path is taken
    |  by exanding the child elements.  The "path" element always takes
    |  precedence.
    |
    |  If no "child" attribute is specified, then a value of "default" is
    |  assumed.
    +-->
<xsl:template match="lookup" mode="eval-attr">
  <xsl:param name="depth"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <xsl:variable name="current-path">
    <xsl:call-template name="path-stack-find-path">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="depth" select="$depth"/>
    </xsl:call-template>
  </xsl:variable>

  <!-- Figure out what our default value should be -->
  <xsl:variable name="default-value">
    <xsl:choose>
      <xsl:when test="@default">
        <xsl:value-of select="@default"/>
      </xsl:when>

      <xsl:when test="not(@child) or @child='default'">
        <xsl:apply-templates mode="eval-attr">
	  <xsl:with-param name="path-stack" select="$path-stack"/>
	  <xsl:with-param name="depth" select="$depth"/>
          <xsl:with-param name="list-item" select="$list-item"/>
        </xsl:apply-templates>
      </xsl:when>

      <xsl:when test="@child='path'">
        <!-- User has supplied child='path' and no default attribute.  The default is then empty. -->
      </xsl:when>

      <xsl:otherwise>
        <xsl:message>WARNING: unknown "child" attribute value &quot;<xsl:value-of select="@child"/>&quot; in <xsl:value-of select="build-XPath"/></xsl:message>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>


  <xsl:variable name="lookup-path">
    <xsl:choose>
      <xsl:when test="@path"><xsl:value-of select="@path"/></xsl:when>

      <xsl:when test="@child = 'path'">
        <xsl:apply-templates mode="eval-attr">
          <xsl:with-param name="path-stack" select="$path-stack"/>
          <xsl:with-param name="depth" select="$depth"/>
          <xsl:with-param name="list-item" select="$list-item"/>
        </xsl:apply-templates>
      </xsl:when>

      <xsl:otherwise>
        <xsl:message>ERROR: path missing in <xsl:value-of select="build-XPath"/></xsl:message>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <!-- Build our result -->
  <xsl:variable name="result">
    <xsl:choose>

      <!-- Check we actually have a decent path defn. -->
      <xsl:when test="not(normalize-space($lookup-path))">
        <xsl:message>ERROR: empty path found for lookup element, falling back to the default value.</xsl:message>
        <xsl:value-of select="$default-value"/>
      </xsl:when>

      <xsl:otherwise>
        <xsl:call-template name="eval-path">
	  <xsl:with-param name="path-stack" select="$path-stack"/>
	  <xsl:with-param name="depth" select="$depth"/>
          <xsl:with-param name="lookup-path" select="$lookup-path"/>
          <xsl:with-param name="default-value" select="$default-value"/>
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <!-- Optionally, normalise it -->
  <xsl:choose>
    <xsl:when test="count(@normalize) > 0 and not(@normalize = '0')">
      <xsl:value-of select="normalize-space($result)"/>
    </xsl:when>

    <xsl:otherwise>
      <xsl:value-of select="$result"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>



<!--+
    |
    |  The <scale/> element
    |
    |  Scale a numerical value.
    |
    +-->
<xsl:template match="scale" mode="eval-attr">
  <xsl:param name="depth"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <xsl:variable name="value">
    <xsl:apply-templates mode="eval-attr">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="depth" select="$depth"/>
      <xsl:with-param name="list-item" select="$list-item"/>
    </xsl:apply-templates>
  </xsl:variable>


  <xsl:choose>
    <xsl:when test="/xylophone/scale/factor[@name=current()/@factor]">

      <xsl:variable name="factor">
        <xsl:value-of select="/xylophone/scale/factor[@name=current()/@factor]"/>
      </xsl:variable>

      <xsl:variable name="mode">
        <xsl:value-of select="/xylophone/scale/factor[@name=current()/@factor]/@mode"/>
      </xsl:variable>

      <xsl:variable name="result">
        <xsl:choose>
          <xsl:when test="$mode = 'divide'">
            <xsl:value-of select="number($value) div number($factor)"/>
          </xsl:when>

          <xsl:when test="$mode = 'multiply'">
            <xsl:value-of select="number($value) * number($factor)"/>
          </xsl:when>

          <xsl:otherwise>
            <xsl:message>Unknown mode: &quot;<xsl:value-of select="$mode"/>&quot;</xsl:message>
            <xsl:value-of select="$value"/>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:variable>

      <xsl:choose>
        <xsl:when test="@to-integer='round'">
          <xsl:value-of select="round($result)"/>
        </xsl:when>
        <xsl:when test="@to-integer='floor'">
          <xsl:value-of select="floor($result)"/>
        </xsl:when>
        <xsl:when test="@to-integer='ceiling'">
          <xsl:value-of select="ceiling($result)"/>
        </xsl:when>
        <xsl:when test="count(@to-integer)=0">
          <xsl:value-of select="$result"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:message>Unknown "to-integer" attribute value &quot;<xsl:value-of select="@toInteger"/>&quot; in scale element</xsl:message>
          <xsl:value-of select="$result"/>
        </xsl:otherwise>
      </xsl:choose>

    </xsl:when>

    <xsl:when test="not(@factor) or @factor = ''">
      <xsl:message>Missing required "factor" attribute in scale element.</xsl:message>
    </xsl:when>

    <xsl:otherwise>
      <xsl:message>Unknown "factor" attribute value &quot;<xsl:value-of select="@factor"/>&quot; in scale element.</xsl:message>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>


<!-- When in mode eval-attr we ignore any suppress elements -->
<xsl:template match="suppress" mode="eval-attr"/>

<!-- When in mode eval-attr we ignore any unique elements -->
<xsl:template match="unique" mode="eval-attr"/>


<!--+
    |
    |  The <parent-rdn/> element
    |
    |  Look up the RDN of a parent element
    |
    |  The @use attribute is required and must be one of
    |  'rdn' or 'value'
    |
    |  NB.  This will only work if within an <object/> element.
    |       Specifically, it will not work within a class.
    +-->
<xsl:template match="parent-rdn" mode="eval-attr">
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <xsl:variable name="prefix">
    <xsl:choose>
      <xsl:when test="@use = 'rdn'">
	<xsl:value-of select="concat(@rdn,'=')"/>
      </xsl:when>
      <xsl:when test="@use = 'value'"/>
      <xsl:otherwise>
	<xsl:message>ERROR: @use ('<xsl:value-of select="@use"/>') is not 'rdn' or 'value' in <xsl:value-of select="build-XPath"/></xsl:message>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <!-- Select the nearest ancestor object that has a matching rdn -->
  <xsl:apply-templates select="ancestor::object[@rdn=current()/@rdn][1]"
                       mode="emit-RDN-for-attribute">
    <xsl:with-param name="path-stack" select="$path-stack"/>
    <xsl:with-param name="list-item" select="$list-item"/>
    <xsl:with-param name="prefix" select="$prefix"/>
  </xsl:apply-templates>
</xsl:template>


<!--+
    |  The <other-rdn/> element
    |
    |  Build an arbitrary RDN.  Really, not much beyond a simple
    |  wrapper around the equals sign.
    +-->
<xsl:template match="other-rdn" mode="eval-attr">
  <xsl:param name="depth" select="count(ancestor-or-self::object)"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <xsl:value-of select="concat(@rdn,'=')"/>

  <xsl:apply-templates mode="eval-attr">
    <xsl:with-param name="path-stack" select="$path-stack"/>
    <xsl:with-param name="depth" select="$depth"/>
    <xsl:with-param name="list-item" select="$list-item"/>
  </xsl:apply-templates>
</xsl:template>


<!--+
    |
    |  The <sum/> element
    |
    |  Add up the value of all the <term/> items.  NB. this uses the
    |  node-set() fn to convert a result-tree-frag into a node-set.
    |
    +-->
<xsl:template match="sum" mode="eval-attr">
  <xsl:param name="depth"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <xsl:choose>
    <xsl:when test="@path">
      <xsl:call-template name="sum-path">
	<xsl:with-param name="path-stack" select="$path-stack"/>
	<xsl:with-param name="depth" select="$depth"/>
      </xsl:call-template>
    </xsl:when>

    <xsl:otherwise>
      <!-- Expand terms as a result tree fragment with term elements -->
      <xsl:variable name="sumTerms">
        <xsl:apply-templates select="term" mode="eval-sum-terms">
          <xsl:with-param name="path-stack" select="$path-stack"/>
          <xsl:with-param name="depth" select="$depth"/>
          <xsl:with-param name="list-item" select="$list-item"/>
        </xsl:apply-templates>
      </xsl:variable>

      <xsl:value-of select="sum(exsl:node-set($sumTerms)/term)"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>



<!--+
    |  Build a simple <term>number</term> element, used within a <sum/>
    |  structure.
    +-->
<xsl:template match="term" mode="eval-sum-terms">
  <xsl:param name="depth"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <xsl:variable name="result">
    <xsl:apply-templates mode="eval-attr">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="depth" select="$depth"/>
      <xsl:with-param name="list-item" select="$list-item"/>
    </xsl:apply-templates>
  </xsl:variable>

  <term>
    <xsl:value-of select="number($result)"/>
  </term>
</xsl:template>


<!--+
    |  The <constant/> element
    |
    |  A simple lookup for elements within the document.  This allows
    |  something like configuration variables to be defined and
    |  within the output referenced.
    +-->
<xsl:template match="constant" mode="eval-attr">

  <xsl:choose>
    <xsl:when test="@id">

      <!-- Now we look up this constant value -->
      <xsl:choose>
        <xsl:when test="/xylophone/constants/constant[@id=current()/@id][1]">
          <xsl:value-of select="/xylophone/constants/constant[@id=current()/@id][1]"/>
        </xsl:when>

        <xsl:otherwise>
          <xsl:message>Unknown id &quot;<xsl:value-of select="@id"/>&quot; in constant element.</xsl:message>
        </xsl:otherwise>
      </xsl:choose>

    </xsl:when>

    <xsl:otherwise>
      <xsl:message>Missing "id" attribute in constant element.</xsl:message>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>



<!--+
    |   The <map/> element
    |
    |   Apply one or more string-altering operations.
    +-->
<xsl:template match="map" mode="eval-attr">
  <xsl:param name="depth"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>


  <!-- Evaluate text before any mappings -->
  <xsl:variable name="before">
    <xsl:apply-templates mode="eval-attr">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="depth" select="$depth"/>
      <xsl:with-param name="list-item" select="$list-item"/>
    </xsl:apply-templates>
  </xsl:variable>


  <!-- Apply mappings -->
  <xsl:choose>
    <xsl:when test="/xylophone/mapping/map[@name=current()/@name]">
      <xsl:apply-templates select="/xylophone/mapping/map[@name=current()/@name][1]/*[1]" mode="eval-mapping">
        <xsl:with-param name="before" select="$before"/>
      </xsl:apply-templates>
    </xsl:when>

    <xsl:otherwise>
      <xsl:message>Unknown mapping <xsl:value-of select="@name"/>.</xsl:message>
      <xsl:value-of select="$before"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>



<!--+
    |  The <item/> element
    |
    |  This expands to the current item within an item iteration or an
    |  empty string otherwise.
    +-->
<xsl:template match="item" mode="eval-attr">
  <xsl:param name="list-item"/>

  <xsl:value-of select="$list-item"/>
</xsl:template>



<!--+
    |  The <xpath/> element
    |
    |  This expands to the XPath of the current element.
    +-->
<xsl:template match="xpath" mode="eval-attr">
  <xsl:param name="depth"/>
  <xsl:param name="path-stack"/>

  <xsl:choose>
    <xsl:when test="not(@of) or @of = 'dynamic'">
      <xsl:call-template name="path-stack-find-path">
	<xsl:with-param name="path-stack" select="$path-stack"/>
	<xsl:with-param name="depth" select="$depth"/>
      </xsl:call-template>
    </xsl:when>

    <xsl:when test="@of = 'xylophone'">
      <xsl:call-template name="build-XPath"/>
    </xsl:when>

    <xsl:otherwise>
      <xsl:message>Unknown value of 'of' attribute, should be either 'xylophone' or 'dynamic'</xsl:message>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>


<!--+
    |  The <date/> element
    |
    |  This expands to a formatted timestamp.
    +-->
<xsl:template match="date" mode="eval-attr">
  <xsl:param name="depth"/>
  <xsl:param name="path-stack"/>

  <xsl:variable name="format" select="@format"/>

  <xsl:choose>
    <xsl:when test="not(@format)">
      <xsl:message>Unable to generate timestamp: format attribute missing</xsl:message>
    </xsl:when>

    <xsl:when test="@format=''">
      <xsl:message>Unable to generate timestamp: format attribute empty</xsl:message>
    </xsl:when>

    <xsl:when test="@tz and @tz != 'Z' and @tz != 'local'">
      <xsl:message>Unable to generate timestamp: tz attribute (<xsl:value-of select='@tz'/>) not 'Z' or 'local'</xsl:message>
    </xsl:when>

    <xsl:when test="@tz = 'local'">
      <xsl:value-of select="date:format-date(date:date-time(), @format)" />
    </xsl:when>

    <xsl:otherwise>
      <xsl:variable name="tz" select="substring(date:time(),9)"/>
      <xsl:variable name="sign">
	<xsl:choose>
	  <xsl:when test="substring($tz,1,1)='-'">+1</xsl:when>
	  <xsl:otherwise>-1</xsl:otherwise>
	</xsl:choose>
      </xsl:variable>
      <xsl:value-of select="date:format-date(concat(substring(date:add(date:date-time(), date:duration((floor(substring($tz,2,2))*60+floor(substring($tz,5,2)))*60*number($sign))),1,19),'Z'), @format)" />
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

</xsl:stylesheet>
