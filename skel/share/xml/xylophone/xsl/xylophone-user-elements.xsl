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
		extension-element-prefixes="exsl">

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

  <!-- Extract our rel-path -->
  <xsl:variable name="rel-path">
    <xsl:call-template name="path-stack-find-path">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="depth" select="$depth"/>
    </xsl:call-template>
  </xsl:variable>


  <!-- Figure out what our default should be -->
  <xsl:variable name="default-result">
    <xsl:choose>
      <xsl:when test="count(@default) &gt; 0">
	<xsl:value-of select="@default"/>
      </xsl:when>

      <xsl:when test="not(@child) or @child='default'">
	<xsl:apply-templates mode="eval-attr">
	  <xsl:with-param name="rel-path" select="$rel-path"/>
	  <xsl:with-param name="list-item" select="$list-item"/>
	</xsl:apply-templates>
      </xsl:when>

      <xsl:when test="@child='path'">
	<!-- Our default is empty... -->
      </xsl:when>

      <xsl:otherwise>
	<xsl:message>Unknown "child" attribute value &quot;<xsl:value-of select="@child"/>&quot; in lookup element.</xsl:message>
      </xsl:otherwise>
      <!-- Otherwise, we have no default -->
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="raw-path">
    <xsl:choose>
      <xsl:when test="@path">
	<!-- Calculate the absolute path -->
	<xsl:call-template name="combine-paths">
	  <xsl:with-param name="rel-path" select="$rel-path"/>
	  <xsl:with-param name="path" select="@path"/>
	</xsl:call-template>
      </xsl:when>

      <xsl:when test="@child = 'path'">

	<!-- Calculate the absolute path -->
	<xsl:call-template name="combine-paths">
	  <xsl:with-param name="rel-path" select="$rel-path"/>
	  
	  <!-- Calculate the path from child elements -->
          <xsl:with-param name="path">
            <xsl:apply-templates mode="eval-attr">
              <xsl:with-param name="path-stack" select="$path-stack"/>
	      <xsl:with-param name="depth" select="$depth"/>
              <xsl:with-param name="list-item" select="$list-item"/>
	    </xsl:apply-templates>
          </xsl:with-param>
        </xsl:call-template>
      </xsl:when>

      <xsl:otherwise>
	<xsl:message>Missing path in lookup element</xsl:message>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="path" select="normalize-space($raw-path)"/>

  <!-- Build our result -->
  <xsl:variable name="result">
    <xsl:choose>

      <!-- Check we actually have a decent path defn. -->
      <xsl:when test="not(normalize-space($path))">
	<xsl:message>Empty path found for lookup element, using default.</xsl:message>
	<xsl:value-of select="$default-result"/>
      </xsl:when>

      <!-- Paths that begin with a '/' are always absolute -->
      <xsl:when test="starts-with($path, '/')">
	<xsl:call-template name="eval-path">
	  <xsl:with-param name="path" select="substring($path,2)"/>
	  <xsl:with-param name="default" select="$default-result"/>
	</xsl:call-template>	
      </xsl:when>

      <!-- Other paths are assumed to be relative to current context -->
      <xsl:otherwise>

	<xsl:variable name="constructed-path">
	  <xsl:choose>
	    <xsl:when test="$rel-path">
	      <xsl:value-of select="concat($rel-path,'/',$path)"/>
	    </xsl:when>

	    <xsl:otherwise>
	      <xsl:value-of select="$path"/>
	    </xsl:otherwise>
	  </xsl:choose>
	</xsl:variable>

	<xsl:call-template name="eval-path">
	  <xsl:with-param name="path" select="$constructed-path"/>
	  <xsl:with-param name="default" select="$default-result"/>
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



<!--+
    |
    |  The <parent-rdn/> element
    |
    |  Look up the RDN of a parent element
    |
    |  NB.  This will only work if within an <object/> element.
    |       Specifically, it will not work within a class.
    +-->
<xsl:template match="parent-rdn" mode="eval-attr">
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <!-- Select the nearest ancestor object that has a matching rdn -->
  <xsl:apply-templates select="ancestor::object[@rdn=current()/@rdn][1]"
		       mode="emit-RDN-for-attribute">
    <xsl:with-param name="path-stack" select="$path-stack"/>
    <xsl:with-param name="list-item" select="$list-item"/>
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

  <!-- Expand terms as a result tree fragment with term elements -->
  <xsl:variable name="sumTerms">
    <xsl:apply-templates select="term" mode="eval-sum-terms">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="depth" select="$depth"/>
      <xsl:with-param name="list-item" select="$list-item"/>
    </xsl:apply-templates>
  </xsl:variable>

  <xsl:value-of select="sum(exsl:node-set($sumTerms)/term)"/>
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
    |   The <count/> element
    |
    |  Mostly available for testing, this counts the number of
    |  elements that match a certain path.
    |
    +-->
<xsl:template match="count" mode="eval-attr">
  <xsl:param name="depth"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

    <!-- Extract our rel-path -->
  <xsl:variable name="rel-path">
    <xsl:call-template name="path-stack-find-path">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="depth" select="$depth"/>
    </xsl:call-template>
  </xsl:variable>


  <!-- First try with rel-path prefix -->
  <xsl:variable name="rel-value">
    <xsl:if test="normalize-space($rel-path)">
      <xsl:call-template name="count-path">
	<xsl:with-param name="path" select="concat($rel-path,'/',@path)"/>
      </xsl:call-template>
    </xsl:if>
  </xsl:variable>

  <!-- Build our result based on rel-value or absolute path -->
  <xsl:variable name="value">
    <xsl:choose>
      <xsl:when test="$rel-value > 0">
	<xsl:value-of select="$rel-value"/>
      </xsl:when>

      <xsl:otherwise>
	<xsl:call-template name="count-path">
	  <xsl:with-param name="path" select="@path"/>
	</xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>


  <xsl:choose>
    <xsl:when test="normalize-space( $value)">
      <xsl:value-of select="$value"/>
    </xsl:when>

    <xsl:otherwise>
      <xsl:message>Count path <xsl:value-of select="@path"/> is invalid.</xsl:message>
      <xsl:text>0</xsl:text>
    </xsl:otherwise>
  </xsl:choose>
  
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


</xsl:stylesheet>
