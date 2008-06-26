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
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform">


<!--+
    |  Lookup a value with given path from $xml-src-uri
    +-->
<xsl:template name="eval-path">
  <xsl:param name="path"/>
  <xsl:param name="default"/>

  <xsl:variable name="subpath">
    <xsl:call-template name="eval-subelement">
      <xsl:with-param name="path" select="$path"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:variable name="childname">
    <xsl:call-template name="eval-childname">
      <xsl:with-param name="next-subpath" select="$subpath"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:apply-templates select="document($xml-src-uri)/*[name()=$childname]" mode="eval-path">
    <xsl:with-param name="path" select="substring-after($path, '/')"/>
    <xsl:with-param name="default" select="$default"/>
  </xsl:apply-templates>
</xsl:template>



<xsl:template match="*" mode="eval-path">
  <xsl:param name="path"/>
  <xsl:param name="attr-name"/>
  <xsl:param name="attr-value"/>
  <xsl:param name="desired-posn"/>
  <xsl:param name="matched"/>
  <xsl:param name="name"/>
  <xsl:param name="default"/>

  <xsl:variable name="node-ok">
    <xsl:call-template name="eval-node-ok">
      <xsl:with-param name="attr-name" select="$attr-name"/>
      <xsl:with-param name="attr-value" select="$attr-value"/>
      <xsl:with-param name="name" select="$name"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:variable name="next-matched">
    <xsl:choose>
      <xsl:when test="$node-ok = 'true'">
	<xsl:value-of select="number($matched+1)"/>
      </xsl:when>
      <xsl:otherwise>
	<xsl:value-of select="$matched"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>


  <xsl:choose>
    <xsl:when test="$next-matched &lt; $desired-posn">
      <!-- Try next sibling element -->
      <xsl:apply-templates select="following-sibling::*[1]" mode="eval-path">
	<xsl:with-param name="path"      select="$path"/>
	<xsl:with-param name="attr-name" select="$attr-name"/>
	<xsl:with-param name="attr-value" select="$attr-value"/>
	<xsl:with-param name="matched"   select="$next-matched"/>
	<xsl:with-param name="desired-posn" select="$desired-posn"/>
	<xsl:with-param name="name"      select="$name"/>
	<xsl:with-param name="default"   select="$default"/>
      </xsl:apply-templates>
    </xsl:when>

    <xsl:when test="$node-ok = 'true'">
      <!-- We've found our match: iterate down -->

      <xsl:variable name="subpath">
	<xsl:call-template name="eval-subelement">
	  <xsl:with-param name="path" select="$path"/>
	</xsl:call-template>
      </xsl:variable>

      <xsl:variable name="childname">
	<xsl:call-template name="eval-childname">
	  <xsl:with-param name="next-subpath" select="$subpath"/>
	</xsl:call-template>
      </xsl:variable>

      <xsl:variable name="new-attr-name">
	<xsl:call-template name="eval-new-attr-name">
	  <xsl:with-param name="next-subpath" select="$subpath"/>
	</xsl:call-template>
      </xsl:variable>

      <xsl:variable name="new-attr-value">
	<xsl:call-template name="eval-new-attr-value">
	  <xsl:with-param name="next-subpath" select="$subpath"/>
	</xsl:call-template>
      </xsl:variable>

      <xsl:variable name="count-value">
	<xsl:call-template name="eval-count">
	  <xsl:with-param name="next-subpath" select="$subpath"/>
	</xsl:call-template>
      </xsl:variable>

      <xsl:variable name="child-count">
	<xsl:choose>
	  <xsl:when test="number($count-value)">
	    <xsl:value-of select="number($count-value)"/>
	  </xsl:when>
	  <xsl:otherwise>
	    <xsl:value-of select="number('1')"/>
	  </xsl:otherwise>
	</xsl:choose>
      </xsl:variable>
    
      <xsl:choose>      
	<!-- Multiple elements left to parse -->
	<xsl:when test="contains($path, '/')">
	  <xsl:apply-templates select="*[1]" mode="eval-path">
	    <xsl:with-param name="path"         select="substring-after($path, '/')"/>
	    <xsl:with-param name="attr-name"    select="$new-attr-name"/>
	    <xsl:with-param name="attr-value"   select="$new-attr-value"/>
	    <xsl:with-param name="matched"      select="number('0')"/>
	    <xsl:with-param name="desired-posn" select="$child-count"/>
	    <xsl:with-param name="name"         select="$childname"/>
	    <xsl:with-param name="default"      select="$default"/>
	  </xsl:apply-templates>
	</xsl:when>
      
	<!-- Only one left, so this element defines the content -->

	<!-- Special case: look up an attribute value of final node -->
	<xsl:when test="starts-with( $path, '@')">
	  <xsl:variable name="req-attr" select="substring-after( $path, '@')"/>

	  <xsl:choose>
	    <xsl:when test="attribute::*[name()=$req-attr]">
	      <xsl:value-of select="attribute::*[name()=$req-attr]"/>
	    </xsl:when>

	    <xsl:otherwise>
	      <xsl:value-of select="$default"/>
	    </xsl:otherwise>
	  </xsl:choose>
	</xsl:when>


	<!-- Otherwise, lookup child element(s) that match -->
	<xsl:otherwise>
	  <xsl:apply-templates select="*[1]" mode="import-echo">
	    <xsl:with-param name="attr-name"    select="$new-attr-name"/>
	    <xsl:with-param name="attr-value"   select="$new-attr-value"/>
	    <xsl:with-param name="matched"      select="number('0')"/>
	    <xsl:with-param name="desired-posn" select="$child-count"/>
	    <xsl:with-param name="name"         select="$childname"/>
	    <xsl:with-param name="default"      select="$default"/>
	  </xsl:apply-templates>
	</xsl:otherwise>
      </xsl:choose>

    </xsl:when>

    <xsl:otherwise>
      <!-- do nothing: shouldn't get here -->
    </xsl:otherwise>
  </xsl:choose>

</xsl:template>


<!--+
    |  Return the next sub-element
    +-->
<xsl:template name="eval-subelement">
  <xsl:param name="path"/>

  <xsl:choose>
    <xsl:when test="contains( $path, '/')">
      <xsl:value-of select="substring-before( $path, '/')"/>
    </xsl:when>

    <xsl:otherwise>
      <xsl:value-of select="$path"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>


<!--+
    |  Path 
    |  Given a next-subpath like tag-a or tag-a[aaa=bbb], return the
    |  element part "tag-a"
    +-->
<xsl:template name="eval-childname">
  <xsl:param name="next-subpath"/>

  <xsl:choose>
    <xsl:when test="contains( $next-subpath, '[')">
      <xsl:value-of select="substring-before($next-subpath, '[')"/>
    </xsl:when>

    <xsl:otherwise>
      <xsl:value-of select="$next-subpath"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>


<!--+
    |  Given a next-subpath like tag-a[aaa=bbb], return the attribute name
    |  "aaa".  If there is no '[', return nothing.
    +-->
<xsl:template name="eval-new-attr-name">
  <xsl:param name="next-subpath"/>

  <xsl:if test="contains( $next-subpath, '[')">
    <xsl:variable name="pred" select="substring-before( substring-after($next-subpath, '['), ']')"/>
    <xsl:value-of select="substring-before( $pred, '=')"/>
  </xsl:if>
</xsl:template>


<!--+
    |  Given a next-subpath like tag-a[aaa=bbb], return the attribute name
    |  "bbb".  If there is no '[', return nothing.
    +-->
<xsl:template name="eval-new-attr-value">
  <xsl:param name="next-subpath"/>

  <xsl:if test="contains( $next-subpath, '[')">
    <xsl:variable name="pred" select="substring-before( substring-after($next-subpath, '['), ']')"/>
    <xsl:value-of select="substring-after( $pred, '=')"/>
  </xsl:if>
</xsl:template>


<!--+
    |  Given a next-subpath like "tag-a[n]" or "tag-a[aaa=bbb][n]",
    |  expand as just the number n.  If no such square-bracket exists,
    |  or the number is invalid, a default value ('1') is returned.
    +-->
<xsl:template name="eval-count">
  <xsl:param name="next-subpath"/>

  <xsl:variable name="bracket-value">
    <xsl:if test="contains( $next-subpath, '[')">
      <!-- Select the square-bracket contents -->
      <xsl:variable name="str-src">
	<xsl:choose>
	  <!-- Two (or more) square brackets => select second -->
	  <xsl:when test="contains( substring-after( $next-subpath, '['), '[')">
	    <xsl:value-of select="substring-before( substring-after( substring-after( $next-subpath, '['), '['), ']')"/>
	  </xsl:when>
	  
	  <!-- Single bracket or none. -->
	  <xsl:otherwise>
	    <!-- Only emit if doesn't contain an equals -->
	    <xsl:if test="not( contains( substring-before( substring-after( $next-subpath, '['), ']'), '='))">
	      <xsl:value-of select="substring-before( substring-after( $next-subpath, '['), ']')"/>
	    </xsl:if>
	  </xsl:otherwise>
	</xsl:choose>
      </xsl:variable>

      <xsl:choose>
	<xsl:when test="number($str-src) or number($str-src) = 0">
	  <xsl:value-of select="number($str-src)"/>
	</xsl:when>

	<xsl:otherwise>
	  <xsl:if test="normalize-space($str-src)">
	    <xsl:message>Invalid number for &quot;<xsl:value-of select="$str-src"/>&quot;</xsl:message>
	  </xsl:if>
	  
	  <!-- Emit nothing -->
	</xsl:otherwise>
      </xsl:choose>
    
    </xsl:if>
  </xsl:variable>


  <xsl:choose>
    <xsl:when test="number($bracket-value)">
      <xsl:value-of select="number($bracket-value)"/>
    </xsl:when>

    <xsl:otherwise>
      <!-- emit nothing -->
    </xsl:otherwise>
  </xsl:choose>

</xsl:template>



<xsl:template name="eval-node-ok">
  <xsl:param name="attr-name"/>
  <xsl:param name="attr-value"/>
  <xsl:param name="name"/>

  <xsl:variable name="name-ok">
    <xsl:choose>
      <xsl:when test="string-length($name)>0">
	<xsl:value-of select="name() = $name"/>
      </xsl:when>

      <xsl:otherwise>
	<xsl:value-of select="true()"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="attr-ok">
    <xsl:choose>
      <xsl:when test="string-length($attr-name)>0">
	<xsl:value-of select="attribute::*[name()=$attr-name] = $attr-value"/>
      </xsl:when>
      <xsl:otherwise>
	<xsl:value-of select="true()"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:value-of select="$attr-ok = 'true' and $name-ok = 'true'"/>
</xsl:template>


<xsl:template match="*" mode="import-echo">
  <xsl:param name="attr-name"/>
  <xsl:param name="attr-value"/>
  <xsl:param name="desired-posn"/>
  <xsl:param name="matched"/>
  <xsl:param name="name"/>
  <xsl:param name="default"/>

  <xsl:variable name="node-ok">
    <xsl:call-template name="eval-node-ok">
      <xsl:with-param name="attr-name" select="$attr-name"/>
      <xsl:with-param name="attr-value" select="$attr-value"/>
      <xsl:with-param name="name" select="$name"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:variable name="next-matched">
    <xsl:choose>
      <xsl:when test="$node-ok = 'true'">
	<xsl:value-of select="number($matched+1)"/>
      </xsl:when>
      <xsl:otherwise>
	<xsl:value-of select="$matched"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:choose>
    <xsl:when test="$next-matched &lt; $desired-posn">

      <xsl:choose>
	<!-- If there are more possibilities... -->
	<xsl:when test="following-sibling::*">

	  <!-- Try the next one -->
	  <xsl:apply-templates select="following-sibling::*[1]" mode="import-echo">
	    <xsl:with-param name="attr-name"  select="$attr-name"/>
	    <xsl:with-param name="attr-value" select="$attr-value"/>
	    <xsl:with-param name="desired-posn" select="$desired-posn"/>
	    <xsl:with-param name="matched"    select="$next-matched"/>
	    <xsl:with-param name="name"       select="$name"/>
	    <xsl:with-param name="default"    select="$default"/>
	  </xsl:apply-templates>
	</xsl:when>

	<!-- Otherwise, we've exhausted our search, emit the default -->
	<xsl:otherwise>
	  <xsl:value-of select="$default"/>
	</xsl:otherwise>
      </xsl:choose>

    </xsl:when>

    <xsl:when test="$node-ok = 'true'">
      <xsl:value-of select="."/>
    </xsl:when>

    <xsl:otherwise>
      <!-- Shouldn't get here -->
    </xsl:otherwise>
  </xsl:choose>

</xsl:template>



<!--+
    |
    |  mode = "count-path"
    |
    |  Support for counting number of elements that match a certain
    |  path
    |
    +-->
<xsl:template name="count-path">
  <xsl:param name="path"/>
  <xsl:param name="rel-path"/>

  <!-- Calculate the absolute path -->
  <xsl:variable name="abs-path">
    <xsl:call-template name="combine-paths">
      <xsl:with-param name="path" select="$path"/>
      <xsl:with-param name="rel-path" select="$rel-path"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:variable name="path-to-use" select="substring-after( $abs-path, '/')"/>

  <xsl:variable name="subpath">
    <xsl:call-template name="eval-subelement">
      <xsl:with-param name="path" select="$path-to-use"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:variable name="childname">
    <xsl:call-template name="eval-childname">
      <xsl:with-param name="next-subpath" select="$subpath"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:apply-templates select="document($xml-src-uri)/*[name()=$childname]" mode="count-path">
    <xsl:with-param name="path" select="substring-after($path-to-use, '/')"/>
  </xsl:apply-templates>
</xsl:template>


<xsl:template match="*" mode="count-path">
  <xsl:param name="path"/>
  <xsl:param name="attr-name"/>
  <xsl:param name="attr-value"/>
  <xsl:param name="desired-posn"/>
  <xsl:param name="matched"/>
  <xsl:param name="name"/>

  <xsl:variable name="node-ok">
    <xsl:call-template name="eval-node-ok">
      <xsl:with-param name="attr-name" select="$attr-name"/>
      <xsl:with-param name="attr-value" select="$attr-value"/>
      <xsl:with-param name="name" select="$name"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:variable name="next-matched">
    <xsl:choose>
      <xsl:when test="$node-ok = 'true'">
	<xsl:value-of select="number($matched+1)"/>
      </xsl:when>
      <xsl:otherwise>
	<xsl:value-of select="$matched"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:choose>
    <xsl:when test="$next-matched &lt; $desired-posn">
      <!-- Try next sibling element -->
      <xsl:apply-templates select="following-sibling::*[1]" mode="count-path">
	<xsl:with-param name="path" select="$path"/>
	<xsl:with-param name="attr-name" select="$attr-name"/>
	<xsl:with-param name="attr-value" select="$attr-value"/>
	<xsl:with-param name="matched" select="$next-matched"/>
	<xsl:with-param name="desired-posn" select="$desired-posn"/>
	<xsl:with-param name="name" select="$name"/>
      </xsl:apply-templates>
    </xsl:when>


    <!-- We've found our match: iterate down -->
    <xsl:when test="$node-ok = 'true'">

      <xsl:variable name="subpath">
	<xsl:call-template name="eval-subelement">
	  <xsl:with-param name="path" select="$path"/>
	</xsl:call-template>
      </xsl:variable>

      <xsl:variable name="childname">
	<xsl:call-template name="eval-childname">
	  <xsl:with-param name="next-subpath" select="$subpath"/>
	</xsl:call-template>
      </xsl:variable>

      <xsl:variable name="new-attr-name">
	<xsl:call-template name="eval-new-attr-name">
	  <xsl:with-param name="next-subpath" select="$subpath"/>
	</xsl:call-template>
      </xsl:variable>

      <xsl:variable name="new-attr-value">
	<xsl:call-template name="eval-new-attr-value">
	  <xsl:with-param name="next-subpath" select="$subpath"/>
	</xsl:call-template>
      </xsl:variable>

      <xsl:variable name="count-value">
	<xsl:call-template name="eval-count">
	  <xsl:with-param name="next-subpath" select="$subpath"/>
	</xsl:call-template>
      </xsl:variable>

      <xsl:variable name="child-count">
	<xsl:choose>
	  <xsl:when test="number($count-value)">
	    <xsl:value-of select="number($count-value)"/>
	  </xsl:when>
	  <xsl:otherwise>
	    <xsl:value-of select="number('1')"/>
	  </xsl:otherwise>
	</xsl:choose>
      </xsl:variable>
    
      <xsl:choose>      
	<!-- Multiple elements left to parse -->
	<xsl:when test="contains($path, '/')">
	  <xsl:apply-templates select="*[1]" mode="count-path">
	    <xsl:with-param name="path" select="substring-after($path, '/')"/>
	    <xsl:with-param name="attr-name" select="$new-attr-name"/>
	    <xsl:with-param name="attr-value" select="$new-attr-value"/>
	    <xsl:with-param name="matched"      select="number('0')"/>
	    <xsl:with-param name="desired-posn" select="$child-count"/>
	    <xsl:with-param name="name"         select="$childname"/>
	  </xsl:apply-templates>
	</xsl:when>
      
	<!-- Only one left, so count matching nodes -->
	<xsl:otherwise>
	  <xsl:apply-templates select="*[1]" mode="count-elements">
	    <xsl:with-param name="attr-name"    select="$new-attr-name"/>
	    <xsl:with-param name="attr-value"   select="$new-attr-value"/>
	    <xsl:with-param name="matched"      select="number('0')"/>
	    <xsl:with-param name="name"         select="$childname"/>
	  </xsl:apply-templates>
	</xsl:otherwise>
      </xsl:choose>
    </xsl:when>

    <xsl:otherwise>
      <!-- Should never get here -->
    </xsl:otherwise>
  </xsl:choose>

</xsl:template>



<xsl:template match="*" mode="count-elements">
  <xsl:param name="attr-name"/>
  <xsl:param name="attr-value"/>
  <xsl:param name="matched"/>
  <xsl:param name="name"/>

  <xsl:variable name="node-ok">
    <xsl:call-template name="eval-node-ok">
      <xsl:with-param name="attr-name" select="$attr-name"/>
      <xsl:with-param name="attr-value" select="$attr-value"/>
      <xsl:with-param name="name" select="$name"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:variable name="next-matched">
    <xsl:choose>
      <xsl:when test="$node-ok = 'true'">
	<xsl:value-of select="number($matched+1)"/>
      </xsl:when>
      <xsl:otherwise>
	<xsl:value-of select="$matched"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:choose>
    <xsl:when test="following-sibling::*[1]">
      <xsl:apply-templates select="following-sibling::*[1]" mode="count-elements">
	<xsl:with-param name="attr-name" select="$attr-name"/>
	<xsl:with-param name="attr-value" select="$attr-value"/>
	<xsl:with-param name="matched" select="$next-matched"/>
	<xsl:with-param name="name" select="$name"/>
      </xsl:apply-templates>
    </xsl:when>

    <xsl:otherwise>
      <xsl:value-of select="$next-matched"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>



</xsl:stylesheet>
