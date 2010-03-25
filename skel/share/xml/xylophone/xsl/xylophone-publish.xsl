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
    |  This file contains support for processing the "publish" branch:
    |  the objects.
    |
    +-->

<xsl:stylesheet version="1.0"
		xmlns:exsl="http://exslt.org/common"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
		extension-element-prefixes="exsl">


<!-- Skip any text nodes when publishing -->
<xsl:template match="text()" mode="publish"/>


<!--+
    |   Process an <object/> element.  This may involve emitting
    |   precisely one LDIF object (if no select attribute is
    |   specified), or zero-or-more LDIF objects (if a select
    |   attribute is specified).
    +-->
<xsl:template match="object" mode="publish">
  <xsl:param name="list-item"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="parent-dn"/>

  <xsl:choose>

    <!-- Publish objects based on external XML data -->
    <xsl:when test="@select">
      <xsl:if test="@list">
        <xsl:message>Both "list" and "select" attributes are specified in object element; I will ignore list.</xsl:message>
      </xsl:if>

      <xsl:call-template name="publish-Nth-of-multiple-select-objects-and-itr">
	<xsl:with-param name="parent-dn" select="$parent-dn"/>
	<xsl:with-param name="list-item" select="$list-item"/>
	<xsl:with-param name="path-stack" select="$path-stack"/>
	<xsl:with-param name="xpaths">
	  <xsl:call-template name="build-item-list">
	    <xsl:with-param name="path-stack" select="$path-stack"/>
	  </xsl:call-template>
	</xsl:with-param>
      </xsl:call-template>
    </xsl:when>

    <!-- Publish objects based on a list of items -->
    <xsl:when test="@list">
      <xsl:choose>
        <xsl:when test="count(/xylophone/lists/list[@name=current()/@list]) = 0">
          <xsl:message>There is no list named &quot;<xsl:value-of select="@list"/>&quot;.</xsl:message>
        </xsl:when>

        <xsl:otherwise>
          <xsl:call-template name="publish-multiple-list-objects">
            <xsl:with-param name="path-stack" select="$path-stack"/>
            <xsl:with-param name="parent-dn" select="$parent-dn"/>
          </xsl:call-template>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:when>

    <!-- Publish just one object -->
    <xsl:otherwise>
      <xsl:call-template name="maybe-publish-object-and-children">
        <xsl:with-param name="path-stack" select="$path-stack"/>
        <xsl:with-param name="parent-dn" select="$parent-dn"/>
      </xsl:call-template>
    </xsl:otherwise>
  </xsl:choose>

</xsl:template>


<!--+
    |  Expands, via iteratation over all potential objects in a list.
    |  Each object may still be suppressed, but this template gives it
    |  a chance.
    +-->
<xsl:template name="publish-multiple-list-objects">
  <xsl:param name="path-stack"/>
  <xsl:param name="parent-dn"/>
  <xsl:param name="count-done" select="'0'"/>

  <xsl:variable name="count-todo"
                select="count(/xylophone/lists/list[@name=current()/@list]/item)"/>

  <xsl:if test="$count-done &lt; $count-todo">
    <xsl:variable name="count-done-next" select="number($count-done)+1"/>

    <!-- Possibly publish this object -->
    <xsl:call-template name="maybe-publish-object-and-children">
      <xsl:with-param name="list-item"
                      select="/xylophone/lists/list[@name=current()/@list]/item[$count-done-next]"/>
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="parent-dn" select="$parent-dn"/>
    </xsl:call-template>

    <!-- Iterate onto next item -->
    <xsl:call-template name="publish-multiple-list-objects">
      <xsl:with-param name="count-done" select="$count-done-next"/>
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="parent-dn" select="$parent-dn"/>
    </xsl:call-template>
  </xsl:if>

</xsl:template>



<xsl:template name="publish-Nth-of-multiple-select-objects-and-itr">
  <xsl:param name="parent-dn"/>
  <xsl:param name="list-item"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="xpaths"/>
  <xsl:param name="index" select="'1'"/>

  <xsl:variable name="count" select="count(exsl:node-set($xpaths)/results/match)"/>

  <xsl:if test="$index &lt;= $count">

    <xsl:variable name="is-duplicate">
      <xsl:call-template name="is-duplicate">
        <xsl:with-param name="list-item" select="$list-item"/>
        <xsl:with-param name="path-stack" select="$path-stack"/>
	<xsl:with-param name="xpaths" select="$xpaths"/>
	<xsl:with-param name="index" select="$index"/>
      </xsl:call-template>
    </xsl:variable>

    <xsl:if test="$is-duplicate = 'false'">
      <xsl:call-template name="maybe-publish-object-and-children">
	<xsl:with-param name="parent-dn" select="$parent-dn"/>
	<xsl:with-param name="list-item" select="$list-item"/>
	<xsl:with-param name="path-stack">
	  <xsl:call-template name="path-stack-add">
	    <xsl:with-param name="current-path-stack" select="$path-stack"/>
	    <xsl:with-param name="path" select="exsl:node-set($xpaths)/results/match[$index]"/>
	  </xsl:call-template>
	</xsl:with-param>
      </xsl:call-template>
    </xsl:if>

    <xsl:call-template name="publish-Nth-of-multiple-select-objects-and-itr">
      <xsl:with-param name="parent-dn" select="$parent-dn"/>
      <xsl:with-param name="list-item" select="$list-item"/>
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="xpaths" select="$xpaths"/>
      <xsl:with-param name="index" select="$index + 1"/>
    </xsl:call-template>
  </xsl:if>
</xsl:template>



<!--+
    |    This template expands to the LDIF output resulting from an object and
    |    any child XML elements (include child objects).  This process will
    |    consider user-supplied XML elements <suppress/> and <allow/>
    |    predicates, and the select-mode attribute of the object.
    |
    |    If the decision is to publish the object then this template will expand
    |    to the LDIF for this LDAP object and all child LDAP objects.
    |
    |    If we should not publish this object then this template will expand to
    |    an empty string, preventing the publishing of any child LDAP object.
    +-->
<xsl:template name="maybe-publish-object-and-children">
  <xsl:param name="parent-dn"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <!-- Evaluate whether we should publishing -->
  <xsl:variable name="should-publish">
    <xsl:call-template name="should-we-publish">
      <xsl:with-param name="list-item" select="$list-item"/>
      <xsl:with-param name="path-stack" select="$path-stack"/>
    </xsl:call-template>
  </xsl:variable>

  <!--  Publish, if we're supposed to -->
  <xsl:if test="normalize-space($should-publish)">
    <xsl:call-template name="publish-object-and-children">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="list-item" select="$list-item"/>
      <xsl:with-param name="parent-dn" select="$parent-dn"/>
    </xsl:call-template>
  </xsl:if>
</xsl:template>




<!--+
    |  A simple template that expands to a non-zero-length phrase if
    |  the current object (and child objects) should be emitted.
    |  This choice is based on the user's choice of
    |  <suppress/> and <allow/> elements, and the "select-mode"
    |  attribute of the object.
    +-->
<xsl:template name="should-we-publish">
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <xsl:variable name="have-suppress">
    <xsl:apply-templates select="suppress" mode="eval-predicate">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="list-item" select="$list-item"/>
    </xsl:apply-templates>
  </xsl:variable>


  <xsl:variable name="have-allow">
    <xsl:apply-templates select="allow" mode="eval-predicate">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="list-item" select="$list-item"/>
    </xsl:apply-templates>
  </xsl:variable>

  <xsl:choose>

    <!-- When we have explicit suppress / allow -->

    <xsl:when test="normalize-space($have-suppress)">
      <!-- Emit nothing, so suppressing output -->
    </xsl:when>

    <xsl:when test="normalize-space($have-allow)">
      <!-- Always emit something, so allowing output -->
      <xsl:text>PUBLISH</xsl:text>
    </xsl:when>


    <!-- When we fall back to default behaviour -->

    <xsl:when test="not(@select-mode) or @select-mode = 'default-allow'">
      <!-- Default is to publish -->
      <xsl:text>PUBLISH</xsl:text>
    </xsl:when>

    <xsl:when test="@select-mode = 'default-suppress'">
      <!-- Default is not to publish -->
    </xsl:when>


    <!-- Catch broken configuration  -->

    <xsl:otherwise>
      <xsl:message>Unknown select-mode attribute "<xsl:value-of select="@select-mode"/>" in object; should be either "default-allow" (default) or "default-suppress"</xsl:message>
      <!-- If we don't know what the user wants, we don't publish. -->
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>


<!--+
    |  Emit everything to do with the current object: the
    |  actualy LDIF for the object and process any child
    |  objects
    +-->
<xsl:template name="publish-object-and-children">
  <xsl:param name="parent-dn"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <xsl:variable name="our-dn">
    <xsl:call-template name="build-DN">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="list-item" select="$list-item"/>
      <xsl:with-param name="parent-dn" select="$parent-dn"/>
    </xsl:call-template>
  </xsl:variable>


  <xsl:if test="not(@hidden)">
    <xsl:call-template name="publish-object">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="list-item" select="$list-item"/>
      <xsl:with-param name="dn" select="$our-dn"/>
    </xsl:call-template>
  </xsl:if>

  <!-- Publish any child objects -->
  <xsl:apply-templates select="object" mode="publish">
    <xsl:with-param name="path-stack" select="$path-stack"/>
    <xsl:with-param name="list-item" select="$list-item"/>
    <xsl:with-param name="parent-dn" select="$our-dn"/>
  </xsl:apply-templates>
</xsl:template>




<!--+
    |  Emit LDIF for an object
    |
    |  Context should be the current object
    +-->
<xsl:template name="publish-object">
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>
  <xsl:param name="dn"/>

  <xsl:call-template name="output-empty-line"/>

  <!-- Optionally emit an explicit comment -->
  <xsl:apply-templates select="@comment" mode="emit-comment"/>
  <xsl:apply-templates select="comment" mode="emit-comment">
    <xsl:with-param name="path-stack" select="$path-stack"/>
    <xsl:with-param name="list-item" select="$list-item"/>
  </xsl:apply-templates>

  <!-- Emit object DN -->
  <xsl:call-template name="output-raw-attribute">
    <xsl:with-param name="key" select="'dn'"/>
    <xsl:with-param name="value" select="$dn"/>
  </xsl:call-template>

  <!-- If the user has supplied any classes -->
  <xsl:if test="@classes">
    <xsl:variable name="norm-classes" select="normalize-space(@classes)"/>

    <!-- Emit the objectClass attribute list -->
    <xsl:call-template name="output-objectClass-attributes">
      <xsl:with-param name="classes" select="$norm-classes"/>
    </xsl:call-template>

    <!-- Emit the objectClass-derived attributes -->
    <xsl:call-template name="publish-all-classes-attr">
      <xsl:with-param name="classes" select="$norm-classes"/>
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="list-item" select="$list-item"/>
    </xsl:call-template>
  </xsl:if>

  <!-- Emit any object-local attributes -->
  <xsl:apply-templates select="attr" mode="publish">
    <xsl:with-param name="list-item" select="$list-item"/>
    <xsl:with-param name="path-stack" select="$path-stack"/>
  </xsl:apply-templates>
</xsl:template>





<!--+
    |  Output all objectClass attributes from a space-separated list of
    |  objectClasses.  We assume there is at least one: the caller is
    |  responsible for checking this.
    +-->
<xsl:template name="output-objectClass-attributes">
  <xsl:param name="classes"/>

  <xsl:choose>

    <!--  Multiple classes to process -->
    <xsl:when test="contains( $classes, ' ')">

      <!-- Publish an object -->
      <xsl:call-template name="output-objectClass">
        <xsl:with-param name="name" select="substring-before($classes, ' ')"/>
      </xsl:call-template>

      <!-- Iterate to next class -->
      <xsl:call-template name="output-objectClass-attributes">
        <xsl:with-param name="classes" select="substring-after($classes, ' ')"/>
      </xsl:call-template>
    </xsl:when>

    <!-- Otherwise, just the one left -->
    <xsl:otherwise>
      <xsl:call-template name="output-objectClass">
        <xsl:with-param name="name" select="$classes"/>
      </xsl:call-template>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>




<!--+
    | Output a single objectClass attribute.
    +-->
<xsl:template name="output-objectClass">
  <xsl:param name="name"/>

  <!--+
      |  We must look up whether this class is to be published
      |  with a different name.
      +-->
  <xsl:variable name="publish-name">
    <xsl:choose>
      <xsl:when test="/xylophone/classes/class[@name=$name]/@publish-as">
        <xsl:value-of select="/xylophone/classes/class[@name=$name]/@publish-as"/>
      </xsl:when>

      <xsl:otherwise>
        <xsl:value-of select="$name"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <!--+
      |  ... and publish it.
      +-->
  <xsl:call-template name="output-attribute">
    <xsl:with-param name="key" select="'objectClass'"/>
    <xsl:with-param name="value" select="$publish-name"/>
  </xsl:call-template>
</xsl:template>





<!--+
    |  Publish attributes based on a single <attr/> element.
    |
    |  Life is complex: a single <attr/> element may result in zero, one
    |  or a variable number of LDIF attributes being emitted depending
    |  on the (XML) attributes of the attr element.
    +-->
<xsl:template match="attr" mode="publish">
  <xsl:param name="depth" select="count(ancestor-or-self::object)"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <xsl:choose>
    <xsl:when test="@hidden">
      <!-- do nothing -->
    </xsl:when>

    <!-- Multiple attributes based on a list in dynamic content -->
    <xsl:when test="@select">
      <xsl:if test="@list">
	<xsl:message>Both "list" and "select" attributes are specified in attr element; I will ignore "list".</xsl:message>
      </xsl:if>

      <xsl:call-template name="publish-multiple-select-attr">
	<xsl:with-param name="path-stack" select="$path-stack"/>
	<xsl:with-param name="depth" select="$depth"/>
	<xsl:with-param name="list-item" select="$list-item"/>
      </xsl:call-template>
    </xsl:when>


    <!-- Multiple attributes based on a list -->
    <xsl:when test="@list">
      <xsl:choose>
	<xsl:when test="count(/xylophone/lists/list[@name=current()/@list]) = 0">
	  <xsl:message>There is no list named &quot;<xsl:value-of select="@list"/>&quot;.</xsl:message>
	</xsl:when>

	<xsl:otherwise>
	  <xsl:call-template name="publish-multiple-list-attr">
	    <xsl:with-param name="path-stack" select="$path-stack"/>
	    <xsl:with-param name="depth" select="$depth"/>
	  </xsl:call-template>
	</xsl:otherwise>
      </xsl:choose>
    </xsl:when>

    <!-- Single attribute -->
    <xsl:otherwise>
      <xsl:call-template name="publish-single-attr">
	<xsl:with-param name="path-stack" select="$path-stack"/>
	<xsl:with-param name="depth" select="$depth"/>
	<xsl:with-param name="list-item" select="$list-item"/>
      </xsl:call-template>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>



<!--+
    |  This template expands to all the LDIF attributes that correspond to a
    |  single (XML) attr element with an XML path attribute specified.  The
    |  path attribute is evaluated as an XPath and an LDIF attribute is
    |  emitted for each node (subject to uniqueness constraints).
    +-->
<xsl:template name="publish-multiple-select-attr">
  <xsl:param name="depth" select="count(ancestor-or-self::object)"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <xsl:call-template name="publish-Nth-of-multiple-select-attr-and-itr">
    <xsl:with-param name="list-item" select="$list-item"/>
    <xsl:with-param name="path-stack" select="$path-stack"/>
    <xsl:with-param name="xpaths">
      <xsl:call-template name="build-item-list">
	<xsl:with-param name="path-stack" select="$path-stack"/>
      </xsl:call-template>
    </xsl:with-param>
    <xsl:with-param name="depth" select="$depth"/>
  </xsl:call-template>
</xsl:template>



<xsl:template name="publish-Nth-of-multiple-select-attr-and-itr">
  <xsl:param name="list-item"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="depth"/>
  <xsl:param name="xpaths"/>
  <xsl:param name="index" select="'1'"/>

  <xsl:variable name="count" select="count(exsl:node-set($xpaths)/results/match)"/>

  <xsl:if test="$index &lt;= $count">

    <!-- Obtain boolean value whether publishing this object would create a duplicate -->
    <xsl:variable name="is-duplicate">
      <xsl:call-template name="is-duplicate">
	<xsl:with-param name="list-item" select="$list-item"/>
	<xsl:with-param name="path-stack" select="$path-stack"/>
	<xsl:with-param name="xpaths" select="$xpaths"/>
	<xsl:with-param name="index" select="$index"/>
      </xsl:call-template>
    </xsl:variable>

    <xsl:if test="$is-duplicate = 'false'">
      <xsl:call-template name="publish-single-attr">
	<xsl:with-param name="list-item" select="$list-item"/>
	<xsl:with-param name="depth" select="$depth"/>
	<xsl:with-param name="path-stack">
	  <xsl:call-template name="path-stack-add">
	    <xsl:with-param name="current-path-stack" select="$path-stack"/>
	    <xsl:with-param name="path" select="exsl:node-set($xpaths)/results/match[$index]"/>
	    <xsl:with-param name="depth" select="$depth"/>
	  </xsl:call-template>
	</xsl:with-param>
      </xsl:call-template>
    </xsl:if>

    <!-- Iterate onto next item -->
    <xsl:call-template name="publish-Nth-of-multiple-select-attr-and-itr">
      <xsl:with-param name="list-item" select="$list-item"/>
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="depth" select="$depth"/>
      <xsl:with-param name="xpaths" select="$xpaths"/>
      <xsl:with-param name="index" select="$index + 1"/>
    </xsl:call-template>
  </xsl:if>
</xsl:template>



<!--+
    |  Iterate over a list increasing count-done each time.
    +-->
<xsl:template name="publish-multiple-list-attr">
  <xsl:param name="depth" select="count(ancestor-or-self::object)"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="count-done" select="'0'"/>

  <xsl:variable name="count-todo" select="count(/xylophone/lists/list[@name=current()/@list]/item)"/>

  <xsl:if test="$count-done &lt; $count-todo">

    <xsl:variable name="count-done-next" select="number($count-done)+1"/>

    <!-- Possibly publish this object -->
    <xsl:call-template name="publish-single-attr">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="depth" select="$depth"/>
      <xsl:with-param name="list-item" select="/xylophone/lists/list[@name=current()/@list]/item[$count-done-next]"/>
    </xsl:call-template>

    <!-- Iterate onto next item -->
    <xsl:call-template name="publish-multiple-list-attr">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="depth" select="$depth"/>
      <xsl:with-param name="count-done" select="$count-done-next"/>
    </xsl:call-template>
  </xsl:if>
</xsl:template>



<!--+
    |  emit the text for a single attribute
    +-->
<xsl:template name="publish-single-attr">
  <xsl:param name="depth" select="count(ancestor-or-self::object)"/>
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <!-- Calculate the output variable -->
  <xsl:variable name="value">
    <xsl:apply-templates mode="eval-attr">
      <xsl:with-param name="list-item" select="$list-item"/>
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="depth" select="$depth"/>
    </xsl:apply-templates>
  </xsl:variable>

  <!-- Expand the name of the attribute -->
  <xsl:variable name="name">
    <xsl:call-template name="expand-attr-name">
      <xsl:with-param name="list-item" select="$list-item"/>
      <xsl:with-param name="text" select="@name"/>
    </xsl:call-template>
  </xsl:variable>

  <!-- Check for suppress elements -->
  <xsl:variable name="suppress-result">
    <xsl:apply-templates select="suppress" mode="eval-predicate">
      <xsl:with-param name="path-stack" select="$path-stack"/>
      <xsl:with-param name="list-item" select="$list-item"/>
    </xsl:apply-templates>
  </xsl:variable>

  <!-- Publish, unless it is empty and we should suppress empty output -->
  <xsl:if test="(not(@not-empty) or @not-empty = '0' or normalize-space($value)) and not(normalize-space($suppress-result))">
    <xsl:call-template name="output-attribute">
      <xsl:with-param name="key" select="string($name)"/>
      <xsl:with-param name="value" select="string($value)"/>
    </xsl:call-template>
  </xsl:if>

</xsl:template>


<!--+
    |  Iterate, expanding any special elements within an attribute's name.
    |
    |    list-item => an item's list.
    +-->
<xsl:template name="expand-attr-name">
  <xsl:param name="list-item"/>
  <xsl:param name="text"/>

  <xsl:choose>
    <xsl:when test="contains( $text, '[ITEM]')">
      <xsl:value-of select="concat(substring-before( $text, '[ITEM]'),$list-item)"/>
      <xsl:call-template name="expand-attr-name">
        <xsl:with-param name="list-item" select="$list-item"/>
        <xsl:with-param name="text" select="substring-after( $text, '[ITEM]')"/>
      </xsl:call-template>
    </xsl:when>

    <xsl:otherwise>
      <xsl:value-of select="$text"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>




<!--+
    |   Publish attributes for a given list of classes.  We assume there
    |   is at least one class in the space-separated list.
    +-->
<xsl:template name="publish-all-classes-attr">
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>
  <xsl:param name="classes"/>

  <xsl:choose>
    <!--  Multiple classes still to process -->
    <xsl:when test="contains( $classes, ' ')">
      <xsl:apply-templates select="/xylophone/classes/class[@name=substring-before($classes, ' ')]/attr" mode="publish">
        <xsl:with-param name="list-item" select="$list-item"/>
        <xsl:with-param name="path-stack" select="$path-stack"/>
        <xsl:with-param name="depth" select="count(ancestor-or-self::object)"/>
      </xsl:apply-templates>

      <xsl:call-template name="publish-all-classes-attr">
        <xsl:with-param name="path-stack" select="$path-stack"/>
        <xsl:with-param name="list-item" select="$list-item"/>
        <xsl:with-param name="classes" select="substring-after($classes, ' ')"/>
      </xsl:call-template>
    </xsl:when>

    <!-- Otherwise, just the one class left -->
    <xsl:otherwise>
      <xsl:apply-templates select="/xylophone/classes/class[@name=$classes]/attr" mode="publish">
        <xsl:with-param name="list-item" select="$list-item"/>
        <xsl:with-param name="path-stack" select="$path-stack"/>
        <xsl:with-param name="depth" select="count(ancestor-or-self::object)"/>
      </xsl:apply-templates>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>


<!--+
    |  Emit the current object's DN
    +-->
<xsl:template name="build-DN">
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>
  <xsl:param name="parent-dn"/>

  <xsl:call-template name="emit-RDN-for-DN">
    <xsl:with-param name="path-stack" select="$path-stack"/>
    <xsl:with-param name="list-item" select="$list-item"/>
  </xsl:call-template>

  <xsl:if test="string-length($parent-dn)>0">
    <xsl:value-of select="concat(',',$parent-dn)"/>
  </xsl:if>
</xsl:template>


<!--+
    |  Emit the current object's RDN suitable for part of a DN.
    +-->
<xsl:template name="emit-RDN-for-DN">
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <xsl:value-of select="concat(@rdn,'=')"/>

  <xsl:call-template name="markup-rdn-value">
    <xsl:with-param name="value">
      <xsl:call-template name="emit-RDN-value">
        <xsl:with-param name="path-stack" select="$path-stack"/>
        <xsl:with-param name="list-item" select="$list-item"/>
      </xsl:call-template>
    </xsl:with-param>
  </xsl:call-template>
</xsl:template>


<!--+
    |  Emit the current object's RDN without any markup.
    +-->
<xsl:template match="object" mode="emit-RDN-for-attribute">
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <xsl:value-of select="concat(@rdn,'=')"/>

  <xsl:call-template name="markup-attribute-value">
    <xsl:with-param name="value">
      <xsl:call-template name="emit-RDN-value">
        <xsl:with-param name="path-stack" select="$path-stack"/>
        <xsl:with-param name="list-item" select="$list-item"/>
      </xsl:call-template>
    </xsl:with-param>
  </xsl:call-template>
</xsl:template>



<!--+
    |  Emit the current object's RDN attribute value.
    |
    |  This involves looking up the named attribute's value.  This might
    |  be an object-local attribute or it might be defined within one of
    |  the object's classes, so some hunting may be required.
    +-->
<xsl:template name="emit-RDN-value">
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>

  <xsl:choose>
    <xsl:when test="not(@rdn)">
      <xsl:message>Missing attribute "rdn" for object.</xsl:message>
    </xsl:when>

    <xsl:when test="not(normalize-space(@rdn))">
      <xsl:message>Empty attribute "rdn" for object.</xsl:message>
    </xsl:when>

    <!-- First look to see if the RDN is an object-local attribute -->
    <xsl:when test="count(attr[@name=current()/@rdn]) > 0">
      <xsl:apply-templates select="attr[@name=current()/@rdn]" mode="eval-attr">
        <xsl:with-param name="list-item" select="$list-item"/>
        <xsl:with-param name="path-stack" select="$path-stack"/>
        <xsl:with-param name="depth" select="count(ancestor-or-self::object)"/>
      </xsl:apply-templates>
    </xsl:when>

    <!-- Otherwise, go hunting... -->
    <xsl:when test="@classes">
      <xsl:call-template name="hunt-all-classes-for-attr">
        <xsl:with-param name="classes" select="normalize-space(@classes)"/>
        <xsl:with-param name="path-stack" select="$path-stack"/>
        <xsl:with-param name="list-item" select="$list-item"/>
        <xsl:with-param name="name" select="@rdn"/>
      </xsl:call-template>
    </xsl:when>

    <xsl:otherwise>
      <xsl:message>Cannot find RDN attribute <xsl:value-of select="@rdn"/></xsl:message>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>



<!--+
    | Iterate through a space-separated list of classes; for each class,
    | check whether it has a matching value.
    +-->
<xsl:template name="hunt-all-classes-for-attr">
  <xsl:param name="path-stack"/>
  <xsl:param name="list-item"/>
  <xsl:param name="classes"/>
  <xsl:param name="name"/>

  <xsl:choose>

    <!--  Multiple classes still to process -->
    <xsl:when test="contains( $classes, ' ')">

      <xsl:apply-templates select="/xylophone/classes/class[@name=substring-before($classes, ' ')]/attr[@name=$name]"
                           mode="eval-attr">
        <xsl:with-param name="path-stack" select="$path-stack"/>
        <xsl:with-param name="depth" select="count(ancestor-or-self::object)"/>
        <xsl:with-param name="list-item" select="$list-item"/>
      </xsl:apply-templates>

      <!-- Iterate onto the next one -->
      <xsl:call-template name="hunt-all-classes-for-attr">
        <xsl:with-param name="classes" select="substring-after($classes, ' ')"/>
        <xsl:with-param name="path-stack" select="$path-stack"/>
        <xsl:with-param name="list-item" select="$list-item"/>
        <xsl:with-param name="name" select="$name"/>
      </xsl:call-template>
    </xsl:when>

    <!-- Otherwise, just the one class left -->
    <xsl:otherwise>
      <xsl:apply-templates select="/xylophone/classes/class[@name=$classes]/attr[@name=$name]"
                           mode="eval-attr">
        <xsl:with-param name="path-stack" select="$path-stack"/>
        <xsl:with-param name="depth" select="count(ancestor-or-self::object)"/>
        <xsl:with-param name="list-item" select="$list-item"/>
      </xsl:apply-templates>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>



</xsl:stylesheet>
