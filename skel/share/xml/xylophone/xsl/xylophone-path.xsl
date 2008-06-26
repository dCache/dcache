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
    |  This file contains a number of useful utility templates for dealing with
    |  path.
    +-->


<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version='1.0'>

<!-- Provide a combined absolute path based on a path and rel-path parameters -->
<xsl:template name="combine-paths">
  <xsl:param name="path"/>
  <xsl:param name="rel-path"/>

  <!--+
      | Expand to either just $path or $combined-path, depending on whether $path
      | $path is absolute.
      +-->
  <xsl:choose>

    <!-- $path is absolute, ignore $rel-path and strip off the leading '/' -->
    <xsl:when test="starts-with($path, '/')">
      <xsl:value-of select="$path"/>
    </xsl:when>


    <!-- $path is relative, so use $rel-path via $combined-path -->
    <xsl:otherwise>

      <!-- First, try concat()ing with rel-path -->
      <xsl:variable name="combined-path">
	<xsl:choose>
	  <xsl:when test="$rel-path">
	    <xsl:value-of select="concat($rel-path,'/',$path)"/>
	  </xsl:when>
	  <xsl:otherwise>
	    <xsl:value-of select="$path"/>
	  </xsl:otherwise>
	</xsl:choose>
      </xsl:variable>


      <!-- Then ensure the path starts with a '/' -->
      <xsl:choose>
	<xsl:when test="starts-with($combined-path, '/')">
	  <xsl:value-of select="$combined-path"/>
	</xsl:when>
	<xsl:otherwise>
	  <xsl:value-of select="concat( '/', $combined-path)"/>
	</xsl:otherwise>
      </xsl:choose>
    </xsl:otherwise>

  </xsl:choose>

</xsl:template>



</xsl:stylesheet>