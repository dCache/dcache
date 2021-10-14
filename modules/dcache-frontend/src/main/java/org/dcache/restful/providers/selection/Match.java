/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.restful.providers.selection;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;

@ApiModel(description = "Specifies parameters for selecting a group of pools.  "
      + "Pools must match all specified fields.")
public final class Match implements Serializable {

    private static final long serialVersionUID = -3298166715830066810L;

    @ApiModelProperty(value = "The operation type.", allowableValues = "READ, WRITE, CACHE, P2P, ANY")
    private String type = "READ";

    @ApiModelProperty("The name of the matching store unit.")
    private String store = "*";

    @ApiModelProperty("The name of the matching dcache unit.")
    private String dcache = "*";

    @ApiModelProperty("The name of the matching net unit.")
    private String net = "*";

    @ApiModelProperty("The name of the matching protocol unit.")
    private String protocol = "*";

    @ApiModelProperty("The linkgroup unit, or 'none' for a request outside of a linkgroup.")
    private String linkGroup = "none";

    public String getDcache() {
        return dcache;
    }

    public String getLinkGroup() {
        return linkGroup;
    }

    public String getNet() {
        return net;
    }

    public String getProtocol() {
        return protocol;
    }

    public String getStore() {
        return store;
    }

    public String getType() {
        return type;
    }

    public void setDcache(String dcache) {
        this.dcache = dcache;
    }

    public void setLinkGroup(String linkGroup) {
        this.linkGroup = linkGroup;
    }

    public void setNet(String net) {
        this.net = net;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public void setStore(String store) {
        this.store = store;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String toPoolManagerCommand() {
        return "psux match " + type + " " + store + " " + dcache + " " + net
              + " " + protocol + (linkGroup.equals("none") ?
              "" : " -linkGroup=" + linkGroup);
    }
}
