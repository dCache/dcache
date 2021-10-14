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
package org.dcache.restful.providers;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.validation.constraints.NotNull;
import org.dcache.util.InvalidatableItem;

@ApiModel(description = "A selection of results from a semi-persistant snapshot. "
      + "Each snapshot has a UUID that identifies it.  The "
      + "request that created the snapshot also accepts the uuid as "
      + "an argument, allowing successive requests from the same "
      + "snapshot with different offset and lengths, providing "
      + "support for paging.")
public class SnapshotList<T extends InvalidatableItem & Serializable>
      implements Serializable {

    @ApiModelProperty("  The list of returned data.")
    @NotNull
    private List<T> items = Collections.EMPTY_LIST;

    @ApiModelProperty("The original offset requested.  The first element in the "
          + "returned list should correspond to "
          + "this index in the underlying complete list.")
    private int currentOffset = 0;

    @ApiModelProperty("Should be the currentOffset plus the size of the returned "
          + "list if the list has more elements; otherwise -1.")
    private int nextOffset = 0;

    @ApiModelProperty("Identifies the snapshot used to service this request. "
          + "May be null only if transfers is empty.")
    private UUID currentToken;

    @ApiModelProperty("Timestamp in unix-time when snapshot was created.")
    private long timeOfCreation = 0L;

    public int getCurrentOffset() {
        return currentOffset;
    }

    public UUID getCurrentToken() {
        return currentToken;
    }

    public List<T> getItems() {
        return items;
    }

    public int getNextOffset() {
        return nextOffset;
    }

    public long getTimeOfCreation() {
        return timeOfCreation;
    }

    public void setCurrentOffset(int currentOffset) {
        this.currentOffset = currentOffset;
    }

    public void setCurrentToken(UUID currentToken) {
        this.currentToken = currentToken;
    }

    public void setItems(List<T> items) {
        this.items = items;
    }

    public void setNextOffset(int nextOffset) {
        this.nextOffset = nextOffset;
    }

    public void setTimeOfCreation(long timeOfCreation) {
        this.timeOfCreation = timeOfCreation;
    }
}
