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
package diskCacheV111.pools.json;

import diskCacheV111.pools.PoolCostInfo.PoolSpaceInfo;
import java.io.Serializable;
import javax.annotation.Nonnull;

/**
 * <p>Bean analogous to {@link PoolSpaceInfo}.</p>
 */
public class PoolSpaceData implements Serializable {

    private static final long serialVersionUID = 3883698337348311670L;
    private Long total;
    private Long free;
    private Long precious;
    private Long removable;
    private Long lru;
    private Long gap;
    private Double breakEven;

    public PoolSpaceData() {
    }

    public PoolSpaceData(@Nonnull PoolSpaceInfo info) {
        total = info.getTotalSpace();
        free = info.getFreeSpace();
        precious = info.getPreciousSpace();
        removable = info.getRemovableSpace();
        lru = info.getLRUSeconds();
        gap = info.getGap();
        breakEven = info.getBreakEven();
    }

    public void aggregateData(@Nonnull PoolSpaceData using) {
        if (total == null) {
            total = using.total;
        } else {
            total += using.total == null ? 0L : using.total;
        }

        if (free == null) {
            free = using.free;
        } else {
            free += using.free == null ? 0L : using.free;
        }

        if (precious == null) {
            precious = using.precious;
        } else {
            precious += using.precious == null ? 0L : using.precious;
        }

        if (removable == null) {
            removable = using.removable;
        } else {
            removable += using.removable == null ? 0L : using.removable;
        }
    }

    public Double getBreakEven() {
        return breakEven;
    }

    public Long getFree() {
        return free;
    }

    public Long getGap() {
        return gap;
    }

    public Long getLru() {
        return lru;
    }

    public Long getPrecious() {
        return precious;
    }

    public Long getRemovable() {
        return removable;
    }

    public Long getTotal() {
        return total;
    }

    public void setBreakEven(Double breakEven) {
        this.breakEven = breakEven;
    }

    public void setFree(Long free) {
        this.free = free;
    }

    public void setGap(Long gap) {
        this.gap = gap;
    }

    public void setLru(Long lru) {
        this.lru = lru;
    }

    public void setPrecious(Long precious) {
        this.precious = precious;
    }

    public void setRemovable(Long removable) {
        this.removable = removable;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

}
