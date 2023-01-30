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
package org.dcache.services.bulk.activity.plugin.log;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.util.FsPath;
import java.util.Map;
import org.dcache.services.bulk.activity.BulkActivity;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.services.bulk.util.BulkRequestTargetBuilder;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This activity is mainly useful to check the correctness of node expansion.
 */
public final class LogTargetActivity extends BulkActivity<BulkRequestTarget> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogTargetActivity.class);

    /**
     * id | updated | ruid | type | [pnfsid]: target
     */
    private static final String FORMAT_TARGET = "%-12s | %19s | %40s | %9s | [%s]: %s";

    protected LogTargetActivity(String name, TargetType targetType) {
        super(name, targetType);
    }

    @Override
    public ListenableFuture<BulkRequestTarget> perform(String ruid, long tid, FsPath path,
          FileAttributes attributes) {
        long now = System.currentTimeMillis();
        BulkRequestTarget t = BulkRequestTargetBuilder.builder().activity(this.getName()).id(tid)
              .ruid(ruid).state(State.RUNNING).path(path).createdAt(now).attributes(attributes)
              .startedAt(now).lastUpdated(now).build();

        String type;
        String pnfsid;
        if (attributes != null) {
            type = attributes.getFileType().name();
            pnfsid = attributes.getPnfsId().toString();
        } else {
            type = "?";
            pnfsid = "?";
        }

        LOGGER.info("{}", String.format(FORMAT_TARGET, tid, now, ruid, type, pnfsid, path));
        return Futures.immediateFuture(t);
    }

    @Override
    protected void handleCompletion(BulkRequestTarget target, ListenableFuture future) {
        target.setState(State.COMPLETED);
    }

    @Override
    protected void configure(Map<String, String> arguments) {
        /** NOP **/
    }
}
