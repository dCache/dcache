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
package org.dcache.services.bulk.activity.plugin.delete;

import static com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly;
import static org.dcache.services.bulk.activity.plugin.delete.DeleteActivityProvider.SKIP_DIRS;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NamespaceHandlerAware;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.activity.BulkActivity;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.vehicles.FileAttributes;

public final class DeleteActivity extends BulkActivity<PnfsDeleteEntryMessage> implements
      NamespaceHandlerAware {

    private PnfsHandler pnfsHandler;
    private boolean skipDirs;

    public DeleteActivity(String name, TargetType targetType) {
        super(name, targetType);
    }

    @Override
    public ListenableFuture<PnfsDeleteEntryMessage> perform(String rid, long tid, FsPath path,
          FileAttributes attributes) {
        PnfsDeleteEntryMessage msg = new PnfsDeleteEntryMessage(path.toString());
        msg.setSubject(subject);
        if (attributes != null && attributes.getFileType() == FileType.DIR && skipDirs) {
            msg.setSucceeded();
            return Futures.immediateFuture(msg);
        }
        return pnfsHandler.requestAsync(msg);
    }

    @Override
    public void setNamespaceHandler(PnfsHandler pnfsHandler) {
        this.pnfsHandler = pnfsHandler;
    }

    @Override
    public void handleCompletion(BulkRequestTarget target,
          Future<PnfsDeleteEntryMessage> future) {
        PnfsDeleteEntryMessage reply;
        try {
            reply = getUninterruptibly(future);
            if (reply.getReturnCode() != 0) {
                target.setErrorObject(reply.getErrorObject());
            } else {
                if (target.getType() == FileType.DIR && skipDirs) {
                    target.setState(State.SKIPPED);
                } else {
                    target.setState(State.COMPLETED);
                }
            }
        } catch (CancellationException e) {
            /*
             *  CANCELLED is set elsewhere
             */
        } catch (ExecutionException e) {
            target.setErrorObject(e.getCause());
        }
    }

    @Override
    protected void configure(Map<String, String> arguments) {
        if (arguments == null) {
            /*
             *  There is only one descriptor.
             */
            skipDirs = Boolean.parseBoolean(descriptors.iterator().next().getDefaultValue());
        } else {
            skipDirs = Boolean.parseBoolean(arguments.get(SKIP_DIRS));
        }
    }
}
