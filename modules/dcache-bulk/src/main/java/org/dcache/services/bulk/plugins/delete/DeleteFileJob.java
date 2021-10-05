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
package org.dcache.services.bulk.plugins.delete;

import static org.dcache.services.bulk.plugins.delete.DeleteFileJobProvider.SKIP_DIRS;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.NamespaceHandlerAware;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.Set;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.job.BulkJobKey;
import org.dcache.services.bulk.job.SingleTargetJob;

/**
 * Removes file entry from namespace.
 */
public class DeleteFileJob extends SingleTargetJob implements NamespaceHandlerAware {

    private static final Set<FileType> TYPES = EnumSet.allOf(FileType.class);
    private static final Set<FileAttribute> ATTRIBUTES = EnumSet.noneOf(FileAttribute.class);

    private PnfsHandler pnfsHandler;

    public DeleteFileJob(BulkJobKey key, BulkJobKey parentKey, String activity) {
        super(key, parentKey, activity);
    }

    @Override
    public void setNamespaceHandler(PnfsHandler pnfsHandler) {
        this.pnfsHandler = pnfsHandler;
    }

    @Override
    protected void doRun() {
        if (attributes.getFileType() == FileType.DIR && isSkipDirs()) {
            return;
        }

        PnfsDeleteEntryMessage msg
              = new PnfsDeleteEntryMessage(attributes.getPnfsId(), path.toString(), TYPES,
              ATTRIBUTES);

        try {
            msg = pnfsHandler.request(msg);
        } catch (CacheException e) {
            setError(e);
            return;
        }

        Serializable error = msg.getErrorObject();
        if (error != null) {
            setError(error);
        } else {
            setState(State.COMPLETED);
        }
    }

    private boolean isSkipDirs() {
        if (arguments == null) {
            return Boolean.parseBoolean(SKIP_DIRS.getDefaultValue());
        }
        return Boolean.parseBoolean(arguments.get(SKIP_DIRS.getName()));
    }
}
