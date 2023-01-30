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
package org.dcache.services.bulk.store.jdbc.rtarget;

import static java.util.stream.Collectors.joining;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.FAILED;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.RUNNING;
import static org.dcache.util.Strings.truncate;

import com.google.common.base.Throwables;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import java.sql.Timestamp;
import org.dcache.db.JdbcUpdate;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.util.BulkRequestTarget.PID;
import org.dcache.services.bulk.util.BulkRequestTarget.State;

/**
 * Implementation of the update class for the request target table.
 */
public final class JdbcRequestTargetUpdate extends JdbcUpdate {

    public String getUpdate() {
        return updates.keySet().stream()
              .map(s -> s + " = ?")
              .collect(joining(","));
    }

    public JdbcRequestTargetUpdate state(State state) {
        if (state != null) {
            set("state", state.name());
            Timestamp now = new Timestamp(System.currentTimeMillis());
            set("last_updated", now);
            if (state == RUNNING) {
                set("started_at", now);
            }
        }
        return this;
    }

    public String getStateName() {
        return (String)updates().get("state");
    }

    public JdbcRequestTargetUpdate createdAt(long createdAt) {
        Timestamp now = new Timestamp(createdAt);
        set("created_at", now);
        return this;
    }

    public JdbcRequestTargetUpdate targetStart(long createdAt) {
        Timestamp now = new Timestamp(createdAt);
        set("created_at", now);
        set("started_at", now);
        set("last_updated", now);
        return this;
    }

    public JdbcRequestTargetUpdate aborted() {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        set("created_at", now);
        set("last_updated", now);
        set("state", FAILED.name());
        return this;
    }

    public JdbcRequestTargetUpdate errorObject(Throwable errorObject) {
        if (errorObject != null) {
            Throwable root = Throwables.getRootCause(errorObject);
            set("error_type", root.getClass().getCanonicalName());
            set("error_message", truncate(root.getMessage(), 256, false));
        }
        return this;
    }

    public JdbcRequestTargetUpdate pid(PID pid) {
        set("pid", pid == null ? PID.INITIAL.ordinal() : pid.ordinal());
        return this;
    }

    public JdbcRequestTargetUpdate rid(Long rid) {
        if (rid != null) {
            set("rid", rid);
        }
        return this;
    }

    public JdbcRequestTargetUpdate pnfsid(PnfsId pnfsId) {
        if (pnfsId != null) {
            set("pnfsid", pnfsId.toString());
        } else {
            set("pnfsid", "?");
        }
        return this;
    }

    public JdbcRequestTargetUpdate path(FsPath path) {
        if (path != null) {
            set("path", truncate(path.toString(), 256,true));
        }
        return this;
    }

    public JdbcRequestTargetUpdate type(FileType type) {
        if (type != null) {
            set("type", type.name());
        } else {
            set("type", "?");
        }
        return this;
    }
}
