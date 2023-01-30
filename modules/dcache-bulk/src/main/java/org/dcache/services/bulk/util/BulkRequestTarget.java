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
package org.dcache.services.bulk.util;

import static java.util.Objects.requireNonNull;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import java.sql.Timestamp;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.vehicles.FileAttributes;

/**
 * Wraps data for an individual target of a request.  This object corresponds to the underlying
 * storage table, except that pnfsid and file type are extracted from the file attributes field
 * and the throwable error is stored as classname and message.
 * <p>
 * Provides state management for the in-memory updates, as well as for cancellation.
 */
public final class BulkRequestTarget {

    public static final State[] NON_TERMINAL = new State[]{State.CREATED, State.READY,
          State.RUNNING};

    public static FsPath computeFsPath(String prefix, String target) {
        if (prefix == null) {
            return FsPath.create(FsPath.ROOT + target);
        } else {
            return FsPath.create(
                  FsPath.ROOT + (prefix.endsWith("/") ? prefix : prefix + "/") + target);
        }
    }

    public static final FsPath ROOT_REQUEST_PATH = computeFsPath(null, "=request_target=");
    public static final PnfsId PLACEHOLDER_PNFSID = new PnfsId("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");

    public enum State {
        CREATED, READY, RUNNING, CANCELLED, COMPLETED, FAILED, SKIPPED
    }

    public enum PID {
        ROOT, INITIAL, DISCOVERED
    }

    private static final String INVALID_STATE_TRANSITION =
          "%s: invalid target state transition %s to %s; please report this to dcache.org.";

    private static final String TARGET_FORMAT =
          "TARGET [%s, %s, %s][%s][%s: (C %s)(S %s)(U %s)(ret %s)][%s] %s : %s (err %s)";

    private static final String KEY_SEPARATOR = "::";

    private static final String KEY_FORMAT = "%s" + KEY_SEPARATOR + "%s";

    public static String[] parse(String key) {
        return key.split(KEY_SEPARATOR);
    }

    private Long id;
    private PID pid;
    private Long rid;
    private String ruid;
    private FsPath path;
    private String activity;
    private State state;
    private long createdAt;
    private Long startedAt;
    private long lastUpdated;
    private int retried;
    private Throwable errorObject;
    private FileAttributes attributes;

    BulkRequestTarget() {
        /**
         * Constructed by fluid builder.
         */
        state = State.CREATED;
        createdAt = System.currentTimeMillis();
    }

    public synchronized boolean cancel() {
        switch (state) {
            case CREATED:
            case READY:
            case RUNNING:
                state = State.CANCELLED;
                if (errorObject == null) {
                    errorObject = new BulkServiceException(getKey() + ": " + state);
                }

                return true;
            default:
                /*
                 * Already terminated.
                 */
                return false;
        }
    }

    public synchronized boolean isCreated() {
        switch (state) {
            case CREATED:
                return true;
            default:
                return false;
        }
    }

    public synchronized boolean isReady() {
        switch (state) {
            case READY:
                return true;
            default:
                return false;
        }
    }

    public synchronized boolean isTerminated() {
        switch (state) {
            case FAILED:
            case CANCELLED:
            case COMPLETED:
            case SKIPPED:
                return true;
            default:
                return false;
        }
    }

    public synchronized void resetToReady() {
        ++retried;
        state = State.READY;
        errorObject = null;
        lastUpdated = System.currentTimeMillis();
    }

    public String getActivity() {
        return activity;
    }

    public FileAttributes getAttributes() {
        return attributes;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public Long getId() {
        return id;
    }

    public String getKey() {
        return String.format(KEY_FORMAT, ruid, id);
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public PID getPid() {
        return pid;
    }

    public PnfsId getPnfsId() {
        return attributes == null ? null : attributes.getPnfsId();
    }

    public FsPath getPath() {
        return path == null ? FsPath.ROOT : path;
    }

    public int getRetried() {
        return retried;
    }

    public Long getRid() {
        return rid;
    }

    public String getRuid() {
        return ruid;
    }

    public Long getStartedAt() {
        return startedAt;
    }

    public State getState() {
        return state;
    }

    public Throwable getThrowable() {
        return errorObject;
    }

    public FileType getType() {
        return attributes == null ? null : attributes.getFileType();
    }

    public void setActivity(String activity) {
        this.activity = activity;
    }

    public void setAttributes(FileAttributes attributes) {
        this.attributes = attributes;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public void setErrorObject(Object error) {
        if (error != null) {
            if (error instanceof Throwable) {
                errorObject = (Throwable) error;
            } else {
                errorObject = new Throwable(String.valueOf(error));
            }
            setState(State.FAILED);
        }
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public void setPath(FsPath path) {
        this.path = path;
    }

    public void setPid(PID pid) {
        this.pid = pid;
    }

    public void setRid(Long rid) {
        this.rid = rid;
    }

    public void setRuid(String ruid) {
        this.ruid = ruid;
    }

    public void setRetried(int retried) {
        this.retried = retried;
    }

    public void setStartedAt(Long startedAt) {
        this.startedAt = startedAt;
    }

    public synchronized boolean setState(State state) {
        requireNonNull(state);

        if (this.state == state) {
            return false;
        }

        switch (this.state) {
            case CANCELLED:
            case COMPLETED:
            case FAILED:
            case SKIPPED:
                return false;
            case RUNNING:
                switch (state) {
                    case CANCELLED:
                    case COMPLETED:
                    case FAILED:
                    case SKIPPED:
                        this.state = state;
                        lastUpdated = System.currentTimeMillis();
                        return true;
                    default:
                        throw new IllegalStateException(
                              String.format(INVALID_STATE_TRANSITION,
                                    id,
                                    this.state,
                                    state));
                }
            case READY:
                switch (state) {
                    case CANCELLED:
                    case COMPLETED:
                    case FAILED:
                    case SKIPPED:
                    case RUNNING:
                        this.state = state;
                        lastUpdated = System.currentTimeMillis();
                        return true;
                    default:
                        throw new IllegalStateException(
                              String.format(INVALID_STATE_TRANSITION,
                                    id,
                                    this.state,
                                    state));
                }
            case CREATED:
            default:
                this.state = state;
                lastUpdated = System.currentTimeMillis();
                return true;
        }
    }

    @Override
    public String toString() {
        return String.format(TARGET_FORMAT, id, pid, ruid, activity,
              state, new Timestamp(createdAt), startedAt == null ? null : new Timestamp(startedAt),
              new Timestamp(lastUpdated), retried, getType(), getPnfsId(), path, errorObject);
    }
}
