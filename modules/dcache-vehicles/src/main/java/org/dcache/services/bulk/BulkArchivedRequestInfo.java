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
package org.dcache.services.bulk;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 *  Representation of the information archived when a bulk request is deleted.
 */
public class BulkArchivedRequestInfo implements Serializable {

    private static final long serialVersionUID = 4552516949298874867L;

    private final List<String> initialTargets = new ArrayList<>();
    private final Map<String, Long> errors = new TreeMap<>();

    private String uid;
    private String owner;
    private String activity;
    private String depth;
    private Long arrivedAt;
    private Long startedAt;
    private Long lastModified;
    private String status;
    private String targetPrefix;
    private long succeeded;
    private long cancelled;
    private long failed;

    public BulkArchivedRequestInfo() {
    }

    public BulkArchivedRequestInfo(BulkRequest request) {
        BulkRequestStatusInfo status = request.getStatusInfo();
        uid = request.getUid();
        owner = status.getUser();
        activity = request.getActivity();
        depth = request.getExpandDirectories().name();
        arrivedAt = status.getCreatedAt();
        startedAt = status.getStartedAt();
        lastModified = status.getLastModified();
        this.status = status.getStatus().name();
        targetPrefix = request.getTargetPrefix();
        succeeded = 0L;
        cancelled = 0L;
        failed = 0L;
    }

    public void addTarget(BulkRequestTargetInfo info) {
       if (info.isInitial()) {
           initialTargets.add(info.getTarget());
       }

       switch (info.getState()) {
           case "CANCELLED":
               ++cancelled;
               break;
           case "COMPLETED":
           case "SKIPPED":
               ++succeeded;
               break;
           case "FAILED":
               ++failed;
               String type = info.getErrorType();
               long count = errors.getOrDefault(type, 0L);
               errors.put(type, ++count);
               break;
       }
    }

    public List<String> getInitialTargets() {
        return initialTargets;
    }

    public Map<String, Long> getErrors() {
        return errors;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getActivity() {
        return activity;
    }

    public void setActivity(String activity) {
        this.activity = activity;
    }

    public Long getArrivedAt() {
        return arrivedAt;
    }

    public void setArrivedAt(Long arrivedAt) {
        this.arrivedAt = arrivedAt;
    }

    public Long getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Long startedAt) {
        this.startedAt = startedAt;
    }

    public Long getLastModified() {
        return lastModified;
    }

    public void setLastModified(Long lastModified) {
        this.lastModified = lastModified;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getTargetPrefix() {
        return targetPrefix;
    }

    public void setTargetPrefix(String targetPrefix) {
        this.targetPrefix = targetPrefix;
    }

    public long getSucceeded() {
        return succeeded;
    }

    public void setSucceeded(long succeeded) {
        this.succeeded = succeeded;
    }

    public long getCancelled() {
        return cancelled;
    }

    public void setCancelled(long cancelled) {
        this.cancelled = cancelled;
    }

    public long getFailed() {
        return failed;
    }

    public void setFailed(long failed) {
        this.failed = failed;
    }

    public String getDepth() {
        return depth;
    }

    public void setDepth(String depth) {
        this.depth = depth;
    }
}
