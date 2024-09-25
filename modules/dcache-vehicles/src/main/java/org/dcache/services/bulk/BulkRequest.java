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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Generic bulk request.  It is up to the request store to map the request to an appropriate id and
 * type.
 */
public class BulkRequest implements Serializable {

    private static final long serialVersionUID = 5314015926727327490L;

    public enum Depth {
        NONE, TARGETS, ALL
    }

    private Long id;
    private String urlPrefix;
    private String uid;
    private List<String> target;
    private String targetPrefix;
    private String activity;
    private boolean clearOnSuccess;
    private boolean clearOnFailure;
    private boolean cancelOnFailure;
    private Map<String, String> arguments;
    private Depth expandDirectories;

    private boolean prestore;

    @JsonIgnore
    private BulkRequestStatusInfo statusInfo;

    public int hashCode() {
        return Objects.hashCode(uid);
    }

    public boolean equals(Object other) {
        if (!(other instanceof BulkRequest)) {
            return false;
        }

        BulkRequest otherRequest = (BulkRequest) other;

        if (uid == null || otherRequest.uid == null) {
            return false;
        }

        return uid.equals(otherRequest.uid);
    }

    public String getActivity() {
        return activity;
    }

    public Map<String, String> getArguments() {
        return arguments;
    }

    public Depth getExpandDirectories() {
        return expandDirectories;
    }

    public String getUid() {
        return uid;
    }

    public Long getId() {
        return id;
    }

    public List<String> getTarget() {
        return target;
    }

    public String getTargetPrefix() {
        return targetPrefix;
    }

    public String getUrlPrefix() {
        return urlPrefix;
    }

    public boolean isCancelOnFailure() {
        return cancelOnFailure;
    }

    public boolean isClearOnFailure() {
        return clearOnFailure;
    }

    public boolean isClearOnSuccess() {
        return clearOnSuccess;
    }

    public boolean isPrestore() { return prestore; }

    public void setActivity(String activity) {
        this.activity = activity;
    }

    public void setArguments(Map<String, String> arguments) {
        this.arguments = arguments;
    }

    public void setCancelOnFailure(boolean cancelOnFailure) {
        this.cancelOnFailure = cancelOnFailure;
    }

    public void setClearOnFailure(boolean clearOnFailure) {
        this.clearOnFailure = clearOnFailure;
    }

    public void setClearOnSuccess(boolean clearOnSuccess) {
        this.clearOnSuccess = clearOnSuccess;
    }

    public void setExpandDirectories(Depth expandDirectories) {
        this.expandDirectories = expandDirectories;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public void setPrestore(boolean prestore) {
        this.prestore = prestore;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setTarget(List<String> target) {
        this.target = target;
    }

    public void setTargetPrefix(String targetPrefix) {
        this.targetPrefix = targetPrefix;
    }

    public void setUrlPrefix(String urlPrefix) {
        this.urlPrefix = urlPrefix;
    }

    public BulkRequestStatusInfo getStatusInfo() {
        return statusInfo;
    }

    public void setStatusInfo(BulkRequestStatusInfo statusInfo) {
        this.statusInfo = statusInfo;
    }
}
