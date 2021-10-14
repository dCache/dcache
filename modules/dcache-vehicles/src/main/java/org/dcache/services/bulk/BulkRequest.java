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
import java.util.Map;

/**
 * Generic bulk request.  It is up to the request store to map the request to an appropriate id and
 * type.
 */
public class BulkRequest implements Serializable {

    private static final long serialVersionUID = 5314015926727327490L;

    public enum Depth {
        NONE, TARGETS, ALL
    }

    private String urlPrefix;
    private String id;
    private String target;
    private String targetPrefix;
    private String activity;
    private boolean clearOnSuccess;
    private boolean clearOnFailure;
    private boolean cancelOnFailure;
    private Integer delayClear;
    private Map<String, String> arguments;
    private Depth expandDirectories;

    public String getActivity() {
        return activity;
    }

    public Map<String, String> getArguments() {
        return arguments;
    }

    public Integer getDelayClear() {
        return delayClear;
    }

    public Depth getExpandDirectories() {
        return expandDirectories;
    }

    public String getId() {
        return id;
    }

    public String getTarget() {
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

    public void setDelayClear(Integer delayClear) {
        this.delayClear = delayClear;
    }

    public void setExpandDirectories(Depth expandDirectories) {
        this.expandDirectories = expandDirectories;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public void setTargetPrefix(String targetPrefix) {
        this.targetPrefix = targetPrefix;
    }

    public void setUrlPrefix(String urlPrefix) {
        this.urlPrefix = urlPrefix;
    }
}
