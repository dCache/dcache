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
package org.dcache.restful.providers.tape;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.dcache.services.bulk.BulkRequestInfo;

/**
 * <p> As per the WLCG TAPE API v1:
 *
 * <table>
 *     <tr>
 *         <td>id</td>
 *         <td>The id of the stage request.</td>
 *     </tr>
 *     <tr>
 *         <td>createdAt</td>
 *         <td>The time when the server received the request.</td>
 *     </tr>
 *     <tr>
 *         <td>startedAt</td>
 *         <td>the timestamp at which the first file belonging to the request has started;
 *             if not set, should be same as createdAt.</td>
 *     </tr>
 *     <tr>
 *         <td>completedAt</td>
 *         <td>Indicates when the last file was finished.</td>
 *     </tr>
 *     <tr>
 *         <td>files</td>
 *         <td>The files that got submitted and their state/disk residency.</td>
 *     </tr>
 * </table>
 */
public class StageRequestInfo implements Serializable {

    private static final long serialVersionUID = 1269517713600606880L;
    private String id;
    private Long createdAt;
    private Long startedAt;
    private Long completedAt;
    private List<StagedFileInfo> files;

    public StageRequestInfo() {}

    public StageRequestInfo(BulkRequestInfo info) {
        id = info.getUid();
        createdAt = info.getArrivedAt();
        startedAt = info.getStartedAt();

        if (startedAt == null) {
            startedAt = createdAt;
        }

        switch (info.getStatus()) {
            case CANCELLED:
            case COMPLETED:
                completedAt = info.getLastModified();
        }

        files = new ArrayList<>();
        info.getTargets().stream().map(StagedFileInfo::new).forEach(files::add);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    public Long getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Long startedAt) {
        this.startedAt = startedAt;
    }

    public Long getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Long completedAt) {
        this.completedAt = completedAt;
    }

    public List<StagedFileInfo> getFiles() {
        return files;
    }

    public void setFiles(List<StagedFileInfo> files) {
        this.files = files;
    }
}
