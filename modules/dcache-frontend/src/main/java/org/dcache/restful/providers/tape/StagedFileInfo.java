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

import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;
import org.dcache.services.bulk.BulkRequestTargetInfo;

/**
 * <p> As per the WLCG TAPE API v1:
 *
 * <table>
 *     <tr>
 *         <td>finishedAt</td>
 *         <td>The time at which the file request transitioned to one of the terminal states.
 *             unsigned integer</td>
 *         <td>if the field state is not set, this field MUST NOT be returned.</td>
 *     </tr>
 *     <tr>
 *         <td>startedAt</td>
 *         <td>The timestamp at which the file request transitioned to STARTED.</td>
 *         <td>if the field state is not set, this field MUST NOT be returned.</td>
 *     </tr>
 *     <tr>
 *         <td>error</td>
 *         <td>The reason why a file could not be staged.</td>
 *         <td>If no error occured for the file, this field MUST NOT be returned.</td>
 *     </tr>
 *     <tr>
 *         <td>onDisk</td>
 *         <td>Is true if the file is on disk, false otherwise.</td>
 *         <td>If this field is set, the state field MUST NOT be returned.</td>
 *     </tr>
 *     <tr>
 *         <td>state</td>
 *         <td>The state of processing this file.</td>
 *         <td>If this field is set, the onDisk field MUST NOT be returned.</td>
 *     </tr>
 * </table>
 *
 * <p> From the above, it will be dCache policy always to return state and not onDisk unless
 *     the former is not set.
 *
 *  The JsonInclude(JsonInclude.Include.NON_NULL) is added to skip null fields that WLCG
 *  REST API expects
 */

@JsonInclude(JsonInclude.Include.NON_NULL)
public class StagedFileInfo implements Serializable {

    private static final long serialVersionUID = 7530936044659226339L;

    enum StagedState {
        SUBMITTED("Stage processing has not started yet."),
        STARTED("Stage processing of this file has started."),
        CANCELLED("Stage has been cancelled by the client. "
              + "The server provides no guarantee of disk-like latency."),
        FAILED("Stage processing is finished. "
              + "The server was unable to provide file on disk."),
        COMPLETED("Stage processing is finished, "
              + "file is provided on disk with expected protection.");

        private String description;

        StagedState(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }

        static StagedState fromState(String value) {
            switch (value.toUpperCase()) {
                case "CREATED":
                case "READY":
                    return SUBMITTED;
                case "RUNNING":
                    return STARTED;
                case "CANCELLED":
                    return CANCELLED;
                case "FAILED":
                case "SKIPPED":
                    return FAILED;
                case "COMPLETED":
                    return COMPLETED;
                default:
                    throw new RuntimeException("Unrecognized state " + value);
            }
        }
    }

    private String path;
    private Long finishedAt;
    private Long startedAt;
    private String error;
    private StagedState state;

    public StagedFileInfo() {}

    public StagedFileInfo(BulkRequestTargetInfo info) {
        path = info.getTarget();
        finishedAt = info.getFinishedAt();
        startedAt = info.getStartedAt();
        if (startedAt == null && finishedAt != null) {
            startedAt = finishedAt;
        }
        error = info.getErrorMessage();
        state = StagedState.fromState(info.getState());
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Long getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(Long finishedAt) {
        this.finishedAt = finishedAt;
    }

    public Long getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Long startedAt) {
        this.startedAt = startedAt;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public StagedState getState() {
        return state;
    }

    public void setState(StagedState state) {
        this.state = state;
    }
}
