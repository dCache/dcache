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
package org.dcache.qos;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 *  Represents the QoS state of the file.   This includes the length of time that this
 *  state should last, and the set of media specifications for the file (disk, tape, how many
 *  copies, etc.).
 */
public class QoSState implements Serializable {

    /**
     *   Amount of time until the transition to the next state.
     *   </p>
     *   The value is one of <code>null</code>, "INF", or an ISO 8601 formatted string
     *   (https://en.wikipedia.org/wiki/ISO_8601).
     *   </p>
     *   There is a difference between <code>null</code>> and INF:  the latter means
     *   this file's state (presumably the last) should be continuously checked;
     *   the former means that once the file has gone to this state, it
     *   no longer need be verified.
     */
    private String duration;

    /**
     *   The set of media involved in this state.  For instance, a file may have two
     *   specifications, one tape and one disk, with the latter specifying 2 copies,
     *   and so forth.
     */
    private List<QoSStorageMediumSpecification> media;

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public List<QoSStorageMediumSpecification> getMedia() {
        return media;
    }

    public void setMedia(List<QoSStorageMediumSpecification> media) {
        this.media = media;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof QoSState)) {
            return false;
        }

        QoSState other = (QoSState) obj;

        if ((duration == null && other.duration != null) ||
              duration != null && !duration.equals(other.duration)) {
            return false;
        }

        return (media == null && other.media == null) ||
              media != null && media.equals(other.media);
    }

    public int hashCode() {
        return Objects.hash(duration, media);
    }
}
