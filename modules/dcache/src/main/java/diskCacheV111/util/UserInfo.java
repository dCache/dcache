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
package diskCacheV111.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.NoSuchElementException;
import javax.security.auth.Subject;

import org.dcache.auth.FQAN;
import org.dcache.auth.Subjects;

/**
 * <p>Used in the representation of active transfer data, shared
 * between core dCache and webadmin modules.</p>
 */
public final class UserInfo implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserInfo.class);
    private String username;
    private Long   uid;
    private Long   gid;
    private FQAN   primaryFqan;

    public UserInfo() {
    }

    public UserInfo(Subject subject) {
        username = Subjects.getUserName(subject);

        /*
         *  The values may not be set by some protocols,
         *  such as anonymous dcap.  We do not want to fail here
         *  in that case.
         *
         *  In the case of an IllegalArgument, this is actually an
         *  error which should be reported, but again we do not
         *  want to fail the interface because of it.
         */

        if (Subjects.isNobody(subject)) {
            uid = null;
        } else {
            try {
                uid = Subjects.getUid(subject);
            } catch (IllegalArgumentException e) {
                uid = null;
                LOGGER.warn("Error when fetching UID from {}: {}.",
                            subject, e.toString());
            }
        }

        try {
            gid = Subjects.getPrimaryGid(subject);
        } catch (NoSuchElementException e) {
            gid = null;
        } catch (IllegalArgumentException e) {
            gid = null;
            LOGGER.warn("Error when fetching GID from {}: {}.",
                        subject, e.toString());
        }

        try {
            primaryFqan = Subjects.getPrimaryFqan(subject);
        } catch (IllegalArgumentException e) {
            primaryFqan = null;
            LOGGER.warn("Error when fetching primary FQAN from {}: {}.",
                        subject, e.toString());
        }
    }

    public String getGid() {
        return gid == null ? "" : gid.toString();
    }

    public FQAN getPrimaryFqan() {
        return primaryFqan;
    }

    public String getPrimaryVOMSGroup() {
        return primaryFqan == null ? "" : primaryFqan.getGroup();
    }

    public String getUid() {
        return uid == null ? "" : uid.toString();
    }

    public String getUsername() {
        return username == null ? "" : username;
    }

    public void setGid(Long gid) {
        this.gid = gid;
    }

    public void setPrimaryFqan(FQAN primaryFqan) {
        this.primaryFqan = primaryFqan;
    }

    public void setUid(Long uid) {
        this.uid = uid;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
