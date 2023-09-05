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
package org.dcache.auth;

import com.google.common.base.Splitter;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

/**
 *  A Principal which assigns a role-based authorization with respect to a uid.
 *  While this code replicates the UidPrincipal, it needs to be independent so
 *  that its presence does not violate the requirement of only one Uid principal
 *  per user.
 */
@AuthenticationOutput
@AuthenticationInput
public class RolePrincipal extends AbstractIdPrincipal {

    private static final long serialVersionUID = -208608738074565124L;

    private static final long PLACEHOLDER_ID = Long.MAX_VALUE;

    private final long internalId;

    public enum Role {
        ADMIN("admin"),
        QOS_USER("qos-user"),
        QOS_GROUP("qos-group");

        private final String tag;

        Role(String tag) {
            this.tag = tag;
        }

        public String getTag() {
            return tag;
        }

        static Role fromTag(String tag) {
            switch (tag.toUpperCase(Locale.ROOT)) {
                case "ADMIN":
                    return ADMIN;
                case "QOS-USER":
                    return QOS_USER;
                case "QOS-GROUP":
                    return QOS_GROUP;
            }

            throw new IllegalArgumentException("Unrecognized role: " + tag);
        }
    }

    private final Set<Role> roles = new HashSet<>();

    public RolePrincipal(String roles) {
        super(PLACEHOLDER_ID);
        internalId = UUID.randomUUID().getLeastSignificantBits();
        List<String> parts = Splitter.on(',').splitToList(roles);
        for (String role: parts) {
            this.roles.add(Role.fromTag(role));
        }
    }

    public long getId() {
        return internalId;
    }

    public Set<Role> getRoles() {
        return Set.copyOf(roles);
    }

    public boolean hasRole(Role role) {
        return roles.contains(role);
    }
}
