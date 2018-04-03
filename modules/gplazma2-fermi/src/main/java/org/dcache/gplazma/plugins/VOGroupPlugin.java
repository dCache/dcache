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
package org.dcache.gplazma.plugins;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import java.security.Principal;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * <p>Mapping plugin which requires FQANPrincipal, and adds a GIDPrincioal
 *    and possibly a UserNamePrincipal.</p>
 *
 * <p>If there is no FQAN, the plugin fails.  Otherwise it will always
 *    add the mapped GIDPrincipal as primary.</p>
 *
 * <p>Adding of a UserNamePrincipal is optional; hence, the plugin
 *    should be run as optional if the setup depends on a UserName principal
 *    being added here or downstream.</p>
 *
 * <p>If a UserNamePrincipal already exists, the UserNamePrincipal
 *    matched by this plugin to an FQAN will be substituted for it.</p>
 */
public class VOGroupPlugin implements GPlazmaMappingPlugin {
    private static final String VO_GROUP_PATH_PROPERY = "vo-group-path";

    private final FileBackedVOGroupMap map;

    public VOGroupPlugin(Properties properties) {
        String path = properties.getProperty(VO_GROUP_PATH_PROPERY, null);
        checkArgument(path != null, VO_GROUP_PATH_PROPERY
                        + " argument must be specified");
        map = new FileBackedVOGroupMap(path);
    }

    @VisibleForTesting
    VOGroupPlugin(FileBackedVOGroupMap map) {
        this.map = map;
    }

    @Override
    public void map(Set<Principal> principals) throws AuthenticationException {
        FQANPrincipal fqan
                        =  principals.stream()
                                     .filter(FQANPrincipal.class::isInstance)
                                     .map(FQANPrincipal.class::cast)
                                     .filter(FQANPrincipal::isPrimaryGroup)
                                     .findFirst()
                                     .orElseThrow(() -> new AuthenticationException("No subjects found with an FQAN."));

        VOGroupEntry voGroupEntry = map.get(fqan.getName());

        principals.add(new GidPrincipal(voGroupEntry.getMappedGid(), true));

        String mappedUname = voGroupEntry.getMappedUname();

        if (Strings.isNullOrEmpty(mappedUname)) {
            return;
        }

        for (Iterator<Principal> i = principals.iterator(); i.hasNext();) {
            Principal principal = i.next();
            if (principal instanceof  UserNamePrincipal) {
                i.remove();
            }
        }

        principals.add(new UserNamePrincipal(mappedUname));
    }
}
