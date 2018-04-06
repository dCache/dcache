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

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.security.Principal;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;

import static org.junit.Assert.*;

public class VOGroupPluginTest {
    private final static String TEST_FIXTURE = "org/dcache/gplazma/plugins/vo-group.json";

    File                 file;
    FileBackedVOGroupMap map;
    VOGroupPlugin        plugin;
    Set<Principal>          principals = new HashSet<>();
    AuthenticationException exception  = null;

    @Before
    public void setUp() throws Exception {
        URL url = ClassLoader.getSystemResource(TEST_FIXTURE);
        file = new File(url.toURI());
        file.setLastModified(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
        map = new FileBackedVOGroupMap(file.getAbsolutePath());
        plugin = new VOGroupPlugin(map);
    }

    @Test
    public void shouldAddGidPrincipalAndUserNameIfPresent() throws Exception {
        givenFQAN(anFqanWithUname());
        whenMapIsCalled();
        assertThatGidPrincipalWasAdded();
        assertThatUnamePrincipalWasAdded();
    }

    @Test
    public void shouldAddGidPrincipalIfNoUserNamePresent() throws Exception {
        givenFQAN(anFqanWithoutUname());
        whenMapIsCalled();
        assertThatGidPrincipalWasAdded();
    }

    @Test
    public void shouldFailIfNoFQAN() throws Exception {
        whenMapIsCalled();
        assertThatPluginFailed();
    }

    @Test
    public void shouldNotFailIfNoUserNamePresent() throws Exception {
        givenFQAN(anFqanWithoutUname());
        whenMapIsCalled();
        assertThatPluginSucceeded();
    }

    @Test
    public void shouldFindThePrimaryFQANWhenMultipleArePresent()
                    throws Exception {
        givenNonPrimaryFQAN(aSecondFqan());
        givenNonPrimaryFQAN(aThirdFqan());
        givenFQAN(anFqanWithUname());
        whenMapIsCalled();
        assertThatUnamePrincipalWasFromPrimary();
    }

    @Test
    public void shouldNotFailIfUserNamePresent() throws Exception {
        givenFQAN(anFqanWithUname());
        whenMapIsCalled();
        assertThatPluginSucceeded();
    }

    @Test
    public void shouldReloadAgainAfterTimestampUpdate() throws Exception {
        givenFQAN(anFqanWithUname());
        givenMapIsCalled();
        givenFileUpdated();
        whenMapIsCalledAgain();
        assertThatReloadWasCalledTwice();
    }

    @Test
    public void shouldReloadOnlyOnceWithoutTimestampChange() throws Exception {
        givenFQAN(anFqanWithUname());
        givenMapIsCalled();
        whenMapIsCalledAgain();
        assertThatReloadWasCalledOnce();
    }

    private String aSecondFqan() {
        return "/fermilab/annie/Role=None";
    }

    private String aThirdFqan() {
        return "/fermilab/Role=admin";
    }

    private String anFqanWithUname() {
        return "/fermilab/accelerator/Role=Production";
    }

    private String anFqanWithoutUname() {
        return "/fermilab/accelerator/Role=None";
    }

    private void assertThatGidPrincipalWasAdded() {
        Optional<GidPrincipal> gidPrincipal
                        = principals.stream()
                                    .filter(p -> p instanceof GidPrincipal)
                                    .map(p -> (GidPrincipal) p)
                                    .findFirst();
        assertTrue("Gid Principal is missing!",
                   gidPrincipal.isPresent());
    }

    private void assertThatPluginFailed() {
        assertNotNull("Mapping should have failed, but did not.",
                      exception);
    }

    private void assertThatPluginSucceeded() {
        assertNull("Mapping should not have failed, but did.",
                   exception);
    }

    private void assertThatReloadWasCalledOnce() {
        assertEquals("file reloaded wrong number of times",
                     1, map.getReloadCount());
    }

    private void assertThatReloadWasCalledTwice() {
        assertEquals("file reloaded wrong number of times",
                     2, map.getReloadCount());
    }

    private String assertThatUnamePrincipalWasAdded() {
        Optional<UserNamePrincipal> userNamePrincipal
                        = principals.stream()
                                    .filter(p -> p instanceof UserNamePrincipal)
                                    .map(p -> (UserNamePrincipal) p)
                                    .findFirst();
        assertTrue("UserName Principal is missing!",
                   userNamePrincipal.isPresent());
        return userNamePrincipal.get().getName();
    }

    private void assertThatUnamePrincipalWasFromPrimary() {
        assertEquals("Wrong user name!", "accelpro",
                     assertThatUnamePrincipalWasAdded());
    }

    private void givenFQAN(String fqan) {
        principals.add(new FQANPrincipal(fqan, true));
    }

    private void givenFileUpdated() {
        file.setLastModified(System.currentTimeMillis());
    }

    private void givenMapIsCalled() {
        whenMapIsCalled();
    }

    private void givenNonPrimaryFQAN(String fqan) {
        principals.add(new FQANPrincipal(fqan, false));
    }

    private void whenMapIsCalled() {
        try {
            plugin.map(principals);
        } catch (AuthenticationException e) {
            exception = e;
        }
    }

    private void whenMapIsCalledAgain() {
        principals.clear();
        principals.add(new FQANPrincipal(anFqanWithUname(), true));
        whenMapIsCalled();
    }
}
