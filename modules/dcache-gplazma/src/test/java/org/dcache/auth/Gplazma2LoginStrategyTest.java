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

import static org.dcache.auth.attributes.Activity.DOWNLOAD;
import static org.dcache.auth.attributes.Activity.UPLOAD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import diskCacheV111.util.FsPath;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Activity;
import org.dcache.auth.attributes.MultiTargetedRestriction;
import org.dcache.auth.attributes.MultiTargetedRestriction.Authorisation;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.GPlazma;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

public class Gplazma2LoginStrategyTest {

    class GplazmaLoginReplyBuilder {
        Set<Object> sessionAttributes = new HashSet<>();
        Set<Authorisation> authorisations = new HashSet<>();

        GplazmaLoginReplyBuilder withUserRoot(String path) {
            sessionAttributes.add(new RootDirectory(path));
            return this;
        }

        GplazmaLoginReplyBuilder withAuthorization(String path, Activity ... activity) {
            authorisations.add(new Authorisation(List.of(activity), FsPath.create(path)));
            return this;
        }

        org.dcache.gplazma.LoginReply build() {
            var reply = new org.dcache.gplazma.LoginReply();
            if (!authorisations.isEmpty()) {
                sessionAttributes.add(new MultiTargetedRestriction(authorisations));
            }
            reply.setSessionAttributes(sessionAttributes);
            reply.setSubject(subject);
            return reply;
        }

        void isReturned() throws Exception {
            loginReply = build();
            when(gplazma.login(any(Subject.class))).thenReturn(loginReply);
        }
    }

    Gplazma2LoginStrategy strategy;
    Subject subject;
    GplazmaLoginReplyBuilder builder;
    org.dcache.gplazma.LoginReply loginReply;
    Restriction restriction;
    GPlazma gplazma;

    @Before
    public void setup() throws Exception {
        strategy = new Gplazma2LoginStrategy();
        subject = Subjects.of(1234, 5678, new int[]{5678});
        gplazma = mock(GPlazma.class);
        strategy.setGplazma(gplazma);
    }

    @Test
    public void shouldAllowDownloadWithPrefixRestrictions() throws Exception {
        givenAGplazmaLoginReply().withUserRoot("/pnfs/fs/usr/dcache").isReturned();
        whenLoginIsCalledOnStrategy();
        assertThat(restriction, not(restricts(DOWNLOAD, "/pnfs/fs/usr/dcache")));
        assertThat(restriction, not(restricts(DOWNLOAD, "/pnfs/fs/usr/dcache/foo")));
        assertThat(restriction, restricts(DOWNLOAD, "/pnfs/fs/usr"));
    }

    @Test
    public void shouldAllowUploadWithPrefixRestrictions() throws Exception {
        givenAGplazmaLoginReply().withUserRoot("/pnfs/fs/usr/dcache").isReturned();
        whenLoginIsCalledOnStrategy();
        assertThat(restriction, not(restricts(UPLOAD, "/pnfs/fs/usr/dcache")));
        assertThat(restriction, not(restricts(UPLOAD, "/pnfs/fs/usr/dcache/foo")));
        assertThat(restriction, restricts(UPLOAD, "/pnfs/fs/usr"));
    }

    @Test
    public void shouldAllowUploadToUploadDirWithPrefixRestrictions() throws Exception {
        givenUploadPath("/upload");
        givenAGplazmaLoginReply().withUserRoot("/pnfs/fs/usr/dcache").isReturned();
        whenLoginIsCalledOnStrategy();
        assertThat(restriction, not(restricts(UPLOAD, "/upload")));
        assertThat(restriction, not(restricts(UPLOAD, "/pnfs/fs/usr/dcache")));
        assertThat(restriction, not(restricts(UPLOAD, "/pnfs/fs/usr/dcache/.upload")));
    }

    @Test
    public void shouldAllowUserSpecificUploadWithPrefixRestrictions() throws Exception {
        givenUploadPath(".upload");
        givenAGplazmaLoginReply().withUserRoot("/pnfs/fs/usr/dcache").isReturned();
        whenLoginIsCalledOnStrategy();
        assertThat(restriction, restricts(UPLOAD, "/upload"));
        assertThat(restriction, not(restricts(UPLOAD, "/pnfs/fs/usr/dcache")));
        assertThat(restriction, not(restricts(UPLOAD, "/pnfs/fs/usr/dcache/.upload")));
    }

    @Test
    public void shouldAllowDownloadWithMultiTargetedRestrictions() throws Exception {
        givenAGplazmaLoginReply().withUserRoot("/pnfs/fs/usr/dcache")
              .withAuthorization("/pnfs/fs/usr/dcache", DOWNLOAD)
              .withAuthorization("/pnfs/fs/usr/dcache/foo", UPLOAD).isReturned();
        whenLoginIsCalledOnStrategy();
        assertThat(restriction, not(restricts(DOWNLOAD, "/pnfs/fs/usr/dcache")));
        assertThat(restriction, not(restricts(DOWNLOAD, "/pnfs/fs/usr/dcache/foo")));
        assertThat(restriction, restricts(DOWNLOAD, "/pnfs/fs/usr"));
    }

    @Test
    public void shouldAllowUploadWithMultiTargetedRestrictions() throws Exception {
        givenAGplazmaLoginReply().withUserRoot("/pnfs/fs/usr/dcache")
              .withAuthorization("/pnfs/fs/usr/dcache", DOWNLOAD)
              .withAuthorization("/pnfs/fs/usr/dcache/foo", UPLOAD).isReturned();
        whenLoginIsCalledOnStrategy();
        assertThat(restriction, restricts(UPLOAD, "/pnfs/fs/usr/dcache"));
        assertThat(restriction, not(restricts(UPLOAD, "/pnfs/fs/usr/dcache/foo")));
    }

    @Test
    public void shouldAllowUploadToUploadDirWithMultiTargetedRestrictions() throws Exception {
        givenUploadPath("/upload");
        givenAGplazmaLoginReply().withUserRoot("/pnfs/fs/usr/dcache")
              .withAuthorization("/pnfs/fs/usr/dcache", DOWNLOAD)
              .withAuthorization("/pnfs/fs/usr/dcache/foo", UPLOAD).isReturned();
        whenLoginIsCalledOnStrategy();
        assertThat(restriction, not(restricts(UPLOAD, "/upload")));
        assertThat(restriction, restricts(UPLOAD, "/pnfs/fs/usr/dcache"));
        assertThat(restriction, restricts(UPLOAD, "/pnfs/fs/usr/dcache/.upload"));
    }

    @Test
    public void shouldAllowUserSpecificUploadWithMultiTargetedRestrictions() throws Exception {
        givenUploadPath(".upload");
        givenAGplazmaLoginReply().withUserRoot("/pnfs/fs/usr/dcache")
              .withAuthorization("/pnfs/fs/usr/dcache", DOWNLOAD)
              .withAuthorization("/pnfs/fs/usr/dcache/foo", UPLOAD).isReturned();
        whenLoginIsCalledOnStrategy();
        assertThat(restriction, restricts(UPLOAD, "/upload"));
        assertThat(restriction, restricts(UPLOAD, "/pnfs/fs/usr/dcache"));
        assertThat(restriction, not(restricts(UPLOAD, "/pnfs/fs/usr/dcache/.upload")));
    }

    private GplazmaLoginReplyBuilder givenAGplazmaLoginReply() {
        return new GplazmaLoginReplyBuilder();
    }

    private void givenUploadPath(String upload) {
        strategy.setUploadPath(upload);
    }

    private Matcher restricts(Activity activity, String path) {
        return new BaseMatcher<Restriction>() {
            @Override
            public boolean matches(Object o) {
                return restriction.isRestricted(activity, FsPath.create(path)) == true;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(
                      "Checks whether restriction restricts activity on a given path.");
            }
        };
    }

    private void whenLoginIsCalledOnStrategy() throws Exception {
        restriction = strategy.login(subject).getRestriction();
    }
}
