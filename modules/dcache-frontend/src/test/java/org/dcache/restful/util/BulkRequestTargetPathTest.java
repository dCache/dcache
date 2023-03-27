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
package org.dcache.restful.util;

import static org.dcache.restful.util.HttpServletRequests.getTargetPrefixFromUserRoot;
import static org.junit.Assert.assertEquals;

import diskCacheV111.util.FsPath;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BulkRequestTargetPathTest {

    String targetPrefix;
    FsPath userRootPath;

    @Test
    public void shouldReturnRootPathWhenTargetPrefixIsNull() throws Exception {
        givenUserRootPath("/pnfs/fs/test-user");
        givenTargetPrefix(null);
        assertThatFullPrefixIs("/pnfs/fs/test-user");
    }

    @Test
    public void shouldReturnRootPathWhenTargetPrefixEqualsRoot() throws Exception {
        givenUserRootPath("/pnfs/fs/test-user");
        givenTargetPrefix("/pnfs/fs/test-user");
        assertThatFullPrefixIs("/pnfs/fs/test-user");
    }

    @Test
    public void shouldConcatenateNonIntersectingRootAndPrefix() throws Exception {
        givenUserRootPath("/pnfs/fs/test-user");
        givenTargetPrefix("/experiment1/run1");
        assertThatFullPrefixIs("/pnfs/fs/test-user/experiment1/run1");
    }

    /*
     *  User will not be given a path whose root is above the user root.
     */
    @Test
    public void shouldConcatenateRootAndPrefixWhenRootIsContainedInsidePrefix() throws Exception {
        givenUserRootPath("/pnfs/fs/test-user");
        givenTargetPrefix("/foo/pnfs/fs/test-user/experiment1");
        assertThatFullPrefixIs("/pnfs/fs/test-user/foo/pnfs/fs/test-user/experiment1");
    }

    @Test
    public void shouldJoinRootAndPathAtIntersectingName() throws Exception {
        givenUserRootPath("/pnfs/fs/test-user");
        givenTargetPrefix("/test-user/experiment1/run1");
        assertThatFullPrefixIs("/pnfs/fs/test-user/experiment1/run1");
    }

    @Test
    public void shouldJoinRootAndPathOverIntersectingNames() throws Exception {
        givenUserRootPath("/pnfs/fs/test-user");
        givenTargetPrefix("/fs/test-user/experiment1/run1");
        assertThatFullPrefixIs("/pnfs/fs/test-user/experiment1/run1");
    }

    @Test
    public void shouldReturnPrefixWhenItHasRootPathAsPrefix() throws Exception {
        givenUserRootPath("/pnfs/fs/test-user");
        givenTargetPrefix("/pnfs/fs/test-user/experiment1");
        assertThatFullPrefixIs("/pnfs/fs/test-user/experiment1");
    }

    @Test
    public void shouldReturnRootPathForRootUserWithNoPrefixExpressed() throws Exception {
        givenUserRootPath(FsPath.ROOT.toString());
        givenTargetPrefix(null);
        assertThatFullPrefixIs(FsPath.ROOT.toString());
    }

    @Test
    public void shouldReturnPrefixForRootUserWhenPrefixIsExpressed() throws Exception {
        givenUserRootPath(FsPath.ROOT.toString());
        givenTargetPrefix("/pnfs/fs");
        assertThatFullPrefixIs("/pnfs/fs");
    }

    /*
     *  Suppose a user submits a bulk-request with "/pnfs/fs" as the target-prefix, and paths
     *  containing "/test-user/...".  These are legitimate if the user's
     *  root is "/pnfs/fs/test-user".  So we take the user at its word as to the prefix.
     */
    @Test
    public void shouldReturnShorterPrefixWhenContainedByUserRoot() throws Exception {
        givenUserRootPath("/pnfs/fs/test-user");
        givenTargetPrefix("/pnfs/fs");
        assertThatFullPrefixIs("/pnfs/fs");
    }

    /*
     *  The following two tests may look like they open security holes, but they really don't.
     *  All we are doing is determining how the paths are constructed.  If the user
     *  shoots itself in the foot by providing a prefix for an area which it has
     *  no permissions to read or write from, all the target paths in its request
     *  will fail anyway.
     */
    @Test
    public void shouldReturnPrefixWhenOnlyPartiallyContainedByUserRoot1() throws Exception {
        givenUserRootPath("/pnfs/fs/test-user");
        givenTargetPrefix("/pnfs/foo");
        assertThatFullPrefixIs("/pnfs/foo");
    }

    @Test
    public void shouldReturnPrefixWhenOnlyPartiallyContainedByUserRoot2() throws Exception {
        givenUserRootPath("/pnfs/fs/test-user");
        givenTargetPrefix("/pnfs/fs/bar");
        assertThatFullPrefixIs("/pnfs/fs/bar");
    }

    private void givenUserRootPath(String root) {
        userRootPath = FsPath.create(root);
    }

    private void givenTargetPrefix(String targetPrefix) {
        this.targetPrefix = targetPrefix;
    }

    private void assertThatFullPrefixIs(String fullPrefix) {
        assertEquals(fullPrefix, getTargetPrefixFromUserRoot(userRootPath, targetPrefix));
    }
}
