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
package org.dcache.chimera.nfsv41.door;

import org.dcache.nfs.util.UnixSubjects;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.security.auth.Subject;
import javax.security.auth.SubjectDomainCombiner;

import java.net.URISyntaxException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.FsInode_SURI;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.PermissionDeniedChimeraFsException;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.StorageLocatable;
import org.dcache.chimera.posix.Stat;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.doAnswer;

public class StorageUriTest {
    private static final String[] INVALID = { "boguspath",
                                              ":boguspath",
                                              "/boguspath",
                                              "foobar:/boguspath",
                                              "foobar://boguspath",
                                              "foobar://path with spaces",
                                              "urn:bogus" };

    private static final String[] VALID = {
                    "foobar://mrhost?somevariable=somevalue&someother=somethingelse" };

    private static final String NEWLINE = "\n";
    private static final String HSMLOC1 =
                    "foobar://mrhost?somethingbogus=1" + NEWLINE;
    private static final String HSMLOC2 =
                    "foobar://mrhost?somethingbogus=2" + NEWLINE;

    private FsInode                root;
    private FsInode                file;
    private FileSystemProvider     fs;
    private List<StorageLocatable> locations;
    private FsInode_SURI           inode_suri;
    private Stat                   stat;

    private static class RootInode extends FsInode
    {
        public RootInode(FileSystemProvider fs, long ino)
        {
            super(fs, ino);
        }

        @Override
        public boolean exists() throws ChimeraFsException
        {
            return true;
        }

        @Override
        public boolean isDirectory()
        {
            return true;
        }

        @Override
        public boolean isLink()
        {
            return false;
        }

        @Override
        public FsInode getParent()
        {
            return null;
        }
    }

    @Before
    public void setUp() throws Exception {
        fs = mock(JdbcFs.class);
        root = new RootInode(fs, 0L);
        file = new FsInode(fs, 1L);
        locations = new ArrayList<>();
        stat = new Stat();
        stat.setSize(0);
        stat.setMode(0);
        stat.setGeneration(0);
        newFsInodeSURI();
        mockFileSystem();
    }

    @After
    public void cleanUp() throws Exception {
        locations.clear();
    }

    @Test
    public void testInvalidURIs() {
        for (int i = 0; i < INVALID.length; ++i) {
            URISyntaxException exception = null;
            try {
                FsInode_SURI.validate(INVALID[i]);
            } catch (URISyntaxException e) {
                exception = e;
            }
            assertNotNull(exception);
        }
    }

    @Test
    public void testStorageUriAppendByRoot() {
        runTestAsRoot(() -> {
            try {
                write(HSMLOC1, true);

                Exception e = null;

                try {
                    write(HSMLOC2, true);
                } catch (PermissionDeniedChimeraFsException permissionDenied) {
                    e = permissionDenied;
                }

                assertNull(e);

                String appended = HSMLOC1 + HSMLOC2;

                byte[] buffer = new byte[128];
                int len = readFrom0(buffer, 128);
                assertThat(len, is(appended.length()));
                assertEquals(appended, new String(buffer, 0, len));

                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testStorageUriAppendDenied() {
        runTestAsNonRootUser(() -> {
            try {
                write(HSMLOC1, true);

                Exception e = null;

                try {
                    write(HSMLOC2, true);
                } catch (PermissionDeniedChimeraFsException permissionDenied) {
                    e = permissionDenied;
                }

                assertNotNull(e);
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testStorageUriOverwriteByRoot() {
        runTestAsRoot(() -> {
            try {
                write(HSMLOC1, false);

                Exception e = null;

                try {
                    write(HSMLOC2, false);
                } catch (PermissionDeniedChimeraFsException permissionDenied) {
                    e = permissionDenied;
                }

                assertNull(e);
                byte[] buffer = new byte[128];
                int len = readFrom0(buffer, 128);
                assertThat(len, is(HSMLOC2.length()));
                assertEquals(HSMLOC2, new String(buffer, 0, len));

                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testStorageUriOverwriteDenied() {
        runTestAsNonRootUser(() -> {
            try {
                write(HSMLOC1, false);

                Exception e = null;

                try {
                    write(HSMLOC2, false);
                } catch (Exception permissionDenied) {
                    e = permissionDenied;
                }

                assertNotNull(e);
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testStorageUriReadEmptyLocations() throws Exception {
        byte[] buffer = new byte[128];
        int len = readFrom0(buffer, 128);
        assertThat(len, is(0));
    }

    @Test
    public void testStorageUriReadMultipleLocations() throws Exception {
        /*
         * FsInode_SURI.write always trims
         */
        addStorageLocation(HSMLOC1.trim());
        addStorageLocation(HSMLOC2.trim());
        byte[] buffer = new byte[128];
        int len = readFrom0(buffer, 128);
        assertThat(len, is(HSMLOC1.length() + HSMLOC2.length()));
        assertEquals(HSMLOC1 + HSMLOC2, new String(buffer, 0, len));
    }

    @Test
    public void testStorageUriReadSingleLocation() throws Exception {
        /*
         * FsInode_SURI.write always trims
         */
        addStorageLocation(HSMLOC1.trim());
        byte[] buffer = new byte[128];
        int len = readFrom0(buffer, 128);
        assertThat(len, is(HSMLOC1.length()));
        assertEquals(HSMLOC1, new String(buffer, 0, len));
    }

    @Test
    public void testStorageUriWrite() throws Exception {
        runTestAsNonRootUser(() -> {
            try {
                write(HSMLOC1, false);
                byte[] buffer = new byte[128];
                int len = readFrom0(buffer, 128);
                assertThat(len, is(HSMLOC1.length()));
                assertEquals(HSMLOC1, new String(buffer, 0, len));
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testValidURIs() {
        for (int i = 0; i < VALID.length; ++i) {
            URISyntaxException exception = null;
            try {
                FsInode_SURI.validate(VALID[i]);
            } catch (URISyntaxException e) {
                exception = e;
            }
            assertNull(exception);
        }
    }

    private int readFrom0(byte[] buffer, int len) throws Exception {
        return inode_suri.read(0, buffer, 0, len);
    }

    private void runTestAsNonRootUser(PrivilegedAction<Void> action) {
        Subject subject = UnixSubjects.toSubject(500, 500, 0, 500);
        runTestWithSubject(subject, action);
    }

    private void runTestAsRoot(PrivilegedAction<Void> action) {
        Subject subject = UnixSubjects.toSubject(0, 0);
        runTestWithSubject(subject, action);
    }

    private void runTestWithSubject(Subject subject,
                                    PrivilegedAction<Void> action) {
        SubjectDomainCombiner domainCombiner = new SubjectDomainCombiner(
                        subject);
        AccessControlContext testContext
                        = new AccessControlContext(
                        AccessController.getContext(),
                        domainCombiner);
        AccessController.doPrivileged(action, testContext);
    }

    private void write(String location, boolean append) throws Exception {
        int len = location.length();
        byte[] buffer = location.getBytes();

        Stat stat = inode_suri.stat();
        /*
         * Emulate OPEN TRUNCATE for write.
         */
        if (!append) {
            if (ChimeraVfs.shouldRejectAttributeUpdates(inode_suri, fs)) {
                throw new PermissionDeniedChimeraFsException("setStat not allowed.");
            }
            stat.setSize(0);
            inode_suri.setStat(stat);
        }

        if (ChimeraVfs.shouldRejectUpdates(inode_suri, fs)) {
            throw new PermissionDeniedChimeraFsException("write not allowed.");
        }

        inode_suri.write(stat.getSize(), buffer, 0, len);
        addStorageLocation(location);
    }

    private void addStorageLocation(String location) {
        long now = System.currentTimeMillis();
        locations.add(new StorageGenericLocation(1, 1,
                                                 location.trim(),
                                                 now, now,
                                                 true));
    }

    private void mockFileSystem() throws ChimeraFsException {
        given(fs.getInodeLocations(inode_suri)).willReturn(locations);
        given(fs.getInodeLocations(inode_suri, StorageGenericLocation.TAPE))
                        .willReturn(locations);
        given(fs.stat(inode_suri, 0)).willReturn(stat);
        Answer<Void> clearAnswer = new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                locations.clear();
                return null;
            }
        };
        doAnswer(clearAnswer).when(fs).clearTapeLocations(inode_suri);
    }

    private FsInode_SURI newFsInodeSURI() throws ChimeraFsException {
        inode_suri = new FsInode_SURI(fs, file.ino());
        inode_suri.setStat(stat);
        return inode_suri;
    }
}
