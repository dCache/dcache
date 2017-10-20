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
package org.dcache.chimera;

import org.junit.Before;
import org.junit.Test;

import javax.security.auth.Subject;
import javax.security.auth.SubjectDomainCombiner;
import java.net.URISyntaxException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.dcache.auth.Subjects;
import org.dcache.chimera.posix.Stat;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class StorageUriTest extends ChimeraTestCaseHelper {
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

    private FsInode file;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        file = _rootInode.create("custodial_data_file", 0, 0, 0644);
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
        FsInode_SURI inode_suri = new FsInode_SURI(_fs, file.ino());
        byte[] buffer = new byte[128];
        int len = readFrom0(buffer, 128);
        assertThat(len, is(0));
    }

    @Test
    public void testStorageUriReadMultipleLocations() throws Exception {
        FsInode_SURI inode_suri = new FsInode_SURI(_fs, file.ino());
        /*
         * FsInode_SURI.write always trims
         */
        _fs.addInodeLocation(inode_suri, 0, HSMLOC1.trim());
        _fs.addInodeLocation(inode_suri, 0, HSMLOC2.trim());
        byte[] buffer = new byte[128];
        int len = readFrom0(buffer, 128);
        assertThat(len, is(HSMLOC1.length() + HSMLOC2.length()));
        assertEquals(HSMLOC1 + HSMLOC2, new String(buffer, 0, len));
    }

    @Test
    public void testStorageUriReadSingleLocation() throws Exception {
        FsInode_SURI inode_suri = new FsInode_SURI(_fs, file.ino());
        /*
         * FsInode_SURI.write always trims
         */
        _fs.addInodeLocation(inode_suri, 0, HSMLOC1.trim());
        byte[] buffer = new byte[128];
        int len = readFrom0(buffer, 128);
        assertThat(len, is(HSMLOC1.length()));
        assertEquals(HSMLOC1, new String(buffer, 0, len));
    }

    @Test
    public void testStorageUriWrite() throws Exception {
        write(HSMLOC1, false);
        byte[] buffer = new byte[128];
        int len = readFrom0(buffer, 128);
        assertThat(len, is(HSMLOC1.length()));
        assertEquals(HSMLOC1, new String(buffer, 0, len));
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
        FsInode_SURI inode_suri = new FsInode_SURI(_fs, file.ino());
        return inode_suri.read(0, buffer, 0, len);
    }

    private void runTestAsNonRootUser(PrivilegedAction<Void> action) {
        Subject subject = Subjects.of(500, 500, new int[] { 0, 500 });
        runTestWithSubject(subject, action);
    }

    private void runTestAsRoot(PrivilegedAction<Void> action) {
        Subject subject = Subjects.of(0, 0, new int[] { 0, 0 });
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
        FsInode_SURI inode_suri = new FsInode_SURI(_fs, file.ino());
        int len = location.length();
        byte[] buffer = location.getBytes();
        Stat stat = inode_suri.stat();
        /*
         * Emulate OPEN TRUNCATE for write.
         */
        if (!append) {
            stat.setSize(0);
            inode_suri.setStat(stat);
        }

        inode_suri.write(stat.getSize(), buffer, 0, len);
    }
}
