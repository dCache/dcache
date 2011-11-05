package org.dcache.tests.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import gplazma.authz.AuthorizationException;
import gplazma.authz.plugins.vorolemap.VORoleMapAuthzPlugin;
import gplazma.authz.records.gPlazmaAuthorizationRecord;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class VoRoleTest {

    private VORoleMapAuthzPlugin _voAuth;
    private static final String MY_DN = "/O=GermanGrid/OU=DESY/CN=Tigran Mkrtchyan";
    private static final String FLAVIA = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=flavia/CN=388195/CN=Flavia Donno";
    private static final String ROLE1 = "/dteam/Role=NULL/Capability=NULL";
    private static final String BADROLE = "/nonexist/Role=NULL/Capability=NULL";

    private static final String VOROLEMAP = "org/dcache/tests/auth/vorolemap";
    private static final String AUTHZDB = "org/dcache/tests/auth/storage-authzdb";

    private File dir;
    private File vorolemapFile;
    private File authzdbFile;

    @Before
    public void setUp() throws Exception
    {
        /* The code to test works on files and isn't easily converted
         * to work on an InputStream. Thus we copy the test fixture to
         * temporary files.
         */
        dir = Files.createTempDir();
        vorolemapFile = new File(dir, "vorolemap");
        authzdbFile = new File(dir, "storage-authzdb");
        Files.write(Resources.toByteArray(Resources.getResource(VOROLEMAP)),
                    vorolemapFile);
        Files.write(Resources.toByteArray(Resources.getResource(AUTHZDB)),
                    authzdbFile);

        _voAuth = new VORoleMapAuthzPlugin(vorolemapFile.getPath(),
                                           authzdbFile.getPath(), 1);
    }

    @After
    public void tearDown()
    {
        authzdbFile.delete();
        vorolemapFile.delete();
        dir.delete();
    }

    @Test
    public void testValidDnValidRole() throws Exception {
        gPlazmaAuthorizationRecord pwdRecord = _voAuth.authorize(MY_DN, ROLE1, null, null, null, null);
        assertNotNull("can't find user record", pwdRecord);
    }

    @Test(expected=AuthorizationException.class)
    public void testVoRoleWithNullRole() throws AuthorizationException {
        _voAuth.authorize(MY_DN, null, null, null, null, null);
    }

    @Test(expected=AuthorizationException.class)
    public void testValidDnInvalidRole() throws Exception {
        _voAuth.authorize(MY_DN, BADROLE, null, null, null, null);
    }

    @Test
    public void testRegexp() throws Exception {
        gPlazmaAuthorizationRecord pwdRecord = _voAuth.authorize(FLAVIA, ROLE1, null, null, null, null);
        assertNotNull("can't find user record", pwdRecord);
        assertEquals("Incorrect user record received", 1001, pwdRecord.getUID());
    }


}
