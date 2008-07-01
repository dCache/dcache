package org.dcache.tests.auth;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import diskCacheV111.services.authorization.AuthorizationServiceException;
import diskCacheV111.services.authorization.GPLAZMALiteVORoleAuthzPlugin;
import org.dcache.auth.UserAuthBase;

public class VoRoleTest {

    private GPLAZMALiteVORoleAuthzPlugin _voAuth;
    private static final String MY_DN = "/O=GermanGrid/OU=DESY/CN=Tigran Mkrtchyan";
    private static final String FLAVIA = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=flavia/CN=388195/CN=Flavia Donno";
    private static final String ROLE1 = "/dteam/Role=NULL/Capability=NULL";
    private static final String BADROLE = "/nonexist/Role=NULL/Capability=NULL";

    @Before
    public void setUp() throws Exception {
        _voAuth = new GPLAZMALiteVORoleAuthzPlugin("modules/dCacheJUnit/org/dcache/tests/auth/vorolemap",
                "modules/dCacheJUnit/org/dcache/tests/auth/storage-authzdb", 1);
    }

    @Test
    public void testVoRole() throws Exception {

        UserAuthBase pwdRecord = _voAuth.authorize(MY_DN, ROLE1, null, null, null);

        assertNotNull("can't find user record", pwdRecord);

    }


    @Test
    public void testRegexp() throws Exception {
        UserAuthBase pwdRecord = _voAuth.authorize(FLAVIA, ROLE1, null, null, null);
        assertNotNull("can't find user record", pwdRecord);
        assertEquals("Incorrect user record received", 1001, pwdRecord.UID);

    }

    @Test
    public void testNonExisting() throws Exception {
        try {
            UserAuthBase pwdRecord = _voAuth.authorize(MY_DN, BADROLE, null, null, null);
            fail("Record for non existing role returned");
        }catch(AuthorizationServiceException ae) {
            // OK
        }
    }

}
