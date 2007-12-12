package org.dcache.tests.auth;

import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

import diskCacheV111.services.authorization.GPLAZMALiteVORoleAuthzPlugin;
import diskCacheV111.util.UserAuthBase;

public class VoRoleTest {

    private GPLAZMALiteVORoleAuthzPlugin _voAuth;
    private static final String MY_DN = "/O=GermanGrid/OU=DESY/CN=Tigran Mkrtchyan";
    private static final String ROLE1 = "/dteam/Role=NULL/Capability=NULL";
    private static final String ROLE2 = "/dteam/Role=NULL/Capability=NULL";

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

}
