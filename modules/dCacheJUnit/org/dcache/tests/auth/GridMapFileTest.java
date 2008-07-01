package org.dcache.tests.auth;

import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

import diskCacheV111.services.authorization.AuthorizationServicePlugin;
import diskCacheV111.services.authorization.GridMapFileAuthzPlugin;
import org.dcache.auth.UserAuthBase;




public class GridMapFileTest {

    private static final String MY_DN = "/O=GermanGrid/OU=DESY/CN=Tigran Mkrtchyan";
    private static final String ROLE1 = "/dteam/Role=NULL/Capability=NULL";
    private AuthorizationServicePlugin _voAuth;

    @Before
    public void setUp() throws Exception {
        _voAuth = new GridMapFileAuthzPlugin ("modules/dCacheJUnit/org/dcache/tests/auth/grid-mapfile",
                "modules/dCacheJUnit/org/dcache/tests/auth/grid-mapfile-storage-authzdb", 1);
    }


    @Test
    public void testVoRole() throws Exception {

        UserAuthBase pwdRecord = _voAuth.authorize(MY_DN, ROLE1, null, null, null);

        assertNotNull("can't find user record", pwdRecord);

    }

}
