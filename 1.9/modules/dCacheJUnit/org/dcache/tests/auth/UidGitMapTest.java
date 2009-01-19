package org.dcache.tests.auth;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import gplazma.authz.plugins.dynamic.UIDMapFileHandler;
import gplazma.authz.plugins.dynamic.GIDMapFileHandler;


public class UidGitMapTest {

    private final static String  MY_DN = "/O=GermanGrid/OU=DESY/CN=Tigran Mkrtchyan";
    private final static String MY_GROUP1 = "/dteam/Role=NULL/Capability=NULL";
    private final static String MY_GROUP2 = "/dteam/Role=production/Capability=NULL";

    private UIDMapFileHandler uidMap;
    private GIDMapFileHandler gidMap;

    @Before
    public void setUp() throws IOException {
        uidMap = new UIDMapFileHandler("modules/dCacheJUnit/org/dcache/tests/auth/uid-map");
        gidMap = new GIDMapFileHandler("modules/dCacheJUnit/org/dcache/tests/auth/gid-map");
    }



    @Test
    public void testGetExistingUser() throws Exception {

        String id = uidMap.getMappedUID(MY_DN);
        assertEquals("Enxpected UID", 3750 ,  Integer.parseInt(id));

    }

    @Test
    public void testGetNonExistingUser() throws Exception {

        String id = uidMap.getMappedUID("do not exist");
        assertNull("uid for non existing user returned", id);

    }

    @Test
    public void testGetExistingGroup() throws Exception {

        String id = gidMap.getMappedGID(MY_GROUP1);
        assertEquals("Enxpected GID", 17 ,  Integer.parseInt(id));

    }


    @Test
    public void testGetNonExistingGroup() throws Exception {

        String id = gidMap.getMappedGID("do not exist");
        assertNull("gid for non existing user returned", id);

    }

}
