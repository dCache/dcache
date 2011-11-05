package org.dcache.tests.auth;

import java.io.IOException;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

import gplazma.authz.plugins.dynamic.UIDMapFileHandler;
import gplazma.authz.plugins.dynamic.GIDMapFileHandler;

import java.io.File;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class UidGitMapTest {

    private final static String  MY_DN = "/O=GermanGrid/OU=DESY/CN=Tigran Mkrtchyan";
    private final static String MY_GROUP1 = "/dteam/Role=NULL/Capability=NULL";
    private final static String MY_GROUP2 = "/dteam/Role=production/Capability=NULL";

    private final static String UIDMAP = "org/dcache/tests/auth/uid-map";
    private final static String GIDMAP = "org/dcache/tests/auth/gid-map";

    private File dir;
    private File uidmapFile;
    private File gidmapFile;

    private UIDMapFileHandler uidMap;
    private GIDMapFileHandler gidMap;

    @Before
    public void setUp() throws IOException
    {
        /* The code to test works on files and isn't easily converted
         * to work on an InputStream. Thus we copy the test fixture to
         * temporary files.
         */
        dir = Files.createTempDir();
        uidmapFile = new File(dir, "uid-map");
        gidmapFile = new File(dir, "gid-map");
        Files.write(Resources.toByteArray(Resources.getResource(UIDMAP)),
                    uidmapFile);
        Files.write(Resources.toByteArray(Resources.getResource(GIDMAP)),
                    gidmapFile);

        uidMap = new UIDMapFileHandler(uidmapFile.getPath());
        gidMap = new GIDMapFileHandler(gidmapFile.getPath());
    }

    @After
    public void tearDown()
    {
        gidmapFile.delete();
        uidmapFile.delete();
        dir.delete();
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
