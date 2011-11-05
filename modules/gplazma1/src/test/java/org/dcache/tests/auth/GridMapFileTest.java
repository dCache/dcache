package org.dcache.tests.auth;

import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import org.dcache.auth.UserAuthBase;
import gplazma.authz.plugins.gridmapfile.GridMapFileAuthzPlugin;
import gplazma.authz.records.gPlazmaAuthorizationRecord;

import com.google.common.io.Files;
import com.google.common.io.Resources;

public class GridMapFileTest {

    private static final String MY_DN = "/O=GermanGrid/OU=DESY/CN=Tigran Mkrtchyan";
    private static final String ROLE1 = "/dteam/Role=NULL/Capability=NULL";

    private static final String GRIDMAP = "org/dcache/tests/auth/grid-mapfile";
    private static final String AUTHZDB = "org/dcache/tests/auth/grid-mapfile-storage-authzdb";
    private File dir;
    private File gridmapFile;
    private File authzdbFile;

    @Before
    public void setUp() throws Exception
    {
        /* The code to test works on files and isn't easily converted
         * to work on an InputStream. Thus we copy the test fixture to
         * temporary files.
         */
        dir = Files.createTempDir();
        gridmapFile = new File(dir, "grid-mapfile");
        authzdbFile = new File(dir, "storage-authzdb");
        Files.write(Resources.toByteArray(Resources.getResource(GRIDMAP)),
                    gridmapFile);
        Files.write(Resources.toByteArray(Resources.getResource(AUTHZDB)),
                    authzdbFile);
    }

    @After
    public void tearDown()
    {
        gridmapFile.delete();
        authzdbFile.delete();
        dir.delete();
    }

    @Test
    public void testVoRole() throws Exception
    {
        GridMapFileAuthzPlugin voAuth =
            new GridMapFileAuthzPlugin(gridmapFile.getPath(),
                                       authzdbFile.getPath(), 1);
        gPlazmaAuthorizationRecord gauthrec =
            voAuth.authorize(MY_DN, ROLE1, null, null, null, null);
        assertNotNull("can't find user record", gauthrec);
    }

}
