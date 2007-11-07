package org.dcache.tests.namespace;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import dmg.cells.nucleus.SystemCell;

import org.dcache.chimera.FsInode;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.XMLconfig;
import org.dcache.chimera.posix.Stat;

import diskCacheV111.namespace.PnfsManagerV3;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PnfsCreateDirectoryMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PnfsGetFileMetaDataMessage;
import diskCacheV111.vehicles.PnfsRenameMessage;

public class PnfsManagerTest {

    /*
     * make Cells happy
     */
    private final static SystemCell _systemCell = new SystemCell("JUnitTestDomain");

    private static PnfsManagerV3 _pnfsManager;
    private static Connection _conn;
    private static JdbcFs _fs;


    @BeforeClass
    public static void setUp() throws Exception {
        /*
         * init Chimera DB
         */

        Class.forName("org.hsqldb.jdbcDriver");

        _conn = DriverManager.getConnection("jdbc:hsqldb:mem:chimeramem", "sa", "");

        File sqlFile = new File("/home/tigran/work/cvs-work/eProject/Chimera/sql/create-hsqldb.sql");
        StringBuilder sql = new StringBuilder();

        BufferedReader dataStr = new BufferedReader(new FileReader(sqlFile));
        String inLine = null;

        while ((inLine = dataStr.readLine()) != null) {
            sql.append(inLine);
        }

        Statement st = _conn.createStatement();

        st.executeUpdate(sql.toString());

        tryToClose(st);


        String args = "org.dcache.chimera.namespace.ChimeraOsmStorageInfoExtractor " +
                "-threads=1 " +
                "-namespace-provider=org.dcache.chimera.namespace.ChimeraNameSpaceProviderFactory " +
                "-storageinfo-provider=org.dcache.chimera.namespace.ChimeraNameSpaceProviderFactory " +
                "-cachelocation-provider=org.dcache.chimera.namespace.ChimeraNameSpaceProviderFactory " +
                "-chimeraConfig=/home/tigran/work/cvs-work/eProject/Chimera/test-config.xml";

        _pnfsManager =  new PnfsManagerV3("testPnfsManager", args);


        _fs = new JdbcFs(new XMLconfig(new File("/home/tigran/work/cvs-work/eProject/Chimera/test-config.xml")));
        _fs.mkdir("/pnfs");
        FsInode baseInode = _fs.mkdir("/pnfs/testRoot");
        byte[] sGroupTagData = "chimera".getBytes();
        byte[] osmTemplateTagData = "StoreName sql".getBytes();

        _fs.createTag(baseInode, "sGroup");
        _fs.createTag(baseInode, "OSMTemplate");

        _fs.setTag(baseInode, "sGroup",sGroupTagData, 0, sGroupTagData.length);
        _fs.setTag(baseInode, "OSMTemplate",osmTemplateTagData, 0, osmTemplateTagData.length);

    }

    @Test
    public void testGetStorageInfo() {

        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage("/pnfs/testRoot/testGetStorageInfo");

        _pnfsManager.createEntry(pnfsCreateEntryMessage);

        assertTrue("failed to create an entry", pnfsCreateEntryMessage.getReturnCode() == 0 );
        assertNotNull("failed to get StrageInfo", pnfsCreateEntryMessage.getStorageInfo());

    }


    @Test
    public void testMkdirPermTest() throws Exception {

        PnfsCreateDirectoryMessage pnfsCreateDirectoryMessage = new PnfsCreateDirectoryMessage("/pnfs/testRoot/testMkdirPermTest", 3750, 1000, 0750);

        _pnfsManager.createDirectory(pnfsCreateDirectoryMessage);

        assertTrue("failed to create a directory", pnfsCreateDirectoryMessage.getReturnCode() == 0 );

        /*
         * while we have a backdoor, let check what really happened
         */

        Stat stat = _fs.stat(new FsInode(_fs, pnfsCreateDirectoryMessage.getPnfsId().toString()));
        assertEquals("new mode do not equal to specified one", (stat.getMode() & 0777) , 0750 );
    }

    /**
     * add cache location
     * get cache location
     * remove cache location with flag to remove if last
     */
    @Test
    public void testRemoveIfLast() {

        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage("/pnfs/testRoot/testRemoveIfLast");

        _pnfsManager.createEntry(pnfsCreateEntryMessage);

        assertTrue("failed to create an entry", pnfsCreateEntryMessage.getReturnCode() == 0 );


        PnfsAddCacheLocationMessage pnfsAddCacheLocationMessage = new PnfsAddCacheLocationMessage(pnfsCreateEntryMessage.getPnfsId(), "aPool");

        _pnfsManager.addCacheLocation(pnfsAddCacheLocationMessage);
        assertTrue("failed to add cache location", pnfsAddCacheLocationMessage.getReturnCode() == 0 );

        PnfsGetCacheLocationsMessage pnfsGetCacheLocationsMessage = new PnfsGetCacheLocationsMessage(pnfsCreateEntryMessage.getPnfsId());

        _pnfsManager.getCacheLocations(pnfsGetCacheLocationsMessage);
        assertTrue("failed to get cache location", pnfsGetCacheLocationsMessage.getReturnCode() == 0 );

        List<String> locations =  pnfsGetCacheLocationsMessage.getCacheLocations();

        assertFalse("location is empty", locations.isEmpty() );
        assertFalse("location contains more than one entry", locations.size() > 1 );
        assertEquals("location do not match", "aPool", locations.get(0));


        PnfsClearCacheLocationMessage pnfsClearcacheLocationMessage = new PnfsClearCacheLocationMessage(pnfsCreateEntryMessage.getPnfsId(), "aPool", true);
        _pnfsManager.clearCacheLocation(pnfsClearcacheLocationMessage);

        assertTrue("failed to clear cache location", pnfsClearcacheLocationMessage.getReturnCode() == 0 );

        PnfsGetFileMetaDataMessage pnfsGetFileMetaDataMessage = new PnfsGetFileMetaDataMessage(pnfsCreateEntryMessage.getPnfsId());
       _pnfsManager.getFileMetaData(pnfsGetFileMetaDataMessage);
       assertTrue("file still exist after removeing last location entry", pnfsGetFileMetaDataMessage.getReturnCode() == CacheException.FILE_NOT_FOUND );
    }

    @AfterClass
    public static void tearDown() throws Exception {

        _systemCell.getNucleus().kill(_pnfsManager.getCellName());

        _conn.createStatement().execute("SHUTDOWN;");
        _conn.close();

    }

    static void tryToClose(Statement o) {
        try {
            if (o != null)
                o.close();
        } catch (SQLException e) {
            // _logNamespace.error("tryToClose PreparedStatement", e);
        }
    }


    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(PnfsManagerTest.class);
    }
}
