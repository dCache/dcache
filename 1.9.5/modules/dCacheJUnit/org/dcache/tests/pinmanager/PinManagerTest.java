package org.dcache.tests.pinmanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import dmg.cells.nucleus.SystemCell;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import org.dcache.tests.cells.CellAdapterHelper;

import org.dcache.services.pinmanager1.PinManager;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PinManagerPinMessage;
import diskCacheV111.vehicles.PinManagerUnpinMessage;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PnfsCreateDirectoryMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PnfsGetFileMetaDataMessage;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class PinManagerTest {

    /*
     * make Cells happy
     */
    private static  final CellAdapterHelper SYSTEM_CELL_HOLDER = new CellAdapterHelper("PinManagerTest", "");

    private  static PinManager pinManager;
    private static EchoCellHelper echoCell;

    private static String driver="org.postgresql.Driver";
    private static String dburl = "jdbc:postgresql://localhost/dcache";
    private static String dbuser = "srmdcache";

    //private static String driver="org.hsqldb.jdbcDriver";
    //private static String dburl = "jdbc:hsqldb:mem:pinmem";
    //private static String dbuser = "srmdcache";

    @BeforeClass
    public static  void setUp() throws Throwable {
        /*
         * init Echo cell
         */
        System.out.println("setting up");

       echoCell = new EchoCellHelper("EchoCell", "");

        Class.forName(driver);

        Connection conn = DriverManager.getConnection(dburl, dbuser, "");
        System.out.println("got connection");
        conn.close();
        String args =
        " -jdbcDriver="+driver+
        " -jdbcUrl="+dburl+
        " -dbUser="+dbuser+
        " -dbPass=\"\" "+
        " -poolManager=EchoCell "+
        " -pnfsManager=EchoCell ";



        pinManager =  new PinManager("testPinManager", args);

    }

    @Test
    public void testPinning () throws Exception {
        //System.out.println("testPinning is running");
        PnfsId pnfsId = new PnfsId("000100000000000000001080");
        PinManagerPinMessage pinManagerPinMessage = new PinManagerPinMessage(
            pnfsId,"localhost",3600L,12345L);

        pinManagerPinMessage =
            (PinManagerPinMessage)
            echoCell.sendAndWait(
                new CellMessage(new CellPath("testPinManager"),
                pinManagerPinMessage),60000L).getMessageObject();

        assertTrue("failed to pin", pinManagerPinMessage.getReturnCode() == 0 );
        if(pinManagerPinMessage.getReturnCode() == 0) {
            PinManagerUnpinMessage unpin =
                new PinManagerUnpinMessage(pnfsId,pinManagerPinMessage.getPinRequestId());
            unpin =
                (PinManagerUnpinMessage)
                echoCell.sendAndWait(
                    new CellMessage(new CellPath("testPinManager"),
                    unpin),60000L).getMessageObject();
            assertTrue("failed to unpin", unpin.getReturnCode() == 0 );

        }

    }

    @Test
    public void test1000Pinning () throws Exception {
        System.out.println("test1000Pinning is running");
        for(int i =0 ; i <1000; ++i) {
            testPinning();
        }
    }

    @AfterClass
    public static  void tearDown() throws Exception {
        System.out.println("Tearing down");
       echoCell.stop();

       // pinManager.stop();

    }


    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(PinManagerTest.class);
    }

    public static final void main(String[] args) {
        org.junit.runner.JUnitCore.main(PinManagerTest.class.getName());
        //org.junit.runner.JUnitCore.runClasses(PinManagerTest.class);
        System.out.println("test completed");
    }


}
