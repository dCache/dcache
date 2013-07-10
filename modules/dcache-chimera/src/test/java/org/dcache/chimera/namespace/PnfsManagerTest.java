package org.dcache.chimera.namespace;

import com.jolbox.bonecp.BoneCPDataSource;
import junit.framework.JUnit4TestAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumSet;
import java.util.List;

import diskCacheV111.namespace.PnfsManagerV3;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PnfsCreateDirectoryMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsSetChecksumMessage;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.UnixPermission;
import org.dcache.chimera.posix.Stat;
import org.dcache.commons.util.SqlHelper;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.PosixPermissionHandler;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.dcache.vehicles.PnfsSetFileAttributes;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class PnfsManagerTest
{
    private static final String OSM_URI_STEM = "osm://example-osm-instance/";

    private PnfsManagerV3 _pnfsManager;
    private Connection _conn;
    private JdbcFs _fs;
    private BoneCPDataSource _ds;

    @Before
    public void setUp() throws Exception {
        /*
         * init Chimera DB
         */

        Class.forName("org.hsqldb.jdbcDriver");

        _conn = DriverManager.getConnection("jdbc:hsqldb:mem:chimeramem", "sa", "");
        StringBuilder sql = new StringBuilder();

        BufferedReader dataStr =
            new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream("org/dcache/chimera/sql/create-hsqldb.sql")));
        String inLine;

        while ((inLine = dataStr.readLine()) != null) {
            sql.append(inLine);
        }

        String[] statements = sql.toString().split(";");
        for (String statement : statements) {
            Statement st = _conn.createStatement();
            st.executeUpdate(statement);
            SqlHelper.tryToClose(st);
        }

        _ds = new BoneCPDataSource();
        _ds.setJdbcUrl("jdbc:hsqldb:mem:chimeramem");
        _ds.setUsername("sa");
        _ds.setPassword("");
        _ds.getConfig().setMaxConnectionsPerPartition(2); // seems to require >= 2
        _ds.getConfig().setMinConnectionsPerPartition(1);
        _ds.getConfig().setPartitionCount(1);

        _fs = new JdbcFs(_ds, "HsqlDB");

        ChimeraNameSpaceProvider chimera = new ChimeraNameSpaceProvider();
        chimera.setExtractor(new ChimeraOsmStorageInfoExtractor(StorageInfo.DEFAULT_ACCESS_LATENCY, StorageInfo.DEFAULT_RETENTION_POLICY));
        chimera.setInheritFileOwnership(true);
        chimera.setVerifyAllLookups(true);
        chimera.setPermissionHandler(new PosixPermissionHandler());
        chimera.setAclEnabled(false);
        chimera.setFileSystem(_fs);


        _pnfsManager = new PnfsManagerV3();
        _pnfsManager.setThreads(1);
        _pnfsManager.setCacheLocationThreads(0);
        _pnfsManager.setListThreads(1);
        _pnfsManager.setThreadGroups(1);
        _pnfsManager.setCacheModificationRelay(null);
        _pnfsManager.setPnfsDeleteNotificationRelay(null);
        _pnfsManager.setLogSlowThreshold(0);
        _pnfsManager.setNameSpaceProvider(chimera);
        _pnfsManager.setCacheLocationProvider(chimera);
        _pnfsManager.setQueueMaxSize(0);
        _pnfsManager.setFolding(true);
        _pnfsManager.setDirectoryListLimit(100);
        _pnfsManager.init();


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
    public void testGetAlAndRpWhenMissing() {
        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage("/pnfs/testRoot/testGetAlAndRpWhenMissing");
        _pnfsManager.createEntry(pnfsCreateEntryMessage);
        assertTrue("failed to create an entry", pnfsCreateEntryMessage.getReturnCode() == 0 );

        PnfsGetFileAttributes request =
                new PnfsGetFileAttributes(pnfsCreateEntryMessage.getPnfsId(),
                        EnumSet.of(FileAttribute.ACCESS_LATENCY, FileAttribute.RETENTION_POLICY));
        _pnfsManager.getFileAttributes(request);
        assertThat(request.getReturnCode(), is(0));
        assertThat(request.getFileAttributes().getAccessLatency(), is(AccessLatency.NEARLINE));
        assertThat(request.getFileAttributes().getRetentionPolicy(), is(RetentionPolicy.CUSTODIAL));
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

        PnfsGetFileAttributes pnfsGetFileAttributes =
            new PnfsGetFileAttributes(pnfsCreateEntryMessage.getPnfsId(),
                                      EnumSet.noneOf(FileAttribute.class));

       _pnfsManager.getFileAttributes(pnfsGetFileAttributes);
       assertTrue("file still exist after removing last location entry", pnfsGetFileAttributes.getReturnCode() == CacheException.FILE_NOT_FOUND );
    }

    @Test
    public void testCreateDupFile() {
        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage("/pnfs/testRoot/testCreateDup");
        _pnfsManager.createEntry(pnfsCreateEntryMessage);

        assertTrue("failed to create an entry", pnfsCreateEntryMessage.getReturnCode() == 0 );

        _pnfsManager.createEntry(pnfsCreateEntryMessage);

        assertTrue("create duplicate should return an error", pnfsCreateEntryMessage.getReturnCode() == CacheException.FILE_EXISTS );
    }

    @Test
    public void testCreateDupDir() {

        PnfsCreateDirectoryMessage pnfsCreateDirectoryMessage = new PnfsCreateDirectoryMessage("/pnfs/testRoot/testCreateDupDir", 3750, 1000, 0750);

        _pnfsManager.createDirectory(pnfsCreateDirectoryMessage);

        assertTrue("failed to create a directory", pnfsCreateDirectoryMessage.getReturnCode() == 0 );

        _pnfsManager.createDirectory(pnfsCreateDirectoryMessage);

        assertTrue("create duplicate should return an error", pnfsCreateDirectoryMessage.getReturnCode() == CacheException.FILE_EXISTS );
    }


    @Test
    public void testGetStoreInfoNonExist() {

        PnfsGetStorageInfoMessage pnfsGetStorageInfoMessage = new PnfsGetStorageInfoMessage(new PnfsId("000000000000000000000000000000000001"));
        _pnfsManager.getFileAttributes(pnfsGetStorageInfoMessage);

        assertTrue("get storageInfo of non existing file should return FILE_NOT_FOUND", pnfsGetStorageInfoMessage.getReturnCode() == CacheException.FILE_NOT_FOUND );

    }


    @Test
    public void testWriteTokenTag() throws ChimeraFsException {

        // use back door to create the tag
        FsInode dirInode = _fs.path2inode("/pnfs/testRoot");
        _fs.createTag(dirInode, "WriteToken");

        String writeToken = "myFavoriteToeken";

        _fs.setTag(dirInode, "WriteToken", writeToken.getBytes(), 0,writeToken.getBytes().length );

        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage("/pnfs/testRoot/writeTokenTestFile");
        _pnfsManager.createEntry(pnfsCreateEntryMessage);

        StorageInfo storageInfo = pnfsCreateEntryMessage.getStorageInfo();

        assertEquals("Invalid entry in storageInfo map", writeToken, storageInfo.getMap().get("writeToken") );

    }

    @Test
    public void testDefaultALandRP() throws ChimeraFsException {


        /*
         * this test relays on the fact that default values in PnfsManager
         * is CUSTODIAL/NEARLINE
         */

        // use back door to create the tag
        FsInode dirInode = _fs.path2inode("/pnfs/testRoot");
        _fs.createTag(dirInode, "AccessLatency");
        _fs.createTag(dirInode, "RetentionPolicy");


        String al = "ONLINE";
        String rp = "OUTPUT";

        _fs.setTag(dirInode, "AccessLatency", al.getBytes(), 0, al.getBytes().length );
        _fs.setTag(dirInode, "RetentionPolicy", rp.getBytes(), 0, rp.getBytes().length);


        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage("/pnfs/testRoot/testDefaultALandRP");

        _pnfsManager.createEntry(pnfsCreateEntryMessage);

        FileAttributes fileAttributes = pnfsCreateEntryMessage.getFileAttributes();

        assertEquals("AccessLatensy is not taken from the parent directory",
                AccessLatency.ONLINE,
                fileAttributes.getAccessLatency());

        assertEquals("RetentionPolicy is not taken from the parent directory",
                RetentionPolicy.OUTPUT,
                fileAttributes.getRetentionPolicy());

    }

    @Test
    public void testRemoveByPath() throws ChimeraFsException {


        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage("/pnfs/testRoot/testRemoveByPath");

        _pnfsManager.createEntry(pnfsCreateEntryMessage);


        PnfsDeleteEntryMessage deleteEntryMessage = new PnfsDeleteEntryMessage("/pnfs/testRoot/testRemoveByPath");
        _pnfsManager.deleteEntry(deleteEntryMessage);

        try {

            _fs.path2inode("/pnfs/testRoot/testRemoveByPath");
            fail("remove by path did not removed file from filesystem");
        }catch(FileNotFoundHimeraFsException fnf) {
            // OK
        }
    }



    @Test
    public void testGetStorageInfoNoTags() throws ChimeraFsException {

        FsInode rootInode = _fs.path2inode("/pnfs");
        PnfsGetStorageInfoMessage pnfsGetStorageInfoMessage = new PnfsGetStorageInfoMessage(new PnfsId(rootInode.toString()));
        _pnfsManager.getFileAttributes(pnfsGetStorageInfoMessage);

        // I don't know yet what is expected reply, but not NPE !
    }
/*
    @Test
    public void testGetParentOf() throws ChimeraFsException {
        PnfsCreateDirectoryMessage pnfsCreateDirectoryMessage = new PnfsCreateDirectoryMessage("/pnfs/testRoot/testDir", 3750, 1000, 0750);
        _pnfsManager.createDirectory(pnfsCreateDirectoryMessage);

        FsInode rootInode = _fs.path2inode("/pnfs");
        FsInode parentDirInode = _fs.path2inode("/pnfs/testRoot");
        FsInode testDirInode = _fs.path2inode("/pnfs/testRoot/testDir");

        PnfsId rootDirPnfs = new PnfsId(rootInode.toString());
        PnfsId parentDirPnfs = new PnfsId(parentDirInode.toString());
        PnfsId testDirPnfs = new PnfsId(testDirInode.toString());

        PnfsGetParentMessage pnfsGetParentMessage1 = new PnfsGetParentMessage(testDirPnfs);
        _pnfsManager.getParent(pnfsGetParentMessage1);
        PnfsId pnfsOfParent1 = pnfsGetParentMessage1.getParent();

        assertEquals("ok: pnfsOfParent1=parentDirPnfs", parentDirPnfs.toString(), pnfsOfParent1.toString());

        PnfsGetParentMessage pnfsGetParentMessage2 = new PnfsGetParentMessage(parentDirPnfs);
        _pnfsManager.getParent(pnfsGetParentMessage2);
        PnfsId pnfsOfParent2 = pnfsGetParentMessage2.getParent();

        assertEquals("ok: pnfsOfParent2=rootDirPnfs", rootDirPnfs.toString(), pnfsOfParent2.toString());

        PnfsGetParentMessage pnfsGetParentMessage3 = new PnfsGetParentMessage(rootDirPnfs);
        _pnfsManager.getParent(pnfsGetParentMessage3);
        PnfsId pnfsOfParent3 = pnfsGetParentMessage3.getParent();

        assertEquals("ok: pnfsOfParent3=000000000000000000000000000000000000","000000000000000000000000000000000000",pnfsOfParent3.toString());

    }

    @Test
    public void testGetParentOfNotExistingResource() throws ChimeraFsException {

        PnfsId tmp=new PnfsId("111113333300000000000000000000222222");
        PnfsGetParentMessage pnfsGetParentMessage = new PnfsGetParentMessage(tmp);

        _pnfsManager.getParent(pnfsGetParentMessage);

        assertTrue("get parent of non existing resource should return FILE_NOT_FOUND", pnfsGetParentMessage.getReturnCode() == CacheException.FILE_NOT_FOUND );

    }
*/
    @Test
    @Ignore
    public void testAddCacheLocationNonExist() {

        PnfsAddCacheLocationMessage pnfsAddCacheLocationMessage = new PnfsAddCacheLocationMessage(new PnfsId("000000000000000000000000000000000001"), "aPool");

        _pnfsManager.addCacheLocation(pnfsAddCacheLocationMessage);
        assertTrue("add cache location of non existing file should return FILE_NOT_FOUND", pnfsAddCacheLocationMessage.getReturnCode() == CacheException.FILE_NOT_FOUND );
    }

    @Test
    public void testGetStorageInfoForFlushedFiles() throws Exception {

        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage("/pnfs/testRoot/tapeFile");
        _pnfsManager.createEntry(pnfsCreateEntryMessage);
        assertThat("Creating entry failed", pnfsCreateEntryMessage.getReturnCode(), is(0));

        StorageInfo si = pnfsCreateEntryMessage.getStorageInfo();
        si.addLocation(new URI(OSM_URI_STEM + "?store=tape"));
        si.isSetAddLocation(true);

        FileAttributes attributesToUpdate = new FileAttributes();
        attributesToUpdate.setAccessLatency(AccessLatency.NEARLINE);
        attributesToUpdate.setRetentionPolicy(RetentionPolicy.CUSTODIAL);
        attributesToUpdate.setStorageInfo(si);
        PnfsSetFileAttributes setFileAttributesMessage =
                new PnfsSetFileAttributes(pnfsCreateEntryMessage.getPnfsId(), attributesToUpdate);

        _pnfsManager.setFileAttributes(setFileAttributesMessage);
        assertThat("Setting storage info failed", setFileAttributesMessage.getReturnCode(), is(0));

        PnfsGetStorageInfoMessage pnfsGetStorageInfoMessage =
            new PnfsGetStorageInfoMessage(pnfsCreateEntryMessage.getPnfsId());
        _pnfsManager.getFileAttributes(pnfsGetStorageInfoMessage);

        assertEquals("failed to get storageInfo for flushed files", 0,pnfsGetStorageInfoMessage.getReturnCode() );

    }

    @Test
    public void testStorageInfoDup() throws Exception {

        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage("/pnfs/testRoot/tapeFileDup");
        _pnfsManager.createEntry(pnfsCreateEntryMessage);

        StorageInfo si = pnfsCreateEntryMessage.getStorageInfo();

        si.addLocation(new URI(OSM_URI_STEM + "?store=tape1"));
        si.isSetAddLocation(true);

        FileAttributes attributesToUpdate = new FileAttributes();
        attributesToUpdate.setStorageInfo(si);
        PnfsSetFileAttributes setFileAttributesMessage =
                new PnfsSetFileAttributes(pnfsCreateEntryMessage.getPnfsId(), attributesToUpdate);

        _pnfsManager.setFileAttributes(setFileAttributesMessage);

        si.addLocation(new URI(OSM_URI_STEM + "?store=tape2"));
        si.isSetAddLocation(true);

        attributesToUpdate = new FileAttributes();
        attributesToUpdate.setStorageInfo(si);
        setFileAttributesMessage =
            new PnfsSetFileAttributes(pnfsCreateEntryMessage.getPnfsId(), attributesToUpdate);

        _pnfsManager.setFileAttributes(setFileAttributesMessage);
        assertEquals("failed to add second tape location", 0, setFileAttributesMessage.getReturnCode() );

    }

    @Test
    public void testAddChecksumNonExist() {

        PnfsSetChecksumMessage pnfsSetChecksumMessage = new PnfsSetChecksumMessage(new PnfsId("000000000000000000000000000000000001"), 1, "12345678");
        pnfsSetChecksumMessage.setReplyRequired(false);
        _pnfsManager.processPnfsMessage(null, pnfsSetChecksumMessage);

        assertEquals("Set checksum for non existing file must return FILE_NOT_FOUND",  CacheException.FILE_NOT_FOUND, pnfsSetChecksumMessage.getReturnCode());

    }

    @Test
    public void testGetCombinedAttributesNonExist() {

        PnfsGetFileAttributes message =
            new PnfsGetFileAttributes(new PnfsId("000000000000000000000000000000000001"),
                                      EnumSet.noneOf(FileAttribute.class));
        _pnfsManager.getFileAttributes(message);
        assertEquals("Get attributes for non existing file have to return FILE_NOT_FOUND",
            CacheException.FILE_NOT_FOUND, message.getReturnCode() );
    }

    @Test
    public void testStorageInfoNoTags() throws Exception {

        FsInode dir = _fs.mkdir("/notags");
        FsInode inode = _fs.createFile(dir, "afile");
        PnfsGetStorageInfoMessage pnfsGetStorageInfoMessage =
            new PnfsGetStorageInfoMessage( new PnfsId(inode.toString()));
        _pnfsManager.getFileAttributes(pnfsGetStorageInfoMessage);

        assertEquals("failed to get storageInfo for a directory without tags", 0,pnfsGetStorageInfoMessage.getReturnCode() );

    }

    @Test
    public void testSetFilePermissions() throws Exception {
        FsInode base = _fs.path2inode("/pnfs");
        FsInode inode = _fs.createFile(base, "afile");
        Stat stat = _fs.stat(inode);

        int mode = 0222;
        FileAttributes attr = new FileAttributes();
        attr.setMode(mode);

        PnfsSetFileAttributes pnfsSetFileAttributes =
                new PnfsSetFileAttributes(new PnfsId(inode.toString()), attr);
        _pnfsManager.setFileAttributes(pnfsSetFileAttributes);

        Stat new_stat = _fs.stat(inode);
        assertEquals("setFileAttributes change file type",
                stat.getMode() & ~UnixPermission.S_PERMS | mode , new_stat.getMode());
    }

    @After
    public void tearDown() throws Exception
    {
        _ds.close();
        _conn.createStatement().execute("SHUTDOWN;");
        _conn.close();
    }

    static void tryToClose(Statement o) {
        try {
            if (o != null) {
                o.close();
            }
        } catch (SQLException e) {
        }
    }


    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(PnfsManagerTest.class);
    }
}
