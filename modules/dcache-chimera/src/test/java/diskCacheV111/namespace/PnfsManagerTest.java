package diskCacheV111.namespace;

import static diskCacheV111.util.AccessLatency.NEARLINE;
import static diskCacheV111.util.RetentionPolicy.CUSTODIAL;
import static org.dcache.namespace.FileAttribute.ACCESS_LATENCY;
import static org.dcache.namespace.FileAttribute.ACCESS_TIME;
import static org.dcache.namespace.FileAttribute.CHANGE_TIME;
import static org.dcache.namespace.FileAttribute.CREATION_TIME;
import static org.dcache.namespace.FileAttribute.MODE;
import static org.dcache.namespace.FileAttribute.MODIFICATION_TIME;
import static org.dcache.namespace.FileAttribute.OWNER;
import static org.dcache.namespace.FileAttribute.OWNER_GROUP;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.RETENTION_POLICY;
import static org.dcache.namespace.FileAttribute.SIZE;
import static org.dcache.namespace.FileAttribute.STORAGEINFO;
import static org.dcache.namespace.FileAttribute.TYPE;
import static org.dcache.namespace.FileType.DIR;
import static org.dcache.namespace.FileType.REGULAR;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.io.Resources;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsCancelUpload;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsCreateUploadPath;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PnfsRenameMessage;
import diskCacheV111.vehicles.StorageInfo;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import junit.framework.JUnit4TestAdapter;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsFactory;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.UnixPermission;
import org.dcache.chimera.namespace.ChimeraNameSpaceProvider;
import org.dcache.chimera.namespace.ChimeraOsmStorageInfoExtractor;
import org.dcache.chimera.posix.Stat;
import org.dcache.namespace.CreateOption;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.PosixPermissionHandler;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.dcache.vehicles.PnfsSetFileAttributes;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class PnfsManagerTest {

    private final static URL DB_TEST_PROPERTIES
          = Resources.getResource("org/dcache/chimera/chimera-test.properties");

    private static final Set<FileAttribute> SOME_ATTRIBUTES =
          EnumSet.of(OWNER, OWNER_GROUP, MODE, TYPE, SIZE,
                CREATION_TIME, ACCESS_TIME, MODIFICATION_TIME, CHANGE_TIME,
                PNFSID, STORAGEINFO, ACCESS_LATENCY, RETENTION_POLICY);

    private static final String OSM_URI_STEM = "osm://example-osm-instance/";

    private PnfsManagerV3 _pnfsManager;
    private Connection _conn;
    private FileSystemProvider _fs;

    @Before
    public void setUp() throws Exception {
        /*
         * init Chimera DB
         */

        Properties dbProperties = new Properties();
        try (InputStream input = Resources.asByteSource(DB_TEST_PROPERTIES).openStream()) {
            dbProperties.load(input);
        }

        _conn = DriverManager.getConnection(dbProperties.getProperty("chimera.db.url"),
              dbProperties.getProperty("chimera.db.user"),
              dbProperties.getProperty("chimera.db.password"));

        _conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        Database database = DatabaseFactory.getInstance()
              .findCorrectDatabaseImplementation(new JdbcConnection(_conn));
        Liquibase liquibase = new Liquibase("org/dcache/chimera/changelog/changelog-master.xml",
              new ClassLoaderResourceAccessor(), database);
        // Uncomment the following line when testing with mysql database
        /*
         * Liquibase liquibase = new Liquibase(changeLogFile, new
         * ClassLoaderResourceAccessor(), new JdbcConnection(conn));
         */

        liquibase.update("");
        _fs = FsFactory.createFileSystem(
              dbProperties.getProperty("chimera.db.url"),
              dbProperties.getProperty("chimera.db.user"),
              dbProperties.getProperty("chimera.db.password"));

        ChimeraNameSpaceProvider chimera = new ChimeraNameSpaceProvider();
        chimera.setExtractor(new ChimeraOsmStorageInfoExtractor(StorageInfo.DEFAULT_ACCESS_LATENCY,
              StorageInfo.DEFAULT_RETENTION_POLICY));
        chimera.setInheritFileOwnership(true);
        chimera.setVerifyAllLookups(true);
        chimera.setAllowMoveToDirectoryWithDifferentStorageClass(true);
        chimera.setPermissionHandler(new PosixPermissionHandler());
        chimera.setAclEnabled(false);
        chimera.setFileSystem(_fs);
        chimera.setUploadDirectory("/upload");
        chimera.setUploadSubDirectory("%d");

        _pnfsManager = new PnfsManagerV3();
        _pnfsManager.setThreads(1);
        _pnfsManager.setListThreads(1);
        _pnfsManager.setCacheModificationRelay(null);
        _pnfsManager.setLogSlowThreshold(0);
        _pnfsManager.setNameSpaceProvider(chimera);
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

        _fs.setTag(baseInode, "sGroup", sGroupTagData, 0, sGroupTagData.length);
        _fs.setTag(baseInode, "OSMTemplate", osmTemplateTagData, 0, osmTemplateTagData.length);
    }

    @Test
    public void testGetStorageInfo() {

        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage(
              "/pnfs/testRoot/testGetStorageInfo",
              FileAttributes.ofFileType(REGULAR));

        _pnfsManager.createEntry(pnfsCreateEntryMessage);

        assertTrue("failed to create an entry", pnfsCreateEntryMessage.getReturnCode() == 0);
        assertNotNull("failed to get StrageInfo", pnfsCreateEntryMessage.getFileAttributes());

    }

    @Test
    public void testGetAlAndRpWhenMissing() {
        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage(
              "/pnfs/testRoot/testGetAlAndRpWhenMissing",
              FileAttributes.ofFileType(REGULAR));
        _pnfsManager.createEntry(pnfsCreateEntryMessage);
        assertTrue("failed to create an entry", pnfsCreateEntryMessage.getReturnCode() == 0);

        PnfsGetFileAttributes request =
              new PnfsGetFileAttributes(pnfsCreateEntryMessage.getPnfsId(),
                    EnumSet.of(FileAttribute.ACCESS_LATENCY, FileAttribute.RETENTION_POLICY));
        _pnfsManager.getFileAttributes(request);
        assertThat(request.getReturnCode(), is(0));
        assertThat(request.getFileAttributes().getAccessLatency(), is(AccessLatency.NEARLINE));
        assertThat(request.getFileAttributes().getRetentionPolicy(), is(RetentionPolicy.CUSTODIAL));
    }

    @Test
    public void testMoveEntry() throws Exception {

        PnfsCreateEntryMessage message = new PnfsCreateEntryMessage("/pnfs/testRoot/testMoveEntry",
              FileAttributes.ofFileType(DIR));
        _pnfsManager.createEntry(message);
        assertTrue("failed to create a directory", message.getReturnCode() == 0);

        FsInode srcInode = _fs.mkdir("/pnfs/testRoot/testMoveEntry/sourceDirectory");
        byte[] srcTagData = "foo".getBytes();
        _fs.setTag(srcInode, "sGroup", srcTagData, 0, srcTagData.length);

        FsInode dstInode = _fs.mkdir("/pnfs/testRoot/testMoveEntry/destinationDirectory");
        byte[] dstTagData = "bar".getBytes();
        _fs.setTag(dstInode, "sGroup", dstTagData, 0, dstTagData.length);

        message = new PnfsCreateEntryMessage(
              "/pnfs/testRoot/testMoveEntry/sourceDirectory/sourceFile",
              FileAttributes.ofFileType(REGULAR));
        _pnfsManager.createEntry(message);
        assertTrue("failed to create an entry", message.getReturnCode() == 0);

        PnfsRenameMessage pnfsRenameMessage = new PnfsRenameMessage(
              "/pnfs/testRoot/testMoveEntry/sourceDirectory/sourceFile",
              "/pnfs/testRoot/testMoveEntry/destinationDirectory/destinationFile", false);

        _pnfsManager.rename(pnfsRenameMessage);
        assertTrue("failed to move file to directory", pnfsRenameMessage.getReturnCode() == 0);

        ChimeraNameSpaceProvider provider = (ChimeraNameSpaceProvider) _pnfsManager.getNameSpaceProvider();
        provider.setAllowMoveToDirectoryWithDifferentStorageClass(false);
        pnfsRenameMessage = new PnfsRenameMessage(
              "/pnfs/testRoot/testMoveEntry/destinationDirectory/destinationFile",
              "/pnfs/testRoot/testMoveEntry/sourceDirectory/sourceFile", false);
        _pnfsManager.rename(pnfsRenameMessage);
        assertTrue("succeeded to move file to directory with different tag, a failure",
              pnfsRenameMessage.getReturnCode() != 0);

        pnfsRenameMessage = new PnfsRenameMessage(
              "/pnfs/testRoot/testMoveEntry/destinationDirectory",
              "/pnfs/testRoot/testMoveEntry/sourceDirectory/destinationDirectory", false);
        _pnfsManager.rename(pnfsRenameMessage);
        assertTrue("succeeded to move directory to directory with different tag, a failure",
              pnfsRenameMessage.getReturnCode() != 0);
    }

    @Test
    public void testMkdirPermTest() throws Exception {

        PnfsCreateEntryMessage message = new PnfsCreateEntryMessage(
              "/pnfs/testRoot/testMkdirPermTest",
              FileAttributes.of().uid(3750).gid(1000).mode(0750).fileType(DIR).build());

        _pnfsManager.createEntry(message);

        assertTrue("failed to create a directory", message.getReturnCode() == 0);

        /*
         * while we have a backdoor, let check what really happened
         */

        Stat stat = _fs.id2inode(message.getPnfsId().toString(),
              FileSystemProvider.StatCacheOption.STAT).statCache();
        assertEquals("new mode do not equal to specified one", (stat.getMode() & 0777), 0750);
    }

    /**
     * add cache location get cache location remove cache location with flag to remove if last
     */
    @Test
    public void testRemoveIfLast() {

        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage(
              "/pnfs/testRoot/testRemoveIfLast",
              FileAttributes.ofFileType(REGULAR));

        _pnfsManager.createEntry(pnfsCreateEntryMessage);

        assertTrue("failed to create an entry", pnfsCreateEntryMessage.getReturnCode() == 0);

        PnfsAddCacheLocationMessage pnfsAddCacheLocationMessage = new PnfsAddCacheLocationMessage(
              pnfsCreateEntryMessage.getPnfsId(), "aPool");

        _pnfsManager.addCacheLocation(pnfsAddCacheLocationMessage);
        assertTrue("failed to add cache location",
              pnfsAddCacheLocationMessage.getReturnCode() == 0);

        PnfsGetCacheLocationsMessage pnfsGetCacheLocationsMessage = new PnfsGetCacheLocationsMessage(
              pnfsCreateEntryMessage.getPnfsId());

        _pnfsManager.getCacheLocations(pnfsGetCacheLocationsMessage);
        assertTrue("failed to get cache location",
              pnfsGetCacheLocationsMessage.getReturnCode() == 0);

        List<String> locations = pnfsGetCacheLocationsMessage.getCacheLocations();

        assertFalse("location is empty", locations.isEmpty());
        assertFalse("location contains more than one entry", locations.size() > 1);
        assertEquals("location do not match", "aPool", locations.get(0));

        PnfsClearCacheLocationMessage pnfsClearcacheLocationMessage = new PnfsClearCacheLocationMessage(
              pnfsCreateEntryMessage.getPnfsId(), "aPool", true);
        _pnfsManager.clearCacheLocation(pnfsClearcacheLocationMessage);

        assertTrue("failed to clear cache location",
              pnfsClearcacheLocationMessage.getReturnCode() == 0);

        PnfsGetFileAttributes pnfsGetFileAttributes =
              new PnfsGetFileAttributes(pnfsCreateEntryMessage.getPnfsId(),
                    EnumSet.noneOf(FileAttribute.class));

        _pnfsManager.getFileAttributes(pnfsGetFileAttributes);
        assertTrue("file still exist after removing last location entry",
              pnfsGetFileAttributes.getReturnCode() == CacheException.FILE_NOT_FOUND);
    }

    @Test
    public void testCreateDupFile() {
        PnfsCreateEntryMessage message = new PnfsCreateEntryMessage("/pnfs/testRoot/testCreateDup",
              FileAttributes.ofFileType(REGULAR));

        _pnfsManager.createEntry(message);

        assertTrue("failed to create an entry", message.getReturnCode() == 0);

        message = new PnfsCreateEntryMessage("/pnfs/testRoot/testCreateDup",
              FileAttributes.ofFileType(REGULAR));

        _pnfsManager.createEntry(message);

        assertTrue("create duplicate should return an error",
              message.getReturnCode() == CacheException.FILE_EXISTS);
    }

    @Test
    public void testCreateDupDir() {

        String path = "/pnfs/testRoot/testCreateDupDir";

        PnfsCreateEntryMessage message = new PnfsCreateEntryMessage(path,
              FileAttributes.of().uid(3750).gid(1000).mode(0750).fileType(DIR).build());

        _pnfsManager.createEntry(message);

        assertTrue("failed to create a directory", message.getReturnCode() == 0);

        message = new PnfsCreateEntryMessage(path,
              FileAttributes.of().uid(3750).gid(1000).mode(0750).fileType(DIR).build());

        _pnfsManager.createEntry(message);

        assertTrue("create duplicate should return an error",
              message.getReturnCode() == CacheException.FILE_EXISTS);
    }


    @Test
    public void testGetFileAttributesNonExist() {

        PnfsGetFileAttributes message = new PnfsGetFileAttributes(
              new PnfsId(FsInode.generateNewID()), EnumSet.noneOf(FileAttribute.class));
        _pnfsManager.getFileAttributes(message);

        assertTrue("get storageInfo of non existing file should return FILE_NOT_FOUND",
              message.getReturnCode() == CacheException.FILE_NOT_FOUND);

    }


    @Test
    public void testWriteTokenTag() throws ChimeraFsException {

        // use back door to create the tag
        FsInode dirInode = _fs.path2inode("/pnfs/testRoot");
        _fs.createTag(dirInode, "WriteToken");

        String writeToken = "myFavoriteToeken";

        _fs.setTag(dirInode, "WriteToken", writeToken.getBytes(), 0, writeToken.getBytes().length);

        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage(
              "/pnfs/testRoot/writeTokenTestFile",
              FileAttributes.ofFileType(REGULAR));
        _pnfsManager.createEntry(pnfsCreateEntryMessage);

        StorageInfo storageInfo = pnfsCreateEntryMessage.getFileAttributes().getStorageInfo();

        assertEquals("Invalid entry in storageInfo map", writeToken,
              storageInfo.getMap().get("writeToken"));

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

        _fs.setTag(dirInode, "AccessLatency", al.getBytes(), 0, al.getBytes().length);
        _fs.setTag(dirInode, "RetentionPolicy", rp.getBytes(), 0, rp.getBytes().length);

        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage(
              "/pnfs/testRoot/testDefaultALandRP",
              FileAttributes.ofFileType(REGULAR));

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

        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage(
              "/pnfs/testRoot/testRemoveByPath",
              FileAttributes.ofFileType(REGULAR));

        _pnfsManager.createEntry(pnfsCreateEntryMessage);

        PnfsDeleteEntryMessage deleteEntryMessage = new PnfsDeleteEntryMessage(
              "/pnfs/testRoot/testRemoveByPath");
        _pnfsManager.deleteEntry(deleteEntryMessage);

        assertNotExists("/pnfs/testRoot/testRemoveByPath");
    }


    @Test
    public void testGetStorageInfoNoTags() throws ChimeraFsException {

        FsInode rootInode = _fs.path2inode("/pnfs");
        PnfsGetFileAttributes message = new PnfsGetFileAttributes(
              new PnfsId(rootInode.statCache().getId()), SOME_ATTRIBUTES);
        _pnfsManager.getFileAttributes(message);

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

        PnfsAddCacheLocationMessage pnfsAddCacheLocationMessage = new PnfsAddCacheLocationMessage(
              new PnfsId(FsInode.generateNewID()), "aPool");

        _pnfsManager.addCacheLocation(pnfsAddCacheLocationMessage);
        assertTrue("add cache location of non existing file should return FILE_NOT_FOUND",
              pnfsAddCacheLocationMessage.getReturnCode() == CacheException.FILE_NOT_FOUND);
    }

    @Test
    public void testGetStorageInfoForFlushedFiles() throws Exception {

        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage(
              "/pnfs/testRoot/tapeFile",
              FileAttributes.ofFileType(REGULAR));
        _pnfsManager.createEntry(pnfsCreateEntryMessage);
        assertThat("Creating entry failed", pnfsCreateEntryMessage.getReturnCode(), is(0));

        StorageInfo si = pnfsCreateEntryMessage.getFileAttributes().getStorageInfo();
        si.addLocation(new URI(OSM_URI_STEM + "?store=tape"));
        si.isSetAddLocation(true);

        PnfsSetFileAttributes setFileAttributesMessage =
              new PnfsSetFileAttributes(pnfsCreateEntryMessage.getPnfsId(),
                    FileAttributes.of().accessLatency(NEARLINE).retentionPolicy(CUSTODIAL)
                          .storageInfo(si).build());

        _pnfsManager.setFileAttributes(setFileAttributesMessage);
        assertThat("Setting storage info failed", setFileAttributesMessage.getReturnCode(), is(0));

        PnfsGetFileAttributes message =
              new PnfsGetFileAttributes(pnfsCreateEntryMessage.getPnfsId(), SOME_ATTRIBUTES);
        _pnfsManager.getFileAttributes(message);

        assertEquals("failed to get storageInfo for flushed files", 0, message.getReturnCode());

    }

    @Test
    public void testIgnoreFilesizeUpdateOnFlush() throws Exception {

        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage(
              "/pnfs/testRoot/tapeFile",
              FileAttributes.ofFileType(REGULAR));
        _pnfsManager.createEntry(pnfsCreateEntryMessage);
        assertThat("Creating entry failed", pnfsCreateEntryMessage.getReturnCode(), is(0));

        // simulate pool location update
        PnfsSetFileAttributes setFileAttributesMessage =
              new PnfsSetFileAttributes(pnfsCreateEntryMessage.getPnfsId(),
                    FileAttributes.of()
                          .location("pool-foo")
                          .size(17)
                          .build());

        _pnfsManager.setFileAttributes(setFileAttributesMessage);
        assertThat("Setting storage info failed", setFileAttributesMessage.getReturnCode(), is(0));

        StorageInfo si = pnfsCreateEntryMessage.getFileAttributes().getStorageInfo();
        si.addLocation(new URI(OSM_URI_STEM + "?store=tape"));
        si.isSetAddLocation(true);

        setFileAttributesMessage =
              new PnfsSetFileAttributes(pnfsCreateEntryMessage.getPnfsId(),
                    FileAttributes.of()
                          .size(1L)
                          .accessLatency(NEARLINE)
                          .retentionPolicy(CUSTODIAL)
                          .storageInfo(si).build());

        _pnfsManager.setFileAttributes(setFileAttributesMessage);
        assertThat("Setting storage info failed", setFileAttributesMessage.getReturnCode(), is(0));

        PnfsGetFileAttributes message =
              new PnfsGetFileAttributes(pnfsCreateEntryMessage.getPnfsId(), SOME_ATTRIBUTES);
        _pnfsManager.getFileAttributes(message);

        assertEquals("failed to get storageInfo for flushed files", 0, message.getReturnCode());
        assertThat("File size get update", message.getFileAttributes().getSize(), is(17L));
    }

    @Test
    public void testStorageInfoDup() throws Exception {

        PnfsCreateEntryMessage pnfsCreateEntryMessage = new PnfsCreateEntryMessage(
              "/pnfs/testRoot/tapeFileDup",
              FileAttributes.ofFileType(REGULAR));
        _pnfsManager.createEntry(pnfsCreateEntryMessage);

        StorageInfo si = pnfsCreateEntryMessage.getFileAttributes().getStorageInfo();

        si.addLocation(new URI(OSM_URI_STEM + "?store=tape1"));
        si.isSetAddLocation(true);

        PnfsSetFileAttributes setFileAttributesMessage =
              new PnfsSetFileAttributes(pnfsCreateEntryMessage.getPnfsId(),
                    FileAttributes.ofStorageInfo(si));

        _pnfsManager.setFileAttributes(setFileAttributesMessage);

        si.addLocation(new URI(OSM_URI_STEM + "?store=tape2"));
        si.isSetAddLocation(true);

        setFileAttributesMessage =
              new PnfsSetFileAttributes(pnfsCreateEntryMessage.getPnfsId(),
                    FileAttributes.ofStorageInfo(si));

        _pnfsManager.setFileAttributes(setFileAttributesMessage);
        assertEquals("failed to add second tape location", 0,
              setFileAttributesMessage.getReturnCode());

    }

    @Test
    public void testAddChecksumNonExist() {

        FileAttributes attr = FileAttributes.ofChecksum(
              new Checksum(ChecksumType.ADLER32, "12345678"));
        PnfsSetFileAttributes pnfsSetAttributesMessage = new PnfsSetFileAttributes(
              new PnfsId(FsInode.generateNewID()), attr);
        pnfsSetAttributesMessage.setReplyRequired(false);
        _pnfsManager.processPnfsMessage(null, pnfsSetAttributesMessage);

        assertEquals("Set checksum for non existing file must return FILE_NOT_FOUND",
              CacheException.FILE_NOT_FOUND, pnfsSetAttributesMessage.getReturnCode());

    }

    @Test
    public void testGetCombinedAttributesNonExist() {

        PnfsGetFileAttributes message =
              new PnfsGetFileAttributes(new PnfsId(FsInode.generateNewID()),
                    EnumSet.noneOf(FileAttribute.class));
        _pnfsManager.getFileAttributes(message);
        assertEquals("Get attributes for non existing file have to return FILE_NOT_FOUND",
              CacheException.FILE_NOT_FOUND, message.getReturnCode());
    }

    @Test
    public void testStorageInfoNoTags() throws Exception {

        FsInode dir = _fs.mkdir("/notags");
        FsInode inode = _fs.createFile(dir, "afile");
        PnfsGetFileAttributes message =
              new PnfsGetFileAttributes(new PnfsId(inode.statCache().getId()), SOME_ATTRIBUTES);
        _pnfsManager.getFileAttributes(message);

        assertEquals("failed to get storageInfo for a directory without tags", 0, message
              .getReturnCode());

    }

    @Test
    public void testSetFilePermissions() throws Exception {
        FsInode base = _fs.path2inode("/pnfs");
        FsInode inode = _fs.createFile(base, "afile");
        Stat stat = _fs.stat(inode);

        int mode = 0222;

        PnfsSetFileAttributes pnfsSetFileAttributes =
              new PnfsSetFileAttributes(new PnfsId(inode.statCache().getId()),
                    FileAttributes.ofMode(mode));
        _pnfsManager.setFileAttributes(pnfsSetFileAttributes);

        Stat new_stat = _fs.stat(inode);
        assertEquals("setFileAttributes change file type",
              stat.getMode() & ~UnixPermission.S_PERMS | mode, new_stat.getMode());
    }

    @Test
    public void testCreationTime() throws Exception {
        FsInode base = _fs.path2inode("/pnfs");
        FsInode inode = _fs.createFile(base, "afile");
        Stat beforeUpdateStat = _fs.stat(inode);
        Stat stat = new Stat();
        stat.setCTime(beforeUpdateStat.getCTime() + 1000);
        _fs.setInodeAttributes(inode, 0, stat);

        PnfsGetFileAttributes pnfsGetFileAttributes
              = new PnfsGetFileAttributes(new PnfsId(inode.statCache().getId()),
              EnumSet.of(FileAttribute.CHANGE_TIME, FileAttribute.CREATION_TIME));
        _pnfsManager.getFileAttributes(pnfsGetFileAttributes);

        assertEquals(beforeUpdateStat.getCrTime(),
              pnfsGetFileAttributes.getFileAttributes().getCreationTime());
        assertTrue("Creation time can't be in the past",
              pnfsGetFileAttributes.getFileAttributes().getCreationTime()
                    < pnfsGetFileAttributes.getFileAttributes().getChangeTime());
    }

    @Test
    public void testCancelUpload() throws ChimeraFsException {
        FsPath root = FsPath.ROOT;
        FsPath path = FsPath.create("/test");

        PnfsCreateUploadPath create =
              new PnfsCreateUploadPath(Subjects.ROOT, Restrictions.none(), path, root, null,
                    null, null, null, EnumSet.noneOf(CreateOption.class));
        _pnfsManager.createUploadPath(create);
        assertThat(create.getReturnCode(), is(0));

        PnfsCancelUpload cancel = new PnfsCancelUpload(Subjects.ROOT, Restrictions.none(),
              create.getUploadPath(), path, EnumSet.noneOf(FileAttribute.class),
              "request aborted");
        _pnfsManager.cancelUpload(cancel);
        assertThat(cancel.getReturnCode(), is(0));

        assertNotExists(create.getUploadPath().toString());
        assertNotExists("/test");
    }

    @Test
    public void testCancelUploadRecursively() throws ChimeraFsException {
        FsPath root = FsPath.ROOT;
        FsPath path = FsPath.create("/test");

        PnfsCreateUploadPath create =
              new PnfsCreateUploadPath(Subjects.ROOT, Restrictions.none(), path, root, null,
                    null, null, null, EnumSet.noneOf(CreateOption.class));
        _pnfsManager.createUploadPath(create);
        assertThat(create.getReturnCode(), is(0));

        _fs.createFile(create.getUploadPath().toString());
        _fs.mkdir(create.getUploadPath().parent() + "/bar");
        _fs.mkdir(create.getUploadPath().parent() + "/baz");
        _fs.createFile(create.getUploadPath().parent() + "/baz/baz");

        PnfsCancelUpload cancel = new PnfsCancelUpload(Subjects.ROOT,
              Restrictions.none(), create.getUploadPath(), path,
              EnumSet.noneOf(FileAttribute.class), "request aborted");
        _pnfsManager.cancelUpload(cancel);
        assertThat(cancel.getReturnCode(), is(0));

        assertNotExists(create.getUploadPath().toString());
        assertNotExists("/test");
    }

    @Test
    public void testUpdateAtimeOnGetFileAttributes() throws ChimeraFsException {

        FsInode inode = _fs.createFile("/file1");
        Stat stat_before = inode.stat();
        _pnfsManager.setAtimeGap(0);

        PnfsGetFileAttributes message = new PnfsGetFileAttributes(new PnfsId(inode.getId()),
              SOME_ATTRIBUTES);
        message.setUpdateAtime(true);

        _pnfsManager.getFileAttributes(message);
        Stat stat_after = inode.stat();

        assertTrue("atime is not updated", stat_after.getATime() != stat_before.getATime());
    }

    @Test
    public void testNoAtimeUpdateOnGetFileAttributesNegativeGap() throws ChimeraFsException {

        FsInode inode = _fs.createFile("/file1");
        Stat stat_before = inode.stat();
        _pnfsManager.setAtimeGap(-1);

        PnfsGetFileAttributes message = new PnfsGetFileAttributes(new PnfsId(inode.getId()),
              SOME_ATTRIBUTES);
        message.setUpdateAtime(true);

        _pnfsManager.getFileAttributes(message);
        Stat stat_after = inode.stat();

        assertTrue("atime is updated, but shouldn't",
              stat_after.getATime() == stat_before.getATime());
    }

    private void assertNotExists(String path) throws ChimeraFsException {
        try {
            _fs.path2inode(path);
            fail(path + " exists when it was expected not to");
        } catch (FileNotFoundHimeraFsException ignored) {
        }
    }

    @After
    public void tearDown() throws Exception {
        _pnfsManager.shutdown();
        _fs.close();
        _conn.createStatement().execute("SHUTDOWN;");
        _conn.close();
    }

    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(PnfsManagerTest.class);
    }
}
