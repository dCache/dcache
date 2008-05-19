package org.dcache.tests.namespace;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.dcache.chimera.acl.Origin;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.AuthType;
import org.dcache.tests.cells.CellAdapterHelper;

import diskCacheV111.services.FsPermissionHandler;
import diskCacheV111.services.acl.UnixPermissionHandler;
import diskCacheV111.util.FileMetaData;

public class UnixPermissionHandlerTest {


    private final static String cellArgs =
        " -meta-data-provider=org.dcache.tests.namespace.FileMetaDataProviderHelper";

    private final static CellAdapterHelper _dummyCell = new CellAdapterHelper("UnixPermissionsTtestCell", cellArgs) ;
    private final FileMetaDataProviderHelper _metaDataSource = new FileMetaDataProviderHelper(_dummyCell);

    private static final AuthType authTypeCONST=AuthType.ORIGIN_AUTHTYPE_STRONG;
    //private static final InetAddressType inetAddressTypeCONST=InetAddressType.IPv4;
    private static final String hostCONST="127.0.0.1";

    private UnixPermissionHandler _permissionHandler;

    @Before
    public void setUp() throws Exception {
        _permissionHandler = new UnixPermissionHandler(_dummyCell);
        _metaDataSource.cleanAll();
    }

    @Test
    public void testCreateFile() throws Exception {

        boolean isAllowed = false;

        Origin origin = new Origin(authTypeCONST,  hostCONST);

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData dirMetaData =  new FileMetaData(true, 3750, 1000, 0755);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/tigran", dirMetaData);

        Subject user = new Subject(3750, 1000, null);

        isAllowed =  _permissionHandler.canCreateFile("/pnfs/desy.de/data/testFile", user, origin);

        assertFalse("Regular user is not allowed to create a file without sufficient permissions", isAllowed);

        isAllowed =  _permissionHandler.canCreateFile("/pnfs/desy.de/data/tigran/testFile", user, origin);

        assertTrue("User should be allowed to create a file with sufficient permissions", isAllowed);
    }


    @Test
    public void testCreateDir() throws Exception {

        Origin origin = new Origin(authTypeCONST, hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);

        Subject user = new Subject(3750, 1000, null);

        isAllowed =  _permissionHandler.canCreateDir("/pnfs/desy.de/data/tigran", user, origin);

        assertFalse("Regular user is not allowed to create a directory without sufficient permissions", isAllowed);

    }


    @Test
    public void testReadPrivateFile() throws Exception {

        Origin origin = new Origin(authTypeCONST,  hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData fileMetaData =  new FileMetaData(true, 3750, 1000, 0600);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        Subject owner = new Subject(3750, 1000, null);
        Subject groupMember = new Subject(3752, 1000, null);
        Subject other = new Subject(3752, 7777, null);

        isAllowed =  _permissionHandler.canReadFile("/pnfs/desy.de/data/privateFile", owner, origin);

        assertTrue("Owner is allowed to read his file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canReadFile("/pnfs/desy.de/data/privateFile", groupMember, origin);
        assertFalse("Group member not allowed to read a file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canReadFile("/pnfs/desy.de/data/privateFile", other, origin);
        assertFalse("Other not allowed to read a file with mode 0600", isAllowed);

    }


    @Test
    public void testWritePrivateFile() throws Exception {

        Origin origin = new Origin(authTypeCONST, hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData fileMetaData =  new FileMetaData(true, 3750, 1000, 0600);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        Subject owner = new Subject(3750, 1000, null);
        Subject groupMember = new Subject(3752, 1000, null);
        Subject other = new Subject(3752, 7777, null);

        isAllowed =  _permissionHandler.canWriteFile("/pnfs/desy.de/data/privateFile", owner, origin);

        assertTrue("Owner is allowed to write into his file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canWriteFile("/pnfs/desy.de/data/privateFile", groupMember, origin);
        assertFalse("Group member not allowed to write into a file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canWriteFile("/pnfs/desy.de/data/privateFile", other, origin);
        assertFalse("Other not allowed to write into a file with mode 0600", isAllowed);

    }


    @Test
    public void testGrouRead() throws Exception {

        Origin origin = new Origin(authTypeCONST, hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData fileMetaData =  new FileMetaData(true, 3750, 1000, 0640);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        Subject owner = new Subject(3750, 1000, null);
        Subject groupMember = new Subject(3752, 1000, null);

        isAllowed =  _permissionHandler.canReadFile("/pnfs/desy.de/data/privateFile", owner, origin);

        assertTrue("Owner is allowed to read his file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canReadFile("/pnfs/desy.de/data/privateFile", groupMember, origin);
        assertTrue("Group member is allowed to read a file with mode 0640", isAllowed);

        isAllowed =  _permissionHandler.canWriteFile("/pnfs/desy.de/data/privateFile", groupMember, origin);
        assertFalse("Group member not allowed to write into a file with mode 0640", isAllowed);

    }


    @Test
    public void testGrouWrite() throws Exception {

        Origin origin = new Origin(authTypeCONST,  hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData fileMetaData =  new FileMetaData(true, 3750, 1000, 0660);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        Subject owner = new Subject(3750, 1000, null);
        Subject groupMember = new Subject(3752, 1000, null);

        isAllowed =  _permissionHandler.canReadFile("/pnfs/desy.de/data/privateFile", owner, origin);

        assertTrue("Owner is allowed to read his file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canReadFile("/pnfs/desy.de/data/privateFile", groupMember, origin);
        assertTrue("Group member is allowed to read a file with mode 0640", isAllowed);

        isAllowed =  _permissionHandler.canWriteFile("/pnfs/desy.de/data/privateFile", groupMember, origin);
        assertTrue("Group member is allowed to write into a file with mode 0660", isAllowed);

    }


    @Test
    public void testGroupCreate() throws Exception {

        Origin origin = new Origin(authTypeCONST,  hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 3750, 1000, 0775);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);

        Subject groupMember = new Subject(3752, 1000, null);

        isAllowed =  _permissionHandler.canCreateDir("/pnfs/desy.de/data/newDir", groupMember, origin);
        assertTrue("Group member is allowed to create a new directory in a parent with mode 0770", isAllowed);

    }

    @Test
    public void testNegativeGroup() throws Exception {

        Origin origin = new Origin(authTypeCONST,  hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 3750, 1000, 0707);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);

        Subject groupMember = new Subject(3752, 1000, null);

        isAllowed =  _permissionHandler.canCreateDir("/pnfs/desy.de/data/newDir", groupMember, origin);
        assertFalse("Negative group member not allowed to create a new directory in a parent with mode 0707", isAllowed);

    }

    @Test
    public void testNegativeOwner() throws Exception {

        Origin origin = new Origin(authTypeCONST,  hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 3750, 1000, 0077);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);

        Subject groupMember = new Subject(3750, 1000, null);

        isAllowed =  _permissionHandler.canCreateDir("/pnfs/desy.de/data/newDir", groupMember, origin);
        assertFalse("Negative owner not allowed to create a new directory in a parent with mode 0077", isAllowed);

    }

    @Ignore // I guess we, should never allow .....
    @Test
    public void testAnonymousWrite() throws Exception {

        Origin origin = new Origin(authTypeCONST,  hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 3750, 1000, 0777);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);

        Subject anonymouos = new Subject(1111, 2222, null);

        isAllowed =  _permissionHandler.canCreateDir("/pnfs/desy.de/data/newDir", anonymouos, origin);
        assertFalse("Anonymous not allowed to create a new files or directories", isAllowed);

        isAllowed =  _permissionHandler.canWriteFile("/pnfs/desy.de/data/newFile", anonymouos, origin);
        assertFalse("Anonymous not allowed to create a new files or directories", isAllowed);

    }
}
