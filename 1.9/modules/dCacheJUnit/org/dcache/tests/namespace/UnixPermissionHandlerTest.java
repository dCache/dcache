package org.dcache.tests.namespace;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.dcache.chimera.acl.Origin;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.AuthType;
import org.dcache.chimera.acl.enums.InetAddressType;
import org.dcache.tests.cells.CellAdapterHelper;

import diskCacheV111.services.UnixPermissionHandler;
import diskCacheV111.util.FileMetaData;

public class UnixPermissionHandlerTest {


    private final static String cellArgs =
        " -meta-data-provider=org.dcache.tests.namespace.FileMetaDataProviderHelper";

    private final static CellAdapterHelper _dummyCell = new CellAdapterHelper("UnixPermissionsTtestCell", cellArgs) ;
    private final FileMetaDataProviderHelper _metaDataSource = new FileMetaDataProviderHelper(_dummyCell);

    private static final AuthType authTypeCONST=AuthType.ORIGIN_AUTHTYPE_STRONG;
    private static final InetAddressType inetAddressTypeCONST=InetAddressType.IPv4;
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

        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData dirMetaData =  new FileMetaData(true, 3750, 1000, 0755);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/tigran", dirMetaData);

        Subject user = new Subject(3750, 1000, null);

        isAllowed =  _permissionHandler.canCreateFile(user, "/pnfs/desy.de/data/testFile", origin);

        assertFalse("Regular user is not allowed to create a file without sufficient permissions", isAllowed);

        isAllowed =  _permissionHandler.canCreateFile(user, "/pnfs/desy.de/data/tigran/testFile", origin);

        assertTrue("User should be allowed to create a file with sufficient permissions", isAllowed);
    }


    @Test
    public void testCreateDir() throws Exception {

        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);

        Subject user = new Subject(3750, 1000, null);

        isAllowed =  _permissionHandler.canCreateDir(user, "/pnfs/desy.de/data/tigran", origin);

        assertFalse("Regular user is not allowed to create a directory without sufficient permissions", isAllowed);

    }


    @Test
    public void testReadPrivateFile() throws Exception {

        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData fileMetaData =  new FileMetaData(true, 3750, 1000, 0600);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        Subject owner = new Subject(3750, 1000, null);
        Subject groupMember = new Subject(3752, 1000, null);
        Subject other = new Subject(3752, 7777, null);

        isAllowed =  _permissionHandler.canReadFile(owner, "/pnfs/desy.de/data/privateFile", origin);

        assertTrue("Owner is allowed to read his file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canReadFile(groupMember, "/pnfs/desy.de/data/privateFile", origin);
        assertFalse("Group member not allowed to read a file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canReadFile(other, "/pnfs/desy.de/data/privateFile", origin);
        assertFalse("Other not allowed to read a file with mode 0600", isAllowed);

    }


    @Test
    public void testWritePrivateFile() throws Exception {

        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData fileMetaData =  new FileMetaData(true, 3750, 1000, 0600);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        Subject owner = new Subject(3750, 1000, null);
        Subject groupMember = new Subject(3752, 1000, null);
        Subject other = new Subject(3752, 7777, null);

        isAllowed =  _permissionHandler.canWriteFile(owner, "/pnfs/desy.de/data/privateFile", origin);

        assertTrue("Owner is allowed to write into his file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canWriteFile(groupMember, "/pnfs/desy.de/data/privateFile", origin);
        assertFalse("Group member not allowed to write into a file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canWriteFile(other, "/pnfs/desy.de/data/privateFile", origin);
        assertFalse("Other not allowed to write into a file with mode 0600", isAllowed);

    }


    @Test
    public void testGrouRead() throws Exception {

        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData fileMetaData =  new FileMetaData(true, 3750, 1000, 0640);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        Subject owner = new Subject(3750, 1000, null);
        Subject groupMember = new Subject(3752, 1000, null);

        isAllowed =  _permissionHandler.canReadFile(owner, "/pnfs/desy.de/data/privateFile", origin);

        assertTrue("Owner is allowed to read his file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canReadFile(groupMember, "/pnfs/desy.de/data/privateFile", origin);
        assertTrue("Group member is allowed to read a file with mode 0640", isAllowed);

        isAllowed =  _permissionHandler.canWriteFile(groupMember, "/pnfs/desy.de/data/privateFile", origin);
        assertFalse("Group member not allowed to write into a file with mode 0640", isAllowed);

    }


    @Test
    public void testGrouWrite() throws Exception {

        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData fileMetaData =  new FileMetaData(true, 3750, 1000, 0660);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        Subject owner = new Subject(3750, 1000, null);
        Subject groupMember = new Subject(3752, 1000, null);

        isAllowed =  _permissionHandler.canReadFile(owner, "/pnfs/desy.de/data/privateFile", origin);

        assertTrue("Owner is allowed to read his file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canReadFile(groupMember, "/pnfs/desy.de/data/privateFile", origin);
        assertTrue("Group member is allowed to read a file with mode 0640", isAllowed);

        isAllowed =  _permissionHandler.canWriteFile(groupMember, "/pnfs/desy.de/data/privateFile", origin);
        assertTrue("Group member is allowed to write into a file with mode 0660", isAllowed);

    }


    @Test
    public void testGroupCreate() throws Exception {

        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 3750, 1000, 0775);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);

        Subject groupMember = new Subject(3752, 1000, null);

        isAllowed =  _permissionHandler.canCreateDir(groupMember, "/pnfs/desy.de/data/newDir", origin);
        assertTrue("Group member is allowed to create a new directory in a parent with mode 0770", isAllowed);

    }

    @Test
    public void testNegativeGroup() throws Exception {

        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 3750, 1000, 0707);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);

        Subject groupMember = new Subject(3752, 1000, null);

        isAllowed =  _permissionHandler.canCreateDir(groupMember, "/pnfs/desy.de/data/newDir", origin);
        assertFalse("Negative group member not allowed to create a new directory in a parent with mode 0707", isAllowed);

    }

    @Test
    public void testNegativeOwner() throws Exception {

        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 3750, 1000, 0077);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);

        Subject groupMember = new Subject(3750, 1000, null);

        isAllowed =  _permissionHandler.canCreateDir(groupMember, "/pnfs/desy.de/data/newDir", origin);
        assertFalse("Negative owner not allowed to create a new directory in a parent with mode 0077", isAllowed);

    }

    @Ignore // I guess we, should never allow .....
    @Test
    public void testAnonymousWrite() throws Exception {

        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 3750, 1000, 0777);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);

        Subject anonymouos = new Subject(1111, 2222, null);

        isAllowed =  _permissionHandler.canCreateDir(anonymouos, "/pnfs/desy.de/data/newDir", origin);
        assertFalse("Anonymous not allowed to create a new files or directories", isAllowed);

        isAllowed =  _permissionHandler.canWriteFile(anonymouos, "/pnfs/desy.de/data/newFile", origin);
        assertFalse("Anonymous not allowed to create a new files or directories", isAllowed);

    }


    @Test
    public void testSecondaryGroup() throws Exception {
        Origin origin = new Origin(authTypeCONST, inetAddressTypeCONST, hostCONST);
        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData fileMetaData =  new FileMetaData(true, 3750, 1000, 0660);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        int[] groups = new int[] {17, 16, 1000};
        Subject groupMember = new Subject(3752, groups);

        isAllowed =  _permissionHandler.canReadFile(groupMember, "/pnfs/desy.de/data/privateFile", origin);
        assertTrue("Group member is allowed to read a file with mode 0640", isAllowed);

        isAllowed =  _permissionHandler.canWriteFile(groupMember, "/pnfs/desy.de/data/privateFile", origin);
        assertTrue("Group member is allowed to write into a file with mode 0660", isAllowed);

    }
}
