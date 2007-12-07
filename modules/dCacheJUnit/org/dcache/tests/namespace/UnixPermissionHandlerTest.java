package org.dcache.tests.namespace;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import diskCacheV111.services.FileMetaDataSource;
import diskCacheV111.services.FsPermissionHandler;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsId;

public class UnixPermissionHandlerTest {


    private final DummyMetadataSource _metaDataSource = new DummyMetadataSource();
    private final FsPermissionHandler _permissionHandler = new FsPermissionHandler(null, _metaDataSource);


    /**
     * TODO: make use of UserAuthRecord class
     */

    private static class UserRecord {
        private final int _uid;
        private final int _gid;
        private final int[] _gids;


        UserRecord(int uid, int gid, int[] gids) {
            _uid = uid;
            _gid = gid;
            _gids = gids;
        }


        public int getUid() {
            return _uid;
        }


        public int getGid() {
            return _gid;
        }


        public int[] getGids() {
            return _gids;
        }


    }
    /**
     *
     * Fake metadata source
     *
     */
    private static class DummyMetadataSource implements FileMetaDataSource {


        private final Map<PnfsId, FileMetaData> _metadataById = new HashMap<PnfsId, FileMetaData>();
        private final Map<String, FileMetaData> _metadataByPath = new HashMap<String, FileMetaData>();

        public FileMetaData getMetaData(String path) throws CacheException {

            FileMetaData metaData = _metadataByPath.get(path);

            if( metaData == null ) {
                throw new FileNotFoundCacheException(path + " not found");
            }
            return metaData;
        }

        public FileMetaData getMetaData(PnfsId pnfsId) throws CacheException {

            FileMetaData metaData = _metadataById.get(pnfsId);

            if( metaData == null ) {
                throw new FileNotFoundCacheException(pnfsId + " not found");
            }
            return metaData;
        }


        public void setMetaData(PnfsId pnfsId,FileMetaData metaData ) {
            _metadataById.put(pnfsId, metaData);
        }


        public void setMetaData(String path,FileMetaData metaData ) {
            _metadataByPath.put(path, metaData);
        }

        public void cleanAll() {
            _metadataById.clear();
            _metadataByPath.clear();
        }

    }

    @Before
    public void setUp() {
        _metaDataSource.cleanAll();
    }

    @Test
    public void testCreateFile() throws Exception {

        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData dirMetaData =  new FileMetaData(true, 3750, 1000, 0755);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/tigran", dirMetaData);

        UserRecord user = new UserRecord(3750, 1000, null);

        isAllowed =  _permissionHandler.canWrite(user.getUid(), user.getGid(), "/pnfs/desy.de/data");

        assertFalse("Regular user is not allowed to create a file without sufficient permissions", isAllowed);

        isAllowed =  _permissionHandler.canWrite(user.getUid(), user.getGid(), "/pnfs/desy.de/data/tigran");

        assertTrue("User should be allowed to create a file with sufficient permissions", isAllowed);
    }


    @Test
    public void testCreateDir() throws Exception {

        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);

        UserRecord user = new UserRecord(3750, 1000, null);

        isAllowed =  _permissionHandler.canCreateDir(user.getUid(), user.getGid(), "/pnfs/desy.de/data/tigran");

        assertFalse("Regular user is not allowed to create a directory without sufficient permissions", isAllowed);

    }


    @Test
    public void testReadPrivateFile() throws Exception {

        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData fileMetaData =  new FileMetaData(true, 3750, 1000, 0600);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        UserRecord owner = new UserRecord(3750, 1000, null);
        UserRecord groupMember = new UserRecord(3752, 1000, null);
        UserRecord other = new UserRecord(3752, 7777, null);

        isAllowed =  _permissionHandler.canRead(owner.getUid(), owner.getGid(), "/pnfs/desy.de/data/privateFile");

        assertTrue("Owner is allowed to read his file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canRead(groupMember.getUid(), groupMember.getGid(), "/pnfs/desy.de/data/privateFile");
        assertFalse("Group member not allowed to read a file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canRead(other.getUid(), other.getGid(), "/pnfs/desy.de/data/privateFile");
        assertFalse("Other not allowed to read a file with mode 0600", isAllowed);

    }


    @Test
    public void testWritePrivateFile() throws Exception {

        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData fileMetaData =  new FileMetaData(true, 3750, 1000, 0600);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        UserRecord owner = new UserRecord(3750, 1000, null);
        UserRecord groupMember = new UserRecord(3752, 1000, null);
        UserRecord other = new UserRecord(3752, 7777, null);

        isAllowed =  _permissionHandler.canWrite(owner.getUid(), owner.getGid(), "/pnfs/desy.de/data/privateFile");

        assertTrue("Owner is allowed to write into his file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canWrite(groupMember.getUid(), groupMember.getGid(), "/pnfs/desy.de/data/privateFile");
        assertFalse("Group member not allowed to write into a file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canWrite(other.getUid(), other.getGid(), "/pnfs/desy.de/data/privateFile");
        assertFalse("Other not allowed to write into a file with mode 0600", isAllowed);

    }


    @Test
    public void testGrouRead() throws Exception {

        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData fileMetaData =  new FileMetaData(true, 3750, 1000, 0640);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        UserRecord owner = new UserRecord(3750, 1000, null);
        UserRecord groupMember = new UserRecord(3752, 1000, null);

        isAllowed =  _permissionHandler.canRead(owner.getUid(), owner.getGid(), "/pnfs/desy.de/data/privateFile");

        assertTrue("Owner is allowed to read his file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canRead(groupMember.getUid(), groupMember.getGid(), "/pnfs/desy.de/data/privateFile");
        assertTrue("Group member is allowed to read a file with mode 0640", isAllowed);

        isAllowed =  _permissionHandler.canWrite(groupMember.getUid(), groupMember.getGid(), "/pnfs/desy.de/data/privateFile");
        assertFalse("Group member not allowed to write into a file with mode 0640", isAllowed);

    }


    @Test
    public void testGrouWrite() throws Exception {

        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 0, 0, 0755);
        FileMetaData fileMetaData =  new FileMetaData(true, 3750, 1000, 0660);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);
        _metaDataSource.setMetaData("/pnfs/desy.de/data/privateFile", fileMetaData);

        UserRecord owner = new UserRecord(3750, 1000, null);
        UserRecord groupMember = new UserRecord(3752, 1000, null);

        isAllowed =  _permissionHandler.canRead(owner.getUid(), owner.getGid(), "/pnfs/desy.de/data/privateFile");

        assertTrue("Owner is allowed to read his file with mode 0600", isAllowed);

        isAllowed =  _permissionHandler.canRead(groupMember.getUid(), groupMember.getGid(), "/pnfs/desy.de/data/privateFile");
        assertTrue("Group member is allowed to read a file with mode 0640", isAllowed);

        isAllowed =  _permissionHandler.canWrite(groupMember.getUid(), groupMember.getGid(), "/pnfs/desy.de/data/privateFile");
        assertTrue("Group member is allowed to write into a file with mode 0660", isAllowed);

    }


    @Test
    public void testGroupCreate() throws Exception {

        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 3750, 1000, 0775);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);

        UserRecord groupMember = new UserRecord(3752, 1000, null);

        isAllowed =  _permissionHandler.canCreateDir(groupMember.getUid(), groupMember.getGid(), "/pnfs/desy.de/data/newDir");
        assertTrue("Group member is allowed to create a new directory in a parent with mode 0770", isAllowed);

    }

    @Test
    public void testNegativeGroup() throws Exception {

        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 3750, 1000, 0707);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);

        UserRecord groupMember = new UserRecord(3752, 1000, null);

        isAllowed =  _permissionHandler.canCreateDir(groupMember.getUid(), groupMember.getGid(), "/pnfs/desy.de/data/newDir");
        assertFalse("Negative group member not allowed to create a new directory in a parent with mode 0707", isAllowed);

    }

    @Test
    public void testNegativeOwner() throws Exception {

        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 3750, 1000, 0077);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);

        UserRecord groupMember = new UserRecord(3750, 1000, null);

        isAllowed =  _permissionHandler.canCreateDir(groupMember.getUid(), groupMember.getGid(), "/pnfs/desy.de/data/newDir");
        assertFalse("Negative owner not allowed to create a new directory in a parent with mode 0077", isAllowed);

    }

    @Ignore // I guess we, should never allow .....
    @Test
    public void testAnonymousWrite() throws Exception {

        boolean isAllowed = false;

        FileMetaData parentMetaData =  new FileMetaData(true, 3750, 1000, 0777);

        _metaDataSource.setMetaData("/pnfs/desy.de/data", parentMetaData);

        UserRecord anonymouos = new UserRecord(1111, 2222, null);

        isAllowed =  _permissionHandler.canCreateDir(anonymouos.getUid(), anonymouos.getGid(), "/pnfs/desy.de/data/newDir");
        assertFalse("Anonymous not allowed to create a new files or directories", isAllowed);

        isAllowed =  _permissionHandler.canWrite(anonymouos.getUid(), anonymouos.getGid(), "/pnfs/desy.de/data/newFile");
        assertFalse("Anonymous not allowed to create a new files or directories", isAllowed);


    }
}
