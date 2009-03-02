package org.dcache.tests.namespace;

import org.dcache.chimera.migration.Comparator;
import org.dcache.chimera.migration.Comparator.MismatchException;
import org.junit.Test;

import diskCacheV111.util.FileMetaData;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.StorageInfo;


public class MigrationTest {

    static final String DUMMY_ID_STRING = "dummy-ID";

    @Test
    public void testEqualMetadataPass() throws MismatchException {
        FileMetaData fileMetaData = new FileMetaData(3750, 1000, 0644);
        FileMetaData otherMetaData = new FileMetaData(3750, 1000, 0644);
        Comparator.assertEquals( DUMMY_ID_STRING, fileMetaData, otherMetaData);
    }

    @Test(expected=org.dcache.chimera.migration.Comparator.MismatchException.class)
    public void testEqualMetadataFail() throws MismatchException {
        FileMetaData fileMetaData = new FileMetaData(3750, 1000, 0644);
        FileMetaData differMetaData = new FileMetaData(3751, 1001, 0666);
        Comparator.assertEquals( DUMMY_ID_STRING, fileMetaData, differMetaData);
    }

    @Test
    public void testEqualsStorageInfoPass() throws MismatchException {
        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        StorageInfo otherInfo = new GenericStorageInfo("osm", "h1:raw");

        Comparator.assertEquals( DUMMY_ID_STRING, storageInfo, otherInfo);
    }

    @Test(expected=org.dcache.chimera.migration.Comparator.MismatchException.class)
    public void testEqualsStorageInfoFail() throws MismatchException {
        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        StorageInfo differInfo = new GenericStorageInfo("osm", "h1:rawd");

        Comparator.assertEquals( DUMMY_ID_STRING, storageInfo, differInfo);
    }

}
