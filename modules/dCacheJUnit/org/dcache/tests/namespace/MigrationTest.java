package org.dcache.tests.namespace;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.dcache.chimera.migration.Comparator;
import org.junit.Test;

import diskCacheV111.util.FileMetaData;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.StorageInfo;


public class MigrationTest {


    @Test
    public void testEqualMetadataPass() {
        FileMetaData fileMetaData = new FileMetaData(3750, 1000, 0644);
        FileMetaData otherMetaData = new FileMetaData(3750, 1000, 0644);
        assertTrue("equal metadata did not pass", Comparator.compare(fileMetaData, otherMetaData));
    }

    @Test
    public void testEqualMetadataFail() {
        FileMetaData fileMetaData = new FileMetaData(3750, 1000, 0644);
        FileMetaData differMetaData = new FileMetaData(3751, 1001, 0666);
        assertFalse("not equal metadata pass", Comparator.compare(fileMetaData, differMetaData));
    }

    @Test
    public void testEqualsStorageInfoPass() {
        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        StorageInfo otherInfo = new GenericStorageInfo("osm", "h1:raw");

        assertTrue("equal storageInfo did not pass", Comparator.compare(storageInfo, otherInfo));
    }

    @Test
    public void testEqualsStorageInfoFail() {
        StorageInfo storageInfo = new GenericStorageInfo("osm", "h1:raw");
        StorageInfo differInfo = new GenericStorageInfo("osm", "h1:rawd");

        assertFalse("not equal storageInfo pass", Comparator.compare(storageInfo, differInfo));
    }

}
