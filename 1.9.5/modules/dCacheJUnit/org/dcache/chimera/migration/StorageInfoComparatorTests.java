package org.dcache.chimera.migration;

import org.dcache.auth.Subjects;
import org.junit.Before;
import org.junit.Test;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StorageInfoComparatorTests {

    public static final PnfsId PNFS_ID =
            new PnfsId( "000200000000000000001060");

    NameSpaceProvider _nsp1;
    NameSpaceProvider _nsp2;
    PnfsIdValidator _comparator;

    @Before
    public void setUp() {
        _nsp1 = new DummyNameSpaceProvider();
        _nsp2 = new DummyNameSpaceProvider();
        _comparator = new StorageInfoComparator( _nsp1, _nsp2);
    }

    @Test
    public void testEqualStorageInfo() throws CacheException {
        StorageInfo si1 = new GenericStorageInfo( "osm", "h1:raw");
        StorageInfo si2 = new GenericStorageInfo( "osm", "h1:raw");

        _nsp1.setStorageInfo( Subjects.ROOT, PNFS_ID, si1, 1);
        _nsp2.setStorageInfo( Subjects.ROOT, PNFS_ID, si2, 1);

        assertTrue( "Checking pnfsId with same StorageInfo", _comparator
                .isOK( PNFS_ID));
    }

    @Test
    public void testStorageInfoDiffersByStorageClass() throws CacheException {
        StorageInfo si1 = new GenericStorageInfo( "osm", "h1:raw");
        StorageInfo si2 = new GenericStorageInfo( "osm", "atlas:raw");

        _nsp1.setStorageInfo( Subjects.ROOT, PNFS_ID, si1, 1);
        _nsp2.setStorageInfo( Subjects.ROOT, PNFS_ID, si2, 1);

        assertFalse( "Checking pnfsId with different StorageInfo", _comparator
                .isOK( PNFS_ID));
    }

    @Test
    public void testStorageInfoDiffersByHSM() throws CacheException {
        StorageInfo si1 = new GenericStorageInfo( "osm", "h1:raw");
        StorageInfo si2 = new GenericStorageInfo( "tsm", "h1:raw");

        _nsp1.setStorageInfo( Subjects.ROOT, PNFS_ID, si1, 1);
        _nsp2.setStorageInfo( Subjects.ROOT, PNFS_ID, si2, 1);

        assertFalse( "Checking pnfsId with different StorageInfo", _comparator
                .isOK( PNFS_ID));
    }

    @Test
    public void testStorageInfoDiffersByKey() throws CacheException {
        StorageInfo si1 = new GenericStorageInfo( "osm", "h1:raw");
        StorageInfo si2 = new GenericStorageInfo( "osm", "h1:raw");

        // Typical values from PNFS namespace-provider
        si1.setKey( "c", "1:b7ed7a02");
        si1.setKey( "l", "1024000");
        si1.setKey( "h", "no");
        si1.setKey( "al", "ONLINE");
        si1.setKey( "rp", "REPLICA");

        _nsp1.setStorageInfo( Subjects.ROOT, PNFS_ID, si1, 1);
        _nsp2.setStorageInfo( Subjects.ROOT, PNFS_ID, si2, 1);

        assertTrue( "Checking pnfsId storage-info differing only by keys", _comparator.isOK( PNFS_ID));
    }
}
