package org.dcache.chimera.migration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.dcache.auth.Subjects;
import org.junit.Before;
import org.junit.Test;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;
import diskCacheV111.namespace.NameSpaceProvider;

public class FileMetaDataComparatorTests {

    public static final PnfsId PNFS_ID =
            new PnfsId( "000200000000000000001060");

    NameSpaceProvider _nsp1;
    NameSpaceProvider _nsp2;
    PnfsIdValidator _comparator;

    @Before
    public void setUp() {
        _nsp1 = new DummyNameSpaceProvider();
        _nsp2 = new DummyNameSpaceProvider();
        _comparator = new FileMetaDataComparator( _nsp1, _nsp2);
    }

    @Test
    public void testEqualFileMetaData() throws CacheException {
        FileMetaData fmd1 = new FileMetaData( 3000, 1000, 0644);
        FileMetaData fmd2 = new FileMetaData( 3000, 1000, 0644);

        _nsp1.setFileAttributes(Subjects.ROOT, PNFS_ID, fmd1.toFileAttributes());
        _nsp2.setFileAttributes(Subjects.ROOT, PNFS_ID, fmd2.toFileAttributes());

        assertTrue( "checking same file metadata", _comparator.isOK( PNFS_ID));
    }

    @Test
    public void testFileMetaDataDifferentByUid() throws CacheException {
        FileMetaData fmd1 = new FileMetaData( 3000, 1000, 0644);
        FileMetaData fmd2 = new FileMetaData( 9999, 1000, 0644);

        _nsp1.setFileAttributes(Subjects.ROOT, PNFS_ID, fmd1.toFileAttributes());
        _nsp2.setFileAttributes(Subjects.ROOT, PNFS_ID, fmd2.toFileAttributes());

        assertFalse( "checking file metadata that differs by UID", _comparator
                .isOK( PNFS_ID));
    }

    @Test
    public void testFileMetaDataDifferentByGid() throws CacheException {
        FileMetaData fmd1 = new FileMetaData( 3000, 1000, 0644);
        FileMetaData fmd2 = new FileMetaData( 3000, 9999, 0644);

        _nsp1.setFileAttributes(Subjects.ROOT, PNFS_ID, fmd1.toFileAttributes());
        _nsp2.setFileAttributes(Subjects.ROOT, PNFS_ID, fmd2.toFileAttributes());

        assertFalse( "checking file metadata that differs by GID", _comparator
                .isOK( PNFS_ID));
    }

    @Test
    public void testFileMetaDataDifferentByPermissions() throws CacheException {
        FileMetaData fmd1 = new FileMetaData( 3000, 1000, 0644);
        FileMetaData fmd2 = new FileMetaData( 3000, 1000, 0755);

        _nsp1.setFileAttributes(Subjects.ROOT, PNFS_ID, fmd1.toFileAttributes());
        _nsp2.setFileAttributes(Subjects.ROOT, PNFS_ID, fmd2.toFileAttributes());

        assertFalse( "checking file metadata that differs by permissions",
                _comparator.isOK( PNFS_ID));
    }
}
