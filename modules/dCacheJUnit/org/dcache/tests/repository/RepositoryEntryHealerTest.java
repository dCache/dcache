package org.dcache.tests.repository;

import org.junit.Before;
import org.junit.Test;

import dmg.cells.nucleus.CellPath;

import org.dcache.chimera.FsInode;
import org.dcache.pool.repository.MetaDataRepository;
import org.dcache.pool.repository.RepositoryEntryHealer;
import org.dcache.tests.cells.GenericMockCellHelper;

import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.StorageInfo;

public class RepositoryEntryHealerTest {


    private static GenericMockCellHelper _cell = new GenericMockCellHelper("RepositoryEntryHealerTestCell", "");
    private PnfsHandler      _pnfsHandler;
    private RepositoryEntryHealer _repositoryEntryHealer;
    private RepositoryHealerTestChimeraHelper _repositoryHealerTestChimeraHelper;
    private MetaDataRepository _metaDataRepository;


    @Before
    public void setUp() throws Exception {

        _pnfsHandler = new PnfsHandler(_cell, new CellPath("PnfsManager"));
        _repositoryHealerTestChimeraHelper = new RepositoryHealerTestChimeraHelper();
        _metaDataRepository = new MetaDataRepositoryHelper(_repositoryHealerTestChimeraHelper);


        _repositoryEntryHealer = new RepositoryEntryHealer(_pnfsHandler, _repositoryHealerTestChimeraHelper, _metaDataRepository);

    }


    @Test
    public void testBadSize() throws Exception {


        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");
        FsInode inode = _repositoryHealerTestChimeraHelper.add(pnfsId);
        inode.setSize(17);


        CacheRepositoryEntry e =  _metaDataRepository.create(pnfsId);
        e.setCached();


       StorageInfo info = new OSMStorageInfo("h1", "rawd");

       PnfsGetStorageInfoMessage getStorageInfoMessage = new PnfsGetStorageInfoMessage(pnfsId);
       getStorageInfoMessage.setStorageInfo(info);


       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), getStorageInfoMessage);

       /*
        * CacheException(TIMEOUT) will indicate that we tried to modify file size in Pnfs
        */
       CacheRepositoryEntry repositoryEntry = _repositoryEntryHealer.entryOf(pnfsId);

    }

}
