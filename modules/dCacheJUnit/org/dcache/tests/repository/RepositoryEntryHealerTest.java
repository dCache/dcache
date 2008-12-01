package org.dcache.tests.repository;

import dmg.cells.nucleus.CellMessage;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import dmg.cells.nucleus.CellPath;

import java.net.URI;

import org.dcache.chimera.FsInode;
import org.dcache.pool.repository.MetaDataRepository;
import org.dcache.pool.repository.RepositoryEntryHealer;
import org.dcache.tests.cells.CellAdapterHelper;
import org.dcache.tests.cells.GenericMockCellHelper;

import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsSetLengthMessage;
import diskCacheV111.vehicles.StorageInfo;
import java.util.concurrent.atomic.AtomicBoolean;

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


        _repositoryEntryHealer = new RepositoryEntryHealer(_pnfsHandler, null, _repositoryHealerTestChimeraHelper, _metaDataRepository);

    }

    @After
    public void tearDown() throws Exception {
        _repositoryHealerTestChimeraHelper.shutdown();
    }

    @Test
    public void testBadSizeCached() throws Exception {


        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");
        FsInode inode = _repositoryHealerTestChimeraHelper.add(pnfsId);
        inode.setSize(17);


        CacheRepositoryEntry e =  _metaDataRepository.create(pnfsId);
        e.setCached();


       StorageInfo info = new OSMStorageInfo("h1", "rawd");
       info.addLocation(new URI("osm://mystore/?store=mystore&group=mygroup&bdid=1"));
       e.setStorageInfo(info);

       PnfsGetStorageInfoMessage getStorageInfoMessage = new PnfsGetStorageInfoMessage(pnfsId);
       getStorageInfoMessage.setStorageInfo(info);
       PnfsAddCacheLocationMessage addCacheLocationMessage = new PnfsAddCacheLocationMessage(pnfsId, "RepositoryEntryHealerTestCell");


       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), getStorageInfoMessage);
       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), addCacheLocationMessage);

       /*
        * CacheException(TIMEOUT) will indicate that we tried to modify file size in Pnfs
        */
       CacheRepositoryEntry repositoryEntry = _repositoryEntryHealer.entryOf(pnfsId);

    }

    @Test
    public void testBadSizeFromClient() throws Exception {


        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");
        FsInode inode = _repositoryHealerTestChimeraHelper.add(pnfsId);
        inode.setSize(17);


        CacheRepositoryEntry e =  _metaDataRepository.create(pnfsId);
        e.setReceivingFromClient();


       StorageInfo info = new OSMStorageInfo("h1", "rawd");
       e.setStorageInfo(info);

       PnfsGetStorageInfoMessage getStorageInfoMessage = new PnfsGetStorageInfoMessage(pnfsId);
       getStorageInfoMessage.setStorageInfo(info);

       PnfsAddCacheLocationMessage addCacheLocationMessage = new PnfsAddCacheLocationMessage(pnfsId, "RepositoryEntryHealerTestCell");
       PnfsSetLengthMessage setSize = new PnfsSetLengthMessage(pnfsId, inode.stat().getSize());

       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), getStorageInfoMessage);
       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), addCacheLocationMessage);
       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), setSize);

       final AtomicBoolean messageSent = new AtomicBoolean(false);
       GenericMockCellHelper.MessageAction action = new GenericMockCellHelper.MessageAction() {

            @Override
            public void messageArraved(CellMessage message) {
                messageSent.set(true);
            }
        };

       GenericMockCellHelper.registerAction("PnfsManager", PnfsSetLengthMessage.class,action );
       CacheRepositoryEntry repositoryEntry = _repositoryEntryHealer.entryOf(pnfsId);

       assertFalse("Entry not recovered", e.isReceivingFromClient() );
       assertTrue("Size not set", messageSent.get() );

    }

    @Test
    public void testBadSizeFromStore() throws Exception {


        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");
        FsInode inode = _repositoryHealerTestChimeraHelper.add(pnfsId);
        inode.setSize(17);


        CacheRepositoryEntry e =  _metaDataRepository.create(pnfsId);
        e.setReceivingFromStore();

       StorageInfo info = new OSMStorageInfo("h1", "rawd");
       e.setStorageInfo(info);

       PnfsGetStorageInfoMessage getStorageInfoMessage = new PnfsGetStorageInfoMessage(pnfsId);
       getStorageInfoMessage.setStorageInfo(info);


       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), getStorageInfoMessage);

       /*
        * CacheException(TIMEOUT) will indicate that we tried to modify file size in Pnfs
        */
       CacheRepositoryEntry repositoryEntry = _repositoryEntryHealer.entryOf(pnfsId);

    }

    @Test
    public void testBadSizeNoState() throws Exception {


        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");
        FsInode inode = _repositoryHealerTestChimeraHelper.add(pnfsId);
        inode.setSize(17);


        CacheRepositoryEntry e =  _metaDataRepository.create(pnfsId);


       StorageInfo info = new OSMStorageInfo("h1", "rawd");
       e.setStorageInfo(info);

       PnfsGetStorageInfoMessage getStorageInfoMessage = new PnfsGetStorageInfoMessage(pnfsId);

       getStorageInfoMessage.setStorageInfo(info);

       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), getStorageInfoMessage);

       PnfsAddCacheLocationMessage addCacheLocationMessage = new PnfsAddCacheLocationMessage(pnfsId, "RepositoryEntryHealerTestCell");
       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), addCacheLocationMessage);


       /*
        * CacheException(TIMEOUT) will indicate that we tried to modify file size in Pnfs
        */
       CacheRepositoryEntry repositoryEntry = _repositoryEntryHealer.entryOf(pnfsId);

    }
    @Test
    public void testSizeOk() throws Exception {


        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");
        FsInode inode = _repositoryHealerTestChimeraHelper.add(pnfsId);
        inode.setSize(17);


        CacheRepositoryEntry e =  _metaDataRepository.create(pnfsId);
        e.setCached();


       StorageInfo info = new OSMStorageInfo("h1", "rawd");
       info.setFileSize(17);

       e.setStorageInfo(info);

       PnfsGetStorageInfoMessage getStorageInfoMessage = new PnfsGetStorageInfoMessage(pnfsId);
       getStorageInfoMessage.setStorageInfo(info);


       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), getStorageInfoMessage);

       /*
        * CacheException(TIMEOUT) will indicate that we tried to modify file size in Pnfs
        */
       CacheRepositoryEntry repositoryEntry = _repositoryEntryHealer.entryOf(pnfsId);

    }



}
