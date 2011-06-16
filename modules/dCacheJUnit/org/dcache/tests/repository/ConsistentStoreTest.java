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
import org.dcache.pool.repository.MetaDataStore;
import org.dcache.pool.repository.ConsistentStore;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.EntryState;
import org.dcache.tests.cells.CellAdapterHelper;
import org.dcache.tests.cells.GenericMockCellHelper;
import org.dcache.vehicles.PnfsSetFileAttributes;
import org.dcache.vehicles.FileAttributes;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsSetLengthMessage;
import diskCacheV111.vehicles.StorageInfo;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsistentStoreTest {


    private static GenericMockCellHelper _cell = new GenericMockCellHelper("ConsistentStoreTestCell", "");
    private PnfsHandler      _pnfsHandler;
    private MetaDataStore _repositoryEntryHealer;
    private RepositoryHealerTestChimeraHelper _repositoryHealerTestChimeraHelper;
    private MetaDataStore _metaDataRepository;


    @Before
    public void setUp() throws Exception {

        _pnfsHandler = new PnfsHandler(_cell, new CellPath("PnfsManager"));
        _repositoryHealerTestChimeraHelper = new RepositoryHealerTestChimeraHelper();
        _metaDataRepository = new MetaDataRepositoryHelper(_repositoryHealerTestChimeraHelper);


        _repositoryEntryHealer = new ConsistentStore(_pnfsHandler, null, _repositoryHealerTestChimeraHelper, _metaDataRepository);

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


        MetaDataRecord e =  _metaDataRepository.create(pnfsId);
        e.setState(EntryState.CACHED);


       StorageInfo info = new OSMStorageInfo("h1", "rawd");
       info.addLocation(new URI("osm://mystore/?store=mystore&group=mygroup&bdid=1"));
       e.setStorageInfo(info);

       PnfsGetStorageInfoMessage getStorageInfoMessage = new PnfsGetStorageInfoMessage(pnfsId);
       getStorageInfoMessage.setStorageInfo(info);
       PnfsAddCacheLocationMessage addCacheLocationMessage = new PnfsAddCacheLocationMessage(pnfsId, "ConsistentStoreTestCell");


       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), getStorageInfoMessage);
       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), addCacheLocationMessage);

       /*
        * CacheException(TIMEOUT) will indicate that we tried to modify file size in Pnfs
        */
       MetaDataRecord repositoryEntry = _repositoryEntryHealer.get(pnfsId);
       assertEquals("Bad flag not set", repositoryEntry.getState(), EntryState.BROKEN );
    }

    @Test
    public void testBadSizeFromClient() throws Exception {


        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");
        FsInode inode = _repositoryHealerTestChimeraHelper.add(pnfsId);
        inode.setSize(17);


        MetaDataRecord e =  _metaDataRepository.create(pnfsId);
        e.setState(EntryState.FROM_CLIENT);

       StorageInfo info = new OSMStorageInfo("h1", "rawd");
       e.setStorageInfo(info);

       PnfsGetStorageInfoMessage getStorageInfoMessage = new PnfsGetStorageInfoMessage(pnfsId);
       getStorageInfoMessage.setStorageInfo(info);

       PnfsSetFileAttributes setFileAttributesMessage = new PnfsSetFileAttributes(pnfsId, new FileAttributes());

       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), getStorageInfoMessage);
       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), setFileAttributesMessage);

       final AtomicBoolean messageSent = new AtomicBoolean(false);
       GenericMockCellHelper.MessageAction action = new GenericMockCellHelper.MessageAction() {

            @Override
            public void messageArraved(CellMessage message) {
                messageSent.set(true);
            }
        };

       GenericMockCellHelper.registerAction("PnfsManager", PnfsSetFileAttributes.class,action );
       MetaDataRecord repositoryEntry = _repositoryEntryHealer.get(pnfsId);

       assertFalse("Entry not recovered", repositoryEntry.getState() ==
                   EntryState.FROM_CLIENT);
       assertTrue("Size not set", messageSent.get() );

    }

    @Test
    public void testFromClientNotExist() throws Exception {


        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");
        FsInode inode = _repositoryHealerTestChimeraHelper.add(pnfsId);
        inode.setSize(17);


        MetaDataRecord e =  _metaDataRepository.create(pnfsId);
        e.setState(EntryState.FROM_CLIENT);

        StorageInfo info = new OSMStorageInfo("h1", "rawd");
        e.setStorageInfo(info);

        PnfsGetStorageInfoMessage getStorageInfoMessage = new PnfsGetStorageInfoMessage(pnfsId);
        getStorageInfoMessage.setReply(CacheException.FILE_NOT_FOUND, "file not found");

        PnfsSetFileAttributes setFileAttributesMessage = new PnfsSetFileAttributes(pnfsId, new FileAttributes());
        setFileAttributesMessage.setReply(CacheException.FILE_NOT_FOUND, "file not found");

       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), getStorageInfoMessage);
       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), setFileAttributesMessage);


       MetaDataRecord repositoryEntry = _repositoryEntryHealer.get(pnfsId);

       assertNull("Missing entry not recognized", repositoryEntry );
    }

    @Test
    public void testNoMetadataNotExists() throws Exception {


        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");
        FsInode inode = _repositoryHealerTestChimeraHelper.add(pnfsId);
        inode.setSize(17);



       PnfsGetStorageInfoMessage getStorageInfoMessage = new PnfsGetStorageInfoMessage(pnfsId);
       getStorageInfoMessage.setReply(CacheException.FILE_NOT_FOUND, "file not found");

       PnfsAddCacheLocationMessage addCacheLocationMessage = new PnfsAddCacheLocationMessage(pnfsId, "ConsistentStoreTestCell");

       addCacheLocationMessage.setReply(CacheException.FILE_NOT_FOUND, "file not found");

       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), getStorageInfoMessage);
       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), addCacheLocationMessage);

       MetaDataRecord repositoryEntry = _repositoryEntryHealer.get(pnfsId);

    }

    @Test
    public void testBadSizeFromStore() throws Exception {


        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");
        FsInode inode = _repositoryHealerTestChimeraHelper.add(pnfsId);
        inode.setSize(17);


        MetaDataRecord e =  _metaDataRepository.create(pnfsId);
        e.setState(EntryState.FROM_STORE);

       StorageInfo info = new OSMStorageInfo("h1", "rawd");
       e.setStorageInfo(info);

       PnfsGetStorageInfoMessage getStorageInfoMessage = new PnfsGetStorageInfoMessage(pnfsId);
       getStorageInfoMessage.setStorageInfo(info);


       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), getStorageInfoMessage);

       /*
        * CacheException(TIMEOUT) will indicate that we tried to modify file size in Pnfs
        */
       MetaDataRecord repositoryEntry = _repositoryEntryHealer.get(pnfsId);

    }

    @Test
    public void testBadSizeNoState() throws Exception {


        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");
        FsInode inode = _repositoryHealerTestChimeraHelper.add(pnfsId);
        inode.setSize(17);


        MetaDataRecord e =  _metaDataRepository.create(pnfsId);


       StorageInfo info = new OSMStorageInfo("h1", "rawd");
       e.setStorageInfo(info);

       PnfsGetStorageInfoMessage getStorageInfoMessage = new PnfsGetStorageInfoMessage(pnfsId);

       getStorageInfoMessage.setStorageInfo(info);

       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), getStorageInfoMessage);

       PnfsAddCacheLocationMessage addCacheLocationMessage = new PnfsAddCacheLocationMessage(pnfsId, "ConsistentStoreTestCell");
       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), addCacheLocationMessage);


       /*
        * CacheException(TIMEOUT) will indicate that we tried to modify file size in Pnfs
        */
       MetaDataRecord repositoryEntry = _repositoryEntryHealer.get(pnfsId);

    }
    @Test
    public void testSizeOk() throws Exception {


        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");
        FsInode inode = _repositoryHealerTestChimeraHelper.add(pnfsId);
        inode.setSize(17);


        MetaDataRecord e =  _metaDataRepository.create(pnfsId);
        e.setState(EntryState.CACHED);


       StorageInfo info = new OSMStorageInfo("h1", "rawd");
       info.setFileSize(17);

       e.setStorageInfo(info);

       PnfsGetStorageInfoMessage getStorageInfoMessage = new PnfsGetStorageInfoMessage(pnfsId);
       getStorageInfoMessage.setStorageInfo(info);


       GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), getStorageInfoMessage);

       /*
        * CacheException(TIMEOUT) will indicate that we tried to modify file size in Pnfs
        */
       MetaDataRecord repositoryEntry = _repositoryEntryHealer.get(pnfsId);

    }

    @Test(expected=RuntimeException.class)
    public void testRemoveMissing() throws Exception {
        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");
        _repositoryEntryHealer.remove(pnfsId);
    }

}
