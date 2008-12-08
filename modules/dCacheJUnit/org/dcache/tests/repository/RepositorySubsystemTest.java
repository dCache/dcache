
package org.dcache.tests.repository;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;

import org.junit.*;

import com.sleepycat.je.DatabaseException;
import diskCacheV111.repository.*;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.vehicles.*;
import dmg.cells.nucleus.*;
import dmg.util.*;
import org.dcache.tests.cells.CellAdapterHelper;
import org.dcache.tests.cells.CellStubHelper;
import org.dcache.tests.cells.Message;

import org.dcache.pool.repository.v4.CacheRepositoryV4;
import org.dcache.pool.repository.v5.CacheRepositoryV5;
import org.dcache.pool.repository.IllegalTransitionException;

import org.dcache.pool.repository.SpaceRecord;
import org.dcache.pool.repository.EntryState;
import static org.dcache.pool.repository.EntryState.*;
import org.dcache.pool.repository.ReadHandle;
import org.dcache.pool.repository.WriteHandle;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.StateChangeListener;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.v3.RepositoryException;

public class RepositorySubsystemTest
    implements StateChangeListener
{
    private long size1 = 1024;
    private long size2 = 1024;
    private long size3 = 1024;
    private long size4 = 1024;

    private PnfsId id1; // Precious
    private PnfsId id2; // Cached
    private PnfsId id3; // Cached + Sticky
    private PnfsId id4; // Non existing entry

    private StorageInfo info1;
    private StorageInfo info2;
    private StorageInfo info3;
    private StorageInfo info4;

    private PnfsHandler pnfs;

    private CacheRepositoryV5 repository;

    private File root;
    private File metaDir;
    private File dataDir;

    private BlockingQueue<StateChangeEvent> stateChangeEvents;

    private String args;
    private CellAdapterHelper cell;

    private void createFile(File file, long size)
        throws IOException
    {
        RandomAccessFile handle = new RandomAccessFile(file, "rw");
        try {
            for (long i = 0; i < size; i++)
                handle.writeByte(0);
        } finally {
            handle.close();
        }
    }

    private CacheRepositoryEntry createEntry(CacheRepository repository,
                                             PnfsId id,
                                             StorageInfo info)
        throws CacheException, IOException
    {
        CacheRepositoryEntry entry = repository.createEntry(id);
        createFile(entry.getDataFile(), info.getFileSize());
        entry.setStorageInfo(info);
        return entry;
    }

    private StorageInfo createStorageInfo(long size)
    {
        GenericStorageInfo info = new GenericStorageInfo();
        info.setFileSize(size);
        return info;
    }

    private void deleteDirectory(File dir)
    {
        File[] fileArray = dir.listFiles();

        if (fileArray != null)
        {
            for (int i = 0; i < fileArray.length; i++)
            {
                if (fileArray[i].isDirectory())
                    deleteDirectory(fileArray[i]);
                else
                    fileArray[i].delete();
            }
        }
        dir.delete();
    }

    @Before
    public void setUp()
        throws IOException, CacheException,
               DatabaseException, InterruptedException
    {
        id1 = new PnfsId("000000000001");
        id2 = new PnfsId("000000000002");
        id3 = new PnfsId("000000000003");
        id4 = new PnfsId("000000000004");

        info1 = createStorageInfo(size1);
        info2 = createStorageInfo(size2);
        info3 = createStorageInfo(size3);
        info4 = createStorageInfo(0);

        root = File.createTempFile("dtest", null);
        if (!root.delete())
            throw new IOException("Could not delete temp file");
        if (!root.mkdir())
            throw new IOException("Could not create temp dir");
        dataDir = new File(root, "data");
        metaDir = new File(root, "meta");

        if (!dataDir.mkdir())
            throw new IOException("Could not create data dir");
        if (!metaDir.mkdir())
            throw new IOException("Could not create meta dir");

        args = "-metaDataRepository=org.dcache.pool.repository.meta.db.BerkeleyDBMetaDataRepository";

        CacheRepositoryV4 rep = new CacheRepositoryV4(root, new Args(args));
        rep.runInventory();
        createEntry(rep, id1, info1).setPrecious(true);
        createEntry(rep, id2, info2).setCached();
        CacheRepositoryEntry entry = createEntry(rep, id3, info3);
        entry.setCached();
        entry.setSticky(true);
        rep.close();

        stateChangeEvents = new LinkedBlockingQueue<StateChangeEvent>();
        rep = new CacheRepositoryV4(root, new Args(args));
        cell = new CellAdapterHelper("pool", args);
        pnfs = new PnfsHandler(new CellPath("pnfs"), "pool");
        pnfs.setCellEndpoint(cell);
        repository = new CacheRepositoryV5();
        repository.setPnfsHandler(pnfs);
        repository.setSize(5120);
        repository.setLegacyRepository(rep);
        repository.setSweeper(new diskCacheV111.pools.SpaceSweeper2(pnfs, rep));
        repository.addListener(this);
        repository.init(0);

        /* Remove scan notifications from queue.
         */
        stateChangeEvents.take();
        stateChangeEvents.take();
        stateChangeEvents.take();
    }

    @After
    public void tearDown()
        throws InterruptedException
    {
        repository.shutdown();
        cell.die();
        if (root != null)
            deleteDirectory(root);
    }

    public void stateChanged(StateChangeEvent event)
    {
        stateChangeEvents.add(event);
    }

    public void expectStateChangeEvent(PnfsId id, EntryState oldState, EntryState newState)
        throws InterruptedException
    {
        StateChangeEvent event = stateChangeEvents.poll(1, TimeUnit.SECONDS);
        assertNotNull(event);
        assertEquals(id, event.getPnfsId());
        assertEquals(oldState, event.getOldState());
        assertEquals(newState, event.getNewState());
    }

    public void assertNoStateChangeEvent()
    {
        if (stateChangeEvents.size() > 0)
            fail("Unexpected state change event: "
                 + stateChangeEvents.remove());
    }

    private void assertSpaceRecord(long total, long free, long precious, long removable)
    {
        SpaceRecord space = repository.getSpaceRecord();
        assertEquals(total, space.getTotalSpace());
        assertEquals(free, space.getFreeSpace());
        assertEquals(precious, space.getPreciousSpace());
        assertEquals(removable, space.getRemovableSpace());
    }

    private void assertCacheEntry(CacheEntry entry, PnfsId id,
                                  long size, EntryState state)
    {
        assertEquals(id, entry.getPnfsId());
        assertEquals(size, entry.getReplicaSize());
        assertEquals(state, entry.getState());
    }

    private void assertCanOpen(PnfsId id, long size, EntryState state)
    {
        try {
            ReadHandle handle = repository.openEntry(id);
            try {
                assertEquals(new File(dataDir, id.toString()), handle.getFile());
                assertCacheEntry(handle.getEntry(), id, size, state);
            } finally {
                handle.close();
            }
        } catch (FileNotInCacheException e) {
            fail("Expected entry " + id + " not found");
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testInitTwiceFails()
        throws IOException, RepositoryException
    {
        repository.init(0);
    }

    @Test
    public void testGetSpaceRecord()
    {
        assertSpaceRecord(5120, 2048, 1024, 1024);
    }

    @Test
    public void testOpenEntry()
    {
        assertCanOpen(id1, size1, PRECIOUS);
        assertCanOpen(id2, size2, CACHED);
        assertCanOpen(id3, size3, CACHED);
    }

    @Test(expected=FileNotInCacheException.class)
    public void testOpenEntryFileNotFound()
        throws Throwable
    {
        new CellStubHelper() {
            /* Attempting to open a non-existing entry triggers a
             * clear cache location message.
             */
            @Message(required=true,step=1,cell="pnfs")
            public Object message(PnfsClearCacheLocationMessage msg)
            {
                msg.setSucceeded();
                return msg;
            }

            protected void run()
                throws FileNotInCacheException
            {
                repository.openEntry(id4);
            }
        };
    }

    @Test
    public void testSetState()
        throws IllegalTransitionException,
               InterruptedException
    {
        assertCanOpen(id1, size1, PRECIOUS);
        repository.setState(id1, CACHED);
        expectStateChangeEvent(id1, PRECIOUS, CACHED);
        assertCanOpen(id1, size1, CACHED);

        repository.setState(id1, PRECIOUS);
        expectStateChangeEvent(id1, CACHED, PRECIOUS);
        assertCanOpen(id1, size1, PRECIOUS);
    }

    @Test(expected=IllegalTransitionException.class)
    public void testSetStateFileNotFound()
        throws IllegalTransitionException
    {
        repository.setState(id4, CACHED);
    }

    @Test(expected=IllegalTransitionException.class)
    public void testSetStateToNew()
        throws IllegalTransitionException
    {
        repository.setState(id1, NEW);
    }

    @Test(expected=IllegalTransitionException.class)
    public void testSetStateToDestroyed()
        throws IllegalTransitionException
    {
        repository.setState(id1, DESTROYED);
    }

    @Test(expected=IllegalStateException.class)
    public void testClosedReadHandleClose()
        throws FileNotInCacheException
    {
        ReadHandle handle = repository.openEntry(id1);
        handle.close();
        handle.close();
    }

    @Test(expected=IllegalStateException.class)
    public void testClosedReadHandleGetFile()
        throws FileNotInCacheException
    {
        ReadHandle handle = repository.openEntry(id1);
        handle.close();
        handle.getFile();
    }

    @Test(expected=IllegalStateException.class)
    public void testClosedReadHandleGetEntry()
        throws FileNotInCacheException
    {
        ReadHandle handle = repository.openEntry(id1);
        handle.close();
        handle.getEntry();
    }

    @Test
    public void testSetSize()
    {
        repository.setSize(3072);
        assertSpaceRecord(3072, 0, 1024, 1024);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetSizeToSmall()
    {
        repository.setSize(3071);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetSizeNegative()
    {
        repository.setSize(-1);
    }

    @Test
    public void testRemoval()
        throws Throwable
    {
        new CellStubHelper() {
            @Message(required=true,step=1,cell="pnfs")
            public Object message(PnfsClearCacheLocationMessage msg)
            {
                msg.setSucceeded();
                return msg;
            }

            protected void run()
                throws IllegalTransitionException,
                       InterruptedException
            {
                repository.setState(id1, REMOVED);
                expectStateChangeEvent(id1, PRECIOUS, REMOVED);
                expectStateChangeEvent(id1, REMOVED, DESTROYED);
                assertStep("Cache location cleared", 1);
                assertEquals(repository.getState(id1), NEW);
            }
        };
    }

    @Test
    public void testRemoveWhileReading()
        throws Throwable
    {
        new CellStubHelper() {
            @Message(required=true,step=0,cell="pnfs")
            public Object message(PnfsClearCacheLocationMessage msg)
            {
                msg.setSucceeded();
                return msg;
            }

            protected void run()
                throws FileNotInCacheException, IllegalTransitionException,
                       InterruptedException
            {
                ReadHandle handle1 = repository.openEntry(id1);
                repository.setState(id1, REMOVED);
                expectStateChangeEvent(id1, PRECIOUS, REMOVED);
                assertNoStateChangeEvent();
                assertStep("Cache location cleared", 1);
                handle1.close();
                expectStateChangeEvent(id1, REMOVED, DESTROYED);
            }
        };
    }

    @Test(expected=FileNotInCacheException.class)
    public void testRemoveOpenAgainFails()
        throws Throwable
    {
        new CellStubHelper() {
            @Message(required=true,step=1,cell="pnfs")
            public Object message(PnfsClearCacheLocationMessage msg)
            {
                msg.setSucceeded();
                return msg;
            }

            /* The second attempt to open the file will cause a second
             * clear cache location to be send.
             */
            @Message(required=true,step=2,cell="pnfs")
            public Object message2(PnfsClearCacheLocationMessage msg)
            {
                msg.setSucceeded();
                return msg;
            }

            protected void run()
                throws FileNotInCacheException, IllegalTransitionException
            {
                repository.openEntry(id1);
                repository.setState(id1, REMOVED);
                repository.openEntry(id1);
            }
        };
    }

    @Test(expected=FileInCacheException.class)
    public void testCreateEntryFileExists()
        throws FileInCacheException
    {
        List<StickyRecord> stickyRecords = Collections.emptyList();
        repository.createEntry(id1, info1, FROM_CLIENT, PRECIOUS, stickyRecords);
    }

    /* Helper method for creating a fourth entry in the repository.
     */
    private void createEntry4(final long overallocation,
                              final boolean failSetLength,
                              final boolean failAddCacheLocation,
                              final boolean cancel,
                              final EntryState transferState,
                              final EntryState finalState)
        throws Throwable
    {
        new CellStubHelper() {
            @Message(required=true,step=1,cell="pnfs")
            public Object message(PnfsSetLengthMessage msg)
            {
                assertEquals(size4, msg.getLength());
                if (failSetLength) {
                    msg.setFailed(1, null);
                } else {
                    msg.setSucceeded();
                }
                return msg;
            }

            @Message(required=true,step=1,cell="pnfs")
            public Object message(PnfsAddCacheLocationMessage msg)
            {
                if (failAddCacheLocation) {
                    msg.setFailed(1, null);
                } else {
                    msg.setSucceeded();
                }
                return msg;
            }

            @Message(required=false,step=0,cell="pnfs")
            public Object message(PnfsClearCacheLocationMessage msg)
            {
                msg.setSucceeded();
                return msg;
            }

            protected void run()
                throws FileInCacheException,
                       CacheException,
                       InterruptedException,
                       IOException
            {
                List<StickyRecord> stickyRecords = Collections.emptyList();
                WriteHandle handle =
                    repository.createEntry(id4, info4, transferState,
                                           finalState, stickyRecords);
                try {
                    handle.allocate(size4 + overallocation);
                    createFile(handle.getFile(), size4);
                    assertStep("No messages received yet", 0);
                    if (!cancel)
                        handle.commit(null);
                } finally {
                    handle.close();
                }
            }
        };
    }


    @Test
    public void testCreateEntry()
        throws Throwable
    {
        createEntry4(0, false, false, false, FROM_CLIENT, PRECIOUS);
        assertCanOpen(id4, size4, PRECIOUS);
        assertSpaceRecord(5120, 1024, 2048, 1024);
    }

    @Test(expected=CacheException.class)
    public void testCreateEntrySetLengthFailed()
        throws Throwable
    {
        try {
            createEntry4(100, true, false, false, FROM_CLIENT, PRECIOUS);
        } finally {
            assertCacheEntry(repository.getEntry(id4), id4, size4, BROKEN);
            assertSpaceRecord(5120, 1024, 1024, 1024);
        }
        // TODO: Check notification
    }

    @Test
    public void testCreateEntryUnderallocation()
        throws Throwable
    {
        createEntry4(-100, false, false, false, FROM_CLIENT, PRECIOUS);
        assertCanOpen(id4, size4, PRECIOUS);
        assertSpaceRecord(5120, 1024, 2048, 1024);
        // TODO: Check notification
    }

    @Test
    public void testCreateEntryOverallocation()
        throws Throwable
    {
        createEntry4(100, false, false, false, FROM_CLIENT, PRECIOUS);
        assertCanOpen(id4, size4, PRECIOUS);
        assertSpaceRecord(5120, 1024, 2048, 1024);
        // TODO: Check notification
    }

    @Test
    public void testCreateEntryOverallocationFail()
        throws Throwable
    {
        createEntry4(100, false, false, true, FROM_CLIENT, PRECIOUS);
        assertCacheEntry(repository.getEntry(id4), id4, size4, BROKEN);
        assertSpaceRecord(5120, 1024, 1024, 1024);
        // TODO: Check notification
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateEntryNegativeAllocation()
        throws Throwable
    {
        WriteHandle handle =
            repository.createEntry(id4, info4, FROM_CLIENT, PRECIOUS, null);
        handle.allocate(-1);
    }

    @Test
    public void testCreateEntryOutOfSpace()
        throws Throwable
    {
        repository.setSize(3072);
        createEntry4(0,false, false, false, FROM_CLIENT, PRECIOUS);
        assertCanOpen(id4, size4, PRECIOUS);
        assertSpaceRecord(3072, 0, 2048, 0);
        // TODO: Check notification
    }

    @Test
    public void testStickyExpiration()
        throws FileNotInCacheException, InterruptedException
    {
        long now = System.currentTimeMillis();
        assertFalse(repository.getEntry(id2).isSticky());
        repository.setSticky(id2, "system", now + 500, true);
        assertTrue(repository.getEntry(id2).isSticky());
        Thread.currentThread().sleep(700);
        assertFalse(repository.getEntry(id2).isSticky());
    }

    @Test
    public void testStickyClear()
        throws FileNotInCacheException
    {
        long now = System.currentTimeMillis();

        assertFalse(repository.getEntry(id2).isSticky());
        repository.setSticky(id2, "system", now + 500, true);
        assertTrue(repository.getEntry(id2).isSticky());
        repository.setSticky(id2, "system", 0, true);
        assertFalse(repository.getEntry(id2).isSticky());
    }
}


